import logging
import math
import os
from datetime import datetime, timezone
from decimal import ROUND_FLOOR, ROUND_HALF_EVEN, Decimal
from typing import Any, Dict, List, Literal, Optional, Set, Tuple

from pydantic import Field

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import MarketDict, OrderType, TradeType
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.event.events import (
    BuyOrderCreatedEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCreatedEvent,
)
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.data_feed.coin_gecko_data_feed import CoinGeckoDataFeed
from hummingbot.data_feed.coin_gecko_data_feed.coin_gecko_constants import CoinGeckoAPITier
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase

SECONDS_PER_DAY = 86400
# The CoinGecko key is never part of the yml: it is read from these environment variables.
COINGECKO_API_KEY_ENV = "COINGECKO_API_KEY"
COINGECKO_API_TIER_ENV = "COINGECKO_API_TIER"


class BeezeeVolumeTargetConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    controllers_config: List[str] = []
    exchange: str = Field("beezee")
    trading_pair: str = Field("BZE-USDC")
    # Volume to print per UTC day, in the connector's quote units. Each self-filled trade counts once.
    daily_volume_quote: Decimal = Field(Decimal("1000"), gt=0)
    cycle_seconds: int = Field(300, gt=0)
    # Bounds of one leg, in base units. The chain minimum at the trade price still applies on top.
    min_order_amount: Decimal = Field(Decimal("0"), ge=0)
    max_order_amount: Decimal = Field(Decimal("1000000"), gt=0)
    # Tradebin fees + gas per UTC day, in BZE. Reaching it stops the script until UTC midnight.
    max_daily_fee_spend: Decimal = Field(Decimal("100"), gt=0)
    coingecko_id_base: Optional[str] = Field(None)
    coingecko_id_quote: Optional[str] = Field(None)
    exclude_markets: List[str] = Field(["BeeZee DEX"])
    # Connector price units per CoinGecko price unit (base priced in the quote token): 1 when both tokens have
    # their display units on chain, 1000000 for BZE quoted in USDC.inj base units.
    anchor_price_scale: Decimal = Field(Decimal("1"), gt=0)
    anchor_refresh_seconds: int = Field(300, gt=0)
    anchor_max_age_seconds: int = Field(1800, gt=0)
    anchor_fallback: Literal["book_mid", "pause"] = Field("book_mid")
    max_anchor_deviation_pct: Decimal = Field(Decimal("5"), gt=0)

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets[self.exchange] = markets.get(self.exchange, set()) | {self.trading_pair}
        return markets


class CoinGeckoTickersFeed(CoinGeckoDataFeed):
    """Hummingbot's CoinGecko feed plus the per-venue tickers of one coin. Its own price loop is never started:
    the script only makes the two requests it needs per anchor refresh."""

    async def get_coin_tickers(self, coin_id: str) -> List[Dict[str, Any]]:
        url = f"{self._api_tier.value.base_url}/coins/{coin_id.lower()}/tickers"
        response = await self._execute_request(url=url, params={"include_exchange_logo": "false"})
        return response.get("tickers", []) if isinstance(response, dict) else []


def external_usd_price(tickers: List[Dict[str, Any]], exclude_markets: List[str]) -> Optional[Decimal]:
    """Volume-weighted `converted_last.usd` of the tickers outside `exclude_markets`. Stale and anomalous tickers
    are left out too. None when no ticker remains or none has volume."""
    excluded = {name.strip().lower() for name in exclude_markets}
    total_volume = Decimal("0")
    weighted = Decimal("0")
    for ticker in tickers:
        market_name = str((ticker.get("market") or {}).get("name", "")).strip().lower()
        if market_name in excluded or ticker.get("is_stale") or ticker.get("is_anomaly"):
            continue
        price = (ticker.get("converted_last") or {}).get("usd")
        volume = (ticker.get("converted_volume") or {}).get("usd")
        if price is None or volume is None:
            continue
        price, volume = Decimal(str(price)), Decimal(str(volume))
        if price <= 0 or volume <= 0:
            continue
        total_volume += volume
        weighted += price * volume
    if total_volume <= 0:
        return None
    return weighted / total_volume


def utc_day(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d")


def trade_key(event: OrderFilledEvent) -> str:
    """One self-filled trade gives both legs a fill. The connector names them `<height>-<index>-maker` and
    `<height>-<index>-taker`, so the trade is counted once."""
    trade_id = event.exchange_trade_id or f"{event.order_id}-{event.timestamp}"
    for suffix in ("-maker", "-taker"):
        if trade_id.endswith(suffix):
            return trade_id[: -len(suffix)]
    return trade_id


class BeezeeVolumeTarget(StrategyV2Base):
    """
    Prints a daily volume on one BeeZee order-book market by self-filling: every `cycle_seconds` it sends a sell
    and a buy of the same size at the same price, strictly inside the current spread, in one batched tx. The two
    orders match each other, so no other trader's order is ever taken and, fees aside, the inventory stays put.
    The price sits at a CoinGecko anchor built from external venues only, clamped into the spread.
    """

    def __init__(
        self,
        connectors: Dict[str, ConnectorBase],
        config: BeezeeVolumeTargetConfig,
        coingecko_feed: Optional[CoinGeckoTickersFeed] = None,
    ):
        super().__init__(connectors, config)
        self.config = config
        self._feed = coingecko_feed
        if self._feed is None and config.coingecko_id_base:
            self._feed = self._build_coingecko_feed()
        self._next_cycle_timestamp = 0.0
        self._sell_first = True
        self._day: Optional[str] = None
        self._filled_quote_today = Decimal("0")
        self._tradebin_fees_today = Decimal("0")
        self._gas_baseline = Decimal("0")
        self._counted_trades: Set[str] = set()
        # Orders this script placed, and of those the ones whose create tx went out (tradebin charges its fee then),
        # the ones with at least one fill and the ones whose tradebin fee is already in the spend.
        self._own_orders: Set[str] = set()
        self._created_orders: Set[str] = set()
        self._filled_orders: Set[str] = set()
        self._fee_counted_orders: Set[str] = set()
        self._anchor: Optional[Decimal] = None
        self._anchor_timestamp: Optional[float] = None
        self._next_anchor_refresh = 0.0
        self._anchor_task = None
        self._anchor_read_finished = False
        self._last_skip_reason: Optional[str] = None
        self._last_anchor_source: Optional[str] = None
        self._last_book_mid: Optional[Decimal] = None

    @staticmethod
    def _build_coingecko_feed() -> CoinGeckoTickersFeed:
        api_key = os.getenv(COINGECKO_API_KEY_ENV, "")
        tier_name = os.getenv(COINGECKO_API_TIER_ENV, "DEMO" if api_key else "PUBLIC").upper()
        return CoinGeckoTickersFeed(api_key=api_key, api_tier=CoinGeckoAPITier[tier_name])

    @property
    def connector(self) -> ConnectorBase:
        return self.connectors[self.config.exchange]

    # === Clock ===

    def start(self, clock: Clock, timestamp: float) -> None:
        super().start(clock, timestamp)
        self._adopt_restored_orders()
        self._refresh_anchor_if_due()

    def _adopt_restored_orders(self):
        """After a crash or restart the connector holds the previous run's open orders on the pair (restored before
        the clock starts). They count as this script's own, so the first cycle cancels a leg left half-done."""
        restored = [
            order.client_order_id for order in self.connector.in_flight_orders.values()
            if order.trading_pair == self.config.trading_pair and not order.is_done
        ]
        if restored:
            self._own_orders.update(restored)
            self.log_with_clock(logging.INFO, f"Adopted {len(restored)} restored order(s): {', '.join(restored)}.")

    def on_tick(self):
        self._roll_day()
        self._refresh_anchor_if_due()
        if self.current_timestamp < self._next_cycle_timestamp:
            return
        if self._uses_coingecko and not self._anchor_read_finished:
            # The first cycle waits for the first CoinGecko read instead of trading on the fallback meanwhile.
            return
        self._next_cycle_timestamp = self.current_timestamp + self.config.cycle_seconds
        skip_reason = self._run_cycle()
        if skip_reason != self._last_skip_reason and skip_reason is not None:
            self.log_with_clock(logging.INFO, f"Volume target cycle skipped: {skip_reason}")
        self._last_skip_reason = skip_reason

    def _roll_day(self):
        day = utc_day(self.current_timestamp)
        if day == self._day:
            return
        self._day = day
        self._filled_quote_today = Decimal("0")
        self._tradebin_fees_today = Decimal("0")
        self._gas_baseline = self.connector.gas_fees_paid()
        self._counted_trades.clear()

    # === One cycle ===

    def _run_cycle(self) -> Optional[str]:
        """Cancels leftover legs or trades one self-filled pair. Returns why it did not trade, or None."""
        leftovers = self._open_orders()
        if leftovers:
            for order in leftovers:
                self.connector.cancel(order.trading_pair, order.client_order_id)
            return f"cancelling {len(leftovers)} leftover leg(s) of an earlier cycle"
        if self.spend_today >= self.config.max_daily_fee_spend:
            return f"daily fee cap reached ({self.spend_today:.6f} >= {self.config.max_daily_fee_spend} BZE)"
        remaining = self.remaining_quote
        if remaining <= 0:
            return "daily volume target reached, idle until UTC midnight"
        book = self._best_bid_ask()
        if book is None:
            return "one side of the book is empty"
        best_bid, best_ask = book
        tick = self._tick()
        if best_ask - best_bid <= tick:
            return f"spread {best_ask - best_bid} is one tick ({tick}) or less"
        book_mid = (best_bid + best_ask) / 2
        self._last_book_mid = book_mid
        anchor, source = self._resolve_anchor(book_mid)
        self._last_anchor_source = source
        if anchor is None:
            return source
        if source == "coingecko":
            deviation_pct = abs(book_mid - anchor) / anchor * 100
            if deviation_pct > self.config.max_anchor_deviation_pct:
                return (
                    f"book mid {book_mid} is {deviation_pct:.2f}% off the anchor {anchor} "
                    f"(max {self.config.max_anchor_deviation_pct}%)"
                )
        price = self.trade_price(anchor, best_bid, best_ask, tick)
        amount = self.order_amount(remaining, price)
        if amount <= 0:
            return "order amount rounds to zero"
        base, quote = self.config.trading_pair.split("-", 1)
        if self.connector.get_available_balance(base) < amount:
            return f"not enough {base} for a {amount} sell leg"
        if self.connector.get_available_balance(quote) < amount * price:
            return f"not enough {quote} for a {amount} buy leg at {price}"
        self._place_pair(amount, price, base, quote)
        return None

    def _open_orders(self) -> List[Any]:
        return [
            order for order in self.connector.in_flight_orders.values()
            if order.client_order_id in self._own_orders and not order.is_done
        ]

    def _best_bid_ask(self) -> Optional[Tuple[Decimal, Decimal]]:
        order_book = self.connector.get_order_book(self.config.trading_pair)
        try:
            best_bid = order_book.get_price(False)
            best_ask = order_book.get_price(True)
        except EnvironmentError:
            return None
        if any(math.isnan(price) or price <= 0 for price in (best_bid, best_ask)):
            return None
        tick = self._tick()
        # The book holds floats; every chain price sits on the tick grid.
        return self._to_tick(Decimal(str(best_bid)), tick), self._to_tick(Decimal(str(best_ask)), tick)

    def _tick(self) -> Decimal:
        return self.connector.trading_rules[self.config.trading_pair].min_price_increment

    @staticmethod
    def _to_tick(price: Decimal, tick: Decimal, rounding: str = ROUND_HALF_EVEN) -> Decimal:
        return (price / tick).to_integral_value(rounding=rounding) * tick

    def trade_price(self, anchor: Decimal, best_bid: Decimal, best_ask: Decimal, tick: Decimal) -> Decimal:
        """The anchor clamped into [best_bid + tick, best_ask - tick] and rounded down to the tick grid, so both legs
        are priced strictly inside the spread: the first rests, the second meets it at exactly its price, and no
        other trader's order is crossed."""
        clamped = min(max(anchor, best_bid + tick), best_ask - tick)
        return self._to_tick(clamped, tick, ROUND_FLOOR)

    def order_amount(self, remaining_quote: Decimal, price: Decimal) -> Decimal:
        """remaining / cycles left today, in base units, clamped to the configured bounds, raised to the chain
        minimum at `price` and rounded to the base increment."""
        amount = remaining_quote / self.cycles_left_today() / price
        amount = min(max(amount, self.config.min_order_amount), self.config.max_order_amount)
        amount = max(amount, self.connector.minimum_order_amount(self.config.trading_pair, price))
        return self.connector.quantize_order_amount(self.config.trading_pair, amount)

    def cycles_left_today(self) -> int:
        seconds_left = SECONDS_PER_DAY - (int(self.current_timestamp) % SECONDS_PER_DAY)
        return max(1, math.ceil(seconds_left / self.config.cycle_seconds))

    def _place_pair(self, amount: Decimal, price: Decimal, base: str, quote: str):
        sides = [False, True] if self._sell_first else [True, False]
        orders = [
            LimitOrder(
                client_order_id="",
                trading_pair=self.config.trading_pair,
                is_buy=is_buy,
                base_currency=base,
                quote_currency=quote,
                price=price,
                quantity=amount,
            )
            for is_buy in sides
        ]
        # The connector puts every order of one batch_order_create call in one tx, in this order.
        created = self.connector.batch_order_create(orders)
        self._own_orders.update(order.client_order_id for order in created)
        self._sell_first = not self._sell_first
        first = "sell" if sides[0] is False else "buy"
        self.log_with_clock(
            logging.INFO,
            f"Self-fill of {amount} {base} at {price} sent in one tx ({first} first). "
            f"Remaining today: {self.remaining_quote} {quote}.",
        )

    # === Price anchor ===

    def _resolve_anchor(self, book_mid: Decimal) -> Tuple[Optional[Decimal], str]:
        """(anchor, source). Without a fresh CoinGecko anchor the fallback applies; with `pause` the anchor is None
        and the source says why."""
        if self.config.coingecko_id_base:
            age = self.anchor_age
            if self._anchor is not None and age is not None and age <= self.config.anchor_max_age_seconds:
                return self._anchor, "coingecko"
            why = "no CoinGecko anchor yet" if self._anchor is None else f"CoinGecko anchor is {age:.0f}s old"
        else:
            why = "no coingecko_id_base"
        if self.config.anchor_fallback == "pause":
            return None, f"{why} and anchor_fallback is pause"
        return book_mid, f"book_mid ({why})"

    @property
    def anchor_age(self) -> Optional[float]:
        if self._anchor_timestamp is None:
            return None
        return self.current_timestamp - self._anchor_timestamp

    @property
    def _uses_coingecko(self) -> bool:
        return self._feed is not None and bool(self.config.coingecko_id_base)

    def _refresh_anchor_if_due(self):
        if not self._uses_coingecko:
            return
        if self.current_timestamp < self._next_anchor_refresh:
            return
        if self._anchor_task is not None and not self._anchor_task.done():
            return
        self._next_anchor_refresh = self.current_timestamp + self.config.anchor_refresh_seconds
        self._anchor_task = safe_ensure_future(self.refresh_anchor())

    async def refresh_anchor(self):
        """One CoinGecko read: the base's external tickers and, when set, the quote's USD price. A failed read keeps
        the previous anchor, which the max age then retires."""
        try:
            tickers = await self._feed.get_coin_tickers(self.config.coingecko_id_base)
            base_usd = external_usd_price(tickers, self.config.exclude_markets)
            if base_usd is None:
                self.logger().warning(
                    f"CoinGecko lists no external ticker with volume for {self.config.coingecko_id_base} "
                    f"(excluding {self.config.exclude_markets}). Keeping the previous anchor."
                )
                return
            quote_usd = Decimal("1")
            if self.config.coingecko_id_quote:
                prices = await self._feed.get_prices_by_token_id("usd", [self.config.coingecko_id_quote])
                quote_usd = Decimal(str(prices[0]["current_price"]))
                if quote_usd <= 0:
                    raise ValueError(f"CoinGecko price of {self.config.coingecko_id_quote} is {quote_usd}")
            self._anchor = base_usd / quote_usd * self.config.anchor_price_scale
            self._anchor_timestamp = self.current_timestamp
        except Exception as error:
            self.logger().warning(f"CoinGecko anchor refresh failed: {error}. Keeping the previous anchor.")
        finally:
            self._anchor_read_finished = True

    # === Accounting ===

    @property
    def remaining_quote(self) -> Decimal:
        return self.config.daily_volume_quote - self._filled_quote_today

    @property
    def gas_today(self) -> Decimal:
        return self.connector.gas_fees_paid() - self._gas_baseline

    @property
    def spend_today(self) -> Decimal:
        return self._tradebin_fees_today + self.gas_today

    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        self._on_created(event.order_id)

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        self._on_created(event.order_id)

    def _on_created(self, order_id: str):
        if order_id in self._own_orders:
            self._created_orders.add(order_id)

    def did_fill_order(self, event: OrderFilledEvent):
        if event.order_id not in self._own_orders:
            return
        self._roll_day()
        self._filled_orders.add(event.order_id)
        # The connector puts each order's tradebin fee on its first fill: the maker fee for both legs of a self-fill,
        # since the chain charges the taker fee only for crossing an order that rested before the tx.
        fee = sum((fee.amount for fee in event.trade_fee.flat_fees), Decimal("0"))
        if fee > 0:
            self._add_order_fee(event.order_id, fee)
        key = trade_key(event)
        if key not in self._counted_trades:
            self._counted_trades.add(key)
            self._filled_quote_today += event.amount * event.price

    def did_cancel_order(self, event: OrderCancelledEvent):
        if event.order_id not in self._own_orders:
            return
        self._roll_day()
        if event.order_id in self._created_orders and event.order_id not in self._filled_orders:
            # Tradebin charged the maker fee when the order was created, and no fill reported it.
            self._add_order_fee(event.order_id, self._maker_fee())

    def _add_order_fee(self, order_id: str, fee: Decimal):
        """Tradebin charges one fee per order, so an order's fee enters the spend once."""
        if order_id not in self._fee_counted_orders:
            self._fee_counted_orders.add(order_id)
            self._tradebin_fees_today += fee

    def _maker_fee(self) -> Decimal:
        base, quote = self.config.trading_pair.split("-", 1)
        fee = self.connector.get_fee(base, quote, OrderType.LIMIT, TradeType.BUY, Decimal("0"), Decimal("0"), is_maker=True)
        return sum((token_amount.amount for token_amount in fee.flat_fees), Decimal("0"))

    # === Status ===

    def format_status(self) -> str:
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        quote = self.config.trading_pair.split("-", 1)[1]
        filled = self._filled_quote_today
        cost_per_1000 = self.spend_today / filled * 1000 if filled > 0 else None
        age = self.anchor_age
        lines = [
            f"  Day (UTC): {self._day}",
            f"  Target / filled / remaining: {self.config.daily_volume_quote} / {filled} / "
            f"{max(self.remaining_quote, Decimal('0'))} {quote}",
            f"  Anchor: {self._anchor if self._anchor is not None else '-'} "
            f"(source: {self._last_anchor_source or '-'}, age: {f'{age:.0f}s' if age is not None else '-'})",
            f"  Book mid: {self._last_book_mid if self._last_book_mid is not None else '-'}",
            f"  Spend today: {self._tradebin_fees_today} tradebin fees + {self.gas_today} gas = "
            f"{self.spend_today} / {self.config.max_daily_fee_spend} BZE",
            f"  Cost per 1,000 {quote} of volume: {f'{cost_per_1000:.6f} BZE' if cost_per_1000 is not None else '-'}",
        ]
        if self._last_skip_reason:
            lines.append(f"  Last cycle skipped: {self._last_skip_reason}")
        return "\n".join(lines)
