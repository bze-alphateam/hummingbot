import logging
import re
import unittest
from decimal import Decimal
from test.hummingbot.connector.exchange.beezee.test_beezee_order_batching import (
    BeezeeBatchingTestCase,
    SentTx,
    decode_tx,
)
from test.hummingbot.connector.exchange.beezee.test_beezee_order_tracking import (
    ADDRESS,
    BZE_USDC,
    OTHER_ADDRESS,
    REST_URL,
    RPC_URL,
    USDC,
    BeezeeChainTestCase,
    H,
    block_event,
    block_results,
    create_message,
    tx,
)
from test.hummingbot.connector.exchange.beezee.test_beezee_swaps import POOL_ID, BeezeeSwapTestCase, swap_event
from typing import Any, Dict, List, Optional

from yarl import URL

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_amm import BeezeeSwapError, BeezeeSwapRequest, swap_output
from hummingbot.connector.exchange.beezee.beezee_exchange import QueuedOrderMessage
from hummingbot.connector.exchange.beezee.beezee_fills import (
    BeezeeFillError,
    levels_to_take,
    prepare_fill_entries,
    resting_side,
)
from hummingbot.connector.exchange.beezee.beezee_utils import BeezeeMarket, BeezeeToken
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import TokenAmount
from hummingbot.core.event.events import MarketEvent

FILL_TX = "F1" * 32
TAKER_FEE = TokenAmount("BZE", Decimal("0.1"))
MAKER_ID = "000000000000000006810500"


def market(base_exponent: int = 6, quote_exponent: int = 6) -> BeezeeMarket:
    return BeezeeMarket(
        market_id=BZE_USDC,
        base=BeezeeToken(denom="ubze", symbol="BZE", display_name="BZE", display_denom="bze", exponent=base_exponent),
        quote=BeezeeToken(denom=USDC, symbol="USDC", display_name="USDC", display_denom="usdc", exponent=quote_exponent),
    )


def executed(amount: str, price: str, message_id: str, order_type: str = "sell", maker: str = OTHER_ADDRESS,
             order_id: str = MAKER_ID) -> Dict[str, Any]:
    """An OrderExecutedEvent of a fill: `order_type` is the side of the resting order taken."""
    return block_event("OrderExecutedEvent", id=order_id, market_id=BZE_USDC, order_type=order_type, amount=amount,
                       price=price, maker=maker, taker=ADDRESS, message_id=message_id)


def refunded(amount: str, message_id: str, reason: str, order_type: str = "buy") -> Dict[str, Any]:
    """Chain v8.3.0's OrderMessageRefundedEvent: `order_type` is the side of the queue message (ours)."""
    return block_event("OrderMessageRefundedEvent", message_id=message_id, market_id=BZE_USDC, order_type=order_type,
                       owner=ADDRESS, amount=amount, refund=f"{amount}{USDC}", reason=reason)


def message_id(index: int) -> str:
    return f"{index:024d}"


class BeezeeFillEntriesTests(unittest.TestCase):
    def test_our_side_maps_to_the_side_of_the_resting_orders(self):
        self.assertEqual("sell", resting_side("buy"))
        self.assertEqual("buy", resting_side("sell"))
        with self.assertRaises(BeezeeFillError):
            resting_side("BUY")

    def test_levels_below_the_chain_minimum_are_dropped(self):
        # The minimum at 0.0003 is ceil(1 / 0.0003) x 2 = 6668 base units, at 0.0002 it is 10000.
        entries, dropped = prepare_fill_entries(market(), "buy", [
            (Decimal("0.0003"), Decimal("0.006668")),
            (Decimal("0.0002"), Decimal("0.009999")),
        ])

        self.assertEqual([("0.0003", 6668)], [(entry.chain_price, entry.chain_amount) for entry in entries])
        self.assertEqual([(Decimal("0.0002"), Decimal("0.009999"))], dropped)

    def test_every_level_below_the_minimum_is_refused(self):
        with self.assertRaisesRegex(BeezeeFillError, "every level is below the chain minimum"):
            prepare_fill_entries(market(), "sell", [(Decimal("0.0003"), Decimal("0.006667"))])

    def test_amounts_round_down_and_equal_prices_merge(self):
        entries, _ = prepare_fill_entries(market(), "buy", [
            (Decimal("0.0003"), Decimal("1.0000009")),
            (Decimal("0.00030"), Decimal("2")),
            (Decimal("0.00031"), Decimal("3")),
        ])

        self.assertEqual(
            [(Decimal("0.0003"), Decimal("3"), "0.0003", 3_000_000), (Decimal("0.00031"), Decimal("3"), "0.00031", 3_000_000)],
            [(entry.price, entry.amount, entry.chain_price, entry.chain_amount) for entry in entries],
        )

    def test_chain_prices_follow_the_market_exponents(self):
        # A base with 3 decimals more than the quote: chain price = display price / 1000.
        [entry], _ = prepare_fill_entries(market(base_exponent=9), "buy", [(Decimal("2.5"), Decimal("1"))])

        self.assertEqual(("0.0025", 1_000_000_000), (entry.chain_price, entry.chain_amount))

    def test_invalid_levels_are_refused(self):
        for levels in ([], [(Decimal("0"), Decimal("1"))], [(Decimal("0.0003"), Decimal("-1"))],
                       [(Decimal("0.0000000001"), Decimal("1"))]):
            with self.assertRaises(BeezeeFillError):
                prepare_fill_entries(market(), "buy", levels)
        with self.assertRaisesRegex(BeezeeFillError, "at most 50"):
            prepare_fill_entries(market(), "buy", [(Decimal("0.0003") + Decimal(index) / 10**7, Decimal("1")) for index in range(51)])


class BeezeeLevelsToTakeTests(unittest.TestCase):
    ASKS = [("0.0003", "5000000"), ("0.00031", "2000000"), ("0.00035", "9000000")]
    BIDS = [("0.00029", "4000000"), ("0.00028", "1000000")]

    def test_buy_takes_asks_up_to_the_worst_price(self):
        levels = levels_to_take(self.ASKS, market(), "buy", Decimal("100"), Decimal("0.00031"))

        self.assertEqual([(Decimal("0.0003"), Decimal("5")), (Decimal("0.00031"), Decimal("2"))], levels)

    def test_buy_stops_at_the_amount_with_a_part_of_the_last_level(self):
        levels = levels_to_take(self.ASKS, market(), "buy", Decimal("6.5"), Decimal("1"))

        self.assertEqual([(Decimal("0.0003"), Decimal("5")), (Decimal("0.00031"), Decimal("1.5"))], levels)

    def test_sell_takes_bids_down_to_the_worst_price(self):
        self.assertEqual(
            [(Decimal("0.00029"), Decimal("4"))],
            levels_to_take(self.BIDS, market(), "sell", Decimal("100"), Decimal("0.000285")),
        )
        self.assertEqual(
            [(Decimal("0.00029"), Decimal("4")), (Decimal("0.00028"), Decimal("0.5"))],
            levels_to_take(self.BIDS, market(), "sell", Decimal("4.5"), Decimal("0.0001")),
        )

    def test_a_last_part_below_the_minimum_is_left_out(self):
        # 0.006 BZE at 0.00031 is below the 6452-unit minimum there.
        levels = levels_to_take(self.ASKS, market(), "buy", Decimal("5.006"), Decimal("1"))

        self.assertEqual([(Decimal("0.0003"), Decimal("5"))], levels)

    def test_nothing_within_the_worst_price(self):
        self.assertEqual([], levels_to_take(self.ASKS, market(), "buy", Decimal("10"), Decimal("0.00029")))
        self.assertEqual([], levels_to_take([], market(), "buy", Decimal("10"), Decimal("1")))

    def test_exact_eighteen_decimal_prices_survive(self):
        # A price a float cannot hold: the chain string is kept, so the fill names the level exactly.
        price = "0.000333333333333333"
        [(display, amount)] = levels_to_take([(price, "9000000")], market(), "buy", Decimal("9"), Decimal("1"))
        [entry], _ = prepare_fill_entries(market(), "buy", [(display, amount)])

        self.assertEqual(price, entry.chain_price)
        self.assertNotEqual(Decimal(price), Decimal(float(price)))


class BeezeeFillTestCase(BeezeeChainTestCase):
    def _mock_queue(self, height: Optional[int], pending: Optional[bool], repeat: bool = False):
        """The raw store read of the tradebin queue at `height` (any height when None): pending, gone, or unanswered."""
        heights = r"\d+" if height is None else str(height)
        url = re.compile(rf"^{re.escape(RPC_URL + CONSTANTS.ABCI_QUERY_PATH_URL)}\?.*height={heights}(&.*)?$")
        if pending is None:
            response = {"code": 26, "log": "height is not available"}
        else:
            response = {"code": 0, "value": "CgQ=" if pending else None}
        self.mock_api.get(url, repeat=repeat, payload={"jsonrpc": "2.0", "id": -1, "result": {"response": response}})

    def _queue_reads(self) -> List[Dict[str, str]]:
        return [
            dict(URL(url).query) for (method, url), calls in self.mock_api.requests.items()
            if method == "GET" and url.path == CONSTANTS.ABCI_QUERY_PATH_URL for _ in calls
        ]

    def _sent(self) -> List[SentTx]:
        return [decode_tx(call.kwargs) for call in self.mock_api.requests.get(("POST", URL(f"{REST_URL}{CONSTANTS.BROADCAST_TX_PATH_URL}")), [])]

    def _balance_reads(self) -> int:
        return sum(len(calls) for (method, url), calls in self.mock_api.requests.items()
                   if method == "GET" and url.path.startswith(CONSTANTS.BALANCES_PATH_URL))

    def _fill_tx(self, entries: List[tuple], height: int = H, side: str = "buy") -> Dict[str, Any]:
        """The fill tx: one OrderCreateMessageEvent per (amount, price) entry, message ids 0.. (ours is `side`)."""
        return tx(FILL_TX, height, [
            create_message(side, amount, price, message_id(index)) for index, (amount, price) in enumerate(entries)
        ])

    def _mock_fill_tx(self, response: Dict[str, Any]):
        # Every entry follows the tx; their lookups usually share one request, but need not.
        self.mock_api.get(f"{REST_URL}{CONSTANTS.GET_TX_PATH_URL}/{FILL_TX}", payload=response, repeat=True)

    def _send_fill(self, entries: List[tuple], side: str = "buy", sent_entries: Optional[List[tuple]] = None) -> List[str]:
        """Sends a fill of (price, amount) `entries`; its tx has an OrderCreateMessageEvent per sent entry (all of
        them unless given)."""
        self._mock_broadcast(FILL_TX)
        sent = entries if sent_entries is None else sent_entries
        self._mock_fill_tx(self._fill_tx([(str(int(amount * 10**6)), str(price)) for price, amount in sent], side=side))
        return self.exchange.fill_orders("BZE-USDC", side, entries)

    def _fill(self, client_order_id: str, exchange_order_id: Optional[str] = FILL_TX, amount: Decimal = Decimal("20"),
              price: Decimal = Decimal("0.0003")) -> InFlightOrder:
        return InFlightOrder(
            client_order_id=client_order_id, trading_pair="BZE-USDC", order_type=OrderType.LIMIT, trade_type=TradeType.BUY,
            amount=amount, price=price, creation_timestamp=1.0, exchange_order_id=exchange_order_id,
            initial_state=OrderState.OPEN if exchange_order_id else OrderState.PENDING_CREATE,
        )


class BeezeeFillTests(BeezeeFillTestCase):

    # --- sending ---

    async def test_buy_takes_asks_with_one_msg_fill_orders_and_fills_completely(self):
        self._mock_latest_height(H, repeat=True)
        self._mock_blocks({H: block_results(H, [executed("20000000", "0.0003", message_id(0))])})

        [order_id] = self._send_fill([(Decimal("0.0003"), Decimal("20"))])
        await self._run_until(lambda: order_id in self._event_ids(MarketEvent.BuyOrderCompleted), "the fill did not complete")

        [sent] = self._sent()
        [message] = sent.messages
        self.assertEqual(("fill", BZE_USDC, "sell", [("0.0003", "20000000")]), (
            message.kind, message.market_id, message.order_type, message.entries,
        ))
        self.assertEqual(f"{CONSTANTS.FILL_MEMO}:{order_id}", sent.memo)
        self._assert_fills([(Decimal("20"), Decimal("0.0003"), [TAKER_FEE])])
        [fill] = self.events[MarketEvent.OrderFilled].event_log
        self.assertEqual((TradeType.BUY, OrderType.LIMIT, FILL_TX), (fill.trade_type, fill.order_type, fill.exchange_order_id))
        self.assertEqual([order_id], self._event_ids(MarketEvent.BuyOrderCreated))
        self.assertNotIn(order_id, self.exchange.in_flight_orders)
        # Settled by its event: the queue is never read.
        self.assertEqual([], self._queue_reads())

    async def test_sell_takes_bids_and_drops_a_level_below_the_minimum(self):
        self._mock_latest_height(H, repeat=True)
        self._mock_blocks({H: block_results(H, [executed("20000000", "0.0003", message_id(0), order_type="buy")])})

        order_ids = self._send_fill(
            [(Decimal("0.0003"), Decimal("20")), (Decimal("0.0002"), Decimal("0.001"))], side="sell",
            sent_entries=[(Decimal("0.0003"), Decimal("20"))],
        )
        await self._run_until(lambda: order_ids[0] in self._event_ids(MarketEvent.SellOrderCompleted), "the fill did not complete")

        self.assertEqual(1, len(order_ids))
        [message] = self._sent()[0].messages
        self.assertEqual(("buy", [("0.0003", "20000000")]), (message.order_type, message.entries))
        self.assertTrue(self._logged(logging.INFO, "dropping the level 0.001 at 0.0002, below the chain minimum amount"))
        self.assertEqual(TradeType.SELL, self.events[MarketEvent.OrderFilled].event_log[0].trade_type)

    async def test_every_level_below_the_minimum_is_refused_up_front(self):
        with self.assertRaisesRegex(BeezeeFillError, "every level is below the chain minimum"):
            self.exchange.fill_orders("BZE-USDC", "buy", [(Decimal("0.0003"), Decimal("0.001"))])

        self.assertEqual({}, self.exchange.in_flight_orders)
        self.assertEqual([], self._sent())

    async def test_a_halted_market_is_refused_up_front(self):
        self.exchange._halted_denoms = {USDC}

        with self.assertRaisesRegex(BeezeeFillError, "is halted"):
            self.exchange.fill_orders("BZE-USDC", "buy", [(Decimal("0.0003"), Decimal("20"))])

        self.assertEqual({}, self.exchange.in_flight_orders)
        self.assertEqual([], self._sent())

    async def test_an_unknown_pair_is_refused(self):
        with self.assertRaisesRegex(BeezeeFillError, "unknown Beezee trading pair"):
            self.exchange.fill_orders("ATOM-USDC", "buy", [(Decimal("1"), Decimal("1"))])

    async def test_a_fill_cannot_be_cancelled(self):
        self._mock_broadcast(FILL_TX)
        [order_id] = self.exchange.fill_orders("BZE-USDC", "buy", [(Decimal("0.0003"), Decimal("20"))])

        self.assertFalse(await self.exchange._place_cancel(order_id, self._order(order_id)))
        self.assertTrue(self._logged(logging.INFO, f"Beezee fill {order_id} settles in EndBlock and cannot be cancelled."))

    # --- settling ---

    async def test_partial_fill_with_a_refund_and_a_full_fill_book_the_taker_fee_once(self):
        # Chain v8.3.0: the refunded part of the first entry has its own event.
        self._mock_latest_height(H, repeat=True)
        self._mock_blocks({H: block_results(H, [
            executed("5000000", "0.0003", message_id(0)),
            refunded("15000000", message_id(0), "unfilled_remainder"),
            executed("10000000", "0.00031", message_id(1), order_id="000000000000000006810501"),
        ])})

        first, second = self._send_fill([(Decimal("0.0003"), Decimal("20")), (Decimal("0.00031"), Decimal("10"))])
        await self._run_until(lambda: first in self._event_ids(MarketEvent.OrderCancelled), "the refunded entry did not close")
        await self._run_until(lambda: second in self._event_ids(MarketEvent.BuyOrderCompleted), "the full entry did not complete")

        self._assert_fills([(Decimal("5"), Decimal("0.0003"), [TAKER_FEE]), (Decimal("10"), Decimal("0.00031"), [])])
        self.assertTrue(self._logged(logging.INFO, f"Beezee order {first}: the chain refunded 15000000 base units (unfilled_remainder)"))
        self.assertTrue(self._logged(logging.INFO, f"Beezee order {first} filled 5.000000 of 20.000000 at height {H} and the chain refunded the rest."))
        self.assertEqual([], self._queue_reads())
        await self._run_until(lambda: self._balance_reads() > 0, "the balances were not read again")

    async def test_no_match_refund_event_closes_the_entry_without_a_fill(self):
        self._mock_latest_height(H, repeat=True)
        self._mock_blocks({H: block_results(H, [refunded("20000000", message_id(0), "no_match")])})

        [order_id] = self._send_fill([(Decimal("0.0003"), Decimal("20"))])
        await self._run_until(lambda: order_id in self._event_ids(MarketEvent.OrderCancelled), "the fill did not close")

        self.assertEqual([], self._event_ids(MarketEvent.OrderFilled))

    async def test_v820_partial_fill_closes_with_its_fill_in_the_block_it_executed(self):
        # Chain v8.2.0 emits nothing for the refunded rest: the message executed in this block, so the rest came back.
        self._mock_latest_height(H, repeat=True)
        self._mock_blocks({H: block_results(H, [executed("5000000", "0.0003", message_id(0))])})

        [order_id] = self._send_fill([(Decimal("0.0003"), Decimal("20"))])
        await self._run_until(lambda: order_id in self._event_ids(MarketEvent.OrderCancelled), "the fill did not close")

        self._assert_fills([(Decimal("5"), Decimal("0.0003"), [TAKER_FEE])])
        self.assertEqual([], self._queue_reads())

    async def test_v820_no_match_is_settled_from_the_queue_state_of_its_block(self):
        self._mock_latest_height(H, repeat=True)
        self._mock_blocks({H: block_results(H)})
        self._mock_queue(H, pending=False)

        [order_id] = self._send_fill([(Decimal("0.0003"), Decimal("20"))])
        await self._run_until(lambda: order_id in self._event_ids(MarketEvent.OrderCancelled), "the fill did not close")

        self.assertEqual([], self._event_ids(MarketEvent.OrderFilled))
        [read] = self._queue_reads()
        self.assertEqual(f'"{CONSTANTS.TRADEBIN_STORE_QUERY_PATH}"', read["path"])
        self.assertEqual(str(H), read["height"])
        self.assertEqual(f"Qm/value/{BZE_USDC}/{message_id(0)}/".encode(), bytes.fromhex(read["data"][2:]))
        await self._run_until(lambda: self._balance_reads() > 0, "the balances were not read again")

    async def test_v820_fill_still_queued_settles_in_the_block_it_executes(self):
        self._mock_latest_height(H + 1, repeat=True)
        self._mock_blocks({H: block_results(H), H + 1: block_results(H + 1, [executed("5000000", "0.0003", message_id(0))])})
        self._mock_queue(H, pending=True)

        [order_id] = self._send_fill([(Decimal("0.0003"), Decimal("20"))])
        await self._run_until(lambda: order_id in self._event_ids(MarketEvent.OrderCancelled), "the fill did not close")

        self._assert_fills([(Decimal("5"), Decimal("0.0003"), [TAKER_FEE])])
        self.assertEqual([str(H)], [read["height"] for read in self._queue_reads()])

    async def test_no_outcome_within_twenty_blocks_fails_the_fill(self):
        self._mock_latest_height(H + 19, repeat=True)
        self._mock_blocks({H: block_results(H)}, empty_up_to=H + 19)
        self._mock_queue(None, pending=None, repeat=True)

        [order_id] = self._send_fill([(Decimal("0.0003"), Decimal("20"))])
        await self._run_until(lambda: order_id in self._event_ids(MarketEvent.OrderFailure), "the fill did not fail")

        self.assertTrue(self._logged(logging.WARNING, f"Beezee order {order_id} failed: no outcome within 20 blocks of its tx."))
        await self._run_until(lambda: self._balance_reads() > 0, "the balances were not read again")

    async def test_reused_message_id_settles_the_fill_as_processed(self):
        self._mock_latest_height(H + 1, repeat=True)
        self._mock_queue(H, pending=None)
        self._mock_blocks({H: block_results(H), H + 1: block_results(H + 1, tx_events=[
            create_message("sell", "1000000", "0.0005", message_id(0)),
        ])})

        [order_id] = self._send_fill([(Decimal("0.0003"), Decimal("20"))])
        await self._run_until(lambda: order_id in self._event_ids(MarketEvent.OrderCancelled), "the fill did not close")

        self.assertEqual([], self._event_ids(MarketEvent.OrderFailure))

    # --- restart ---

    async def test_restart_mid_flight_resumes_the_fill_from_its_saved_message(self):
        first, second = self._fill("OID-F1"), self._fill("OID-F2", amount=Decimal("10"), price=Decimal("0.00031"))
        saved = {
            order.client_order_id: QueuedOrderMessage(
                client_order_id=order.client_order_id, tx_hash=FILL_TX, msg_index=index, tx_height=H,
                message_id=message_id(index), next_height=H, fill_group="OID-F1",
            ).to_json()
            for index, order in enumerate((first, second))
        }
        self._restore([first, second], {
            "last_processed_height": H - 1, "creates": saved, "cancels": {}, "fills": {"OID-F1": "OID-F1", "OID-F2": "OID-F1"},
        })
        self.assertEqual({"OID-F1": "OID-F1", "OID-F2": "OID-F1"}, self.exchange.tracking_states[CONSTANTS.TRACKING_STATE_KEY]["fills"])
        self.assertFalse(await self.exchange._place_cancel("OID-F1", first))
        self._mock_latest_height(H, repeat=True)
        self._mock_queue(H, pending=False)
        self._mock_blocks({H: block_results(H, [executed("10000000", "0.00031", message_id(1))])})

        await self._status_update()
        await self._run_until(lambda: "OID-F1" in self._event_ids(MarketEvent.OrderCancelled), "the first entry did not close")
        await self._run_until(lambda: "OID-F2" in self._event_ids(MarketEvent.BuyOrderCompleted), "the second entry did not complete")

        self._assert_fills([(Decimal("10"), Decimal("0.00031"), [TAKER_FEE])])
        self.assertEqual({}, self.exchange.tracking_states[CONSTANTS.TRACKING_STATE_KEY]["creates"])

    async def test_restart_with_a_fill_saved_before_its_tx_was_known_fails_it_without_reading_the_book(self):
        self._restore([self._fill("OID-F1", exchange_order_id=None)], {
            "last_processed_height": H, "creates": {}, "cancels": {}, "fills": {"OID-F1": "OID-F1"},
        })
        self._mock_latest_height(H, repeat=True)

        await self._status_update()
        await self._run_until(lambda: "OID-F1" in self._event_ids(MarketEvent.OrderFailure), "the fill did not fail")

        self.assertTrue(self._logged(logging.WARNING, "Beezee fill OID-F1 was saved before its tx was known"))
        self.assertFalse(any(url.path.startswith("/bze/tradebin/user_market_orders") for _, url in self.mock_api.requests))

    async def test_restart_with_only_the_tx_hash_matches_the_entry_by_its_values(self):
        self._restore([self._fill("OID-F1")], {"last_processed_height": H, "creates": {}, "cancels": {}, "fills": {"OID-F1": "OID-F1"}})
        self.assertEqual("OID-F1", self.exchange._queued_creates["OID-F1"].fill_group)
        self._mock_fill_tx(self._fill_tx([("20000000", "0.0003")]))
        self._mock_latest_height(H, repeat=True)
        self._mock_queue(H, pending=False)
        self._mock_blocks({H: block_results(H)})

        await self._status_update()
        await self._run_until(lambda: "OID-F1" in self._event_ids(MarketEvent.OrderCancelled), "the fill did not close")

    # --- levels_to_take ---

    async def test_levels_to_take_reads_the_last_book_in_chain_units(self):
        self.exchange._orderbook_ds._remember_levels("BZE-USDC", {
            "asks": [{"price": "0.00031", "amount": "2000000"}, {"price": "0.0003", "amount": "5000000"}],
            "bids": [{"price": "0.00028", "amount": "1000000"}, {"price": "0.00029", "amount": "4000000"}],
        })

        self.assertEqual(
            [(Decimal("0.0003"), Decimal("5")), (Decimal("0.00031"), Decimal("1"))],
            self.exchange.levels_to_take("BZE-USDC", "buy", Decimal("6"), Decimal("0.0004")),
        )
        self.assertEqual(
            [(Decimal("0.00029"), Decimal("4"))],
            self.exchange.levels_to_take("BZE-USDC", "sell", Decimal("6"), Decimal("0.00029")),
        )
        self.assertEqual([], self.exchange.levels_to_take("TKN-BZE", "buy", Decimal("6"), Decimal("1")))


class BeezeeFillBatchingTests(BeezeeBatchingTestCase):
    async def test_a_fill_shares_a_batch_tx_with_a_create(self):
        self.node.taker_book[(BZE_USDC, "sell", Decimal("0.0003"))] = 8_000_000

        create_id = self.exchange.buy("BZE-USDC", Decimal("10"), OrderType.LIMIT, Decimal("0.0002"))
        fill_ids = self.exchange.fill_orders("BZE-USDC", "buy", [(Decimal("0.0003"), Decimal("8")), (Decimal("0.00031"), Decimal("1"))])
        await self._run_until(lambda: len(self.windows) == 1, "the batch window did not start")
        self.windows[0].set_result(None)
        await self._run_until(lambda: fill_ids[0] in self._event_ids(MarketEvent.BuyOrderCompleted), "the first entry did not fill")
        await self._run_until(lambda: fill_ids[1] in self._event_ids(MarketEvent.OrderCancelled), "the second entry did not close")
        await self._run_until(lambda: self._state(create_id) == OrderState.OPEN and len(self._exchange_id(create_id) or "") == 24,
                              "the create did not rest")

        [sent] = self.node.broadcast
        self.assertEqual(["create", "fill"], sorted(sent.kinds))
        self.assertEqual(CONSTANTS.BATCH_MEMO, sent.memo)
        self._assert_fills([(Decimal("8"), Decimal("0.0003"), [TokenAmount("BZE", Decimal("0.1"))])])


class BeezeeFillTxTests(BeezeeSwapTestCase):
    """Fills against txs the node refuses or that also swap (no default simulation answer)."""

    async def test_a_simulation_refusal_fails_every_entry(self):
        self.mock_api.post(f"{REST_URL}{CONSTANTS.SIMULATE_TX_PATH_URL}", repeat=True, status=400, payload={
            "code": 5, "message": "insufficient funds", "details": [],
        })

        order_ids = self.exchange.fill_orders(
            "BZE-USDC", "buy", [(Decimal("0.0003"), Decimal("20")), (Decimal("0.00031"), Decimal("10"))]
        )
        await self._run_until(lambda: len(self._failure_messages()) == 2, "the fill did not fail")

        self.assertEqual({order_id: "Beezee fill failed: insufficient funds" for order_id in order_ids}, self._failure_messages())
        self.assertEqual([], self._posted(CONSTANTS.BROADCAST_TX_PATH_URL))

    async def test_fill_and_swap_go_out_in_one_tx_and_each_settles_from_its_own_events(self):
        # Buy BZE on the book, sell it on the pool: the arbitrage shape.
        quoted = swap_output(self.pool, "ubze", 20_000_000)
        self._mock_simulate_gas(300000)
        self._mock_broadcast(FILL_TX)
        self.mock_api.get(f"{REST_URL}{CONSTANTS.GET_TX_PATH_URL}/{FILL_TX}", repeat=True, payload=tx(FILL_TX, H, [
            create_message("buy", "20000000", "0.0003", message_id(0)),
            swap_event(1, "ubze", 20_000_000, USDC, quoted.output_amount),
        ]))
        self._mock_latest_height(H, repeat=True)
        self._mock_blocks({H: block_results(H, [executed("20000000", "0.0003", message_id(0))])})

        [fill_id], [swap_id] = self.exchange.fill_orders_and_swap(
            "BZE-USDC", "buy", [(Decimal("0.0003"), Decimal("20"))],
            [BeezeeSwapRequest("BZE-USDC", "sell", Decimal("20"), Decimal("1"))],
        )
        await self._run_until(lambda: swap_id in self._event_ids(MarketEvent.SellOrderCompleted), "the swap did not settle")
        await self._run_until(lambda: fill_id in self._event_ids(MarketEvent.BuyOrderCompleted), "the fill did not settle")

        [sent] = self._posted(CONSTANTS.BROADCAST_TX_PATH_URL)
        self.assertEqual(["fill", "swap"], sent.kinds)
        self.assertEqual(("sell", [("0.0003", "20000000")]), (sent.messages[0].order_type, sent.messages[0].entries))
        self.assertEqual(([POOL_ID], "ubze", "20000000"), (sent.messages[1].routes, sent.messages[1].input_denom, sent.messages[1].amount))
        self.assertEqual(f"{CONSTANTS.FILL_SWAP_MEMO}:{fill_id}", sent.memo)
        fills = {fill.order_id: fill for fill in self.events[MarketEvent.OrderFilled].event_log}
        self.assertEqual((Decimal("20"), Decimal("0.0003"), [TokenAmount("BZE", Decimal("0.1"))]), (
            fills[fill_id].amount, fills[fill_id].price, fills[fill_id].trade_fee.flat_fees,
        ))
        self.assertEqual(f"{FILL_TX}-0", fills[swap_id].exchange_trade_id)

    async def test_a_refused_tx_fails_the_fill_and_the_swap(self):
        self.mock_api.post(f"{REST_URL}{CONSTANTS.SIMULATE_TX_PATH_URL}", status=400, payload={
            "code": 5, "message": "insufficient funds", "details": [],
        })

        [fill_id], [swap_id] = self.exchange.fill_orders_and_swap(
            "BZE-USDC", "buy", [(Decimal("0.0003"), Decimal("20"))],
            [BeezeeSwapRequest("BZE-USDC", "sell", Decimal("20"), Decimal("1"))],
        )
        await self._run_until(lambda: len(self._event_ids(MarketEvent.OrderFailure)) == 2, "the tx did not fail")

        self.assertEqual({fill_id, swap_id}, set(self._event_ids(MarketEvent.OrderFailure)))
        self.assertEqual([], self._posted(CONSTANTS.BROADCAST_TX_PATH_URL))

    async def test_a_swap_refused_up_front_tracks_no_fill(self):
        with self.assertRaises(BeezeeSwapError):
            self.exchange.fill_orders_and_swap(
                "BZE-USDC", "buy", [(Decimal("0.0003"), Decimal("20"))],
                [BeezeeSwapRequest("BZE-USDC", "sell", Decimal("100000"), Decimal("1"))],
            )

        self.assertEqual({}, self.exchange.in_flight_orders)
