import asyncio
import hashlib
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set
from urllib.parse import quote

from bidict import bidict

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_signer import BeezeeSigner
from hummingbot.connector.exchange.beezee.beezee_tx import (
    build_broadcast_request,
    build_signed_transaction,
    fee_amount_from_gas,
    msg_cancel_order,
    msg_create_order,
)
from hummingbot.connector.exchange.beezee.beezee_utils import (
    BeezeeMarket,
    chain_amount_to_display,
    chain_price_to_display,
    display_amount_to_chain,
    display_price_to_chain,
    market_from_data,
    unique_market_symbols,
)
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


@dataclass
class BeezeeAccountInfo:
    account_number: int
    sequence: int


class BeezeeAPIDataSource:
    def __init__(
        self,
        api_factory: WebAssistantsFactory,
        rest_endpoint: str,
        chain_id: str,
        address_prefix: str,
        native_denom: str,
        gas_price: Decimal,
        account_address: Optional[str] = None,
        signer: Optional[BeezeeSigner] = None,
    ):
        self._api_factory = api_factory
        self._rest_endpoint = rest_endpoint.rstrip("/")
        self._chain_id = chain_id
        self._address_prefix = address_prefix
        self._native_denom = native_denom
        self._gas_price = gas_price
        self._account_address = account_address or (signer.address if signer is not None else None)
        self._signer = signer
        self._metadata_by_denom: Dict[str, Dict[str, Any]] = {}
        self._markets_by_id: Dict[str, BeezeeMarket] = {}
        self._symbol_map: Optional[bidict] = None
        self._known_limit_ids = {rate_limit.limit_id for rate_limit in CONSTANTS.RATE_LIMITS}
        self._warned_missing_metadata_denoms: Set[str] = set()
        self._account_info_lock = asyncio.Lock()
        self._cached_account_number: Optional[int] = None
        self._next_account_sequence: Optional[int] = None

    @classmethod
    def logger(cls):
        return logging.getLogger(__name__)

    @property
    def account_address(self) -> Optional[str]:
        return self._account_address

    async def _request(self, path_url: str, params: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None, method: RESTMethod = RESTMethod.GET, limit_id: Optional[str] = None) -> Dict[str, Any]:
        rest_assistant = await self._api_factory.get_rest_assistant()
        throttler_limit_id = limit_id or path_url
        if throttler_limit_id not in self._known_limit_ids:
            throttler_limit_id = CONSTANTS.QUERY_LIMIT_ID if method == RESTMethod.GET else CONSTANTS.TX_LIMIT_ID
        return await rest_assistant.execute_request(
            url=f"{self._rest_endpoint}{path_url}",
            params=params,
            data=data,
            method=method,
            throttler_limit_id=throttler_limit_id,
        )

    async def _paged_request(self, path_url: str, item_key: str, params: Optional[Dict[str, Any]] = None, page_limit: int = CONSTANTS.DEFAULT_MARKET_PAGE_SIZE) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        next_key: Optional[str] = None
        while True:
            page_params = dict(params or {})
            page_params["pagination.limit"] = page_limit
            if next_key:
                page_params["pagination.key"] = next_key
            response = await self._request(path_url=path_url, params=page_params)
            items.extend(response.get(item_key, []))
            next_key = (response.get("pagination") or {}).get("next_key")
            if not next_key:
                break
        return items

    async def get_tradebin_params(self) -> Dict[str, Any]:
        return (await self._request(CONSTANTS.TRADEBIN_PARAMS_PATH_URL)).get("params", {})

    async def get_denom_metadata(self, denom: str) -> Dict[str, Any]:
        encoded_denom = quote(denom, safe="")
        metadata: Dict[str, Any] = {}
        try:
            metadata = (
                await self._request(
                    f"{CONSTANTS.DENOM_METADATA_PATH_URL}/{encoded_denom}",
                    limit_id=CONSTANTS.DENOM_METADATA_PATH_URL,
                )
            ).get("metadata", {})
        except Exception:
            try:
                metadata = (
                    await self._request(
                        CONSTANTS.DENOM_METADATA_BY_QUERY_PATH_URL,
                        params={"denom": denom},
                        limit_id=CONSTANTS.DENOM_METADATA_BY_QUERY_PATH_URL,
                    )
                ).get("metadata", {})
            except Exception:
                metadata = self._fallback_metadata_for_denom(denom)
                if denom not in self._warned_missing_metadata_denoms:
                    self.logger().warning(
                        f"Beezee denom metadata unavailable for '{denom}'. Falling back to raw denom metadata."
                    )
                    self._warned_missing_metadata_denoms.add(denom)
        self._metadata_by_denom[denom] = metadata
        return metadata

    def _fallback_metadata_for_denom(self, denom: str) -> Dict[str, Any]:
        if denom == self._native_denom:
            display = self._native_denom[1:] if self._native_denom.startswith("u") else self._native_denom
            return {
                "description": "Beezee native token",
                "denom_units": [
                    {"denom": self._native_denom, "exponent": 0},
                    {"denom": display, "exponent": 6},
                ],
                "base": self._native_denom,
                "display": display,
                "name": display.upper(),
                "symbol": display.upper(),
            }
        return {}

    async def get_all_markets(self, refresh: bool = False) -> Dict[str, BeezeeMarket]:
        if self._markets_by_id and not refresh:
            return self._markets_by_id
        raw_markets = await self._paged_request(CONSTANTS.TRADING_PAIRS_PATH_URL, "market")
        denoms: Set[str] = set()
        for market in raw_markets:
            denoms.update({market["base"], market["quote"]})
        for denom in denoms:
            if denom not in self._metadata_by_denom:
                self._metadata_by_denom[denom] = await self.get_denom_metadata(denom)
        self._symbol_map = unique_market_symbols([(market["base"], market["quote"]) for market in raw_markets], self._metadata_by_denom)
        self._markets_by_id = {
            f"{market['base']}/{market['quote']}": market_from_data(market, self._metadata_by_denom, self._symbol_map)
            for market in raw_markets
        }
        return self._markets_by_id

    async def get_symbol_map(self) -> bidict:
        if self._symbol_map is None:
            await self.get_all_markets()
        return self._symbol_map or bidict()

    async def get_market_by_trading_pair(self, trading_pair: str) -> BeezeeMarket:
        symbol_map = await self.get_symbol_map()
        market_id = symbol_map.inverse[trading_pair]
        return (await self.get_all_markets())[market_id]

    async def get_order_book(self, market_id: str) -> Dict[str, List[Dict[str, Any]]]:
        bids = await self._paged_request(CONSTANTS.MARKET_AGGREGATED_ORDERS_PATH_URL, "list", params={"market": market_id, "order_type": CONSTANTS.SIDE_BUY}, page_limit=CONSTANTS.DEFAULT_ORDER_BOOK_DEPTH)
        asks = await self._paged_request(CONSTANTS.MARKET_AGGREGATED_ORDERS_PATH_URL, "list", params={"market": market_id, "order_type": CONSTANTS.SIDE_SELL}, page_limit=CONSTANTS.DEFAULT_ORDER_BOOK_DEPTH)
        return {"bids": bids, "asks": asks}

    async def get_market_history(self, market_id: str, limit: int = CONSTANTS.DEFAULT_HISTORY_LIMIT) -> List[Dict[str, Any]]:
        return (await self._request(CONSTANTS.MARKET_HISTORY_PATH_URL, params={"market": market_id, "pagination.limit": limit})).get("list", [])

    async def get_last_traded_price(self, market_id: str) -> Optional[Decimal]:
        market = (await self.get_all_markets())[market_id]
        history = await self.get_market_history(market_id, limit=1)
        if history:
            price = chain_price_to_display(history[0]["price"], market)
            return price if price.is_finite() and price > Decimal("0") else None
        order_book = await self.get_order_book(market_id)
        if order_book["bids"] and order_book["asks"]:
            mid_price = (
                chain_price_to_display(order_book["bids"][0]["price"], market)
                + chain_price_to_display(order_book["asks"][0]["price"], market)
            ) / Decimal("2")
            return mid_price if mid_price.is_finite() and mid_price > Decimal("0") else None
        return None

    async def get_balances(self) -> List[Dict[str, Any]]:
        if self._account_address is None:
            return []
        return (await self._request(f"{CONSTANTS.BALANCES_PATH_URL}/{self._account_address}")).get("balances", [])

    async def get_user_dust(self) -> List[Dict[str, Any]]:
        if self._account_address is None:
            return []
        return (await self._request(CONSTANTS.ALL_USER_DUST_PATH_URL, params={"address": self._account_address})).get("list", [])

    async def get_account_info(self, refresh: bool = False) -> BeezeeAccountInfo:
        if self._account_address is None:
            raise ValueError("Beezee account address is required for trading.")
        if not refresh and self._cached_account_number is not None and self._next_account_sequence is not None:
            return BeezeeAccountInfo(account_number=self._cached_account_number, sequence=self._next_account_sequence)
        response = await self._request(f"{CONSTANTS.ACCOUNT_INFO_PATH_URL}/{self._account_address}")
        account = response.get("account", {})
        base_account = account.get("base_account") or account.get("value") or account
        account_info = BeezeeAccountInfo(account_number=int(base_account["account_number"]), sequence=int(base_account["sequence"]))
        self._cached_account_number = account_info.account_number
        self._next_account_sequence = account_info.sequence
        return account_info

    async def reserve_account_info_sequence(self, refresh: bool = False) -> BeezeeAccountInfo:
        async with self._account_info_lock:
            account_info = await self.get_account_info(refresh=refresh)
            reserved = BeezeeAccountInfo(account_number=account_info.account_number, sequence=account_info.sequence)
            self._cached_account_number = account_info.account_number
            self._next_account_sequence = account_info.sequence + 1
            return reserved

    async def reset_account_sequence(self):
        async with self._account_info_lock:
            self._cached_account_number = None
            self._next_account_sequence = None

    async def get_user_market_orders(self, market_id: Optional[str] = None) -> List[Dict[str, Any]]:
        if self._account_address is None:
            return []
        params = {"market": market_id} if market_id else None
        return await self._paged_request(f"/bze/tradebin/user_market_orders/{self._account_address}", "list", params=params)

    async def get_market_order(self, market_id: str, order_type: str, order_id: str) -> Optional[Dict[str, Any]]:
        try:
            return (await self._request(CONSTANTS.MARKET_ORDER_PATH_URL, params={"market": market_id, "order_type": order_type, "order_id": order_id})).get("order")
        except Exception:
            return None

    async def get_tx(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        try:
            return await self._request(f"{CONSTANTS.GET_TX_PATH_URL}/{tx_hash}")
        except Exception:
            return None

    async def broadcast_create_order(self, market: BeezeeMarket, order_type: str, amount: Decimal, price: Decimal, gas_limit: int, memo: str = "") -> Dict[str, Any]:
        if self._signer is None:
            raise ValueError("Beezee wallet mode is required for order creation.")
        account = await self.reserve_account_info_sequence()
        tx_bytes = build_signed_transaction(
            signer=self._signer,
            chain_id=self._chain_id,
            account_number=account.account_number,
            sequence=account.sequence,
            messages=[msg_create_order(self._signer.address, order_type, display_amount_to_chain(amount, market.base), display_price_to_chain(price, market), market.market_id)],
            fee_denom=self._native_denom,
            fee_amount=fee_amount_from_gas(gas_limit, self._gas_price),
            gas_limit=gas_limit,
            memo=memo,
        )
        return await self._request(CONSTANTS.BROADCAST_TX_PATH_URL, method=RESTMethod.POST, data=build_broadcast_request(tx_bytes))

    async def broadcast_cancel_order(self, market_id: str, order_type: str, order_id: str, gas_limit: int, memo: str = "") -> Dict[str, Any]:
        if self._signer is None:
            raise ValueError("Beezee wallet mode is required for cancels.")
        account = await self.reserve_account_info_sequence()
        tx_bytes = build_signed_transaction(
            signer=self._signer,
            chain_id=self._chain_id,
            account_number=account.account_number,
            sequence=account.sequence,
            messages=[msg_cancel_order(self._signer.address, market_id, order_id, order_type)],
            fee_denom=self._native_denom,
            fee_amount=fee_amount_from_gas(gas_limit, self._gas_price),
            gas_limit=gas_limit,
            memo=memo,
        )
        return await self._request(CONSTANTS.BROADCAST_TX_PATH_URL, method=RESTMethod.POST, data=build_broadcast_request(tx_bytes))

    def candidate_order_ids(self, refs: List[Dict[str, Any]], market_id: str, order_type: str) -> List[str]:
        return sorted([ref["id"] for ref in refs if ref.get("market_id") == market_id and ref.get("order_type") == order_type], reverse=True)

    def fingerprint_history_trade(self, trade: Dict[str, Any]) -> str:
        fingerprint = "|".join([str(trade.get("executed_at")), trade.get("market_id", ""), trade.get("order_type", ""), trade.get("amount", ""), trade.get("price", ""), trade.get("maker", ""), trade.get("taker", "")])
        return hashlib.sha1(fingerprint.encode()).hexdigest()
