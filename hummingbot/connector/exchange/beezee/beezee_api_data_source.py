import asyncio
import hashlib
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

from aiohttp import ContentTypeError
from bidict import bidict

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_signer import BeezeeSigner
from hummingbot.connector.exchange.beezee.beezee_tx import (
    build_broadcast_request,
    build_signed_transaction,
    build_simulate_request,
    build_simulation_transaction,
    fee_amount_from_gas,
    gas_limit_from_simulation,
    msg_cancel_order,
    msg_create_order,
    msg_fill_orders,
    msg_multi_swap,
)
from hummingbot.connector.exchange.beezee.beezee_utils import (
    BeezeeMarket,
    BeezeePair,
    BeezeeVenueError,
    chain_amount_to_display,
    chain_price_to_display,
    display_amount_to_chain,
    display_price_to_chain,
    market_from_data,
    parse_coin,
    token_from_metadata,
    unique_market_symbols,
)
from hummingbot.core.data_type.trade_fee import TokenAmount
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


@dataclass
class BeezeeAccountInfo:
    account_number: int
    sequence: int


class BeezeeTxRefusedError(IOError):
    """The chain refused a transaction, either in simulation or at broadcast (CheckTx)."""

    def __init__(self, stage: str, message: str, code: Optional[int] = None, codespace: Optional[str] = None):
        super().__init__(f"Beezee tx {stage} failed: {message}")
        self.stage = stage
        self.chain_message = message
        self.code = code
        self.codespace = codespace

    @property
    def is_sequence_mismatch(self) -> bool:
        return CONSTANTS.ACCOUNT_SEQUENCE_MISMATCH_MESSAGE in self.chain_message.lower()

    @property
    def is_denom_halted(self) -> bool:
        return is_denom_halted_error(self.code, self.codespace, self.chain_message)


def is_not_found_error(error: Exception) -> bool:
    """True for the node's explicit "not found" answer (HTTP 404), not for a failed request."""
    message = str(error).lower()
    return "http status is 404" in message or "not found" in message


def height_header(height: Optional[int]) -> Optional[Dict[str, str]]:
    """Header that runs a REST query against the state at a past height."""
    return None if height is None else {"x-cosmos-block-height": str(height)}


def is_denom_halted_error(code: Optional[int], codespace: Optional[str], message: Optional[str]) -> bool:
    if code == CONSTANTS.DENOM_HALTED_ERROR_CODE and codespace == CONSTANTS.TRADEBIN_CODESPACE:
        return True
    return CONSTANTS.DENOM_HALTED_ERROR_MESSAGE in (message or "").lower()


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
        gas_adjustment: Decimal = CONSTANTS.DEFAULT_GAS_ADJUSTMENT,
        rpc_endpoint: Optional[str] = None,
    ):
        self._api_factory = api_factory
        self._rest_endpoint = rest_endpoint.rstrip("/")
        self._rpc_endpoint = (rpc_endpoint or "").rstrip("/")
        self._chain_id = chain_id
        self._address_prefix = address_prefix
        self._native_denom = native_denom
        self._gas_price = gas_price
        self._gas_adjustment = gas_adjustment
        self._account_address = account_address or (signer.address if signer is not None else None)
        self._signer = signer
        self._metadata_by_denom: Dict[str, Dict[str, Any]] = {}
        self._markets_by_id: Dict[str, BeezeeMarket] = {}
        self._pairs_by_trading_pair: Dict[str, BeezeePair] = {}
        self._pairs_loaded = False
        self._symbol_map: Optional[bidict] = None
        self._known_limit_ids = {rate_limit.limit_id for rate_limit in CONSTANTS.RATE_LIMITS}
        self._warned_missing_metadata_denoms: Set[str] = set()
        self._account_info_lock = asyncio.Lock()
        # One tx at a time from sequence reservation to broadcast: the node only sees sequence N+1 as valid
        # once the tx with N is in its check state, so a concurrent simulation with N+1 would be refused.
        self._tx_lock = asyncio.Lock()
        self._cached_account_number: Optional[int] = None
        self._next_account_sequence: Optional[int] = None
        self._queue_message_scan_gas = CONSTANTS.DEFAULT_QUEUE_MESSAGE_SCAN_GAS
        # Gas fees of the txs accepted at broadcast, in base units of the native denom.
        self._gas_fees_paid = 0

    @classmethod
    def logger(cls):
        return logging.getLogger(__name__)

    @property
    def gas_fees_paid(self) -> Decimal:
        """Gas fees of every tx this data source broadcast and the node accepted, in display units of the native
        token. A tx accepted at broadcast pays its fee even when its messages fail in the block."""
        metadata = self._metadata_by_denom.get(self._native_denom) or self._fallback_metadata_for_denom(self._native_denom)
        return chain_amount_to_display(self._gas_fees_paid, token_from_metadata(self._native_denom, metadata))

    @property
    def account_address(self) -> Optional[str]:
        return self._account_address

    async def _request(self, path_url: str, params: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None, method: RESTMethod = RESTMethod.GET, limit_id: Optional[str] = None, return_err: bool = False, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
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
            return_err=return_err,
            headers=headers,
        )

    async def _rpc_request(self, path_url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if not self._rpc_endpoint:
            raise ValueError("Beezee RPC endpoint is required to read block results.")
        rest_assistant = await self._api_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=f"{self._rpc_endpoint}{path_url}",
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.RPC_LIMIT_ID,
        )
        if response.get("error"):
            raise IOError(f"Beezee RPC {path_url} failed: {response['error']}")
        return response.get("result") or {}

    async def get_latest_block_height(self) -> int:
        status = await self._rpc_request(CONSTANTS.RPC_STATUS_PATH_URL)
        return int(status["sync_info"]["latest_block_height"])

    async def get_block_results(self, height: int) -> Dict[str, Any]:
        return await self._rpc_request(CONSTANTS.BLOCK_RESULTS_PATH_URL, params={"height": height})

    async def is_queue_message_pending(self, market_id: str, message_id: str, height: int) -> Optional[bool]:
        """Whether the tradebin queue still held the message in the state committed at `height` (raw store read).

        A queued message is deleted in the EndBlock that processes it, so False at the height of its tx block, or of
        any block after it, means it was processed by then. None when the node cannot answer for that height.
        """
        key = f"{CONSTANTS.QUEUE_MESSAGE_KEY_PREFIX}{market_id}/{message_id}/".encode()
        result = await self._rpc_request(CONSTANTS.ABCI_QUERY_PATH_URL, params={
            "path": f'"{CONSTANTS.TRADEBIN_STORE_QUERY_PATH}"', "data": f"0x{key.hex()}", "height": height,
        })
        response = result.get("response") or {}
        if int(response.get("code") or 0) != 0:
            return None
        return bool(response.get("value"))

    async def _paged_request(self, path_url: str, item_key: str, params: Optional[Dict[str, Any]] = None, page_limit: int = CONSTANTS.DEFAULT_MARKET_PAGE_SIZE, headers: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        next_key: Optional[str] = None
        while True:
            page_params = dict(params or {})
            page_params["pagination.limit"] = page_limit
            if next_key:
                page_params["pagination.key"] = next_key
            response = await self._request(path_url=path_url, params=page_params, headers=headers)
            items.extend(response.get(item_key, []))
            next_key = (response.get("pagination") or {}).get("next_key")
            if not next_key:
                break
        return items

    async def get_tradebin_params(self) -> Dict[str, Any]:
        params = (await self._request(CONSTANTS.TRADEBIN_PARAMS_PATH_URL)).get("params", {})
        raw_scan_gas = next(
            (params[key] for key in ("orderBookQueueMessageScanExtraGas", "order_book_queue_message_scan_extra_gas") if key in params),
            None,
        )
        try:
            self._queue_message_scan_gas = int(raw_scan_gas) if raw_scan_gas is not None else CONSTANTS.DEFAULT_QUEUE_MESSAGE_SCAN_GAS
        except (TypeError, ValueError):
            self._queue_message_scan_gas = CONSTANTS.DEFAULT_QUEUE_MESSAGE_SCAN_GAS
        return params

    def queued_message_gas(self, market_id: str) -> int:
        """Extra gas a create on `market_id` pays for each message queued on that market before it in the same block.

        The scan gas comes from the tradebin params of the last `get_tradebin_params` call (the chain default before).
        """
        message_bytes = 2 * len(market_id) + CONSTANTS.QUEUE_MESSAGE_FIXED_BYTES
        return self._queue_message_scan_gas + CONSTANTS.KV_ITER_NEXT_COST_FLAT + CONSTANTS.KV_READ_COST_PER_BYTE * message_bytes

    async def get_trade_fees(self) -> Dict[str, Optional[TokenAmount]]:
        """Returns the tradebin maker and taker fees as flat token amounts in display units.

        Chain v8.2.0 returns camelCase Coin objects (`marketMakerFee`); older chains a snake_case "1000ubze" string.
        """
        params = await self.get_tradebin_params()
        fees: Dict[str, Optional[TokenAmount]] = {}
        for side, keys in (("maker", ("marketMakerFee", "market_maker_fee")), ("taker", ("marketTakerFee", "market_taker_fee"))):
            raw_fee = next((params[key] for key in keys if key in params), None)
            coin = parse_coin(raw_fee)
            if coin is None or coin[1] <= 0:
                fees[side] = None
                continue
            denom, amount = coin
            metadata = self._metadata_by_denom.get(denom) or await self.get_denom_metadata(denom)
            token = token_from_metadata(denom, metadata)
            fees[side] = TokenAmount(token.symbol, chain_amount_to_display(amount, token))
        return fees

    async def get_halted_denoms(self) -> Set[str]:
        return set(await self._paged_request(CONSTANTS.HALTED_DENOMS_PATH_URL, "denoms"))

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

    async def get_all_pairs(self, refresh: bool = False) -> Dict[str, BeezeePair]:
        """Every trading pair, by name, with its venues: the order-book markets and the liquidity pools, read apart.

        A market and a pool are unrelated on chain, so a pair is listed for each market and for each pool whose two
        denoms have no market. A market's pair keeps the market's orientation and gets the pool of the same two denoms
        when there is one; a pool-only pair takes the pool's orientation (denoms sorted). Names come from the denoms'
        metadata either way, markets first, so a market's pair name never changes because of a pool.
        """
        if self._pairs_loaded and not refresh:
            return self._pairs_by_trading_pair
        raw_markets = await self._paged_request(CONSTANTS.TRADING_PAIRS_PATH_URL, "market")
        raw_pools = await self.get_all_liquidity_pools()
        market_denoms = [(market["base"], market["quote"]) for market in raw_markets]
        pool_ids = {frozenset((pool["base"], pool["quote"])): pool["id"] for pool in raw_pools}
        listed = {frozenset(denoms) for denoms in market_denoms}
        pool_only: List[Tuple[str, str]] = []
        for pool in raw_pools:
            denoms = frozenset((pool["base"], pool["quote"]))
            if denoms not in listed:
                listed.add(denoms)
                pool_only.append((pool["base"], pool["quote"]))
        for denom in {denom for denoms in market_denoms + pool_only for denom in denoms}:
            if denom not in self._metadata_by_denom:
                self._metadata_by_denom[denom] = await self.get_denom_metadata(denom)
        self._symbol_map = unique_market_symbols(market_denoms + pool_only, self._metadata_by_denom)
        self._markets_by_id = {
            f"{market['base']}/{market['quote']}": market_from_data(market, self._metadata_by_denom, self._symbol_map)
            for market in raw_markets
        }
        pairs: Dict[str, BeezeePair] = {}
        for market in self._markets_by_id.values():
            pool_id = pool_ids.get(frozenset((market.base.denom, market.quote.denom)))
            pairs[market.trading_pair] = BeezeePair(base=market.base, quote=market.quote, market=market, pool_id=pool_id)
        for base, quote_denom in pool_only:
            # The tokens of a market that does not exist: only its base, quote and pair name are used.
            tokens = market_from_data({"base": base, "quote": quote_denom}, self._metadata_by_denom, self._symbol_map)
            pairs[tokens.trading_pair] = BeezeePair(
                base=tokens.base, quote=tokens.quote, pool_id=pool_ids[frozenset((base, quote_denom))]
            )
        self._pairs_by_trading_pair = pairs
        self._pairs_loaded = True
        return self._pairs_by_trading_pair

    async def get_all_markets(self, refresh: bool = False) -> Dict[str, BeezeeMarket]:
        """The order-book markets by market id (base/quote denoms). Pool-only pairs are not in it."""
        await self.get_all_pairs(refresh=refresh)
        return self._markets_by_id

    async def get_symbol_map(self) -> bidict:
        """Exchange symbol (base/quote denoms, in the pair's orientation) to pair name, for every pair."""
        if self._symbol_map is None:
            await self.get_all_pairs()
        return self._symbol_map or bidict()

    async def get_pair(self, trading_pair: str) -> BeezeePair:
        """The pair and its venues. KeyError for an unknown pair."""
        return (await self.get_all_pairs())[trading_pair]

    async def get_market_by_trading_pair(self, trading_pair: str) -> BeezeeMarket:
        """The pair's order-book market. KeyError for an unknown pair, BeezeeVenueError (a KeyError) for a pair with
        only a liquidity pool."""
        pair = await self.get_pair(trading_pair)
        if pair.market is None:
            raise BeezeeVenueError(
                f"Beezee pair {trading_pair} has no order-book market, only a liquidity pool. Order-book calls are "
                f"refused on it; use the pool calls (quote_swap, amm_swap)."
            )
        return pair.market

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

    async def get_all_liquidity_pools(self) -> List[Dict[str, Any]]:
        return await self._paged_request(CONSTANTS.ALL_LIQUIDITY_POOLS_PATH_URL, "list")

    async def get_liquidity_pool(self, pool_id: str) -> Optional[Dict[str, Any]]:
        """The pool, or None when the chain says there is none. Other errors raise."""
        try:
            response = await self._request(CONSTANTS.LIQUIDITY_POOL_PATH_URL, params={"pool_id": pool_id})
        except IOError as error:
            if is_not_found_error(error):
                return None
            raise
        return response.get("pool")

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

    async def get_user_market_orders(self, market_id: Optional[str] = None, height: Optional[int] = None) -> List[Dict[str, Any]]:
        if self._account_address is None:
            return []
        params = {"market": market_id} if market_id else None
        return await self._paged_request(
            f"/bze/tradebin/user_market_orders/{self._account_address}", "list", params=params, headers=height_header(height)
        )

    async def get_market_order(self, market_id: str, order_type: str, order_id: str, height: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """The order on the book, or None when the chain says it is not there. Other errors raise."""
        try:
            response = await self._request(
                CONSTANTS.MARKET_ORDER_PATH_URL,
                params={"market": market_id, "order_type": order_type, "order_id": order_id},
                headers=height_header(height),
            )
        except IOError as error:
            if is_not_found_error(error):
                return None
            raise
        return response.get("order")

    async def get_tx(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        """The tx, or None when the node does not know it (yet). Other errors raise."""
        try:
            return await self._request(f"{CONSTANTS.GET_TX_PATH_URL}/{tx_hash}")
        except IOError as error:
            if is_not_found_error(error):
                return None
            raise

    async def simulate_gas(self, messages: List[bytes], sequence: int, memo: str = "") -> int:
        if self._signer is None:
            raise ValueError("Beezee wallet mode is required to simulate transactions.")
        tx_bytes = build_simulation_transaction(self._signer.public_key_bytes, sequence, messages, memo=memo)
        try:
            response = await self._request(
                CONSTANTS.SIMULATE_TX_PATH_URL,
                method=RESTMethod.POST,
                data=build_simulate_request(tx_bytes),
                return_err=True,
            )
        except ContentTypeError as error:
            raise BeezeeTxRefusedError("simulation", f"unexpected non-JSON response: {error}") from error
        if not isinstance(response, dict):
            # e.g. a 502/504 HTML or text page from a proxy in front of the node
            raise BeezeeTxRefusedError("simulation", f"unexpected non-JSON response: {str(response)[:200]}")
        gas_used = (response.get("gas_info") or {}).get("gas_used")
        if gas_used is None:
            raise BeezeeTxRefusedError("simulation", str(response.get("message") or response))
        return int(gas_used)

    async def simulate_sign_and_broadcast(self, messages: List[bytes], memo: str = "", extra_gas: int = 0) -> Dict[str, Any]:
        """Simulates the messages for gas, then signs and broadcasts them in a single tx.

        gas limit = ceil(gas_used * gas_adjustment) + extra_gas, fee = ceil(gas limit * gas_price) in the native denom.
        `extra_gas` covers what the simulation cannot see, e.g. messages queued earlier in the same block.
        A refused simulation is never broadcast. An account sequence mismatch, in simulation or at broadcast,
        resets the cached sequence and retries once; any other refusal raises BeezeeTxRefusedError. Any failure
        before the broadcast resets the cached sequence. Calls are serialized from sequence reservation to
        broadcast, so concurrent orders get consecutive sequences.
        """
        if self._signer is None:
            raise ValueError("Beezee wallet mode is required to sign transactions.")
        async with self._tx_lock:
            for attempt in range(2):
                account = await self.reserve_account_info_sequence()
                try:
                    tx_bytes, fee_amount = await self._simulate_and_sign(messages, account, memo, extra_gas)
                except BeezeeTxRefusedError as error:
                    await self.reset_account_sequence()
                    if error.is_sequence_mismatch and attempt == 0:
                        continue
                    raise
                except Exception:
                    # Nothing was broadcast, so the reserved sequence is unused on chain.
                    await self.reset_account_sequence()
                    raise
                response = await self._request(CONSTANTS.BROADCAST_TX_PATH_URL, method=RESTMethod.POST, data=build_broadcast_request(tx_bytes))
                tx_response = response.get("tx_response", {})
                code = int(tx_response.get("code", 0))
                if code == 0:
                    self._gas_fees_paid += int(fee_amount)
                    return response
                # Refused at CheckTx: the reserved sequence was not used on chain.
                await self.reset_account_sequence()
                error = BeezeeTxRefusedError(
                    "broadcast",
                    tx_response.get("raw_log") or str(tx_response),
                    code=code,
                    codespace=tx_response.get("codespace"),
                )
                if error.is_sequence_mismatch and attempt == 0:
                    continue
                raise error
            raise RuntimeError("Beezee tx retry loop exited without a result.")

    async def _simulate_and_sign(
        self, messages: List[bytes], account: BeezeeAccountInfo, memo: str, extra_gas: int
    ) -> Tuple[bytes, str]:
        gas_used = await self.simulate_gas(messages, account.sequence, memo=memo)
        gas_limit = gas_limit_from_simulation(gas_used, self._gas_adjustment) + extra_gas
        fee_amount = fee_amount_from_gas(gas_limit, self._gas_price)
        return build_signed_transaction(
            signer=self._signer,
            chain_id=self._chain_id,
            account_number=account.account_number,
            sequence=account.sequence,
            messages=messages,
            fee_denom=self._native_denom,
            fee_amount=fee_amount,
            gas_limit=gas_limit,
            memo=memo,
        ), fee_amount

    def create_order_message(self, market: BeezeeMarket, order_type: str, amount: Decimal, price: Decimal) -> bytes:
        if self._signer is None:
            raise ValueError("Beezee wallet mode is required for order creation.")
        return msg_create_order(self._signer.address, order_type, display_amount_to_chain(amount, market.base), display_price_to_chain(price, market), market.market_id)

    def cancel_order_message(self, market_id: str, order_type: str, order_id: str) -> bytes:
        if self._signer is None:
            raise ValueError("Beezee wallet mode is required for cancels.")
        return msg_cancel_order(self._signer.address, market_id, order_id, order_type)

    def multi_swap_message(self, pool_id: str, input_denom: str, input_amount: int, output_denom: str, min_output_amount: int) -> bytes:
        """A MsgMultiSwap on the single pool `pool_id`. Amounts are in base units."""
        if self._signer is None:
            raise ValueError("Beezee wallet mode is required for swaps.")
        return msg_multi_swap(self._signer.address, [pool_id], input_denom, str(input_amount), output_denom, str(min_output_amount))

    def fill_orders_message(self, market: BeezeeMarket, order_type: str, orders: List[Tuple[str, int]]) -> bytes:
        """A MsgFillOrders taking resting `order_type` orders at exactly each (chain price, base amount) entry."""
        if self._signer is None:
            raise ValueError("Beezee wallet mode is required for fills.")
        return msg_fill_orders(
            self._signer.address, market.market_id, order_type, [(price, str(amount)) for price, amount in orders]
        )

    async def broadcast_create_order(self, market: BeezeeMarket, order_type: str, amount: Decimal, price: Decimal, memo: str = "") -> Dict[str, Any]:
        message = self.create_order_message(market, order_type, amount, price)
        return await self.simulate_sign_and_broadcast([message], memo=memo)

    async def broadcast_cancel_order(self, market_id: str, order_type: str, order_id: str, memo: str = "") -> Dict[str, Any]:
        message = self.cancel_order_message(market_id, order_type, order_id)
        return await self.simulate_sign_and_broadcast([message], memo=memo)

    def fingerprint_history_trade(self, trade: Dict[str, Any]) -> str:
        fingerprint = "|".join([str(trade.get("executed_at")), trade.get("market_id", ""), trade.get("order_type", ""), trade.get("amount", ""), trade.get("price", ""), trade.get("maker", ""), trade.get("taker", "")])
        return hashlib.sha1(fingerprint.encode()).hexdigest()
