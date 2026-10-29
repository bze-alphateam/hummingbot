import asyncio
import copy
import json
import logging
import re
import unittest
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

from aioresponses import CallbackResult, aioresponses
from yarl import URL

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_exchange import BeezeeExchange, QueuedOrderMessage
from hummingbot.connector.exchange.beezee.beezee_utils import (
    BeezeeConfigMap,
    BeezeeCustomNetworkMode,
    BeezeeMnemonicWalletAccountMode,
)
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import TokenAmount
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent

FIXTURES = Path(__file__).parent / "fixtures"
TEST_MNEMONIC = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"
ADDRESS = "bze19rl4cm2hmr8afy4kldpxz3fka4jguq0apypkdz"
OTHER_ADDRESS = "bze1k8py7ynzrrnmzqwk2mkz3a6znatmqshdnnsehy"
# The fixtures are real mainnet (chain v8.2.0) responses. Their order owners become the test wallet.
RECORDED_OWNERS = ("bze1z3mkcr2jz424w6m49frgjmy9uhlrx69phqwg3l", "bze10kw8lpqd9emyxn94gkm038t4jj90ark4x8ls0d")

REST_URL = "https://rest.test"
RPC_URL = "https://rpc.test"
USDC = "ibc/81DA528F4C5546208D1D50F84C2D0B388B45D37AA36FCA622A3CB2D8EF014838"  # noqa: mock
TKN = "ibc/ED07A3391A112B175915CD8FAF43A2DA8E4790EDE12566649D0C2F97716B8518"  # noqa: mock
BZE_USDC = f"ubze/{USDC}"
TKN_BZE = f"{TKN}/ubze"

RESTED_TX = "7BD40C4BB9A5C56C060AD9803916ACEE243699D2CA8606FA2D6935EE6EA34250"  # noqa: mock
FILLED_TX = "A4F94E2AB63E35F71E5E2FBCFA3D0C366110658F2327D7D6ACE47B0D78A6CCBE"  # noqa: mock
CANCEL_BATCH_TX = "667A971A4F776682092E4F0426BC10B844CE66356FBDA66862E378A8E6582466"  # noqa: mock
CREATE_BATCH_TX = "C9C1A37554F0D37A1411784E553A1A9F006A044124EF1FE8521BB8AF5ECEC5A9"  # noqa: mock
H = 25278996


def load_fixture(name: str) -> Dict[str, Any]:
    text = (FIXTURES / name).read_text()
    for owner in RECORDED_OWNERS:
        text = text.replace(owner, ADDRESS)
    return json.loads(text)


def typed_event(event_type: str, attributes: Dict[str, str], extra: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """A typed event shaped like the chain's: JSON-encoded values in key order, then the sdk's plain attributes."""
    raw = [{"key": key, "value": json.dumps(attributes[key]), "index": True} for key in sorted(attributes)]
    raw += [{"key": key, "value": value, "index": True} for key, value in (extra or {"mode": "EndBlock"}).items()]
    return {"type": f"bze.tradebin.{event_type}", "attributes": raw}


def tx_event(event_type: str, msg_index: int = 0, **attributes) -> Dict[str, Any]:
    return typed_event(event_type, attributes, {"msg_index": str(msg_index)})


def block_event(event_type: str, **attributes) -> Dict[str, Any]:
    return typed_event(event_type, attributes)


def block_results(height: int, finalize_events: List[Dict[str, Any]] = (), tx_events: Optional[List[Dict[str, Any]]] = None):
    txs = None if tx_events is None else [{"code": 0, "events": tx_events}]
    return {"jsonrpc": "2.0", "id": -1, "result": {
        "height": str(height), "txs_results": txs, "finalize_block_events": list(finalize_events),
    }}


def tx(tx_hash: str, height: int, events: List[Dict[str, Any]], code: int = 0) -> Dict[str, Any]:
    response = load_fixture("tx_create_rested_25278996.json")
    tx_response = response["tx_response"]
    tx_response.update({"txhash": tx_hash, "height": str(height), "code": code})
    tx_response["events"] = [event for event in tx_response["events"] if not event["type"].startswith("bze.")] + events
    return response


def create_message(order_type: str, amount: str, price: str, message_id: str, market_id: str = BZE_USDC):
    return tx_event(
        "OrderCreateMessageEvent", creator=ADDRESS, market_id=market_id, order_type=order_type, amount=amount,
        price=price, message_id=message_id,
    )


def recorded_events(fixture: str, event_type: str) -> List[Dict[str, Any]]:
    result = load_fixture(fixture)["result"]
    events = list(result["finalize_block_events"])
    for tx_result in result["txs_results"] or []:
        events += tx_result["events"]
    return [event for event in events if event["type"] == f"bze.tradebin.{event_type}"]


def without_events(fixture: str, event_type: str) -> Dict[str, Any]:
    response = load_fixture(fixture)
    result = response["result"]
    result["finalize_block_events"] = [
        event for event in result["finalize_block_events"] if event["type"] != f"bze.tradebin.{event_type}"
    ]
    return response


def at_height(response: Dict[str, Any], height: int) -> Dict[str, Any]:
    response = copy.deepcopy(response)
    response["result"]["height"] = str(height)
    return response


METADATA = {
    "ubze": {"symbol": "BZE", "display": "bze", "denom_units": [{"denom": "ubze", "exponent": 0}, {"denom": "bze", "exponent": 6}]},
    USDC: {"symbol": "USDC", "display": "usdc", "denom_units": [{"denom": USDC, "exponent": 0}, {"denom": "usdc", "exponent": 6}]},
    TKN: {"symbol": "TKN", "display": "tkn", "denom_units": [{"denom": TKN, "exponent": 0}, {"denom": "tkn", "exponent": 6}]},
}


class BeezeeChainTestCase(unittest.IsolatedAsyncioTestCase):
    """Harness: the connector against mocked REST and RPC, driven through buy/sell/cancel and the status update loop."""

    level = 0
    # Connector settings beyond the network and the wallet.
    config_overrides: Dict[str, Any] = {}
    # The pairs the connector is started with, i.e. the pairs the strategy trades.
    trading_pairs: List[str] = ["BZE-USDC", "TKN-BZE"]

    def handle(self, record):
        self.log_records.append(record)

    def _logged(self, level: int, fragment: str) -> bool:
        return any(record.levelno == level and fragment in record.getMessage() for record in self.log_records)

    async def asyncSetUp(self) -> None:
        self.log_records = []
        self.mock_api = aioresponses()
        self.mock_api.start()
        self.addCleanup(self.mock_api.stop)
        config = BeezeeConfigMap(
            connector="beezee",
            network=BeezeeCustomNetworkMode(rest_endpoint=REST_URL, rpc_endpoint=RPC_URL, chain_id="beezee-1"),
            account_type=BeezeeMnemonicWalletAccountMode(mnemonic=TEST_MNEMONIC),
            **self.config_overrides,
        )
        self.exchange = BeezeeExchange(connector_configuration=config, trading_pairs=self.trading_pairs)
        self.assertEqual(ADDRESS, self.exchange._account_address)
        self.exchange._set_current_timestamp(1640780000)

        async def no_wait(_delay: float):
            await asyncio.sleep(0)

        self.exchange._sleep = no_wait
        self.exchange.logger().setLevel(1)
        self.exchange.logger().addHandler(self)
        self.addCleanup(self.exchange.logger().removeHandler, self)
        self.events: Dict[MarketEvent, EventLogger] = {}
        for event in (
            MarketEvent.BuyOrderCreated, MarketEvent.SellOrderCreated, MarketEvent.OrderFilled,
            MarketEvent.OrderCancelled, MarketEvent.OrderFailure, MarketEvent.BuyOrderCompleted,
            MarketEvent.SellOrderCompleted,
        ):
            self.events[event] = EventLogger()
            self.exchange.add_listener(event, self.events[event])
        self._mock_chain_state()
        await self.exchange._update_trading_rules()
        self.broadcasts: List[str] = []

    async def asyncTearDown(self) -> None:
        # Nothing the connector started may outlive the test.
        await self.exchange.stop_network()
        for _ in range(5):
            await asyncio.sleep(0)
        leftover = [task for task in asyncio.all_tasks() if task is not asyncio.current_task() and not task.done()]
        for task in leftover:
            task.cancel()
        self.assertEqual([], [task.get_coro() for task in leftover])

    def _mock_chain_state(self):
        api = self.mock_api
        api.get(re.compile(rf"^{re.escape(REST_URL + CONSTANTS.TRADING_PAIRS_PATH_URL)}(\?.*)?$"), repeat=True, payload={
            "market": [{"base": "ubze", "quote": USDC}, {"base": TKN, "quote": "ubze"}], "pagination": {"next_key": None},
        })
        for denom, metadata in METADATA.items():
            api.get(
                f"{REST_URL}{CONSTANTS.DENOM_METADATA_PATH_URL}/{denom.replace('/', '%2F')}",
                repeat=True, payload={"metadata": metadata},
            )
        api.get(f"{REST_URL}{CONSTANTS.TRADEBIN_PARAMS_PATH_URL}", repeat=True, payload={"params": {
            "marketMakerFee": {"denom": "ubze", "amount": "1000"},
            "marketTakerFee": {"denom": "ubze", "amount": "100000"},
        }})
        api.get(re.compile(rf"^{re.escape(REST_URL + CONSTANTS.HALTED_DENOMS_PATH_URL)}(\?.*)?$"), repeat=True, payload={
            "denoms": [], "pagination": {"next_key": None},
        })
        api.get(f"{REST_URL}{CONSTANTS.ACCOUNT_INFO_PATH_URL}/{ADDRESS}", repeat=True, payload={
            "account": {"account_number": "7", "sequence": "3"},
        })
        self._mock_simulate()
        api.get(f"{REST_URL}{CONSTANTS.BALANCES_PATH_URL}/{ADDRESS}", repeat=True, payload={"balances": []})
        api.get(re.compile(rf"^{re.escape(REST_URL + CONSTANTS.ALL_USER_DUST_PATH_URL)}(\?.*)?$"), repeat=True, payload={"list": []})

    def _mock_simulate(self):
        self.mock_api.post(f"{REST_URL}{CONSTANTS.SIMULATE_TX_PATH_URL}", repeat=True, payload={"gas_info": {"gas_used": "200000"}})

    def _mock_broadcast(self, tx_hash: str):
        self.mock_api.post(f"{REST_URL}{CONSTANTS.BROADCAST_TX_PATH_URL}", payload={"tx_response": {"code": 0, "txhash": tx_hash}})

    def _mock_tx(self, tx_hash: str, response: Dict[str, Any]):
        self.mock_api.get(f"{REST_URL}{CONSTANTS.GET_TX_PATH_URL}/{tx_hash}", payload=response)

    def _mock_latest_height(self, height: int, repeat: bool = False):
        self.mock_api.get(f"{RPC_URL}{CONSTANTS.RPC_STATUS_PATH_URL}", repeat=repeat, payload={
            "jsonrpc": "2.0", "id": -1, "result": {"sync_info": {"latest_block_height": str(height)}},
        })

    def _block_url(self, height: int) -> str:
        return f"{RPC_URL}{CONSTANTS.BLOCK_RESULTS_PATH_URL}?height={height}"

    def _mock_market_order(self, order_id: str, amount: Optional[str], price: str = "0.0003"):
        url = re.compile(rf"^{re.escape(REST_URL + CONSTANTS.MARKET_ORDER_PATH_URL)}\?.*order_id={order_id}.*$")
        if amount is None:
            self.mock_api.get(url, repeat=True, status=404, payload={"code": 5, "message": "not found", "details": []})
        else:
            self.mock_api.get(url, repeat=True, payload={"order": {"id": order_id, "amount": amount, "price": price}})

    def _market_order_heights(self) -> List[Optional[str]]:
        heights = []
        for (method, url), calls in self.mock_api.requests.items():
            if method == "GET" and url.path == CONSTANTS.MARKET_ORDER_PATH_URL:
                heights += [(call.kwargs.get("headers") or {}).get("x-cosmos-block-height") for call in calls]
        return heights

    async def _drain(self):
        for _ in range(50):
            await asyncio.sleep(0)

    def _mock_blocks(self, blocks: Dict[int, Dict[str, Any]], empty_up_to: Optional[int] = None):
        for height in range(min(blocks), (empty_up_to or max(blocks)) + 1):
            self.mock_api.get(
                f"{RPC_URL}{CONSTANTS.BLOCK_RESULTS_PATH_URL}?height={height}",
                payload=blocks.get(height) or block_results(height),
            )

    def _mock_user_orders(self, market_id: str, order_ids: List[str], repeat: bool = False):
        self.mock_api.get(
            re.compile(rf"^{re.escape(REST_URL)}/bze/tradebin/user_market_orders/{ADDRESS}(\?.*)?$"),
            repeat=repeat,
            payload={"list": [{"id": order_id, "market_id": market_id} for order_id in order_ids], "pagination": {"next_key": None}},
        )

    def _requested_heights(self) -> List[int]:
        heights = []
        for (method, url) in self.mock_api.requests:
            if method == "GET" and url.path == CONSTANTS.BLOCK_RESULTS_PATH_URL:
                heights.append(int(URL(url).query["height"]))
        return heights

    async def _run_until(self, condition, message: str):
        for _ in range(500):
            if condition():
                return
            await asyncio.sleep(0)
        self.fail(message)

    async def _status_update(self):
        await self.exchange._status_polling_loop_fetch_updates()

    def _order(self, client_order_id: str) -> Optional[InFlightOrder]:
        return self.exchange.in_flight_orders.get(client_order_id)

    def _exchange_id(self, client_order_id: str) -> Optional[str]:
        order = self._order(client_order_id)
        return order.exchange_order_id if order is not None else None

    def _state(self, client_order_id: str) -> Optional[OrderState]:
        order = self._order(client_order_id)
        return order.current_state if order is not None else None

    def _assert_fills(self, expected: List[tuple]):
        """(amount, price, flat fees) of every OrderFilled event, in order."""
        self.assertEqual(expected, [
            (fill.amount, fill.price, fill.trade_fee.flat_fees) for fill in self.events[MarketEvent.OrderFilled].event_log
        ])

    def _event_ids(self, event: MarketEvent) -> List[str]:
        return [logged.order_id for logged in self.events[event].event_log]

    def _restore(self, orders: List[InFlightOrder], block_state: Optional[Dict[str, Any]] = None):
        states = {order.client_order_id: order.to_json() for order in orders}
        if block_state is not None:
            states[CONSTANTS.TRACKING_STATE_KEY] = block_state
        self.exchange.restore_tracking_states(states)

    def _resting_buy(
        self, client_order_id="OID-REST", exchange_order_id="000000000000000006810887", amount: Decimal = Decimal("10")
    ) -> InFlightOrder:
        return InFlightOrder(
            client_order_id=client_order_id, trading_pair="BZE-USDC", order_type=OrderType.LIMIT, trade_type=TradeType.BUY,
            amount=amount, price=Decimal("0.0003"), creation_timestamp=1.0, exchange_order_id=exchange_order_id,
            initial_state=OrderState.OPEN,
        )


class BeezeeOrderTrackingTests(BeezeeChainTestCase):
    """Order lifecycle against mocked REST and RPC, driven through buy/sell/cancel and the status update loop."""

    # --- fixtures ---

    def test_typed_event_helper_matches_the_recorded_chain_shape(self):
        saved = recorded_events("block_results_25278996.json", "OrderSavedEvent")[0]
        create = recorded_events("block_results_25278996.json", "OrderCreateMessageEvent")[0]

        self.assertEqual(saved, block_event(
            "OrderSavedEvent", id="000000000000000006810895", market_id=BZE_USDC, order_type="sell",
            amount="1200000000", price="0.0005", owner=ADDRESS, message_id="000000000000000000000000",
        ))
        self.assertEqual(create, create_message("sell", "1200000000", "0.0005", "000000000000000000000000"))

    # --- create ---

    async def test_create_rests_in_the_same_block(self):
        self._mock_broadcast(RESTED_TX)
        self._mock_tx(RESTED_TX, load_fixture("tx_create_rested_25278996.json"))
        self._mock_latest_height(H)
        self._mock_blocks({H: load_fixture("block_results_25278996.json")})

        order_id = self.exchange.sell("BZE-USDC", Decimal("1200"), OrderType.LIMIT, Decimal("0.0005"))
        await self._run_until(
            lambda: self._exchange_id(order_id) == "000000000000000006810895",
            "the order did not get its chain order id",
        )
        await self._run_until(lambda: self._state(order_id) == OrderState.OPEN, "the order is not open")

        self.assertEqual([], self._event_ids(MarketEvent.OrderFilled))
        self.assertTrue(self._logged(logging.INFO, f"Beezee order {order_id} processed at height {H}"))

    async def test_create_rests_three_blocks_later(self):
        self._mock_broadcast(RESTED_TX)
        self._mock_tx(RESTED_TX, load_fixture("tx_create_rested_25278996.json"))
        self._mock_latest_height(H + 3)
        saved = recorded_events("block_results_25278996.json", "OrderSavedEvent")
        self._mock_blocks({
            H: without_events("block_results_25278996.json", "OrderSavedEvent"),
            H + 3: block_results(H + 3, saved),
        })

        order_id = self.exchange.sell("BZE-USDC", Decimal("1200"), OrderType.LIMIT, Decimal("0.0005"))
        await self._run_until(
            lambda: self._exchange_id(order_id) == "000000000000000006810895",
            "the order did not rest",
        )

        self.assertEqual([H, H + 1, H + 2, H + 3], self._requested_heights())
        self.assertTrue(self._logged(logging.INFO, f"processed at height {H + 3}"))
        await self._run_until(lambda: self._state(order_id) == OrderState.OPEN, "the order is not open")
        self.assertEqual(Decimal("0"), self._order(order_id).executed_amount_base)
        self.assertEqual([], self._event_ids(MarketEvent.OrderFilled))

    async def test_partial_fill_then_rests(self):
        tx_hash = "11" * 32
        message_id = "000000000000000000000004"
        self._mock_broadcast(tx_hash)
        self._mock_tx(tx_hash, tx(tx_hash, H, [create_message("buy", "20000000", "0.0003", message_id)]))
        self._mock_latest_height(H)
        self._mock_blocks({H: block_results(H, [
            block_event("OrderExecutedEvent", id="000000000000000006810001", market_id=BZE_USDC, order_type="sell",
                        amount="5000000", price="0.0003", maker=OTHER_ADDRESS, taker=ADDRESS, message_id=message_id),
            block_event("OrderSavedEvent", id="000000000000000006810002", market_id=BZE_USDC, order_type="buy",
                        amount="15000000", price="0.0003", owner=ADDRESS, message_id=message_id),
        ])})

        order_id = self.exchange.buy("BZE-USDC", Decimal("20"), OrderType.LIMIT, Decimal("0.0003"))
        await self._run_until(
            lambda: self._state(order_id) == OrderState.PARTIALLY_FILLED, "the order is not partially filled"
        )

        order = self._order(order_id)
        self.assertEqual("000000000000000006810002", order.exchange_order_id)
        self.assertEqual(Decimal("5"), order.executed_amount_base)
        fill = self.events[MarketEvent.OrderFilled].event_log[0]
        self.assertEqual((Decimal("5"), Decimal("0.0003")), (fill.amount, fill.price))
        self.assertEqual([TokenAmount("BZE", Decimal("0.1"))], fill.trade_fee.flat_fees)

    async def test_fully_filled_with_no_saved_event(self):
        self._mock_broadcast(FILLED_TX)
        self._mock_tx(FILLED_TX, load_fixture("tx_create_filled_25278999.json"))
        self._mock_latest_height(H + 3)
        self._mock_blocks({H + 3: load_fixture("block_results_25278999.json")})

        order_id = self.exchange.sell("BZE-USDC", Decimal("10"), OrderType.LIMIT, Decimal("0.0003"))
        await self._run_until(lambda: order_id in self._event_ids(MarketEvent.SellOrderCompleted), "the order did not fill")

        fill = self.events[MarketEvent.OrderFilled].event_log[0]
        self.assertEqual((Decimal("10"), Decimal("0.0003")), (fill.amount, fill.price))
        self.assertEqual(f"{H + 3}-1-taker", fill.exchange_trade_id)

    async def test_no_outcome_within_twenty_blocks_fails_the_order(self):
        self._mock_broadcast(RESTED_TX)
        self._mock_tx(RESTED_TX, load_fixture("tx_create_rested_25278996.json"))
        self._mock_latest_height(H + 19)
        self._mock_blocks({H: without_events("block_results_25278996.json", "OrderSavedEvent")}, empty_up_to=H + 19)

        order_id = self.exchange.sell("BZE-USDC", Decimal("1200"), OrderType.LIMIT, Decimal("0.0005"))
        await self._run_until(lambda: order_id in self._event_ids(MarketEvent.OrderFailure), "the order did not fail")

        self.assertEqual(list(range(H, H + 20)), self._requested_heights())
        self.assertTrue(self._logged(
            logging.WARNING,
            f"no outcome within 20 blocks of its tx. tx_hash={RESTED_TX}, message_id=000000000000000000000000",
        ))

    async def test_two_orders_in_one_tx_are_mapped_by_event_order(self):
        first = InFlightOrder(
            client_order_id="OID-A", trading_pair="TKN-BZE", order_type=OrderType.LIMIT, trade_type=TradeType.BUY,
            amount=Decimal("0.38125"), price=Decimal("133"), creation_timestamp=1.0, exchange_order_id=CREATE_BATCH_TX,
            initial_state=OrderState.OPEN,
        )
        second = InFlightOrder(
            client_order_id="OID-B", trading_pair="TKN-BZE", order_type=OrderType.LIMIT, trade_type=TradeType.BUY,
            amount=Decimal("0.947706"), price=Decimal("130"), creation_timestamp=1.0, exchange_order_id=CREATE_BATCH_TX,
            initial_state=OrderState.OPEN,
        )
        self._restore([first, second], {"last_processed_height": H + 3, "creates": {
            "OID-A": {"client_order_id": "OID-A", "tx_hash": CREATE_BATCH_TX, "msg_index": 0},
            "OID-B": {"client_order_id": "OID-B", "tx_hash": CREATE_BATCH_TX, "msg_index": 1},
        }, "cancels": {}})
        self._mock_tx(CREATE_BATCH_TX, load_fixture("tx_create_batch_25279000.json"))
        self._mock_tx(CREATE_BATCH_TX, load_fixture("tx_create_batch_25279000.json"))
        self._mock_latest_height(H + 3)
        self._mock_latest_height(H + 4)
        self._mock_latest_height(H + 4)
        self._mock_blocks({H + 4: load_fixture("block_results_25279000.json")})

        await self._status_update()
        await self._run_until(
            lambda: self._exchange_id("OID-A") == "000000000000000006810896"
            and self._exchange_id("OID-B") == "000000000000000006810897",
            "the batch orders were not mapped by event order",
        )

    async def test_reused_message_id_does_not_match_an_earlier_order(self):
        # Order A is processed in its tx block without any event (v8.2.0 emits none for a refund). The queue
        # empties, so order B, three blocks later, gets the same message id. B's tx lookup is still pending when
        # its block is read: B's save must not be taken by A.
        later_tx = "22" * 32
        message_id = "000000000000000000000000"
        self._mock_broadcast(RESTED_TX)
        self._mock_tx(RESTED_TX, load_fixture("tx_create_rested_25278996.json"))
        self._mock_latest_height(H + 3)
        b_create = create_message("sell", "1200000000", "0.0005", message_id)
        self._mock_blocks({
            H: without_events("block_results_25278996.json", "OrderSavedEvent"),
            H + 3: block_results(
                H + 3,
                [block_event("OrderSavedEvent", id="000000000000000006810990", market_id=BZE_USDC, order_type="sell",
                             amount="1200000000", price="0.0005", owner=ADDRESS, message_id=message_id)],
                tx_events=[b_create],
            ),
        })

        order_a = self.exchange.sell("BZE-USDC", Decimal("1200"), OrderType.LIMIT, Decimal("0.0005"))
        await self._run_until(lambda: order_a in self._event_ids(MarketEvent.OrderFailure), "order A did not fail")
        self._mock_broadcast(later_tx)
        self._mock_tx(later_tx, tx(later_tx, H + 3, [b_create]))
        self._mock_latest_height(H + 3)
        order_b = self.exchange.sell("BZE-USDC", Decimal("1200"), OrderType.LIMIT, Decimal("0.0005"))
        await self._run_until(
            lambda: self._exchange_id(order_b) == "000000000000000006810990",
            "order B did not rest",
        )

        self.assertTrue(self._logged(logging.WARNING, f"its message id was reused at height {H + 3}. tx_hash={RESTED_TX}"))

    # --- resting orders ---

    async def test_later_maker_fill_is_attributed_by_order_id(self):
        self._restore([self._resting_buy()], {"last_processed_height": H + 2, "creates": {}, "cancels": {}})
        self._mock_latest_height(H + 2)
        self._mock_user_orders(BZE_USDC, ["000000000000000006810887"])
        await self._status_update()
        self._mock_latest_height(H + 3)
        self._mock_blocks({H + 3: load_fixture("block_results_25278999.json")})

        await self._status_update()
        await self._run_until(lambda: "OID-REST" in self._event_ids(MarketEvent.BuyOrderCompleted), "no maker fill")

        fill = self.events[MarketEvent.OrderFilled].event_log[0]
        self.assertEqual((Decimal("10"), Decimal("0.0003")), (fill.amount, fill.price))
        self.assertEqual([TokenAmount("BZE", Decimal("0.001"))], fill.trade_fee.flat_fees)
        self.assertEqual(f"{H + 3}-1-maker", fill.exchange_trade_id)

    async def test_missed_blocks_are_caught_up_in_order(self):
        self._restore([self._resting_buy()], {"last_processed_height": H - 1, "creates": {}, "cancels": {}})
        self._mock_latest_height(H + 3)
        self._mock_blocks({H: load_fixture("block_results_25278996.json"), H + 3: load_fixture("block_results_25278999.json")})
        self._mock_user_orders(BZE_USDC, [])

        await self._status_update()
        await self._run_until(lambda: "OID-REST" in self._event_ids(MarketEvent.BuyOrderCompleted), "no maker fill")

        self.assertEqual([H, H + 1, H + 2, H + 3], self._requested_heights())
        self._assert_fills([(Decimal("10"), Decimal("0.0003"), [TokenAmount("BZE", Decimal("0.001"))])])

    # --- cancel ---

    async def test_cancel_is_confirmed_by_its_event(self):
        resting_sell = InFlightOrder(
            client_order_id="OID-CXL", trading_pair="TKN-BZE", order_type=OrderType.LIMIT, trade_type=TradeType.SELL,
            amount=Decimal("0.803126"), price=Decimal("229"), creation_timestamp=1.0,
            exchange_order_id="000000000000000006810894", initial_state=OrderState.OPEN,
        )
        self._restore([resting_sell], {"last_processed_height": H + 3, "creates": {}, "cancels": {}})
        self._mock_latest_height(H + 3)
        self._mock_user_orders(TKN_BZE, ["000000000000000006810894"])
        await self._status_update()
        self._mock_broadcast(CANCEL_BATCH_TX)
        self._mock_tx(CANCEL_BATCH_TX, load_fixture("tx_cancel_batch_25279000.json"))
        self._mock_latest_height(H + 4)
        self._mock_blocks({H + 4: load_fixture("block_results_25279000.json")})

        self.exchange.cancel("TKN-BZE", "OID-CXL")
        await self._run_until(lambda: "OID-CXL" in self._event_ids(MarketEvent.OrderCancelled), "the cancel was not confirmed")

        self.assertEqual([], self._event_ids(MarketEvent.OrderFilled))

    async def test_cancel_racing_a_fill_ends_filled(self):
        self._restore([self._resting_buy()], {"last_processed_height": H + 2, "creates": {}, "cancels": {}})
        self._mock_latest_height(H + 2)
        self._mock_user_orders(BZE_USDC, ["000000000000000006810887"])
        await self._status_update()
        cancel_tx = "33" * 32
        self._mock_broadcast(cancel_tx)
        self._mock_tx(cancel_tx, tx(cancel_tx, H + 3, [tx_event(
            "OrderCancelMessageEvent", creator=ADDRESS, marketId=BZE_USDC, orderId="000000000000000006810887",
            order_type="buy", message_id="000000000000000000000001",
        )]))
        self._mock_latest_height(H + 3)
        self._mock_blocks({H + 3: load_fixture("block_results_25278999.json")})

        self.exchange.cancel("BZE-USDC", "OID-REST")
        await self._run_until(lambda: "OID-REST" in self._event_ids(MarketEvent.BuyOrderCompleted), "the order did not fill")

        self.assertEqual([], self._event_ids(MarketEvent.OrderCancelled))
        self._assert_fills([(Decimal("10"), Decimal("0.0003"), [TokenAmount("BZE", Decimal("0.001"))])])

    # --- restart ---

    async def test_restart_resumes_a_create_from_its_tx(self):
        # A state saved before the block tracking existed: only the tx hash, matched by the order's values.
        pending = InFlightOrder(
            client_order_id="OID-RESUME", trading_pair="BZE-USDC", order_type=OrderType.LIMIT, trade_type=TradeType.SELL,
            amount=Decimal("1200"), price=Decimal("0.0005"), creation_timestamp=1.0, exchange_order_id=RESTED_TX,
            initial_state=OrderState.OPEN,
        )
        self._restore([pending, self._resting_buy()])
        self._mock_tx(RESTED_TX, load_fixture("tx_create_rested_25278996.json"))
        self._mock_latest_height(H)
        self._mock_latest_height(H)
        self._mock_blocks({H: load_fixture("block_results_25278996.json")})
        self._mock_user_orders(BZE_USDC, ["000000000000000006810887"])

        await self._status_update()
        await self._run_until(
            lambda: self._exchange_id("OID-RESUME") == "000000000000000006810895", "the create was not resumed"
        )

        self.assertEqual(OrderState.OPEN, self._order("OID-REST").current_state)
        saved_state = self.exchange.tracking_states[CONSTANTS.TRACKING_STATE_KEY]
        self.assertEqual(H, saved_state["last_processed_height"])
        self.assertEqual({}, saved_state["creates"])

    # --- refusals ---

    async def test_delivered_create_tx_with_code_4017_fails_and_halts_the_market(self):
        self._mock_broadcast(RESTED_TX)
        self._mock_tx(RESTED_TX, {"tx_response": {
            "txhash": RESTED_TX, "height": str(H), "code": 4017, "codespace": "tradebin",
            "raw_log": "market is halted: denom is halted", "events": [],
        }})

        first = self.exchange.sell("BZE-USDC", Decimal("1200"), OrderType.LIMIT, Decimal("0.0005"))
        await self._run_until(lambda: first in self._event_ids(MarketEvent.OrderFailure), "the order did not fail")
        second = self.exchange.sell("BZE-USDC", Decimal("1200"), OrderType.LIMIT, Decimal("0.0005"))
        await self._run_until(lambda: second in self._event_ids(MarketEvent.OrderFailure), "the second order was placed")

        self.assertEqual(1, len(self.mock_api.requests.get(("POST", URL(f"{REST_URL}{CONSTANTS.BROADCAST_TX_PATH_URL}")), [])))

    # --- review fixes ---

    async def test_tx_found_while_a_pass_reads_a_later_block_still_reads_the_tx_block(self):
        reading_next_block, release_next_block, tx_readable = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def tx_once_readable(url, **kwargs):
            await tx_readable.wait()
            return CallbackResult(payload=load_fixture("tx_create_rested_25278996.json"))

        async def slow_next_block(url, **kwargs):
            reading_next_block.set()
            await release_next_block.wait()
            return CallbackResult(payload=block_results(H + 1))

        self._mock_broadcast(RESTED_TX)
        self.mock_api.get(f"{REST_URL}{CONSTANTS.GET_TX_PATH_URL}/{RESTED_TX}", callback=tx_once_readable)
        self._mock_latest_height(H)
        self._mock_latest_height(H + 1)
        self._mock_latest_height(H + 1)
        self.mock_api.get(self._block_url(H + 1), callback=slow_next_block)
        self.mock_api.get(self._block_url(H), payload=load_fixture("block_results_25278996.json"))

        order_id = self.exchange.sell("BZE-USDC", Decimal("1200"), OrderType.LIMIT, Decimal("0.0005"))
        await self._run_until(lambda: self._exchange_id(order_id) == RESTED_TX, "the order was not placed")
        await self._status_update()
        second_pass = asyncio.ensure_future(self._status_update())
        await asyncio.wait_for(reading_next_block.wait(), timeout=1)
        tx_readable.set()
        await self._drain()
        release_next_block.set()
        await second_pass

        await self._run_until(
            lambda: self._exchange_id(order_id) == "000000000000000006810895", "the save in the tx block was skipped"
        )

    async def test_an_unreadable_block_is_skipped_and_tracking_continues(self):
        self._restore([self._resting_buy()], {"last_processed_height": H + 1, "creates": {}, "cancels": {}})
        self._mock_latest_height(H + 3, repeat=True)
        for _ in range(CONSTANTS.MAX_BLOCK_READ_ATTEMPTS):
            self.mock_api.get(self._block_url(H + 2), status=500, body="block results pruned")
        self.mock_api.get(self._block_url(H + 3), payload=load_fixture("block_results_25278999.json"))
        self._mock_market_order("000000000000000006810887", "10000000")
        self._mock_user_orders(BZE_USDC, [], repeat=True)

        for _ in range(CONSTANTS.MAX_BLOCK_READ_ATTEMPTS):
            await self._status_update()
        await self._run_until(lambda: "OID-REST" in self._event_ids(MarketEvent.BuyOrderCompleted), "tracking stalled")

        self.assertTrue(self._logged(logging.WARNING, f"Beezee block {H + 2} could not be read 3 times"))
        fill = self.events[MarketEvent.OrderFilled].event_log[0]
        self.assertEqual(f"{H + 3}-1-maker", fill.exchange_trade_id)
        self.assertEqual([str(H + 2)], self._market_order_heights())

    async def test_long_downtime_settles_from_the_book_without_reading_old_blocks(self):
        pending = InFlightOrder(
            client_order_id="OID-OLD", trading_pair="BZE-USDC", order_type=OrderType.LIMIT, trade_type=TradeType.SELL,
            amount=Decimal("1200"), price=Decimal("0.0005"), creation_timestamp=1.0, exchange_order_id=RESTED_TX,
            initial_state=OrderState.OPEN,
        )
        latest = H + 5000
        self._restore([pending, self._resting_buy()], {"last_processed_height": H, "cancels": {}, "creates": {"OID-OLD": {
            "client_order_id": "OID-OLD", "tx_hash": RESTED_TX, "msg_index": 0, "tx_height": H,
            "message_id": "000000000000000000000000", "next_height": H + 1,
        }}})
        self._mock_latest_height(latest)
        self._mock_market_order("000000000000000006810887", "4000000")
        self._mock_market_order("000000000000000006810895", "1200000000", price="0.0005")
        self._mock_user_orders(BZE_USDC, ["000000000000000006810887", "000000000000000006810895"], repeat=True)

        await self._status_update()
        await self._run_until(lambda: self._exchange_id("OID-OLD") == "000000000000000006810895", "the create was not settled")
        await self._drain()

        self.assertEqual([], self._requested_heights())
        self.assertEqual([f"reconcile-{latest}"], [fill.exchange_trade_id for fill in self.events[MarketEvent.OrderFilled].event_log])
        self.assertEqual(Decimal("6"), self._order("OID-REST").executed_amount_base)
        self.assertEqual(OrderState.PARTIALLY_FILLED, self._state("OID-REST"))

    LATEST = H + 5000

    async def _restart_with_create_partly_filled_while_down(self, other_creates: Optional[Dict[str, Any]] = None):
        """Killed right after the create tx; while down past the catch-up limit, another wallet took 500 of its 1200."""
        pending = InFlightOrder(
            client_order_id="OID-OLD", trading_pair="BZE-USDC", order_type=OrderType.LIMIT, trade_type=TradeType.SELL,
            amount=Decimal("1200"), price=Decimal("0.0005"), creation_timestamp=1.0, exchange_order_id=RESTED_TX,
            initial_state=OrderState.PENDING_CREATE,
        )
        orders, creates = [pending], {"OID-OLD": {
            "client_order_id": "OID-OLD", "tx_hash": RESTED_TX, "msg_index": 0, "tx_height": H,
            "message_id": "000000000000000000000000", "next_height": H + 1,
        }}
        for order, create in (other_creates or {}).values():
            orders.append(order)
            creates[order.client_order_id] = create
        self._restore(orders, {"last_processed_height": H, "cancels": {}, "creates": creates})
        self._mock_latest_height(self.LATEST, repeat=True)
        self._mock_market_order("000000000000000006810895", "700000000", price="0.0005")
        self._mock_user_orders(BZE_USDC, ["000000000000000006810895"], repeat=True)

        await self._status_update()
        await self._run_until(lambda: self._state("OID-OLD") == OrderState.PARTIALLY_FILLED, "not partly filled")
        await self._drain()

    async def test_create_partly_filled_during_long_downtime_takes_its_book_order_with_the_fill(self):
        await self._restart_with_create_partly_filled_while_down()

        self.assertEqual("000000000000000006810895", self._exchange_id("OID-OLD"))
        self.assertEqual([], self._event_ids(MarketEvent.OrderFailure))
        self.assertEqual([(f"reconcile-{self.LATEST}", Decimal("500"), Decimal("0.0005"))], [
            (fill.exchange_trade_id, fill.amount, fill.price) for fill in self.events[MarketEvent.OrderFilled].event_log
        ])

    async def test_fill_already_in_the_book_amount_is_not_counted_again_when_its_block_is_read(self):
        # A recent create makes the pass read the last blocks, which hold the 500 fill the book amount already shows.
        recent = InFlightOrder(
            client_order_id="OID-NEW", trading_pair="BZE-USDC", order_type=OrderType.LIMIT, trade_type=TradeType.BUY,
            amount=Decimal("10"), price=Decimal("0.0003"), creation_timestamp=1.0, exchange_order_id=FILLED_TX,
            initial_state=OrderState.PENDING_CREATE,
        )
        self._mock_blocks({self.LATEST - 1: block_results(self.LATEST - 1, [
            block_event("OrderExecutedEvent", id="000000000000000006810895", market_id=BZE_USDC, order_type="sell",
                        amount="500000000", price="0.0005", maker=ADDRESS, taker=OTHER_ADDRESS,
                        message_id="000000000000000000000003"),
        ])}, empty_up_to=self.LATEST)
        await self._restart_with_create_partly_filled_while_down({"OID-NEW": (recent, {
            "client_order_id": "OID-NEW", "tx_hash": FILLED_TX, "msg_index": 0, "tx_height": self.LATEST - 2,
            "message_id": "000000000000000000000001", "next_height": self.LATEST - 1,
        })})

        self.assertEqual([self.LATEST - 1, self.LATEST], self._requested_heights())
        self.assertEqual(Decimal("500"), self._order("OID-OLD").executed_amount_base)
        self.assertEqual([f"reconcile-{self.LATEST}"], [
            fill.exchange_trade_id for fill in self.events[MarketEvent.OrderFilled].event_log
        ])

    async def test_order_gone_after_long_downtime_is_canceled_without_a_fill(self):
        self._restore([self._resting_buy()], {"last_processed_height": H, "creates": {}, "cancels": {}})
        self._mock_latest_height(H + 5000)
        self._mock_market_order("000000000000000006810887", None)
        self._mock_user_orders(BZE_USDC, [], repeat=True)

        await self._status_update()
        await self._run_until(lambda: "OID-REST" in self._event_ids(MarketEvent.OrderCancelled), "the order was not closed")

        self.assertEqual([], self._event_ids(MarketEvent.OrderFilled))
        self.assertTrue(self._logged(logging.WARNING, "left the book while its fills were not read"))

    async def test_rest_errors_never_fail_a_create(self):
        attempts = len(CONSTANTS.TX_LOOKUP_DELAYS) + 1
        self._mock_broadcast(RESTED_TX)
        for _ in range(attempts):
            self.mock_api.get(f"{REST_URL}{CONSTANTS.GET_TX_PATH_URL}/{RESTED_TX}", status=503, body="upstream timed out")
        self._mock_tx(RESTED_TX, load_fixture("tx_create_rested_25278996.json"))
        self._mock_latest_height(H, repeat=True)
        self._mock_blocks({H: load_fixture("block_results_25278996.json")})

        order_id = self.exchange.sell("BZE-USDC", Decimal("1200"), OrderType.LIMIT, Decimal("0.0005"))
        await self._run_until(lambda: self._exchange_id(order_id) == RESTED_TX, "the order was not placed")
        for _ in range(attempts):
            await self._drain()
            await self._status_update()
        await self._run_until(
            lambda: self._exchange_id(order_id) == "000000000000000006810895", "the order did not resolve"
        )

        self.assertEqual([], self._event_ids(MarketEvent.OrderFailure))

    async def test_create_tx_not_found_takes_a_matching_order_from_the_book(self):
        self._mock_broadcast(RESTED_TX)
        self.mock_api.get(
            f"{REST_URL}{CONSTANTS.GET_TX_PATH_URL}/{RESTED_TX}", repeat=True, status=404,
            payload={"code": 5, "message": f"tx not found: {RESTED_TX}", "details": []},
        )
        self._mock_user_orders(BZE_USDC, ["000000000000000006810895"])
        self._mock_market_order("000000000000000006810895", "1200000000", price="0.0005")

        order_id = self.exchange.sell("BZE-USDC", Decimal("1200"), OrderType.LIMIT, Decimal("0.0005"))
        await self._run_until(
            lambda: self._exchange_id(order_id) == "000000000000000006810895", "the order on the book was not taken"
        )

        self.assertEqual([], self._event_ids(MarketEvent.OrderFailure))
        self.assertTrue(self._logged(logging.WARNING, "its tx was not found within 60 s, but a matching order"))

    async def test_trading_fee_is_booked_once_per_order(self):
        tx_hash = "44" * 32
        message_id = "000000000000000000000004"
        self._mock_broadcast(tx_hash)
        self._mock_tx(tx_hash, tx(tx_hash, H, [create_message("buy", "20000000", "0.0003", message_id)]))
        self._mock_latest_height(H)
        self._mock_blocks({H: block_results(H, [
            block_event("OrderExecutedEvent", id="000000000000000006810001", market_id=BZE_USDC, order_type="sell",
                        amount="5000000", price="0.0003", maker=OTHER_ADDRESS, taker=ADDRESS, message_id=message_id),
            block_event("OrderSavedEvent", id="000000000000000006810002", market_id=BZE_USDC, order_type="buy",
                        amount="15000000", price="0.0003", owner=ADDRESS, message_id=message_id),
        ])})

        order_id = self.exchange.buy("BZE-USDC", Decimal("20"), OrderType.LIMIT, Decimal("0.0003"))
        await self._run_until(lambda: self._state(order_id) == OrderState.PARTIALLY_FILLED, "the order did not rest")
        self._mock_latest_height(H + 1)
        self._mock_blocks({H + 1: block_results(H + 1, [
            block_event("OrderExecutedEvent", id="000000000000000006810002", market_id=BZE_USDC, order_type="buy",
                        amount="15000000", price="0.0003", maker=ADDRESS, taker=OTHER_ADDRESS,
                        message_id="000000000000000000000000"),
        ])})
        await self._status_update()
        await self._run_until(lambda: order_id in self._event_ids(MarketEvent.BuyOrderCompleted), "the order did not fill")

        fees = [fill.trade_fee.flat_fees for fill in self.events[MarketEvent.OrderFilled].event_log]
        self.assertEqual([[TokenAmount("BZE", Decimal("0.1"))], []], fees)

    # --- fees: tradebin charges the taker fee only for crossing an order that rested before the create tx ---

    async def test_crossing_an_order_resting_before_the_tx_pays_the_taker_fee(self):
        tx_hash = "45" * 32
        message_id = "000000000000000000000004"
        self._mock_broadcast(tx_hash)
        self._mock_tx(tx_hash, tx(tx_hash, H, [create_message("buy", "20000000", "0.0003", message_id)]))
        self._mock_latest_height(H)
        self._mock_blocks({H: block_results(H, [
            block_event("OrderExecutedEvent", id="000000000000000006810001", market_id=BZE_USDC, order_type="sell",
                        amount="20000000", price="0.0003", maker=OTHER_ADDRESS, taker=ADDRESS, message_id=message_id),
        ])})

        order_id = self.exchange.buy("BZE-USDC", Decimal("20"), OrderType.LIMIT, Decimal("0.0003"))
        await self._run_until(lambda: order_id in self._event_ids(MarketEvent.BuyOrderCompleted), "the order did not fill")

        self._assert_fills([(Decimal("20"), Decimal("0.0003"), [TokenAmount("BZE", Decimal("0.1"))])])

    async def test_crossing_an_order_saved_in_the_same_block_pays_the_maker_fee(self):
        tx_hash = "46" * 32
        message_id = "000000000000000000000005"
        self._mock_broadcast(tx_hash)
        self._mock_tx(tx_hash, tx(tx_hash, H, [create_message("buy", "20000000", "0.0003", message_id)]))
        self._mock_latest_height(H)
        self._mock_blocks({H: block_results(H, [
            block_event("OrderSavedEvent", id="000000000000000006810001", market_id=BZE_USDC, order_type="sell",
                        amount="20000000", price="0.0003", owner=OTHER_ADDRESS, message_id="000000000000000000000004"),
            block_event("OrderExecutedEvent", id="000000000000000006810001", market_id=BZE_USDC, order_type="sell",
                        amount="20000000", price="0.0003", maker=OTHER_ADDRESS, taker=ADDRESS, message_id=message_id),
        ])})

        order_id = self.exchange.buy("BZE-USDC", Decimal("20"), OrderType.LIMIT, Decimal("0.0003"))
        await self._run_until(lambda: order_id in self._event_ids(MarketEvent.BuyOrderCompleted), "the order did not fill")

        fill = self.events[MarketEvent.OrderFilled].event_log[0]
        self.assertEqual([TokenAmount("BZE", Decimal("0.001"))], fill.trade_fee.flat_fees)
        self.assertEqual(f"{H}-1-taker", fill.exchange_trade_id)

    async def test_crossing_an_order_saved_after_the_tx_block_pays_the_maker_fee(self):
        tx_hash = "47" * 32
        message_id = "000000000000000000000005"
        self._mock_broadcast(tx_hash)
        self._mock_tx(tx_hash, tx(tx_hash, H, [create_message("buy", "20000000", "0.0003", message_id)]))
        self._mock_latest_height(H + 2)
        self._mock_blocks({
            H + 1: block_results(H + 1, [
                block_event("OrderSavedEvent", id="000000000000000006810001", market_id=BZE_USDC, order_type="sell",
                            amount="20000000", price="0.0003", owner=OTHER_ADDRESS, message_id="000000000000000000000004"),
            ]),
            H + 2: block_results(H + 2, [
                block_event("OrderExecutedEvent", id="000000000000000006810001", market_id=BZE_USDC, order_type="sell",
                            amount="20000000", price="0.0003", maker=OTHER_ADDRESS, taker=ADDRESS, message_id=message_id),
            ]),
        }, empty_up_to=H + 2)
        self._mock_blocks({H: block_results(H)})

        order_id = self.exchange.buy("BZE-USDC", Decimal("20"), OrderType.LIMIT, Decimal("0.0003"))
        await self._run_until(lambda: order_id in self._event_ids(MarketEvent.BuyOrderCompleted), "the order did not fill")

        self.assertEqual([H, H + 1, H + 2], self._requested_heights())
        self._assert_fills([(Decimal("20"), Decimal("0.0003"), [TokenAmount("BZE", Decimal("0.001"))])])

    async def test_self_fill_in_one_tx_charges_both_legs_the_maker_fee(self):
        sell = InFlightOrder(
            client_order_id="OID-SELL", trading_pair="BZE-USDC", order_type=OrderType.LIMIT, trade_type=TradeType.SELL,
            amount=Decimal("20"), price=Decimal("0.0003"), creation_timestamp=1.0, exchange_order_id=CREATE_BATCH_TX,
            initial_state=OrderState.OPEN,
        )
        buy = InFlightOrder(
            client_order_id="OID-BUY", trading_pair="BZE-USDC", order_type=OrderType.LIMIT, trade_type=TradeType.BUY,
            amount=Decimal("20"), price=Decimal("0.0003"), creation_timestamp=1.0, exchange_order_id=CREATE_BATCH_TX,
            initial_state=OrderState.OPEN,
        )
        self._restore([sell, buy], {"last_processed_height": H - 1, "creates": {
            "OID-SELL": {"client_order_id": "OID-SELL", "tx_hash": CREATE_BATCH_TX, "msg_index": 0},
            "OID-BUY": {"client_order_id": "OID-BUY", "tx_hash": CREATE_BATCH_TX, "msg_index": 1},
        }, "cancels": {}})
        sell_id, buy_id = "000000000000000000000001", "000000000000000000000002"
        tx_events = [
            tx_event("OrderCreateMessageEvent", 0, creator=ADDRESS, market_id=BZE_USDC, order_type="sell",
                     amount="20000000", price="0.0003", message_id=sell_id),
            tx_event("OrderCreateMessageEvent", 1, creator=ADDRESS, market_id=BZE_USDC, order_type="buy",
                     amount="20000000", price="0.0003", message_id=buy_id),
        ]
        self._mock_tx(CREATE_BATCH_TX, tx(CREATE_BATCH_TX, H, tx_events))
        self._mock_tx(CREATE_BATCH_TX, tx(CREATE_BATCH_TX, H, tx_events))
        self._mock_latest_height(H - 1)
        self._mock_latest_height(H)
        self._mock_latest_height(H)
        self._mock_blocks({H: block_results(H, [
            block_event("OrderSavedEvent", id="000000000000000006810001", market_id=BZE_USDC, order_type="sell",
                        amount="20000000", price="0.0003", owner=ADDRESS, message_id=sell_id),
            block_event("OrderExecutedEvent", id="000000000000000006810001", market_id=BZE_USDC, order_type="sell",
                        amount="20000000", price="0.0003", maker=ADDRESS, taker=ADDRESS, message_id=buy_id),
        ], tx_events=tx_events)})

        await self._status_update()
        await self._run_until(
            lambda: "OID-SELL" in self._event_ids(MarketEvent.SellOrderCompleted)
            and "OID-BUY" in self._event_ids(MarketEvent.BuyOrderCompleted),
            "the self-fill did not complete both legs",
        )

        fees = {fill.order_id: fill.trade_fee.flat_fees for fill in self.events[MarketEvent.OrderFilled].event_log}
        self.assertEqual({
            "OID-SELL": [TokenAmount("BZE", Decimal("0.001"))],
            "OID-BUY": [TokenAmount("BZE", Decimal("0.001"))],
        }, fees)

    async def test_restart_resumes_a_create_from_its_saved_tx_height(self):
        pending = InFlightOrder(
            client_order_id="OID-SAVED", trading_pair="BZE-USDC", order_type=OrderType.LIMIT, trade_type=TradeType.SELL,
            amount=Decimal("1200"), price=Decimal("0.0005"), creation_timestamp=1.0, exchange_order_id=RESTED_TX,
            initial_state=OrderState.OPEN,
        )
        self._restore([pending], {"last_processed_height": H - 1, "cancels": {}, "creates": {"OID-SAVED": {
            "client_order_id": "OID-SAVED", "tx_hash": RESTED_TX, "msg_index": 0, "tx_height": H,
            "message_id": "000000000000000000000000", "next_height": H,
        }}})
        self._mock_latest_height(H)
        self._mock_blocks({H: load_fixture("block_results_25278996.json")})

        await self._status_update()
        await self._run_until(
            lambda: self._exchange_id("OID-SAVED") == "000000000000000006810895", "the create was not resumed"
        )

        self.assertNotIn(("GET", URL(f"{REST_URL}{CONSTANTS.GET_TX_PATH_URL}/{RESTED_TX}")), self.mock_api.requests)

    async def test_restart_closes_a_resting_order_that_left_the_book(self):
        self._restore([self._resting_buy()], {"last_processed_height": H, "creates": {}, "cancels": {}})
        self._mock_latest_height(H)
        self._mock_user_orders(BZE_USDC, [])

        await self._status_update()
        await self._run_until(lambda: "OID-REST" in self._event_ids(MarketEvent.OrderCancelled), "the order was not closed")

        self.assertEqual([], self._event_ids(MarketEvent.OrderFilled))

    # --- mutation-check tests ---

    def test_in_window_takes_a_message_only_at_its_next_height(self):
        message = QueuedOrderMessage(client_order_id="OID", tx_hash=RESTED_TX, tx_height=H, next_height=H + 2)

        self.assertFalse(message.in_window(H + 1))
        self.assertTrue(message.in_window(H + 2))
        self.assertFalse(message.in_window(H + 3))

    async def test_fill_of_another_makers_order_with_the_same_id_is_ignored(self):
        self._restore([self._resting_buy()], {"last_processed_height": H + 2, "creates": {}, "cancels": {}})
        self._mock_latest_height(H + 3)
        self._mock_user_orders(BZE_USDC, ["000000000000000006810887"], repeat=True)
        self._mock_blocks({H + 3: block_results(H + 3, [
            block_event("OrderExecutedEvent", id="000000000000000006810887", market_id=BZE_USDC, order_type="buy",
                        amount="10000000", price="0.0003", maker=OTHER_ADDRESS, taker=OTHER_ADDRESS,
                        message_id="000000000000000000000000"),
        ])})

        await self._status_update()
        await self._drain()

        self.assertEqual([H + 3], self._requested_heights())
        self.assertEqual([], self._event_ids(MarketEvent.OrderFilled))
        self.assertEqual(Decimal("0"), self._order("OID-REST").executed_amount_base)
        self.assertEqual(OrderState.OPEN, self._state("OID-REST"))

    async def test_reused_message_id_is_matched_to_the_latest_tx(self):
        def queued(client_order_id: str, tx_hash: str, tx_height: int) -> Dict[str, Any]:
            return {"client_order_id": client_order_id, "tx_hash": tx_hash, "msg_index": 0, "tx_height": tx_height,
                    "message_id": "000000000000000000000000", "next_height": H + 3}

        def sell(client_order_id: str, tx_hash: str) -> InFlightOrder:
            return InFlightOrder(
                client_order_id=client_order_id, trading_pair="BZE-USDC", order_type=OrderType.LIMIT,
                trade_type=TradeType.SELL, amount=Decimal("1200"), price=Decimal("0.0005"), creation_timestamp=1.0,
                exchange_order_id=tx_hash, initial_state=OrderState.OPEN,
            )

        later_tx = "55" * 32
        self._restore([sell("OID-A", RESTED_TX), sell("OID-B", later_tx)], {"last_processed_height": H + 2, "cancels": {}, "creates": {
            "OID-A": queued("OID-A", RESTED_TX, H), "OID-B": queued("OID-B", later_tx, H + 3),
        }})
        self._mock_latest_height(H + 3)
        self._mock_blocks({H + 3: block_results(H + 3, recorded_events("block_results_25278996.json", "OrderSavedEvent"))})

        await self._status_update()
        await self._run_until(lambda: self._exchange_id("OID-B") == "000000000000000006810895", "B did not get the order")
        await self._drain()

        self.assertEqual(RESTED_TX, self._exchange_id("OID-A"))
        self.assertEqual(OrderState.OPEN, self._state("OID-A"))
        queued_creates = self.exchange.tracking_states[CONSTANTS.TRACKING_STATE_KEY]["creates"]
        self.assertEqual(["OID-A"], list(queued_creates))
        self.assertEqual(H + 4, queued_creates["OID-A"]["next_height"])

    async def _skip_block_with_pruned_state(self, amount: Decimal, latest_order_amount: Optional[str]):
        """Block H+2 cannot be read and the state at H+2 is gone too, so the resting order is reconciled at H+3,
        the block holding its real 10 BZE maker fill."""
        self._restore([self._resting_buy(amount=amount)], {"last_processed_height": H + 1, "creates": {}, "cancels": {}})
        self._mock_latest_height(H + 3, repeat=True)
        for _ in range(CONSTANTS.MAX_BLOCK_READ_ATTEMPTS):
            self.mock_api.get(self._block_url(H + 2), status=500, body="block results pruned")
        self.mock_api.get(self._block_url(H + 3), payload=load_fixture("block_results_25278999.json"))
        url = re.compile(rf"^{re.escape(REST_URL + CONSTANTS.MARKET_ORDER_PATH_URL)}\?.*$")

        def market_order_at(url, headers=None, **kwargs):
            if (headers or {}).get("x-cosmos-block-height") == str(H + 2):
                return CallbackResult(status=500, body="state pruned")
            if latest_order_amount is None:
                return CallbackResult(status=404, payload={"code": 5, "message": "not found", "details": []})
            return CallbackResult(payload={"order": {"id": "000000000000000006810887", "amount": latest_order_amount, "price": "0.0003"}})

        self.mock_api.get(url, repeat=True, callback=market_order_at)
        self._mock_user_orders(BZE_USDC, ["000000000000000006810887"] if latest_order_amount else [], repeat=True)
        for _ in range(CONSTANTS.MAX_BLOCK_READ_ATTEMPTS):
            await self._status_update()
        await self._drain()

    async def test_fills_reconciled_at_the_latest_height_are_not_counted_again(self):
        await self._skip_block_with_pruned_state(Decimal("20"), latest_order_amount="10000000")

        self.assertEqual([str(H + 2), str(H + 3)], self._market_order_heights())
        self.assertEqual([f"reconcile-{H + 3}"], [fill.exchange_trade_id for fill in self.events[MarketEvent.OrderFilled].event_log])
        self.assertEqual(Decimal("10"), self._order("OID-REST").executed_amount_base)
        self.assertEqual(OrderState.PARTIALLY_FILLED, self._state("OID-REST"))

    async def test_order_gone_at_the_fallback_height_keeps_its_block_fills(self):
        await self._skip_block_with_pruned_state(Decimal("10"), latest_order_amount=None)
        await self._run_until(lambda: "OID-REST" in self._event_ids(MarketEvent.BuyOrderCompleted), "the block fill was dropped")

        self.assertEqual([f"{H + 3}-1-maker"], [fill.exchange_trade_id for fill in self.events[MarketEvent.OrderFilled].event_log])
        self.assertEqual([], self._event_ids(MarketEvent.OrderCancelled))

    async def test_cancel_tx_is_matched_by_its_order_id(self):
        batch = [  # (exchange id, amount, price) of the batch's cancelled orders, in cancel message order
            ("000000000000000006810894", "0.803126", "229"), ("000000000000000006810893", "0.819926", "226"),
            ("000000000000000006810892", "0.999034", "223"), ("000000000000000006810891", "0.215644", "220"),
        ]
        orders = [
            InFlightOrder(
                client_order_id=f"OID-{index}", trading_pair="TKN-BZE", order_type=OrderType.LIMIT, trade_type=TradeType.SELL,
                amount=Decimal(amount), price=Decimal(price), creation_timestamp=1.0, exchange_order_id=order_id,
                initial_state=OrderState.OPEN,
            )
            for index, (order_id, amount, price) in enumerate(batch)
        ]
        self._restore(orders, {"last_processed_height": H + 3, "creates": {}, "cancels": {}})
        self._mock_latest_height(H + 3)
        self._mock_user_orders(TKN_BZE, [order_id for order_id, _, _ in batch], repeat=True)
        await self._status_update()
        await self._run_until(lambda: all(self._state(order.client_order_id) == OrderState.OPEN for order in orders), "not open")
        self._mock_broadcast(CANCEL_BATCH_TX)
        self._mock_tx(CANCEL_BATCH_TX, load_fixture("tx_cancel_batch_25279000.json"))
        self._mock_latest_height(H + 3)

        self.exchange.cancel("TKN-BZE", "OID-3")
        await self._run_until(
            lambda: self.exchange.tracking_states[CONSTANTS.TRACKING_STATE_KEY]["cancels"].get("OID-3", {}).get("tx_height"),
            "the cancel tx was not looked up",
        )

        queued_cancel = self.exchange.tracking_states[CONSTANTS.TRACKING_STATE_KEY]["cancels"]["OID-3"]
        self.assertEqual(("000000000000000006810891", "000000000000000000000003"),
                         (queued_cancel["order_id"], queued_cancel["message_id"]))

        # The recorded block cancels the whole batch: each tracked order is settled by its own cancel event.
        self._mock_latest_height(H + 4)
        self._mock_blocks({H + 4: load_fixture("block_results_25279000.json")})
        await self._status_update()
        await self._run_until(lambda: "OID-3" in self._event_ids(MarketEvent.OrderCancelled), "the cancel was not confirmed")

        self.assertEqual(["OID-0", "OID-1", "OID-2", "OID-3"], sorted(self._event_ids(MarketEvent.OrderCancelled)))
        self.assertEqual({}, self.exchange.tracking_states[CONSTANTS.TRACKING_STATE_KEY]["cancels"])

    async def test_canceled_event_of_another_order_does_not_confirm_a_cancel(self):
        order_x = self._resting_buy("OID-X", "000000000000000006810887")
        order_y = self._resting_buy("OID-Y", "000000000000000006810888")
        self._restore([order_x, order_y], {"last_processed_height": H + 2, "creates": {}, "cancels": {}})
        self._mock_latest_height(H + 2)
        self._mock_user_orders(BZE_USDC, ["000000000000000006810887", "000000000000000006810888"], repeat=True)
        await self._status_update()
        cancel_tx, message_id = "66" * 32, "000000000000000000000002"
        self._mock_broadcast(cancel_tx)
        self._mock_tx(cancel_tx, tx(cancel_tx, H + 3, [tx_event(
            "OrderCancelMessageEvent", creator=ADDRESS, marketId=BZE_USDC, orderId="000000000000000006810887",
            order_type="buy", message_id=message_id,
        )]))
        self._mock_latest_height(H + 3)

        def canceled(order_id: str) -> Dict[str, Any]:
            return block_event("OrderCanceledEvent", id=order_id, market_id=BZE_USDC, order_type="buy", amount="10000000",
                               price="0.0003", owner=ADDRESS, message_id=message_id)

        self._mock_blocks({H + 3: block_results(H + 3, [canceled("000000000000000006810888")])})

        self.exchange.cancel("BZE-USDC", "OID-X")
        await self._run_until(lambda: self._requested_heights() == [H + 3], "block H+3 was not read")
        await self._drain()

        # Y's own cancel event settles Y (an order id is never reused), but it must not confirm X's queued cancel.
        self.assertEqual(["OID-Y"], self._event_ids(MarketEvent.OrderCancelled))
        self.assertEqual(OrderState.PENDING_CANCEL, self._state("OID-X"))

        self._mock_latest_height(H + 4)
        self._mock_blocks({H + 4: block_results(H + 4, [canceled("000000000000000006810887")])})
        await self._status_update()
        await self._run_until(lambda: "OID-X" in self._event_ids(MarketEvent.OrderCancelled), "X was not cancelled")

        self.assertEqual(["OID-Y", "OID-X"], self._event_ids(MarketEvent.OrderCancelled))

    # --- cancels without a queued cancel ---

    def _canceled_event(self, order_id: str = "000000000000000006810887", owner: str = ADDRESS,
                        message_id: str = "000000000000000000000009", market_id: str = BZE_USDC) -> Dict[str, Any]:
        return block_event("OrderCanceledEvent", id=order_id, market_id=market_id, order_type="buy", amount="10000000",
                           price="0.0003", owner=owner, message_id=message_id)

    def _broadcasts(self) -> int:
        return len(self.mock_api.requests.get(("POST", URL(f"{REST_URL}{CONSTANTS.BROADCAST_TX_PATH_URL}")), []))

    async def test_restored_create_cancelled_before_the_restart_ends_canceled(self):
        # The state saved at create time: the create still queued, its cancel (sent by the previous stop) never saved.
        pending = InFlightOrder(
            client_order_id="OID-GHOST", trading_pair="BZE-USDC", order_type=OrderType.LIMIT, trade_type=TradeType.SELL,
            amount=Decimal("1200"), price=Decimal("0.0005"), creation_timestamp=1.0, exchange_order_id=RESTED_TX,
            initial_state=OrderState.OPEN,
        )
        self._restore([pending], {"last_processed_height": H - 1, "cancels": {}, "creates": {"OID-GHOST": {
            "client_order_id": "OID-GHOST", "tx_hash": RESTED_TX, "msg_index": 0, "tx_height": H,
            "message_id": "000000000000000000000000", "next_height": H,
        }}})
        self._mock_latest_height(H + 2, repeat=True)
        self._mock_user_orders(BZE_USDC, [], repeat=True)
        self._mock_blocks({
            H: load_fixture("block_results_25278996.json"),
            H + 2: block_results(H + 2, [block_event(
                "OrderCanceledEvent", id="000000000000000006810895", market_id=BZE_USDC, order_type="sell",
                amount="1200000000", price="0.0005", owner=ADDRESS, message_id="000000000000000000000003",
            )]),
        })

        await self._status_update()
        await self._run_until(lambda: "OID-GHOST" in self._event_ids(MarketEvent.OrderCancelled), "the ghost stayed open")
        await self.exchange.cancel_all(timeout_seconds=1)

        self.assertEqual([H, H + 1, H + 2], self._requested_heights())
        self.assertNotIn("OID-GHOST", self.exchange.in_flight_orders)
        self.assertEqual([], self._event_ids(MarketEvent.OrderFailure))
        self.assertEqual(0, self._broadcasts())

    async def test_order_cancelled_outside_the_bot_ends_canceled(self):
        self._restore([self._resting_buy()], {"last_processed_height": H + 2, "creates": {}, "cancels": {}})
        self._mock_latest_height(H + 3)
        self._mock_user_orders(BZE_USDC, ["000000000000000006810887"], repeat=True)
        self._mock_blocks({H + 3: block_results(H + 3, [self._canceled_event()])})

        await self._status_update()
        await self._run_until(lambda: "OID-REST" in self._event_ids(MarketEvent.OrderCancelled), "the cancel was missed")

        self.assertEqual([H + 3], self._requested_heights())
        self.assertEqual([], self._event_ids(MarketEvent.OrderFilled))

    async def test_partly_filled_order_cancelled_outside_the_bot_keeps_its_fills(self):
        self._restore([self._resting_buy(amount=Decimal("20"))], {"last_processed_height": H + 2, "creates": {}, "cancels": {}})
        self._mock_latest_height(H + 4)
        self._mock_user_orders(BZE_USDC, ["000000000000000006810887"], repeat=True)
        self._mock_blocks({
            H + 3: load_fixture("block_results_25278999.json"),
            H + 4: block_results(H + 4, [self._canceled_event(order_id="000000000000000006810887")]),
        })

        await self._status_update()
        await self._run_until(lambda: "OID-REST" in self._event_ids(MarketEvent.OrderCancelled), "the cancel was missed")

        self.assertEqual([H + 3, H + 4], self._requested_heights())
        self._assert_fills([(Decimal("10"), Decimal("0.0003"), [TokenAmount("BZE", Decimal("0.001"))])])
        self.assertEqual([], self._event_ids(MarketEvent.BuyOrderCompleted))

    async def test_cancel_event_of_another_owner_is_ignored(self):
        self._restore([self._resting_buy()], {"last_processed_height": H + 2, "creates": {}, "cancels": {}})
        self._mock_latest_height(H + 3)
        self._mock_user_orders(BZE_USDC, ["000000000000000006810887"], repeat=True)
        self._mock_blocks({H + 3: block_results(H + 3, [self._canceled_event(owner=OTHER_ADDRESS)])})

        await self._status_update()
        await self._drain()

        self.assertEqual([H + 3], self._requested_heights())
        self.assertEqual([], self._event_ids(MarketEvent.OrderCancelled))
        self.assertEqual(OrderState.OPEN, self._state("OID-REST"))
