import asyncio
import copy
import json
import logging
import re
import unittest
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

from aioresponses import aioresponses
from yarl import URL

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_exchange import BeezeeExchange
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


class BeezeeOrderTrackingTests(unittest.IsolatedAsyncioTestCase):
    """Order lifecycle against mocked REST and RPC, driven through buy/sell/cancel and the status update loop."""

    level = 0

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
        )
        self.exchange = BeezeeExchange(connector_configuration=config, trading_pairs=["BZE-USDC", "TKN-BZE"])
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
        api.post(f"{REST_URL}{CONSTANTS.SIMULATE_TX_PATH_URL}", repeat=True, payload={"gas_info": {"gas_used": "200000"}})
        api.get(f"{REST_URL}{CONSTANTS.BALANCES_PATH_URL}/{ADDRESS}", repeat=True, payload={"balances": []})
        api.get(re.compile(rf"^{re.escape(REST_URL + CONSTANTS.ALL_USER_DUST_PATH_URL)}(\?.*)?$"), repeat=True, payload={"list": []})

    def _mock_broadcast(self, tx_hash: str):
        self.mock_api.post(f"{REST_URL}{CONSTANTS.BROADCAST_TX_PATH_URL}", payload={"tx_response": {"code": 0, "txhash": tx_hash}})

    def _mock_tx(self, tx_hash: str, response: Dict[str, Any]):
        self.mock_api.get(f"{REST_URL}{CONSTANTS.GET_TX_PATH_URL}/{tx_hash}", payload=response)

    def _mock_latest_height(self, height: int):
        self.mock_api.get(f"{RPC_URL}{CONSTANTS.RPC_STATUS_PATH_URL}", payload={
            "jsonrpc": "2.0", "id": -1, "result": {"sync_info": {"latest_block_height": str(height)}},
        })

    def _mock_blocks(self, blocks: Dict[int, Dict[str, Any]], empty_up_to: Optional[int] = None):
        for height in range(min(blocks), (empty_up_to or max(blocks)) + 1):
            self.mock_api.get(
                f"{RPC_URL}{CONSTANTS.BLOCK_RESULTS_PATH_URL}?height={height}",
                payload=blocks.get(height) or block_results(height),
            )

    def _mock_user_orders(self, market_id: str, order_ids: List[str]):
        self.mock_api.get(
            re.compile(rf"^{re.escape(REST_URL)}/bze/tradebin/user_market_orders/{ADDRESS}(\?.*)?$"),
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

    def _event_ids(self, event: MarketEvent) -> List[str]:
        return [logged.order_id for logged in self.events[event].event_log]

    def _restore(self, orders: List[InFlightOrder], block_state: Optional[Dict[str, Any]] = None):
        states = {order.client_order_id: order.to_json() for order in orders}
        if block_state is not None:
            states[CONSTANTS.TRACKING_STATE_KEY] = block_state
        self.exchange.restore_tracking_states(states)

    def _resting_buy(self, client_order_id="OID-REST", exchange_order_id="000000000000000006810887") -> InFlightOrder:
        return InFlightOrder(
            client_order_id=client_order_id, trading_pair="BZE-USDC", order_type=OrderType.LIMIT, trade_type=TradeType.BUY,
            amount=Decimal("10"), price=Decimal("0.0003"), creation_timestamp=1.0, exchange_order_id=exchange_order_id,
            initial_state=OrderState.OPEN,
        )

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
