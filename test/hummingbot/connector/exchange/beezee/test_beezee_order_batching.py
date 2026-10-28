import asyncio
import base64
import json
import re
from dataclasses import dataclass, field
from decimal import Decimal
from test.hummingbot.connector.exchange.beezee.test_beezee_order_tracking import (
    ADDRESS,
    BZE_USDC,
    REST_URL,
    RPC_URL,
    TKN_BZE,
    BeezeeChainTestCase,
    H,
    block_event,
    tx,
    tx_event,
)
from typing import Any, Callable, Dict, List, Optional, Tuple

from aioresponses import CallbackResult
from yarl import URL

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.event.events import MarketEvent

WINDOW_MS = 250
TKN_HALTED_REFUSAL = f"failed to execute message; message index: 2: market {TKN_BZE} is halted: denom is halted"


# --- protobuf decoding of the txs the connector sends ---

def _fields(data: bytes) -> List[Tuple[int, Any]]:
    fields, position = [], 0

    def varint() -> int:
        nonlocal position
        value, shift = 0, 0
        while True:
            byte = data[position]
            position += 1
            value |= (byte & 0x7F) << shift
            shift += 7
            if not byte & 0x80:
                return value

    while position < len(data):
        key = varint()
        if key & 7 == 0:
            fields.append((key >> 3, varint()))
        else:
            length = varint()
            fields.append((key >> 3, data[position:position + length]))
            position += length
    return fields


def _first(fields: List[Tuple[int, Any]], number: int, default: Any = None) -> Any:
    return next((value for field_number, value in fields if field_number == number), default)


@dataclass
class SentMessage:
    kind: str  # "create" or "cancel"
    market_id: str
    order_type: str
    amount: Optional[str] = None
    price: Optional[str] = None
    order_id: Optional[str] = None


@dataclass
class SentTx:
    messages: List[SentMessage]
    memo: str
    sequence: int

    @property
    def kinds(self) -> List[str]:
        return [message.kind for message in self.messages]


def decode_tx(request_kwargs: Dict[str, Any]) -> SentTx:
    tx_raw = _fields(base64.b64decode(json.loads(request_kwargs["data"])["tx_bytes"]))
    body, auth_info = _fields(_first(tx_raw, 1)), _fields(_first(tx_raw, 2))
    messages = []
    for number, any_bytes in body:
        if number != 1:
            continue
        any_fields = _fields(any_bytes)
        type_url, payload = _first(any_fields, 1).decode(), _fields(_first(any_fields, 2))
        text = {number: value.decode() for number, value in payload}
        if type_url == "/bze.tradebin.MsgCreateOrder":
            assert text[1] == ADDRESS
            messages.append(SentMessage("create", text[5], text[2], amount=text[3], price=text[4]))
        else:
            assert type_url == "/bze.tradebin.MsgCancelOrder" and text[1] == ADDRESS
            messages.append(SentMessage("cancel", text[2], text[4], order_id=text[3]))
    signer_info = _fields(_first(auth_info, 1))
    return SentTx(messages, _first(body, 2, b"").decode(), _first(signer_info, 3, 0))


@dataclass
class FakeNode:
    """A BZE node for the connector's tx path: simulation, CheckTx and the tradebin queue.

    Each broadcast tx lands in its own block. Its create messages queue with message ids 0.. in message order and rest
    in that block with ids from 6820000 on; its cancels cancel their orders in that block.
    """

    sequence: int = 3
    refuse_simulation: Callable[[SentTx], Optional[str]] = lambda sent: None
    # (code, raw_log) answers for the next broadcasts; afterwards every broadcast passes CheckTx.
    check_tx_answers: List[Tuple[int, str]] = field(default_factory=list)
    simulated: List[SentTx] = field(default_factory=list)
    broadcast: List[SentTx] = field(default_factory=list)
    landed: List[Tuple[str, SentTx]] = field(default_factory=list)
    blocks: Dict[int, Dict[str, Any]] = field(default_factory=dict)
    txs: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    resting: Dict[str, SentMessage] = field(default_factory=dict)
    next_order_id: int = 6820000

    @property
    def height(self) -> int:
        return H + len(self.landed)

    def saved_order_id(self, market_id: str, order_type: str, amount: str, price: Decimal) -> str:
        return next(
            order_id for order_id, message in self.resting.items()
            if (message.market_id, message.order_type, message.amount) == (market_id, order_type, amount)
            and Decimal(message.price) == price
        )

    def simulate(self, url, **kwargs) -> CallbackResult:
        sent = decode_tx(kwargs)
        self.simulated.append(sent)
        if sent.sequence != self.sequence:
            return CallbackResult(status=400, payload={
                "code": 32, "message": f"account sequence mismatch, expected {self.sequence}, got {sent.sequence}",
            })
        refusal = self.refuse_simulation(sent)
        if refusal is not None:
            return CallbackResult(status=400, payload={"code": 2, "message": refusal, "details": []})
        return CallbackResult(payload={"gas_info": {"gas_used": str(80000 * len(sent.messages))}})

    def check_tx(self, url, **kwargs) -> CallbackResult:
        sent = decode_tx(kwargs)
        self.broadcast.append(sent)
        if self.check_tx_answers:
            code, raw_log = self.check_tx_answers.pop(0)
            if "account sequence mismatch" in raw_log:
                self.sequence += 1  # another client's tx took the sequence
            return CallbackResult(payload={"tx_response": {"code": code, "raw_log": raw_log, "txhash": ""}})
        self.sequence += 1
        tx_hash = f"{len(self.landed) + 1:02X}" * 32
        self.landed.append((tx_hash, sent))
        self._execute(tx_hash, sent)
        return CallbackResult(payload={"tx_response": {"code": 0, "txhash": tx_hash}})

    def _execute(self, tx_hash: str, sent: SentTx):
        height = self.height
        tx_events, block_events = [], []
        for msg_index, message in enumerate(sent.messages):
            message_id = f"{msg_index:024d}"
            if message.kind == "create":
                tx_events.append(tx_event(
                    "OrderCreateMessageEvent", msg_index, creator=ADDRESS, market_id=message.market_id,
                    order_type=message.order_type, amount=message.amount, price=message.price, message_id=message_id,
                ))
                order_id = f"{self.next_order_id:024d}"
                self.next_order_id += 1
                self.resting[order_id] = message
                block_events.append(block_event(
                    "OrderSavedEvent", id=order_id, market_id=message.market_id, order_type=message.order_type,
                    amount=message.amount, price=message.price, owner=ADDRESS, message_id=message_id,
                ))
            else:
                tx_events.append(tx_event(
                    "OrderCancelMessageEvent", msg_index, creator=ADDRESS, marketId=message.market_id,
                    orderId=message.order_id, order_type=message.order_type, message_id=message_id,
                ))
                if self.resting.pop(message.order_id, None) is not None:
                    block_events.append(block_event(
                        "OrderCanceledEvent", id=message.order_id, market_id=message.market_id,
                        order_type=message.order_type, amount="1", price="1", owner=ADDRESS, message_id=message_id,
                    ))
        self.txs[tx_hash] = tx(tx_hash, height, tx_events)
        self.blocks[height] = {"jsonrpc": "2.0", "id": -1, "result": {
            "height": str(height), "txs_results": [{"code": 0, "events": tx_events}],
            "finalize_block_events": block_events,
        }}

    def block_results(self, url, **kwargs) -> CallbackResult:
        height = int(URL(url).query["height"])
        return CallbackResult(payload=self.blocks.get(height) or {"jsonrpc": "2.0", "id": -1, "result": {
            "height": str(height), "txs_results": None, "finalize_block_events": [],
        }})

    def get_tx(self, url, **kwargs) -> CallbackResult:
        response = self.txs.get(URL(url).path.rsplit("/", 1)[-1])
        if response is None:
            return CallbackResult(status=404, payload={"code": 5, "message": "tx not found"})
        return CallbackResult(payload=response)

    def status(self, url, **kwargs) -> CallbackResult:
        return CallbackResult(payload={
            "jsonrpc": "2.0", "id": -1, "result": {"sync_info": {"latest_block_height": str(self.height)}},
        })

    def account(self, url, **kwargs) -> CallbackResult:
        return CallbackResult(payload={"account": {"account_number": "7", "sequence": str(self.sequence)}})

    def user_market_orders(self, url, **kwargs) -> CallbackResult:
        return CallbackResult(payload={"list": [
            {"id": order_id, "market_id": message.market_id} for order_id, message in self.resting.items()
        ], "pagination": {"next_key": None}})


class BeezeeBatchingTestCase(BeezeeChainTestCase):
    """Harness: the connector against a FakeNode, with a batch window that passes only when the test lets it."""

    config_overrides = {"batch_window_ms": WINDOW_MS}

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.windows: List[asyncio.Future] = []

        async def sleep(delay: float):
            # The batch window passes only when the test lets it; the tx lookup delays pass at once.
            if delay == WINDOW_MS / 1000:
                window = asyncio.get_event_loop().create_future()
                self.windows.append(window)
                await window
            else:
                await asyncio.sleep(0)

        self.exchange._sleep = sleep

    def _mock_simulate(self):
        self.node = FakeNode()
        api = self.mock_api
        api.post(f"{REST_URL}{CONSTANTS.SIMULATE_TX_PATH_URL}", repeat=True, callback=self.node.simulate)
        api.post(f"{REST_URL}{CONSTANTS.BROADCAST_TX_PATH_URL}", repeat=True, callback=self.node.check_tx)
        api.get(re.compile(rf"^{re.escape(REST_URL + CONSTANTS.GET_TX_PATH_URL)}/.*$"), repeat=True, callback=self.node.get_tx)
        api.get(f"{RPC_URL}{CONSTANTS.RPC_STATUS_PATH_URL}", repeat=True, callback=self.node.status)
        api.get(re.compile(rf"^{re.escape(RPC_URL + CONSTANTS.BLOCK_RESULTS_PATH_URL)}\?.*$"), repeat=True, callback=self.node.block_results)
        api.get(
            re.compile(rf"^{re.escape(REST_URL)}/bze/tradebin/user_market_orders/{ADDRESS}(\?.*)?$"),
            repeat=True, callback=self.node.user_market_orders,
        )

    def _mock_chain_state(self):
        # The node answers the account query (registered first, so it wins over the harness's fixed sequence).
        self.mock_api.get(
            f"{REST_URL}{CONSTANTS.ACCOUNT_INFO_PATH_URL}/{ADDRESS}", repeat=True,
            callback=lambda url, **kwargs: self.node.account(url, **kwargs),
        )
        super()._mock_chain_state()

    # --- helpers ---

    def _limit_order(self, trading_pair: str, is_buy: bool, amount: str, price: str) -> LimitOrder:
        base, quote = trading_pair.split("-")
        return LimitOrder("", trading_pair, is_buy, base, quote, Decimal(price), Decimal(amount))

    def _ladder(self, count: int) -> List[LimitOrder]:
        """`count` BZE-USDC orders, bids and asks alternating, each with its own amount and price."""
        return [
            self._limit_order("BZE-USDC", index % 2 == 0, str(10 + index), str(Decimal("0.0003") + Decimal(index) / 100000))
            for index in range(count)
        ]

    def _requests(self, method: str, path: str) -> int:
        return sum(len(calls) for (request_method, url), calls in self.mock_api.requests.items()
                   if request_method == method and url.path.startswith(path))

    async def _settle(self):
        for _ in range(300):
            await asyncio.sleep(0)

    def _assert_rests_as_sent(self, client_order_id: str):
        """The order rests with the id the chain gave the message that carried its own side, amount and price."""
        order = self._order(client_order_id)
        market_id = BZE_USDC if order.trading_pair == "BZE-USDC" else TKN_BZE
        expected = self.node.saved_order_id(
            market_id, "buy" if order.trade_type is TradeType.BUY else "sell", str(int(order.amount * 1000000)), order.price,
        )
        self.assertEqual((expected, OrderState.OPEN), (order.exchange_order_id, order.current_state), client_order_id)

    def _resting(self, client_order_id: str, trading_pair: str, order_id: str, amount: str, price: str) -> InFlightOrder:
        market_id = BZE_USDC if trading_pair == "BZE-USDC" else TKN_BZE
        self.node.resting[order_id] = SentMessage("create", market_id, "sell", amount=amount, price=price)
        return InFlightOrder(
            client_order_id=client_order_id, trading_pair=trading_pair, order_type=OrderType.LIMIT,
            trade_type=TradeType.SELL, amount=Decimal(amount) / 1000000, price=Decimal(price), creation_timestamp=1.0,
            exchange_order_id=order_id, initial_state=OrderState.OPEN,
        )

    async def _restore_resting(self, orders: List[InFlightOrder]):
        self._restore(orders, {"last_processed_height": H, "creates": {}, "cancels": {}})
        await self._status_update()
        await self._run_until(lambda: all(self._state(order.client_order_id) == OrderState.OPEN for order in orders), "not open")

    def _failure_messages(self) -> Dict[str, str]:
        return {event.order_id: event.error_message for event in self.events[MarketEvent.OrderFailure].event_log}


class BeezeeOrderBatchingTests(BeezeeBatchingTestCase):
    """Batched create/cancel txs, driven through buy/sell/cancel, batch_order_create and batch_order_cancel."""

    # --- one tx per batch ---

    async def test_twenty_creates_go_out_in_one_tx_and_each_rests_with_its_own_order_id(self):
        created = self.exchange.batch_order_create(self._ladder(20))
        await self._settle()

        self.assertEqual(20, len({order.client_order_id for order in created}))
        self.assertTrue(all(order.client_order_id for order in created))
        self.assertEqual(1, len(self.node.landed))
        sent = self.node.landed[0][1]
        self.assertEqual(["create"] * 20, sent.kinds)
        self.assertEqual(CONSTANTS.BATCH_MEMO, sent.memo)
        self.assertEqual(
            [(order.is_buy, str(int(order.quantity * 1000000)), order.price) for order in created],
            [(message.order_type == "buy", message.amount, Decimal(message.price)) for message in sent.messages],
        )
        await self._run_until(
            lambda: all(self._state(order.client_order_id) == OrderState.OPEN
                        and len(self._exchange_id(order.client_order_id)) == 24 for order in created),
            "the orders did not rest",
        )
        for order in created:
            self._assert_rests_as_sent(order.client_order_id)
        self.assertEqual(20, len({self._exchange_id(order.client_order_id) for order in created}))
        # One simulation, one broadcast and one tx lookup for the whole batch; no window was waited for.
        self.assertEqual((1, 1), (len(self.node.simulated), len(self.node.broadcast)))
        self.assertEqual(1, self._requests("GET", CONSTANTS.GET_TX_PATH_URL))

    async def test_cancels_and_creates_share_one_tx_with_the_cancels_first(self):
        stale = [
            self._resting("OID-OLD-1", "BZE-USDC", "000000000000000006810001", "10000000", "0.0004"),
            self._resting("OID-OLD-2", "BZE-USDC", "000000000000000006810002", "11000000", "0.0005"),
        ]
        await self._restore_resting(stale)

        created = self.exchange.batch_order_create(self._ladder(2))
        self.exchange.batch_order_cancel([
            LimitOrder(order.client_order_id, "BZE-USDC", False, "BZE", "USDC", order.price, order.amount) for order in stale
        ])
        await self._settle()

        self.assertEqual(1, len(self.node.landed))
        sent = self.node.landed[0][1]
        self.assertEqual(["cancel", "cancel", "create", "create"], sent.kinds)
        self.assertEqual(["000000000000000006810001", "000000000000000006810002"], [m.order_id for m in sent.messages[:2]])
        await self._run_until(
            lambda: {"OID-OLD-1", "OID-OLD-2"} <= set(self._event_ids(MarketEvent.OrderCancelled)), "not cancelled"
        )
        await self._run_until(
            lambda: all(len(self._exchange_id(order.client_order_id) or "") == 24 for order in created), "not rested"
        )
        for order in created:
            self._assert_rests_as_sent(order.client_order_id)

    # --- coalescing of single calls ---

    async def test_single_calls_within_the_window_share_a_tx_and_later_ones_get_the_next(self):
        resting = [self._resting("OID-OLD", "BZE-USDC", "000000000000000006810009", "12000000", "0.0006")]
        await self._restore_resting(resting)

        bid = self.exchange.buy("BZE-USDC", Decimal("10"), OrderType.LIMIT, Decimal("0.0003"))
        self.exchange.cancel("BZE-USDC", "OID-OLD")
        ask = self.exchange.sell("BZE-USDC", Decimal("11"), OrderType.LIMIT, Decimal("0.0007"))
        await self._settle()

        self.assertEqual([], self.node.broadcast, "sent before the window passed")
        self.assertEqual(1, len(self.windows))
        self.windows[0].set_result(None)
        await self._settle()

        self.assertEqual([["cancel", "create", "create"]], [sent.kinds for _, sent in self.node.landed])
        await self._run_until(lambda: "OID-OLD" in self._event_ids(MarketEvent.OrderCancelled), "not cancelled")
        await self._run_until(lambda: len(self._exchange_id(ask) or "") == 24, "not rested")
        for client_order_id in (bid, ask):
            self._assert_rests_as_sent(client_order_id)

        late = self.exchange.buy("BZE-USDC", Decimal("12"), OrderType.LIMIT, Decimal("0.0002"))
        await self._settle()
        self.assertEqual(1, len(self.node.landed), "a call after the window went into the earlier tx")
        self.assertEqual(2, len(self.windows))
        self.windows[1].set_result(None)
        await self._settle()

        self.assertEqual(2, len(self.node.landed))
        self.assertEqual(["create"], self.node.landed[1][1].kinds)
        # A lone message keeps its own memo.
        self.assertEqual(f"hbot:{late}", self.node.landed[1][1].memo)
        await self._run_until(lambda: len(self._exchange_id(late) or "") == 24, "not rested")
        self._assert_rests_as_sent(late)

    async def test_a_second_cancel_of_the_same_order_in_the_window_sends_one_message(self):
        await self._restore_resting([self._resting("OID-OLD", "BZE-USDC", "000000000000000006810011", "12000000", "0.0006")])

        self.exchange.cancel("BZE-USDC", "OID-OLD")
        # cancel_all waits for its own cancel of the order, which shares the queued message.
        cancel_all = asyncio.ensure_future(self.exchange.cancel_all(timeout_seconds=1))
        await self._settle()
        self.windows[0].set_result(None)
        await self._settle()

        self.assertEqual([("OID-OLD", True)], [(result.order_id, result.success) for result in await cancel_all])
        self.assertEqual([["cancel"]], [sent.kinds for _, sent in self.node.landed])
        await self._run_until(lambda: "OID-OLD" in self._event_ids(MarketEvent.OrderCancelled), "not cancelled")
        self.assertEqual(["OID-OLD"], self._event_ids(MarketEvent.OrderCancelled))

    async def test_orders_an_explicit_batch_never_submits_do_not_hold_back_its_tx(self):
        resting = self._resting("OID-OLD", "BZE-USDC", "000000000000000006810010", "12000000", "0.0006")
        unsaved = InFlightOrder(
            client_order_id="OID-UNSAVED", trading_pair="BZE-USDC", order_type=OrderType.LIMIT, trade_type=TradeType.BUY,
            amount=Decimal("5"), price=Decimal("0.0002"), creation_timestamp=1.0, exchange_order_id="AA" * 32,
            initial_state=OrderState.OPEN,
        )
        self._restore([unsaved], {"last_processed_height": H, "creates": {
            "OID-UNSAVED": {"client_order_id": "OID-UNSAVED", "tx_hash": "AA" * 32, "msg_index": 0},
        }, "cancels": {}})
        await self._restore_resting([resting])

        # The cancel of an order the chain has not saved yet is deferred, and a dust order is refused before signing.
        self.exchange.batch_order_cancel([
            LimitOrder("OID-OLD", "BZE-USDC", False, "BZE", "USDC", Decimal("0.0006"), Decimal("12")),
            LimitOrder("OID-UNSAVED", "BZE-USDC", True, "BZE", "USDC", Decimal("0.0002"), Decimal("5")),
        ])
        created = self.exchange.batch_order_create([
            self._limit_order("BZE-USDC", True, "10", "0.0003"), self._limit_order("BZE-USDC", True, "0.0000001", "0.0003"),
        ])
        await self._settle()

        self.assertEqual([["cancel", "create"]], [sent.kinds for _, sent in self.node.landed])
        self.assertEqual("000000000000000006810010", self.node.landed[0][1].messages[0].order_id)
        self.assertEqual(OrderState.FAILED, self.exchange._order_tracker.all_orders[created[1].client_order_id].current_state)

    # --- failures ---

    async def test_batch_refused_for_a_halted_market_is_resent_without_that_markets_orders(self):
        # Cancels stay possible on a halted market; only its creates make the chain refuse the tx.
        resting = self._resting("OID-TKN", "TKN-BZE", "000000000000000006810020", "803126", "229")
        await self._restore_resting([resting])
        self.node.refuse_simulation = lambda sent: TKN_HALTED_REFUSAL if any(
            message.market_id == TKN_BZE and message.kind == "create" for message in sent.messages
        ) else None
        self.exchange.batch_order_cancel([LimitOrder("OID-TKN", "TKN-BZE", False, "TKN", "BZE", Decimal("229"), Decimal("0.803126"))])
        created = self.exchange.batch_order_create([
            self._limit_order("BZE-USDC", True, "10", "0.0003"),
            self._limit_order("BZE-USDC", False, "11", "0.0007"),
            self._limit_order("TKN-BZE", True, "0.38125", "133"),
            self._limit_order("TKN-BZE", False, "0.947706", "230"),
        ])
        await self._settle()

        # The batch, then the batch without the halted market's orders; nothing was sent alone.
        self.assertEqual([5, 3], [len(sent.messages) for sent in self.node.simulated])
        self.assertEqual(
            [[("cancel", TKN_BZE), ("create", BZE_USDC), ("create", BZE_USDC)]],
            [[(m.kind, m.market_id) for m in sent.messages] for _, sent in self.node.landed],
        )
        await self._run_until(lambda: "OID-TKN" in self._event_ids(MarketEvent.OrderCancelled), "not cancelled")
        failures = self._failure_messages()
        self.assertEqual({created[2].client_order_id, created[3].client_order_id}, set(failures))
        self.assertTrue(all("is halted: denom is halted" in message for message in failures.values()))
        await self._run_until(
            lambda: all(len(self._exchange_id(order.client_order_id) or "") == 24 for order in created[:2]), "not rested"
        )
        for order in created[:2]:
            self._assert_rests_as_sent(order.client_order_id)
        # The market stays halted: a new order on it is refused before signing.
        refused = self.exchange.sell("TKN-BZE", Decimal("0.5"), OrderType.LIMIT, Decimal("231"))
        await self._settle()
        self.assertIn("is halted", self._failure_messages()[refused])
        self.assertEqual(2, len(self.node.simulated))

    async def test_batch_refused_for_another_reason_sends_each_order_alone_and_fails_only_the_bad_one(self):
        refusal = "failed to execute message; message index: 1: amount should be bigger than: 50000000: invalid order amount"
        self.node.refuse_simulation = (
            lambda sent: refusal if any(message.amount == "11000000" for message in sent.messages) else None
        )
        created = self.exchange.batch_order_create(self._ladder(3))
        await self._settle()

        self.assertEqual([3, 1, 1, 1], [len(sent.messages) for sent in self.node.simulated])
        self.assertEqual(
            [f"hbot:{created[0].client_order_id}", f"hbot:{created[2].client_order_id}"],
            [sent.memo for _, sent in self.node.landed],
        )
        self.assertEqual({created[1].client_order_id: f"Beezee order create failed: {refusal}"}, self._failure_messages())
        await self._run_until(
            lambda: all(len(self._exchange_id(created[index].client_order_id) or "") == 24 for index in (0, 2)), "not rested"
        )
        for index in (0, 2):
            self._assert_rests_as_sent(created[index].client_order_id)

    async def test_sequence_mismatch_at_broadcast_retries_the_whole_batch_once(self):
        self.node.check_tx_answers = [(32, "account sequence mismatch, expected 4, got 3: incorrect account sequence")]
        created = self.exchange.batch_order_create(self._ladder(4))
        await self._settle()

        self.assertEqual([4, 4], [len(sent.messages) for sent in self.node.broadcast])
        # The retry was signed again with the sequence read back from the node.
        self.assertEqual([3, 4], [sent.sequence for sent in self.node.broadcast])
        self.assertEqual(1, len(self.node.landed))
        self.assertEqual({}, self._failure_messages())
        await self._run_until(
            lambda: all(len(self._exchange_id(order.client_order_id) or "") == 24 for order in created), "not rested"
        )
        for order in created:
            self._assert_rests_as_sent(order.client_order_id)

    async def test_a_second_sequence_mismatch_fails_the_batch_without_sending_orders_alone(self):
        mismatch = "account sequence mismatch, expected 4, got 3: incorrect account sequence"
        self.node.check_tx_answers = [(32, mismatch), (32, mismatch)]
        created = self.exchange.batch_order_create(self._ladder(3))
        await self._settle()

        self.assertEqual([3, 3], [len(sent.messages) for sent in self.node.broadcast])
        self.assertEqual([], self.node.landed)
        self.assertEqual(
            {order.client_order_id: f"Beezee order create failed: {mismatch}" for order in created}, self._failure_messages()
        )

    async def test_stopping_with_a_batch_waiting_for_its_window_fails_its_orders_and_sends_nothing(self):
        order_id = self.exchange.buy("BZE-USDC", Decimal("10"), OrderType.LIMIT, Decimal("0.0003"))
        await self._settle()
        self.assertEqual(1, len(self.windows))

        await self.exchange.stop_network()
        await self._settle()

        self.assertEqual([], self.node.broadcast)
        self.assertIn(order_id, self._failure_messages())


class BeezeeMaxMessagesPerTxTests(BeezeeBatchingTestCase):
    config_overrides = {"batch_window_ms": WINDOW_MS, "max_messages_per_tx": 3}

    async def test_max_messages_per_tx_splits_a_batch_into_txs_in_order(self):
        created = self.exchange.batch_order_create(self._ladder(7))
        await self._settle()

        self.assertEqual([3, 3, 1], [len(sent.messages) for _, sent in self.node.landed])
        # Consecutive sequences, one per tx, and the orders in their submission order.
        self.assertEqual([3, 4, 5], [sent.sequence for _, sent in self.node.landed])
        self.assertEqual(
            [Decimal(order.price) for order in created],
            [Decimal(message.price) for _, sent in self.node.landed for message in sent.messages],
        )
        await self._run_until(
            lambda: all(len(self._exchange_id(order.client_order_id) or "") == 24 for order in created), "not rested"
        )
        for order in created:
            self._assert_rests_as_sent(order.client_order_id)

    async def test_single_calls_filling_a_tx_send_it_without_waiting_for_the_window(self):
        orders = [
            self.exchange.buy("BZE-USDC", Decimal(10 + index), OrderType.LIMIT, Decimal("0.0003")) for index in range(4)
        ]
        await self._settle()

        self.assertEqual([3], [len(sent.messages) for _, sent in self.node.landed])
        self.assertTrue(self.windows and not any(window.done() for window in self.windows))
        self.windows[-1].set_result(None)
        await self._settle()

        self.assertEqual([3, 1], [len(sent.messages) for _, sent in self.node.landed])
        await self._run_until(lambda: all(len(self._exchange_id(order) or "") == 24 for order in orders), "not rested")
        for order in orders:
            self._assert_rests_as_sent(order)
