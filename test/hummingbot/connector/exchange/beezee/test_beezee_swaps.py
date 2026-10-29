import asyncio
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
    OTHER_ADDRESS,
    REST_URL,
    USDC,
    BeezeeChainTestCase,
    H,
    tx,
    tx_event,
)
from typing import Any, Dict, List

from aioresponses import CallbackResult
from yarl import URL

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_amm import (
    BeezeePool,
    BeezeePoolSwap,
    BeezeeSwapError,
    BeezeeSwapRequest,
    apply_swap,
    min_output_after_slippage,
    swap_fee_to_pool,
    swap_output,
)
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import TokenAmount
from hummingbot.core.event.events import MarketEvent

# The BZE-USDC pool: its id sorts USDC first, so the pool's base is the pair's quote.
POOL_ID = f"{USDC}_ubze"
SWAP_TX = "5A" * 32
TAKER_FEE = TokenAmount("BZE", Decimal("0.1"))
MICRO = Decimal("0.000001")


def pool_data(reserve_usdc: int = 10_000_000_000, reserve_bze: int = 33_000_000_000_000) -> Dict[str, Any]:
    return {
        "id": POOL_ID, "base": USDC, "quote": "ubze", "lp_denom": "ulp/BZEUSDC", "creator": OTHER_ADDRESS,
        "fee": "0.003000000000000000",
        "fee_dest": {"treasury": "0.250000000000000000", "burner": "0.250000000000000000", "providers": "0.500000000000000000"},
        "reserve_base": str(reserve_usdc), "reserve_quote": str(reserve_bze), "stable": False,
    }


def swap_event(msg_index: int, input_denom: str, input_amount: int, output_denom: str, output_amount: int,
               creator: str = ADDRESS, pool_id: str = POOL_ID) -> Dict[str, Any]:
    return tx_event("SwapEvent", msg_index, **{
        "creator": creator, "pool_id": pool_id,
        "in": {"denom": input_denom, "amount": str(input_amount)},
        "out": {"denom": output_denom, "amount": str(output_amount)},
    })


class BeezeeSwapMathTests(unittest.TestCase):
    def test_min_output_rounds_down_on_base_units(self):
        self.assertEqual(995, min_output_after_slippage(1000, Decimal("0.5")))
        # 999 x 0.995 = 994.005 and 10 x 0.95 = 9.5: always down, never to the nearest unit.
        self.assertEqual(994, min_output_after_slippage(999, Decimal("0.5")))
        self.assertEqual(9, min_output_after_slippage(10, Decimal("5")))
        self.assertEqual(1, min_output_after_slippage(3, Decimal("50")))
        self.assertEqual(1234567, min_output_after_slippage(1234567, Decimal("0")))
        self.assertEqual(0, min_output_after_slippage(3, Decimal("80")))

    def test_min_output_refuses_slippage_outside_0_to_100(self):
        for slippage in (Decimal("-0.1"), Decimal("100"), Decimal("150")):
            with self.assertRaises(BeezeeSwapError):
                min_output_after_slippage(1000, slippage)

    def test_fee_to_pool_leaves_the_providers_share(self):
        pool = BeezeePool.from_data(pool_data())

        # 0.25 treasury and 0.25 burner, each truncated from the whole fee.
        self.assertEqual(1500, swap_fee_to_pool(pool, 3000))
        self.assertEqual(5, swap_fee_to_pool(pool, 7))  # treasury 1, burner 1 (1.75 each, truncated)
        self.assertEqual(0, swap_fee_to_pool(pool, 0))
        no_destination = BeezeePool.from_data({**pool_data(), "fee_dest": {}})
        self.assertEqual(3000, swap_fee_to_pool(no_destination, 3000))

    def test_apply_swap_moves_the_reserves_as_the_chain_does(self):
        pool = BeezeePool.from_data(pool_data())
        swap = BeezeePoolSwap(input_denom=USDC, output_denom="ubze", input_amount=1_000_000, fee_amount=3000, output_amount=3_289_000_000)

        after = apply_swap(pool, swap)

        # The input reserve gets the input after the fee plus the fee share that stays (1500 of 3000).
        self.assertEqual(10_000_000_000 + 997_000 + 1500, after.reserve_base)
        self.assertEqual(33_000_000_000_000 - 3_289_000_000, after.reserve_quote)
        reverse = apply_swap(pool, BeezeePoolSwap("ubze", USDC, 1_000_000_000, 3_000_000, 300_000))
        self.assertEqual(33_000_000_000_000 + 997_000_000 + 1_500_000, reverse.reserve_quote)
        self.assertEqual(10_000_000_000 - 300_000, reverse.reserve_base)


class BeezeeSwapTestCase(BeezeeChainTestCase):
    """The connector against mocked REST and RPC, with the BZE-USDC pool loaded and balances of 1000 BZE and 100
    USDC. Simulation answers are registered per test."""

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.pool = BeezeePool.from_data(pool_data())
        await self.exchange._refresh_pools()
        self._set_balances(BZE=Decimal("1000"), USDC=Decimal("100"))

    def _mock_chain_state(self):
        super()._mock_chain_state()
        self.mock_api.get(
            re.compile(rf"^{re.escape(REST_URL + CONSTANTS.ALL_LIQUIDITY_POOLS_PATH_URL)}(\?.*)?$"), repeat=True,
            payload={"list": [pool_data()], "pagination": {"next_key": None}},
        )
        self.mock_api.get(
            re.compile(rf"^{re.escape(REST_URL + CONSTANTS.LIQUIDITY_POOL_PATH_URL)}\?.*$"), repeat=True,
            callback=lambda url, **kwargs: self._pool_read(url),
        )

    def _pool_read(self, url) -> CallbackResult:
        self.pool_reads = getattr(self, "pool_reads", []) + [URL(url).query["pool_id"]]
        return CallbackResult(payload={"pool": pool_data()})

    def _mock_simulate(self):
        pass

    def _mock_simulate_gas(self, gas_used: int = 150000):
        self.mock_api.post(f"{REST_URL}{CONSTANTS.SIMULATE_TX_PATH_URL}", repeat=True, payload={"gas_info": {"gas_used": str(gas_used)}})

    def _set_balances(self, **balances: Decimal):
        self.exchange._account_balances = dict(balances)
        self.exchange._account_available_balances = dict(balances)

    def _posted(self, path: str) -> List[SentTx]:
        return [decode_tx(call.kwargs) for call in self.mock_api.requests.get(("POST", URL(f"{REST_URL}{path}")), [])]

    def _failure_messages(self) -> Dict[str, str]:
        return {event.order_id: event.error_message for event in self.events[MarketEvent.OrderFailure].event_log}


class BeezeeSwapTests(BeezeeSwapTestCase):

    # --- one swap ---

    async def test_buy_swap_sends_one_msg_multi_swap_and_fills_as_a_taker_trade(self):
        quoted = swap_output(self.pool, USDC, 2_000_000)
        paid = quoted.output_amount - 7  # the pool moved a little before the tx
        self._mock_simulate_gas()
        self._mock_broadcast(SWAP_TX)
        self._mock_tx(SWAP_TX, tx(SWAP_TX, H, [swap_event(0, USDC, 2_000_000, "ubze", paid)]))

        order_id = self.exchange.amm_swap("BZE-USDC", "buy", Decimal("2"), Decimal("0.5"))
        await self._run_until(lambda: order_id in self._event_ids(MarketEvent.BuyOrderCompleted), "the swap did not fill")

        [sent] = self._posted(CONSTANTS.BROADCAST_TX_PATH_URL)
        [message] = sent.messages
        self.assertEqual(
            ("swap", [POOL_ID], USDC, "2000000", "ubze", str(quoted.output_amount * 995 // 1000)),
            (message.kind, message.routes, message.input_denom, message.amount, message.min_output_denom, message.min_output),
        )
        self.assertEqual(f"hbot-swap:{order_id}", sent.memo)
        # Gas from the simulation with the usual adjustment.
        self.assertEqual(195000, sent.gas_limit)
        # Reported before the pool fee (0.006 USDC), which is booked as a fee next to the tradebin taker fee.
        base = paid * MICRO
        quote = Decimal("1.994")
        self._assert_fills([(base, quote / base, [TokenAmount("USDC", Decimal("0.006")), TAKER_FEE])])
        [fill] = self.events[MarketEvent.OrderFilled].event_log
        self.assertEqual((TradeType.BUY, OrderType.MARKET, f"{SWAP_TX}-0", SWAP_TX), (fill.trade_type, fill.order_type, fill.exchange_trade_id, fill.exchange_order_id))
        self.assertEqual([order_id], self._event_ids(MarketEvent.BuyOrderCreated))
        [completed] = self.events[MarketEvent.BuyOrderCompleted].event_log
        self.assertEqual((base, quote), (completed.base_asset_amount, completed.quote_asset_amount))
        self.assertNotIn(order_id, self.exchange.in_flight_orders)

        result = self.exchange.get_swap_result(order_id)
        self.assertEqual((Decimal("2"), base, Decimal("0.006"), 2_000_000, paid, 6000), (
            result.input, result.output, result.pool_fee, result.input_amount, result.output_amount, result.pool_fee_amount,
        ))
        # Balances come from the event: the full input out, the output in, the taker fee out.
        self.assertEqual(Decimal("98"), self.exchange.get_available_balance("USDC"))
        self.assertEqual(Decimal("1000") + base - Decimal("0.1"), self.exchange.get_available_balance("BZE"))
        self.assertEqual(Decimal("1000") + base - Decimal("0.1"), self.exchange.get_balance("BZE"))
        # The pool is read again after the swap, never predicted.
        self.assertIn(POOL_ID, self.pool_reads)

    async def test_sell_swap_fills_at_the_pool_price_with_the_pool_fee_in_base(self):
        self._set_balances(BZE=Decimal("5000"), USDC=Decimal("100"))
        quoted = swap_output(self.pool, "ubze", 3_000_000_000)
        self._mock_simulate_gas()
        self._mock_broadcast(SWAP_TX)
        self._mock_tx(SWAP_TX, tx(SWAP_TX, H, [swap_event(0, "ubze", 3_000_000_000, USDC, quoted.output_amount)]))

        order_id = self.exchange.amm_swap("BZE-USDC", "sell", Decimal("3000"), Decimal("1"))
        await self._run_until(lambda: order_id in self._event_ids(MarketEvent.SellOrderCompleted), "the swap did not fill")

        [message] = self._posted(CONSTANTS.BROADCAST_TX_PATH_URL)[0].messages
        self.assertEqual(("ubze", "3000000000", USDC, str(quoted.output_amount * 99 // 100)), (
            message.input_denom, message.amount, message.min_output_denom, message.min_output,
        ))
        base = Decimal("2991")  # 3000 BZE less the 0.3% pool fee
        received = quoted.output_amount * MICRO
        self._assert_fills([(base, received / base, [TokenAmount("BZE", Decimal("9")), TAKER_FEE])])
        self.assertEqual(TradeType.SELL, self.events[MarketEvent.OrderFilled].event_log[0].trade_type)
        self.assertEqual(Decimal("5000") - Decimal("3000.1"), self.exchange.get_available_balance("BZE"))
        self.assertEqual(Decimal("100") + received, self.exchange.get_available_balance("USDC"))

    # --- several swaps in one tx ---

    async def test_round_trip_goes_out_in_one_tx_and_each_leg_fills_from_its_own_event(self):
        first = swap_output(self.pool, USDC, 2_000_000)
        moved = apply_swap(self.pool, first)
        second = swap_output(moved, "ubze", 6_000_000_000)
        # The second leg is quoted on the pool as the first leaves it.
        self.assertNotEqual(swap_output(self.pool, "ubze", 6_000_000_000).output_amount, second.output_amount)
        self._mock_simulate_gas(300000)
        self._mock_broadcast(SWAP_TX)
        self._mock_tx(SWAP_TX, tx(SWAP_TX, H, [
            swap_event(0, USDC, 2_000_000, "ubze", first.output_amount),
            # A module fee swap in the same tx: not ours.
            swap_event(0, "ubze", 1000, USDC, 1, creator=OTHER_ADDRESS),
            swap_event(1, "ubze", 6_000_000_000, USDC, second.output_amount),
        ]))

        buy_id, sell_id = self.exchange.amm_swaps([
            BeezeeSwapRequest("BZE-USDC", "buy", Decimal("2"), Decimal("0.5")),
            BeezeeSwapRequest("BZE-USDC", "sell", Decimal("6000"), Decimal("1")),
        ])
        await self._run_until(lambda: sell_id in self._event_ids(MarketEvent.SellOrderCompleted), "the round trip did not fill")

        [sent] = self._posted(CONSTANTS.BROADCAST_TX_PATH_URL)
        self.assertEqual(CONSTANTS.SWAP_BATCH_MEMO, sent.memo)
        self.assertEqual(
            [(USDC, "2000000", str(first.output_amount * 995 // 1000)), ("ubze", "6000000000", str(second.output_amount * 99 // 100))],
            [(message.input_denom, message.amount, message.min_output) for message in sent.messages],
        )
        self.assertEqual([buy_id], self._event_ids(MarketEvent.BuyOrderCompleted))
        bought = first.output_amount * MICRO
        received = second.output_amount * MICRO
        self._assert_fills([
            (bought, Decimal("1.994") / bought, [TokenAmount("USDC", Decimal("0.006")), TAKER_FEE]),
            (Decimal("5982"), received / Decimal("5982"), [TokenAmount("BZE", Decimal("18")), TAKER_FEE]),
        ])
        self.assertEqual([f"{SWAP_TX}-0", f"{SWAP_TX}-1"], [fill.exchange_trade_id for fill in self.events[MarketEvent.OrderFilled].event_log])
        self.assertEqual(Decimal("1000") + bought - Decimal("6000") - Decimal("0.2"), self.exchange.get_available_balance("BZE"))
        self.assertEqual(Decimal("98") + received, self.exchange.get_available_balance("USDC"))

    async def test_a_later_leg_may_spend_the_guaranteed_output_of_an_earlier_one(self):
        self._set_balances(BZE=Decimal("0.5"), USDC=Decimal("2"))
        first = swap_output(self.pool, USDC, 2_000_000)
        spendable = (first.output_amount * 995 // 1000) * MICRO
        self._mock_simulate_gas()
        self._mock_broadcast(SWAP_TX)
        self._mock_tx(SWAP_TX, tx(SWAP_TX, H, [
            swap_event(0, USDC, 2_000_000, "ubze", first.output_amount), swap_event(1, "ubze", 1_000_000_000, USDC, 300_000),
        ]))

        with self.assertRaisesRegex(BeezeeSwapError, "balance too low"):
            # More than the first leg guarantees (the taker fees come out of the BZE too).
            self.exchange.amm_swaps([
                BeezeeSwapRequest("BZE-USDC", "buy", Decimal("2"), Decimal("0.5")),
                BeezeeSwapRequest("BZE-USDC", "sell", spendable + Decimal("0.3") + MICRO, Decimal("1")),
            ])
        self.assertEqual({}, self.exchange.in_flight_orders)

        ids = self.exchange.amm_swaps([
            BeezeeSwapRequest("BZE-USDC", "buy", Decimal("2"), Decimal("0.5")),
            BeezeeSwapRequest("BZE-USDC", "sell", Decimal("1000"), Decimal("1")),
        ])
        await self._run_until(lambda: ids[1] in self._event_ids(MarketEvent.SellOrderCompleted), "not filled")
        self.assertEqual(1, len(self._posted(CONSTANTS.BROADCAST_TX_PATH_URL)))

    # --- refused up front: nothing tracked, nothing sent ---

    def _assert_nothing_sent(self):
        self.assertEqual({}, self.exchange.in_flight_orders)
        self.assertEqual([], self._posted(CONSTANTS.SIMULATE_TX_PATH_URL))
        self.assertEqual([], self._posted(CONSTANTS.BROADCAST_TX_PATH_URL))

    async def test_swap_on_a_halted_pool_is_refused_up_front(self):
        self.exchange._halted_denoms = {USDC}

        with self.assertRaisesRegex(BeezeeSwapError, f"pool {re.escape(POOL_ID)} is halted"):
            self.exchange.amm_swap("BZE-USDC", "buy", Decimal("2"), Decimal("0.5"))
        await self._drain()
        self._assert_nothing_sent()

    async def test_swap_without_a_pool_is_refused_up_front(self):
        with self.assertRaisesRegex(BeezeeSwapError, "no liquidity pool for TKN-BZE"):
            self.exchange.amm_swap("TKN-BZE", "buy", Decimal("2"), Decimal("0.5"))
        self._assert_nothing_sent()

    async def test_swap_whose_output_quote_is_zero_is_refused_up_front(self):
        # 0.001 BZE buys less than one micro USDC.
        with self.assertRaisesRegex(BeezeeSwapError, "non positive output"):
            self.exchange.amm_swap("BZE-USDC", "sell", Decimal("0.001"), Decimal("0.5"))
        # 300 micro USDC pays no pool fee: the chain refuses it as too low.
        with self.assertRaisesRegex(BeezeeSwapError, "amount is too low to be traded"):
            self.exchange.amm_swap("BZE-USDC", "buy", Decimal("0.0003"), Decimal("0.5"))
        # 0.01 BZE quotes 3 micro USDC, which 80% slippage rounds down to 0.
        self.assertEqual(3, swap_output(self.pool, "ubze", 10000).output_amount)
        with self.assertRaisesRegex(BeezeeSwapError, "rounds to 0"):
            self.exchange.amm_swap("BZE-USDC", "sell", Decimal("0.01"), Decimal("80"))
        self._assert_nothing_sent()

    async def test_swap_with_a_balance_too_low_is_refused_up_front(self):
        self._set_balances(BZE=Decimal("1000"), USDC=Decimal("1.5"))
        with self.assertRaisesRegex(BeezeeSwapError, r"balance too low for 2(\.0+)? USDC"):
            self.exchange.amm_swap("BZE-USDC", "buy", Decimal("2"), Decimal("0.5"))

        # Enough to swap, not for the taker fee on top.
        self._set_balances(BZE=Decimal("0.05"), USDC=Decimal("100"))
        with self.assertRaisesRegex(BeezeeSwapError, r"short of 0\.050* BZE"):
            self.exchange.amm_swap("BZE-USDC", "buy", Decimal("2"), Decimal("0.5"))
        self._assert_nothing_sent()

    async def test_bad_side_or_slippage_is_refused_up_front(self):
        with self.assertRaisesRegex(BeezeeSwapError, "side must be"):
            self.exchange.amm_swap("BZE-USDC", "long", Decimal("2"), Decimal("0.5"))
        with self.assertRaisesRegex(BeezeeSwapError, "slippage"):
            self.exchange.amm_swap("BZE-USDC", "buy", Decimal("2"), Decimal("100"))
        self._assert_nothing_sent()

    # --- failures: reported once, never retried ---

    async def test_slippage_refusal_in_simulation_fails_the_swap_without_a_retry(self):
        refusal = (
            "failed to execute message; message index: 0: expected minimum 6551113266ubze, got 6540000000ubze: "
            "the resulted amount is too low"
        )
        self.mock_api.post(f"{REST_URL}{CONSTANTS.SIMULATE_TX_PATH_URL}", repeat=True, status=400,
                           payload={"code": 4014, "message": refusal, "details": []})

        order_id = self.exchange.amm_swap("BZE-USDC", "buy", Decimal("2"), Decimal("0.5"))
        await self._run_until(lambda: order_id in self._failure_messages(), "the swap did not fail")
        await self._drain()

        self.assertEqual(f"Beezee swap failed: {refusal}", self._failure_messages()[order_id])
        self.assertEqual(1, len(self._posted(CONSTANTS.SIMULATE_TX_PATH_URL)))
        self.assertEqual([], self._posted(CONSTANTS.BROADCAST_TX_PATH_URL))
        self.assertEqual([], self._event_ids(MarketEvent.OrderFilled))
        self.assertEqual([], self._event_ids(MarketEvent.BuyOrderCreated))
        self.assertNotIn(order_id, self.exchange.in_flight_orders)
        self.assertEqual(Decimal("100"), self.exchange.get_available_balance("USDC"))

    async def test_swap_tx_failing_in_its_block_fails_every_swap_of_it(self):
        raw_log = (
            "failed to execute message; message index: 1: expected minimum 1900000ibc/81DA, got 1899000ibc/81DA: "
            "the resulted amount is too low"
        )
        self._mock_simulate_gas()
        self._mock_broadcast(SWAP_TX)
        failed = tx(SWAP_TX, H, [], code=4014)
        failed["tx_response"].update({"raw_log": raw_log, "codespace": "tradebin"})
        self.mock_api.get(f"{REST_URL}{CONSTANTS.GET_TX_PATH_URL}/{SWAP_TX}", payload=failed)

        ids = self.exchange.amm_swaps([
            BeezeeSwapRequest("BZE-USDC", "buy", Decimal("2"), Decimal("0.5")),
            BeezeeSwapRequest("BZE-USDC", "sell", Decimal("6000"), Decimal("1")),
        ])
        await self._run_until(lambda: set(ids) <= set(self._failure_messages()), "the swaps did not fail")

        self.assertEqual(ids, self._event_ids(MarketEvent.BuyOrderCreated) + self._event_ids(MarketEvent.SellOrderCreated))
        for order_id in ids:
            self.assertEqual(f"Beezee swap tx {SWAP_TX} failed with code 4014: {raw_log}", self._failure_messages()[order_id])
        self.assertEqual(1, len(self._posted(CONSTANTS.BROADCAST_TX_PATH_URL)))
        self.assertEqual([], self._event_ids(MarketEvent.OrderFilled))
        self.assertEqual({}, self.exchange.in_flight_orders)

    async def test_halted_refusal_in_simulation_fails_the_swap_and_rereads_the_halted_denoms(self):
        self.mock_api.post(f"{REST_URL}{CONSTANTS.SIMULATE_TX_PATH_URL}", repeat=True, status=400, payload={
            "code": 4017, "message": f"failed to execute message; message index: 0: pool {POOL_ID} is halted: denom is halted",
            "details": [],
        })

        order_id = self.exchange.amm_swap("BZE-USDC", "buy", Decimal("2"), Decimal("0.5"))
        await self._run_until(lambda: order_id in self._failure_messages(), "the swap did not fail")
        await self._drain()

        self.assertIn("denom is halted", self._failure_messages()[order_id])
        self.assertEqual([], self._posted(CONSTANTS.BROADCAST_TX_PATH_URL))
        # The node does not list the halted denom yet, so the market is held halted until the next refresh.
        with self.assertRaisesRegex(BeezeeSwapError, "is halted .denom unknown, refused by the chain"):
            self.exchange.amm_swap("BZE-USDC", "buy", Decimal("2"), Decimal("0.5"))

    async def test_swap_tx_never_found_fails_without_a_resend(self):
        self._mock_simulate_gas()
        self._mock_broadcast(SWAP_TX)
        self.mock_api.get(f"{REST_URL}{CONSTANTS.GET_TX_PATH_URL}/{SWAP_TX}", repeat=True, status=404,
                          payload={"code": 5, "message": "tx not found"})

        order_id = self.exchange.amm_swap("BZE-USDC", "buy", Decimal("2"), Decimal("0.5"))
        await self._run_until(lambda: order_id in self._failure_messages(), "the swap did not fail")
        await self._drain()

        self.assertIn("was not found within 60 s", self._failure_messages()[order_id])
        self.assertEqual(1, len(self._posted(CONSTANTS.BROADCAST_TX_PATH_URL)))
        self.assertEqual(len(CONSTANTS.TX_LOOKUP_DELAYS), len(self.mock_api.requests[("GET", URL(f"{REST_URL}{CONSTANTS.GET_TX_PATH_URL}/{SWAP_TX}"))]))

    # --- swaps are not orders on the book ---

    async def test_a_swap_cannot_be_cancelled(self):
        gate = asyncio.get_event_loop().create_future()

        async def simulate(url, **kwargs):
            await gate
            return CallbackResult(payload={"gas_info": {"gas_used": "150000"}})

        self.mock_api.post(f"{REST_URL}{CONSTANTS.SIMULATE_TX_PATH_URL}", callback=simulate)
        self._mock_broadcast(SWAP_TX)
        quoted = swap_output(self.pool, USDC, 2_000_000)
        self._mock_tx(SWAP_TX, tx(SWAP_TX, H, [swap_event(0, USDC, 2_000_000, "ubze", quoted.output_amount)]))

        order_id = self.exchange.amm_swap("BZE-USDC", "buy", Decimal("2"), Decimal("0.5"))
        await self._drain()
        cancelled = await self.exchange._execute_cancel("BZE-USDC", order_id)
        gate.set_result(None)
        await self._run_until(lambda: order_id in self._event_ids(MarketEvent.BuyOrderCompleted), "the swap did not fill")

        self.assertIsNone(cancelled)
        self.assertEqual([], self._event_ids(MarketEvent.OrderCancelled))
        self.assertEqual(["swap"], [message.kind for sent in self._posted(CONSTANTS.BROADCAST_TX_PATH_URL) for message in sent.messages])

    # --- restart ---

    async def test_swap_saved_with_its_tx_is_settled_from_that_tx_after_a_restart(self):
        quoted = swap_output(self.pool, USDC, 2_000_000)
        swap = InFlightOrder(
            client_order_id="BZE-SWAP-1", trading_pair="BZE-USDC", order_type=OrderType.MARKET, trade_type=TradeType.BUY,
            amount=quoted.output_amount * MICRO, price=Decimal("0.0003"), creation_timestamp=1.0, exchange_order_id=SWAP_TX,
            initial_state=OrderState.OPEN,
        )
        self._restore([swap], {"last_processed_height": H, "creates": {}, "cancels": {}, "swaps": {"BZE-SWAP-1": {
            "client_order_id": "BZE-SWAP-1", "trading_pair": "BZE-USDC", "side": "buy", "pool_id": POOL_ID,
            "input_denom": USDC, "output_denom": "ubze", "pool_fee": "0.003", "msg_index": 0, "tx_hash": SWAP_TX,
        }}})
        self._mock_latest_height(H, repeat=True)
        self._mock_tx(SWAP_TX, tx(SWAP_TX, H, [swap_event(0, USDC, 2_000_000, "ubze", quoted.output_amount)]))

        await self._status_update()
        await self._run_until(lambda: "BZE-SWAP-1" in self._event_ids(MarketEvent.BuyOrderCompleted), "the swap did not settle")

        base = quoted.output_amount * MICRO
        self._assert_fills([(base, Decimal("1.994") / base, [TokenAmount("USDC", Decimal("0.006")), TAKER_FEE])])
        self.assertEqual([], self._posted(CONSTANTS.BROADCAST_TX_PATH_URL))
        self.assertEqual([], self._requested_heights())

    async def test_swap_saved_before_its_tx_was_accepted_fails_after_a_restart(self):
        swap = InFlightOrder(
            client_order_id="BZE-SWAP-2", trading_pair="BZE-USDC", order_type=OrderType.MARKET, trade_type=TradeType.SELL,
            amount=Decimal("3000"), price=Decimal("0.0003"), creation_timestamp=1.0,
        )
        self._restore([swap], {"last_processed_height": H, "creates": {}, "cancels": {}, "swaps": {"BZE-SWAP-2": {
            "client_order_id": "BZE-SWAP-2", "trading_pair": "BZE-USDC", "side": "sell", "pool_id": POOL_ID,
            "input_denom": "ubze", "output_denom": USDC, "pool_fee": "0.003", "msg_index": 0, "tx_hash": None,
        }}})
        self._mock_latest_height(H, repeat=True)

        await self._status_update()
        await self._run_until(lambda: "BZE-SWAP-2" in self._failure_messages(), "the swap did not fail")

        self.assertIn("whether it was sent is unknown", self._failure_messages()["BZE-SWAP-2"])
        self.assertEqual([], self._posted(CONSTANTS.SIMULATE_TX_PATH_URL))

    async def test_pending_swap_is_saved_in_the_tracking_states(self):
        gate = asyncio.get_event_loop().create_future()

        async def simulate(url, **kwargs):
            await gate
            return CallbackResult(status=400, payload={"code": 4014, "message": "the resulted amount is too low"})

        self.mock_api.post(f"{REST_URL}{CONSTANTS.SIMULATE_TX_PATH_URL}", callback=simulate)

        order_id = self.exchange.amm_swap("BZE-USDC", "sell", Decimal("300"), Decimal("1"))
        await self._drain()
        saved = self.exchange.tracking_states[CONSTANTS.TRACKING_STATE_KEY]["swaps"][order_id]
        gate.set_result(None)
        await self._run_until(lambda: order_id in self._failure_messages(), "the swap did not fail")

        self.assertEqual(
            {"client_order_id": order_id, "trading_pair": "BZE-USDC", "side": "sell", "pool_id": POOL_ID,
             "input_denom": "ubze", "output_denom": USDC, "pool_fee": "0.003000000000000000", "msg_index": 0, "tx_hash": None},
            saved,
        )
        self.assertEqual({}, self.exchange.tracking_states[CONSTANTS.TRACKING_STATE_KEY]["swaps"])


class BeezeeSwapSequenceTests(BeezeeBatchingTestCase):
    """A swap and an order batch from the same wallet, against the FakeNode: they never race for a sequence."""

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.mock_api.get(
            re.compile(rf"^{re.escape(REST_URL + CONSTANTS.ALL_LIQUIDITY_POOLS_PATH_URL)}(\?.*)?$"), repeat=True,
            payload={"list": [pool_data()], "pagination": {"next_key": None}},
        )
        self.mock_api.get(
            re.compile(rf"^{re.escape(REST_URL + CONSTANTS.LIQUIDITY_POOL_PATH_URL)}\?.*$"), repeat=True,
            payload={"pool": pool_data()},
        )
        await self.exchange._refresh_pools()
        self.exchange._account_available_balances = {"BZE": Decimal("1000"), "USDC": Decimal("100")}

    async def test_swap_and_order_batch_sent_together_get_consecutive_sequences(self):
        self.node.simulate_gate = asyncio.get_event_loop().create_future()

        created = self.exchange.batch_order_create(self._ladder(2))
        swap_id = self.exchange.amm_swap("BZE-USDC", "buy", Decimal("2"), Decimal("0.5"))
        await self._settle()
        # One tx waits in simulation; the other waits for it before it takes a sequence.
        self.assertEqual(1, len(self.node.simulated))
        self.node.simulate_gate.set_result(None)
        self.node.simulate_gate = None
        await self._run_until(lambda: swap_id in self._event_ids(MarketEvent.BuyOrderCompleted), "the swap did not fill")
        await self._run_until(
            lambda: all(len(self._exchange_id(order.client_order_id) or "") == 24 for order in created), "not rested"
        )

        self.assertEqual([3, 4], [sent.sequence for _, sent in self.node.landed])
        self.assertEqual(
            [["create", "create"], ["swap"]], sorted(sorted(sent.kinds) for _, sent in self.node.landed)
        )
        self.assertEqual([3, 4], [sent.sequence for sent in self.node.simulated])
        self.assertEqual(5, self.node.sequence)
