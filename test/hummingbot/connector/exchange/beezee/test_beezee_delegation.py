import asyncio
import logging
import re
from datetime import datetime, timezone
from decimal import Decimal
from test.hummingbot.connector.exchange.beezee.test_beezee_delegated import (
    BOT_ADDRESS,
    BOT_MNEMONIC,
    allowance_url,
    grants_url,
)
from test.hummingbot.connector.exchange.beezee.test_beezee_order_tracking import (
    ADDRESS,
    REST_URL,
    RESTED_TX,
    BeezeeChainTestCase,
)
from typing import Any, Dict, List, Optional

from aioresponses import CallbackResult

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_utils import BeezeeDelegatedMnemonicAccountMode
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.event.events import MarketEvent

CREATE, CANCEL, SWAP, FILL = CONSTANTS.DELEGATED_MESSAGE_TYPE_URLS
GENERIC = "/cosmos.authz.v1beta1.GenericAuthorization"
BASIC = "/cosmos.feegrant.v1beta1.BasicAllowance"
PERIODIC = "/cosmos.feegrant.v1beta1.PeriodicAllowance"
ALLOWED_MSG = "/cosmos.feegrant.v1beta1.AllowedMsgAllowance"
MSG_EXEC = "/cosmos.authz.v1beta1.MsgExec"
# 2026-10-31T00:00:00Z, the fake clock's start.
T0 = 1793404800.0
HOUR = 3600.0
FIVE_MINUTES = 300.0
STOPPED_LOG = "Beezee delegated trading stopped"
RESUMED_LOG = "Beezee delegated trading resumed"
NOT_FOUND_BODY = {"code": 13, "message": "fee-grant not found: not found", "details": []}

# Every refusal that means a grant problem: (codespace, code, the text the module error carries).
REFUSALS = [
    ("authz", 2, "authorization not found"),
    ("authz", 6, "authorization expired"),
    ("feegrant", 2, "fee limit exceeded"),
    ("feegrant", 3, "fee allowance expired"),
    ("feegrant", 5, "no allowance"),
    ("feegrant", 7, "message not allowed"),
]


def rfc3339(timestamp: float) -> str:
    """The chain's timestamp form, nanoseconds included."""
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000000000Z")


def grant(message_type: str, expiration: Optional[float] = None) -> Dict[str, Any]:
    return {
        "authorization": {"@type": GENERIC, "msg": message_type},
        "expiration": rfc3339(expiration) if expiration is not None else None,
    }


def basic(spend_limit: Optional[str] = None, expiration: Optional[float] = None) -> Dict[str, Any]:
    return {
        "@type": BASIC,
        "spend_limit": [{"denom": "ubze", "amount": spend_limit}] if spend_limit is not None else [],
        "expiration": rfc3339(expiration) if expiration is not None else None,
    }


def periodic(period_can_spend: List[Dict[str, str]], period_reset: float, spend_limit: Optional[str] = None):
    return {
        "@type": PERIODIC,
        "basic": basic(spend_limit),
        "period": "86400s",
        "period_spend_limit": [{"denom": "ubze", "amount": "5000000"}],
        "period_can_spend": period_can_spend,
        "period_reset": rfc3339(period_reset),
    }


def allowed(inner: Dict[str, Any], messages: List[str]) -> Dict[str, Any]:
    return {"@type": ALLOWED_MSG, "allowance": inner, "allowed_messages": messages}


class BeezeeDelegationTestCase(BeezeeChainTestCase):
    """The delegated connector against mocked REST and a fake clock. The grants, the allowance and the node's answers
    to txs are the test's state; the test drives the connector through its status updates, orders and status."""

    signer_address = BOT_ADDRESS

    def _account_type(self):
        return BeezeeDelegatedMnemonicAccountMode(mnemonic=BOT_MNEMONIC, granter_address=ADDRESS)

    async def asyncSetUp(self) -> None:
        self.now = T0
        self.grants: List[Dict[str, Any]] = [grant(message_type) for message_type in CONSTANTS.DELEGATED_MESSAGE_TYPE_URLS]
        self.allowance: Optional[Dict[str, Any]] = basic()
        self.read_error = False
        # When set, the next grants read waits for it.
        self.grants_read_gate: Optional[asyncio.Future] = None
        # A simulation refusal: the node's error text. A broadcast refusal: (codespace, code, raw log).
        self.simulation_refusal: Optional[str] = None
        self.broadcast_refusal: Optional[tuple] = None
        await super().asyncSetUp()
        self.exchange._time = lambda: self.now

    def _mock_chain_state(self):
        super()._mock_chain_state()
        self.mock_api.get(grants_url(), repeat=True, callback=self._grants_response)
        self.mock_api.get(allowance_url(), repeat=True, callback=self._allowance_response)

    def _mock_simulate(self):
        self.mock_api.post(f"{REST_URL}{CONSTANTS.SIMULATE_TX_PATH_URL}", repeat=True, callback=self._simulate_response)
        self.mock_api.post(f"{REST_URL}{CONSTANTS.BROADCAST_TX_PATH_URL}", repeat=True, callback=self._broadcast_response)

    async def _grants_response(self, url, **kwargs):
        gate, self.grants_read_gate = self.grants_read_gate, None
        if gate is not None:
            await gate
        if self.read_error:
            return CallbackResult(status=503, payload={"code": 14, "message": "unavailable", "details": []})
        return CallbackResult(payload={"grants": self.grants, "pagination": {"next_key": None}})

    def _allowance_response(self, url, **kwargs):
        if self.read_error:
            return CallbackResult(status=503, payload={"code": 14, "message": "unavailable", "details": []})
        if self.allowance is None:
            return CallbackResult(status=500, payload=NOT_FOUND_BODY)
        return CallbackResult(payload={"allowance": {"granter": ADDRESS, "grantee": BOT_ADDRESS, "allowance": self.allowance}})

    def _simulate_response(self, url, **kwargs):
        if self.simulation_refusal is not None:
            # The REST gateway's error body: its code is the gRPC code, not the module's.
            return CallbackResult(status=500, payload={"code": 2, "message": self.simulation_refusal, "details": []})
        return CallbackResult(payload={"gas_info": {"gas_used": "200000"}})

    def _broadcast_response(self, url, **kwargs):
        if self.broadcast_refusal is not None:
            codespace, code, raw_log = self.broadcast_refusal
            return CallbackResult(payload={"tx_response": {"code": code, "codespace": codespace, "raw_log": raw_log, "txhash": ""}})
        return CallbackResult(payload={"tx_response": {"code": 0, "txhash": RESTED_TX}})

    # --- helpers ---

    def _requests(self, method: str, path: str) -> int:
        return sum(len(calls) for (request_method, url), calls in self.mock_api.requests.items()
                   if request_method == method and url.path.startswith(path))

    def _grant_reads(self) -> int:
        return self._requests("GET", CONSTANTS.AUTHZ_GRANTS_PATH_URL)

    def _txs_sent(self) -> int:
        return self._requests("POST", CONSTANTS.SIMULATE_TX_PATH_URL)

    def _log_count(self, level: int, fragment: str) -> int:
        return sum(1 for record in self.log_records if record.levelno == level and fragment in record.getMessage())

    @property
    def _grants_valid(self) -> bool:
        return self.exchange.status_dict["delegation_grants_valid"]

    async def _check_after(self, seconds: float):
        self.now += seconds
        await self._status_update()

    def _place(self) -> str:
        return self.exchange.sell("BZE-USDC", Decimal("1200"), OrderType.LIMIT, Decimal("0.0005"))

    async def _place_and_fail(self) -> str:
        order_id = self._place()
        await self._run_until(lambda: order_id in self._event_ids(MarketEvent.OrderFailure), "the order did not fail")
        return order_id

    def _assert_stopped(self, fragment: str):
        self.assertFalse(self._grants_valid)
        self.assertFalse(self.exchange.ready)
        self.assertIn(fragment, self.exchange.delegation_status)
        self.assertIn("INVALID", self.exchange.delegation_status)


class BeezeeDelegationCheckTests(BeezeeDelegationTestCase):
    async def test_all_grants_valid_makes_the_grants_flag_true(self):
        self.assertFalse(self._grants_valid)  # not checked yet: not ready
        self.assertFalse(self.exchange.ready)
        self.assertEqual("Delegated grants: not checked yet.", self.exchange.delegation_status)

        await self._status_update()

        self.assertTrue(self._grants_valid)
        self.assertEqual("Delegated grants: valid, no expiry.", self.exchange.delegation_status)
        self.assertEqual(0, self._log_count(logging.WARNING, STOPPED_LOG))

    async def test_each_missing_grant_stops_trading_and_is_named(self):
        for missing in CONSTANTS.DELEGATED_MESSAGE_TYPE_URLS:
            with self.subTest(missing=missing):
                self.grants = [grant(message_type) for message_type in CONSTANTS.DELEGATED_MESSAGE_TYPE_URLS if message_type != missing]
                await self._check_after(FIVE_MINUTES)

                self._assert_stopped(f"missing authz grants for {missing}")
                others = [message_type for message_type in CONSTANTS.DELEGATED_MESSAGE_TYPE_URLS if message_type != missing]
                self.assertFalse(any(other in self.exchange.delegation_status for other in others))
        # One log line for the transition, naming the first missing type and pointing to the run guide.
        self.assertEqual(1, self._log_count(logging.WARNING, STOPPED_LOG))
        self.assertTrue(self._logged(logging.WARNING, f"missing authz grants for {CREATE}"))
        self.assertTrue(self._logged(logging.WARNING, '"When grants are revoked, expire or run out" in README_BZE.md'))

    async def test_a_grant_expired_by_timestamp_but_still_listed_stops_trading(self):
        self.grants = [grant(CREATE), grant(CANCEL, expiration=T0 - 1), grant(SWAP), grant(FILL)]

        await self._status_update()

        self._assert_stopped(f"expired authz grants for {CANCEL}")

    async def test_no_allowance_stops_trading(self):
        self.allowance = None  # the node answers HTTP 500 "fee-grant not found"

        await self._status_update()

        self._assert_stopped("no feegrant allowance")

    async def test_allowed_msg_allowance_without_msg_exec_stops_trading(self):
        self.allowance = allowed(basic(), [CREATE, CANCEL, SWAP, FILL])

        await self._status_update()

        self._assert_stopped(f"the feegrant allowance does not allow {MSG_EXEC}")

    async def test_allowed_msg_allowance_with_msg_exec_is_valid(self):
        self.allowance = allowed(periodic([{"denom": "ubze", "amount": "1000"}], T0 + HOUR), [MSG_EXEC])

        await self._status_update()

        self.assertTrue(self._grants_valid)

    async def test_periodic_allowance_with_nothing_left_this_period_stops_trading(self):
        # The chain drops zero coins, so a spent period lists nothing; a listed zero means the same.
        for can_spend in ([], [{"denom": "ubze", "amount": "0"}]):
            with self.subTest(can_spend=can_spend):
                self.allowance = periodic(can_spend, period_reset=T0 + HOUR)
                await self._check_after(FIVE_MINUTES)

                self._assert_stopped("the feegrant allowance has no ubze left to spend until the period resets")

    async def test_periodic_allowance_whose_period_is_over_is_valid_again(self):
        self.allowance = periodic([], period_reset=T0 + HOUR)
        await self._status_update()
        self.assertFalse(self._grants_valid)

        # The chain refills the period on the next use once the reset time has passed.
        await self._check_after(HOUR)

        self.assertTrue(self._grants_valid)

    async def test_expired_allowance_stops_trading(self):
        self.allowance = basic(expiration=T0 - HOUR)

        await self._status_update()

        self._assert_stopped("the feegrant allowance expired on")

    async def test_expiry_within_72_hours_warns_once_and_status_shows_the_earliest(self):
        self.grants = [grant(CREATE, expiration=T0 + 48 * HOUR), grant(CANCEL, expiration=T0 + 300 * HOUR), grant(SWAP), grant(FILL)]
        self.allowance = basic(spend_limit="2500000", expiration=T0 + 200 * HOUR)

        await self._status_update()
        await self._check_after(FIVE_MINUTES)

        self.assertTrue(self._grants_valid)
        self.assertEqual(1, self._log_count(logging.WARNING, "expires on"))
        self.assertTrue(self._logged(logging.WARNING, f"the authz grant for {CREATE} expires on 2026-11-02 00:00 UTC"))
        self.assertEqual(
            f"Delegated grants: valid, earliest expiry 2026-11-02 00:00 UTC ({CREATE}), feegrant allowance left 2.5 BZE.",
            self.exchange.delegation_status,
        )

        # The allowance comes within 72 hours later: one warning for it, none again for the grant.
        await self._check_after(130 * HOUR)

        self.assertEqual(2, self._log_count(logging.WARNING, "expires on"))
        self.assertTrue(self._logged(logging.WARNING, "the feegrant allowance expires on 2026-11-08 08:00 UTC"))

    async def test_a_node_read_error_keeps_the_state(self):
        await self._status_update()
        self.assertTrue(self._grants_valid)

        self.read_error = True
        await self._check_after(FIVE_MINUTES)
        await self._check_after(FIVE_MINUTES)

        self.assertTrue(self._grants_valid)
        self.assertEqual(1, self._log_count(logging.WARNING, "Failed to read the Beezee delegated grants"))

        # An invalid state is kept the same way.
        self.read_error = False
        self.allowance = None
        await self._check_after(FIVE_MINUTES)
        self.read_error = True
        await self._check_after(FIVE_MINUTES)

        self._assert_stopped("no feegrant allowance")

    async def test_checks_run_every_five_minutes(self):
        await self._status_update()
        reads = self._grant_reads()

        await self._check_after(FIVE_MINUTES - 1)
        self.assertEqual(reads, self._grant_reads())

        await self._check_after(1)
        self.assertEqual(reads + 1, self._grant_reads())

    async def test_invalid_grants_send_no_order_and_renewed_grants_resume_without_restart(self):
        self.allowance = None
        await self._status_update()

        await self._place_and_fail()

        self.assertEqual(0, self._txs_sent())
        self.assertIn("delegated trading is stopped", self.events[MarketEvent.OrderFailure].event_log[0].error_message)

        self.allowance = basic()
        await self._check_after(FIVE_MINUTES)

        self.assertTrue(self._grants_valid)
        self.assertEqual(1, self._log_count(logging.INFO, RESUMED_LOG))
        self._place()
        await self._run_until(lambda: self._requests("POST", CONSTANTS.BROADCAST_TX_PATH_URL) == 1, "no tx after resuming")


class BeezeeDelegationRefusalTests(BeezeeDelegationTestCase):
    """A refusal that means a grant problem stops trading after that one tx, logs once and reads the grants once at
    once; only the next 5-minute check can make the connector ready again."""

    async def _assert_one_refused_tx_stops_trading(self, stage: str, codespace: str, code: int, text: str):
        await self._status_update()
        self.assertTrue(self._grants_valid)
        reads = self._grant_reads()
        if stage == "simulation":
            self.simulation_refusal = (
                f"rpc error: code = Unknown desc = failed to execute message; message index: 0: {text} With gas wanted: "
                f"'0' and gas used: '51234' : unknown request"
            )
        else:
            self.broadcast_refusal = (codespace, code, f"{ADDRESS} does not allow to pay fees for {ADDRESS}: {text}")

        await self._place_and_fail()
        # The re-check right after the refusal: the grants still read valid, yet trading stays stopped.
        await self._run_until(lambda: self._grant_reads() == reads + 1, "no re-check after the refusal")
        await self._drain()

        self.assertEqual(reads + 1, self._grant_reads())
        self.assertEqual(1, self._txs_sent())
        self._assert_stopped(text)
        self.assertEqual(1, self._log_count(logging.WARNING, STOPPED_LOG))

        # Nothing more is sent while stopped, and no further read happens before the 5-minute check.
        await self._place_and_fail()
        await self._status_update()
        self.assertEqual(1, self._txs_sent())
        self.assertEqual(reads + 1, self._grant_reads())
        self.assertEqual(1, self._log_count(logging.WARNING, STOPPED_LOG))

        # The user renewed the grants: the 5-minute check resumes trading.
        self.simulation_refusal = None
        self.broadcast_refusal = None
        await self._check_after(FIVE_MINUTES)

        self.assertTrue(self._grants_valid)
        self.assertEqual(1, self._log_count(logging.INFO, RESUMED_LOG))
        self._place()
        await self._run_until(lambda: self._txs_sent() == 2, "no tx after resuming")

    async def test_refused_simulation_with_a_missing_allowance_text_stops_trading(self):
        await self._status_update()
        self.simulation_refusal = "fee-grant not found: not found: unknown request"

        await self._place_and_fail()
        await self._run_until(lambda: self._grant_reads() == 2, "no re-check after the refusal")

        self._assert_stopped("no allowance")

    async def test_status_update_while_the_first_re_check_runs_after_a_refusal_before_any_check(self):
        # The refusal comes before the first status update, and its re-check is still reading when the next status
        # update runs: the update must read the grants itself, not fail on a check time that was never set.
        self.grants_read_gate = asyncio.get_event_loop().create_future()
        self.simulation_refusal = "failed to execute message; message index: 0: authorization not found"
        await self._place_and_fail()
        await self._run_until(lambda: self._grant_reads() == 1, "no re-check after the refusal")
        self.simulation_refusal = None

        await self._status_update()

        self.assertEqual(2, self._grant_reads())

    async def test_other_refusals_leave_the_grants_alone(self):
        await self._status_update()
        reads = self._grant_reads()
        self.broadcast_refusal = ("sdk", 5, "insufficient funds")

        await self._place_and_fail()
        await self._drain()

        self.assertTrue(self._grants_valid)
        self.assertEqual(reads, self._grant_reads())
        self.assertEqual(0, self._log_count(logging.WARNING, STOPPED_LOG))


def _refusal_test(stage: str, codespace: str, code: int, text: str):
    async def test(self):
        await self._assert_one_refused_tx_stops_trading(stage, codespace, code, text)

    return test


for _stage in ("simulation", "broadcast"):
    for _codespace, _code, _text in REFUSALS:
        setattr(
            BeezeeDelegationRefusalTests,
            f"test_{_stage}_refused_by_{_codespace}_{_code}_stops_trading_then_resumes",
            _refusal_test(_stage, _codespace, _code, _text),
        )


class BeezeeWalletModeGrantTests(BeezeeChainTestCase):
    async def test_wallet_mode_never_reads_the_grants(self):
        self.mock_api.post(
            f"{REST_URL}{CONSTANTS.BROADCAST_TX_PATH_URL}", repeat=True,
            payload={"tx_response": {"code": 0, "txhash": RESTED_TX}},
        )

        await self._status_update()
        self.exchange.sell("BZE-USDC", Decimal("1200"), OrderType.LIMIT, Decimal("0.0005"))
        await self._run_until(lambda: any(
            method == "POST" and url.path == CONSTANTS.BROADCAST_TX_PATH_URL for (method, url) in self.mock_api.requests
        ), "the order was not sent")
        await self._status_update()

        paths = [url.path for (_method, url) in self.mock_api.requests]
        self.assertFalse([path for path in paths if re.match(r"^/cosmos/(authz|feegrant)/", path)])
        self.assertNotIn("delegation_grants_valid", self.exchange.status_dict)
        self.assertIsNone(self.exchange.delegation_status)
