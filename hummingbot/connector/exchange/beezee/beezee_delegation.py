import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Coroutine, Dict, List, Optional, Protocol, Set, Tuple

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_api_data_source import BeezeeTxRefusedError
from hummingbot.core.utils.async_utils import safe_ensure_future

README_SECTION = '"When grants are revoked, expire or run out" in README_BZE.md'
ALLOWANCE_ITEM = "feegrant allowance"


class BeezeeGrantReader(Protocol):
    """What the grant checks read from the chain (the data source in a delegated mode)."""

    @property
    def native_denom(self) -> str:
        ...

    @property
    def native_symbol(self) -> str:
        ...

    async def get_authz_grants(self) -> List[Dict[str, Any]]:
        ...

    async def get_fee_allowance(self) -> Optional[Dict[str, Any]]:
        ...

    def native_amount_to_display(self, amount: int) -> Decimal:
        ...


def parse_timestamp(value: Optional[str]) -> Optional[float]:
    """An RFC3339 timestamp of the chain (nanoseconds allowed) as a unix time; None for an empty value."""
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parts = re.match(r"^(.*T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(.*)$", text)
    if parts is not None:
        fraction = f".{(parts.group(2) or '')[:6].ljust(6, '0')}"
        text = parts.group(1) + fraction + parts.group(3)
    moment = datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.timestamp()


def format_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def delegation_refusal_reason(error: BeezeeTxRefusedError) -> Optional[str]:
    """The grant problem a refusal reports, or None when the refusal is about something else.

    A broadcast refusal carries its codespace and code. A simulation refusal carries only the node's text, which holds
    the module error's text (the fee decorator wraps the feegrant error, keeping it)."""
    if error.codespace is not None and error.code is not None:
        known = CONSTANTS.DELEGATION_REFUSALS.get(error.codespace, {}).get(error.code)
        if known is not None:
            return known
    message = error.chain_message.lower()
    if CONSTANTS.FEE_GRANT_NOT_FOUND_MESSAGE in message:
        return CONSTANTS.DELEGATION_REFUSALS["feegrant"][5]
    for texts in CONSTANTS.DELEGATION_REFUSALS.values():
        for text in texts.values():
            if text in message:
                return text
    return None


@dataclass
class BeezeeGrantsReport:
    """What one read of the grants found."""

    problems: List[str] = field(default_factory=list)
    # Item (a message type, or the feegrant allowance) -> when it expires, for the items that have an expiry.
    expiries: Dict[str, float] = field(default_factory=dict)
    # Base units of the native denom the allowance can still spend; None when it is not limited.
    allowance_left: Optional[int] = None


def _native_amount(coins: Optional[List[Dict[str, Any]]], denom: str) -> int:
    return sum(int(coin.get("amount") or 0) for coin in coins or [] if coin.get("denom") == denom)


def evaluate_grants(
    grants: List[Dict[str, Any]], allowance: Optional[Dict[str, Any]], native_denom: str, now: float
) -> BeezeeGrantsReport:
    """Checks the granter's grants to the bot key against what a delegated tx needs (see README_BZE.md)."""
    report = BeezeeGrantsReport()
    live: Dict[str, Optional[float]] = {}
    expired: Set[str] = set()
    for grant in grants:
        authorization = grant.get("authorization") or {}
        if authorization.get("@type") != CONSTANTS.GENERIC_AUTHORIZATION_TYPE_URL:
            continue
        message_type = authorization.get("msg")
        expiration = parse_timestamp(grant.get("expiration"))
        # The chain prunes an expired grant only in a later block: until then it is listed but refused.
        if expiration is not None and expiration <= now:
            expired.add(message_type)
        else:
            live[message_type] = expiration
    missing = [message_type for message_type in CONSTANTS.DELEGATED_MESSAGE_TYPE_URLS if message_type not in live]
    expired_required = [message_type for message_type in missing if message_type in expired]
    if expired_required:
        report.problems.append(f"expired authz grants for {', '.join(expired_required)}")
    absent = [message_type for message_type in missing if message_type not in expired]
    if absent:
        report.problems.append(f"missing authz grants for {', '.join(absent)}")
    for message_type, expiration in live.items():
        if expiration is not None and message_type in CONSTANTS.DELEGATED_MESSAGE_TYPE_URLS:
            report.expiries[message_type] = expiration
    _evaluate_allowance(report, allowance, native_denom, now)
    return report


def _evaluate_allowance(report: BeezeeGrantsReport, allowance: Optional[Dict[str, Any]], native_denom: str, now: float):
    if allowance is None:
        report.problems.append("no feegrant allowance from the account to the bot key")
        return
    if allowance.get("@type") == CONSTANTS.ALLOWED_MSG_ALLOWANCE_TYPE_URL:
        # The chain checks the tx's outer messages, and a delegated tx holds one MsgExec.
        if CONSTANTS.MSG_EXEC_TYPE_URL not in (allowance.get("allowed_messages") or []):
            report.problems.append(f"the feegrant allowance does not allow {CONSTANTS.MSG_EXEC_TYPE_URL}")
        allowance = allowance.get("allowance") or {}
    allowance_type = allowance.get("@type")
    if allowance_type == CONSTANTS.BASIC_ALLOWANCE_TYPE_URL:
        basic = allowance
    elif allowance_type == CONSTANTS.PERIODIC_ALLOWANCE_TYPE_URL:
        basic = allowance.get("basic") or {}
    else:
        report.problems.append(f"unsupported feegrant allowance type {allowance_type}")
        return
    expiration = parse_timestamp(basic.get("expiration"))
    if expiration is not None and expiration <= now:
        report.problems.append(f"the feegrant allowance expired on {format_timestamp(expiration)}")
    elif expiration is not None:
        report.expiries[ALLOWANCE_ITEM] = expiration
    # An empty spend limit is unlimited.
    left = _native_amount(basic["spend_limit"], native_denom) if basic.get("spend_limit") else None
    period_reset = None
    if allowance_type == CONSTANTS.PERIODIC_ALLOWANCE_TYPE_URL:
        period_reset = parse_timestamp(allowance.get("period_reset"))
        # Once the period is over the chain refills what the period can spend on the next use.
        if period_reset is not None and period_reset <= now:
            period_left = _native_amount(allowance.get("period_spend_limit"), native_denom)
        else:
            period_left = _native_amount(allowance.get("period_can_spend"), native_denom)
        left = period_left if left is None else min(left, period_left)
    report.allowance_left = left
    if left is not None and left <= 0:
        until = f" until the period resets on {format_timestamp(period_reset)}" if period_reset is not None else ""
        report.problems.append(f"the feegrant allowance has no {native_denom} left to spend{until}")


class BeezeeDelegation:
    """The grant checks of the delegated modes.

    The granter's authz grants (one per tradebin message) and its feegrant allowance to the bot key are read before
    the connector reports ready, every `check_interval` seconds after that, and once right after the chain refuses a
    tx for a grant. While they are invalid no tx is sent (`check_tx_allowed`). A refusal keeps them invalid until a
    periodic check finds them valid, so a refusal costs one tx and is never retried in a loop.
    """

    def __init__(
        self,
        reader: BeezeeGrantReader,
        clock: Callable[[], float],
        spawn: Callable[[Coroutine], asyncio.Future] = safe_ensure_future,
        check_interval: float = CONSTANTS.DELEGATION_CHECK_INTERVAL,
        expiry_warning: float = CONSTANTS.DELEGATION_EXPIRY_WARNING,
    ):
        self._reader = reader
        self._clock = clock
        self._spawn = spawn
        self._check_interval = check_interval
        self._expiry_warning = expiry_warning
        # None until the first successful read.
        self._valid: Optional[bool] = None
        self._reason: Optional[str] = None
        self._held_by_refusal = False
        self._last_check: Optional[float] = None
        self._read_failing = False
        self._warned_expiries: Set[str] = set()
        self._earliest_expiry: Optional[Tuple[str, float]] = None
        self._allowance_left: Optional[int] = None
        self._recheck_task: Optional[asyncio.Future] = None

    @classmethod
    def logger(cls):
        return logging.getLogger(__name__)

    @property
    def grants_valid(self) -> bool:
        """True once a read found every grant valid, and as long as no later read or refusal found otherwise."""
        return self._valid is True

    @property
    def status_text(self) -> str:
        if self._valid is None:
            return "Delegated grants: not checked yet."
        if not self._valid:
            return f"Delegated grants INVALID: {self._reason}. Trading is stopped until they are fixed."
        parts = ["Delegated grants: valid"]
        if self._earliest_expiry is not None:
            item, expiration = self._earliest_expiry
            parts.append(f"earliest expiry {format_timestamp(expiration)} ({item})")
        else:
            parts.append("no expiry")
        if self._allowance_left is not None:
            amount = self._reader.native_amount_to_display(self._allowance_left)
            parts.append(f"feegrant allowance left {amount.normalize():f} {self._reader.native_symbol}")
        return ", ".join(parts) + "."

    def is_check_due(self) -> bool:
        if self._valid is None:
            # Until the first read succeeds the connector is not ready: every status update tries again.
            return True
        return self._clock() - self._last_check >= self._check_interval

    async def check_if_due(self):
        if self.is_check_due():
            await self.check()

    async def check(self, resume: bool = True):
        """Reads the grants and updates the state. A read failure keeps the last known state. With `resume` False
        (the check right after a refusal) the state can turn invalid or get a better reason, but not valid again."""
        now = self._clock()
        try:
            grants = await self._reader.get_authz_grants()
            allowance = await self._reader.get_fee_allowance()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._last_check = now
            if not self._read_failing:
                self.logger().warning(
                    f"Failed to read the Beezee delegated grants. Keeping the last known state "
                    f"({'valid' if self._valid else 'not valid'}). Error: {error}"
                )
                self._read_failing = True
            return
        self._last_check = now
        self._read_failing = False
        report = evaluate_grants(grants, allowance, self._reader.native_denom, now)
        self._allowance_left = report.allowance_left
        self._earliest_expiry = min(report.expiries.items(), key=lambda item: item[1]) if report.expiries else None
        self._warn_expiring(report, now)
        if report.problems:
            self._set_invalid("; ".join(report.problems))
        elif resume or not self._held_by_refusal:
            self._held_by_refusal = False
            self._set_valid()

    def check_tx_allowed(self):
        if self._valid is False:
            raise BeezeeTxRefusedError(CONSTANTS.DELEGATION_STAGE, f"delegated trading is stopped: {self._reason}")

    def on_tx_refused(self, error: BeezeeTxRefusedError):
        problem = delegation_refusal_reason(error)
        if problem is None:
            return
        self._held_by_refusal = True
        self._set_invalid(f"the chain refused a tx in {error.stage} ({problem}): {error.chain_message}")
        if self._recheck_task is None or self._recheck_task.done():
            self._recheck_task = self._spawn(self.check(resume=False))

    def stop(self):
        if self._recheck_task is not None:
            self._recheck_task.cancel()
            self._recheck_task = None

    def _set_invalid(self, reason: str):
        self._reason = reason
        if self._valid is False:
            return
        self._valid = False
        self.logger().warning(
            f"Beezee delegated trading stopped: {reason}. The connector is not ready and sends no orders, cancels, swaps "
            f"or fills; resting orders stay on the book (cancel them from the account's own wallet if needed). Fix the "
            f"grants and trading resumes by itself within {self._check_interval / 60:g} minutes. See {README_SECTION}."
        )

    def _set_valid(self):
        self._reason = None
        if self._valid is True:
            return
        resumed = self._valid is False
        self._valid = True
        if resumed:
            self.logger().info("Beezee delegated trading resumed: the authz grants and the feegrant allowance are valid.")
        else:
            self.logger().info("Beezee delegated grants checked: the authz grants and the feegrant allowance are valid.")

    def _warn_expiring(self, report: BeezeeGrantsReport, now: float):
        for item, expiration in sorted(report.expiries.items(), key=lambda entry: entry[1]):
            if item in self._warned_expiries or expiration - now > self._expiry_warning:
                continue
            self._warned_expiries.add(item)
            name = "the feegrant allowance" if item == ALLOWANCE_ITEM else f"the authz grant for {item}"
            self.logger().warning(
                f"Beezee delegated trading: {name} expires on {format_timestamp(expiration)}. Renew it before then, or "
                f"the bot stops trading. See {README_SECTION}."
            )
