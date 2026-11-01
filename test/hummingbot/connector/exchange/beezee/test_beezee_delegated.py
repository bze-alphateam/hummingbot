import json
import re
import unittest
from decimal import Decimal
from test.hummingbot.connector.exchange.beezee import (
    test_beezee_fills as fills,
    test_beezee_order_batching as batching,
    test_beezee_order_tracking as tracking,
    test_beezee_swaps as swaps,
)
from test.hummingbot.connector.exchange.beezee.test_beezee_order_batching import SentTx, _fields, _first, decode_tx
from test.hummingbot.connector.exchange.beezee.test_beezee_order_tracking import ADDRESS, REST_URL, TEST_MNEMONIC
from typing import Any, Dict, List

from aioresponses import aioresponses
from pydantic import SecretStr
from yarl import URL

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS, beezee_web_utils as web_utils
from hummingbot.connector.exchange.beezee.beezee_api_data_source import BeezeeAPIDataSource
from hummingbot.connector.exchange.beezee.beezee_exchange import BeezeeExchange
from hummingbot.connector.exchange.beezee.beezee_signer import BeezeeSigner
from hummingbot.connector.exchange.beezee.beezee_tx import (
    build_broadcast_request,
    build_signed_transaction,
    build_simulate_request,
    build_simulation_transaction,
    msg_exec,
)
from hummingbot.connector.exchange.beezee.beezee_utils import (
    BeezeeConfigMap,
    BeezeeCustomNetworkMode,
    BeezeeDelegatedAccountMode,
    BeezeeDelegatedMnemonicAccountMode,
    derive_private_key_from_mnemonic,
    market_from_data,
)
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.event.events import MarketEvent

MSG_EXEC = "/cosmos.authz.v1beta1.MsgExec"
# The bot key: another public BIP39 test mnemonic, and the address `bzed keys add --recover` (v8.2.0) gives for it. The
# granter is ADDRESS, the order owner of every fixture, so the wallet-mode fixtures replay unchanged.
BOT_MNEMONIC = "zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo wrong"
BOT_ADDRESS = "bze1am058pdux3hyulcmfgj4m3hhrlfn8nzm6rsvah"
METADATA = {
    "ubze": {"symbol": "BZE", "display": "bze", "denom_units": [{"denom": "ubze", "exponent": 0}, {"denom": "bze", "exponent": 6}]},
    "uusdc": {"symbol": "USDC", "display": "usdc", "denom_units": [{"denom": "uusdc", "exponent": 0}, {"denom": "usdc", "exponent": 6}]},
}


def valid_grants() -> Dict[str, Any]:
    """The authz grants response of a granter that gave the bot key every tradebin message, without expiry."""
    return {
        "grants": [
            {"authorization": {"@type": "/cosmos.authz.v1beta1.GenericAuthorization", "msg": message_type}, "expiration": None}
            for message_type in CONSTANTS.DELEGATED_MESSAGE_TYPE_URLS
        ],
        "pagination": {"next_key": None, "total": "4"},
    }


def grants_url() -> re.Pattern:
    return re.compile(rf"^{re.escape(REST_URL + CONSTANTS.AUTHZ_GRANTS_PATH_URL)}\?.*$")


def allowance_url() -> str:
    return f"{REST_URL}{CONSTANTS.FEE_ALLOWANCE_PATH_URL}/{ADDRESS}/{BOT_ADDRESS}"


def unlimited_allowance() -> Dict[str, Any]:
    return {"allowance": {"granter": ADDRESS, "grantee": BOT_ADDRESS, "allowance": {
        "@type": "/cosmos.feegrant.v1beta1.BasicAllowance", "spend_limit": [], "expiration": None,
    }}}


class DelegatedAccountMixin:
    """Runs a connector suite in delegated mode: the bot key signs for ADDRESS (the granter), which gave it every grant.
    After every test, each tx the connector simulated or broadcast must be one MsgExec from the bot key, with ADDRESS
    as the fee granter and as the creator of every inner message (decode_tx checks the creators)."""

    signer_address = BOT_ADDRESS

    def _account_type(self):
        return BeezeeDelegatedMnemonicAccountMode(mnemonic=BOT_MNEMONIC, granter_address=ADDRESS)

    def _mock_chain_state(self):
        super()._mock_chain_state()
        self.mock_api.get(grants_url(), repeat=True, payload=valid_grants())
        self.mock_api.get(allowance_url(), repeat=True, payload=unlimited_allowance())

    def _posted_txs(self) -> List[SentTx]:
        sent = []
        for path in (CONSTANTS.SIMULATE_TX_PATH_URL, CONSTANTS.BROADCAST_TX_PATH_URL):
            sent += [decode_tx(call.kwargs) for call in self.mock_api.requests.get(("POST", URL(f"{REST_URL}{path}")), [])]
        return sent

    def _assert_every_tx_is_delegated(self):
        for sent in self._posted_txs():
            self.assertEqual(([MSG_EXEC], BOT_ADDRESS, ADDRESS), (sent.outer_types, sent.exec_grantee, sent.fee_granter))
            self.assertTrue(sent.messages)

    async def asyncTearDown(self) -> None:
        self._assert_every_tx_is_delegated()
        await super().asyncTearDown()


# The wallet-mode suites, replayed in delegated mode: orders placed, filled and cancelled, batches, swaps and fills are
# tracked from the same fixtures, with the granter as creator and owner.

class BeezeeDelegatedOrderTrackingTests(DelegatedAccountMixin, tracking.BeezeeOrderTrackingTests):
    pass


class BeezeeDelegatedOrderBatchingTests(DelegatedAccountMixin, batching.BeezeeOrderBatchingTests):
    pass


class BeezeeDelegatedSwapTests(DelegatedAccountMixin, swaps.BeezeeSwapTests):
    pass


class BeezeeDelegatedFillTests(DelegatedAccountMixin, fills.BeezeeFillTests):
    pass


class BeezeeDelegatedFillTxTests(DelegatedAccountMixin, fills.BeezeeFillTxTests):
    pass


class BeezeeDelegatedConnectorTests(DelegatedAccountMixin, batching.BeezeeBatchingTestCase):
    """The delegated connector against the FakeNode."""

    def _account_requests(self, path: str) -> List[str]:
        return [url.path for (method, url) in self.mock_api.requests if method == "GET" and url.path.startswith(path)]

    def test_the_account_is_the_granter_and_the_bot_key_signs(self):
        self.assertEqual(ADDRESS, self.exchange._account_address)
        self.assertEqual(BOT_ADDRESS, self.exchange._signer.address)
        self.assertEqual(
            f"account {ADDRESS} (delegated: bot key {BOT_ADDRESS}, gas paid by the account's feegrant)",
            self.exchange.account_description,
        )

    async def test_mixed_batch_goes_out_as_one_msg_exec_in_message_order(self):
        stale = [self._resting("OID-OLD-1", "BZE-USDC", "000000000000000006810001", "10000000", "0.0004")]
        await self._restore_resting(stale)

        created = self.exchange.batch_order_create(self._ladder(2))
        self.exchange.batch_order_cancel([
            LimitOrder(order.client_order_id, "BZE-USDC", False, "BZE", "USDC", order.price, order.amount) for order in stale
        ])
        await self._settle()

        self.assertEqual(1, len(self.node.landed))
        for sent in (self.node.simulated[-1], self.node.landed[0][1]):
            self.assertEqual([MSG_EXEC], sent.outer_types)
            self.assertEqual((BOT_ADDRESS, ADDRESS), (sent.exec_grantee, sent.fee_granter))
            self.assertEqual(["cancel", "create", "create"], sent.kinds)
        self.assertEqual(0, self.node.simulated[-1].gas_limit)  # the simulation's fee is empty but for the granter
        await self._run_until(lambda: "OID-OLD-1" in self._event_ids(MarketEvent.OrderCancelled), "not cancelled")
        await self._run_until(
            lambda: all(len(self._exchange_id(order.client_order_id) or "") == 24 for order in created), "not rested"
        )
        for order in created:
            self._assert_rests_as_sent(order.client_order_id)

    async def test_sequence_is_read_for_the_bot_key_and_balances_for_the_granter(self):
        self.exchange.batch_order_create(self._ladder(1))
        await self._settle()
        await self.exchange._update_balances()

        accounts = self._account_requests(CONSTANTS.ACCOUNT_INFO_PATH_URL)
        self.assertTrue(accounts)
        self.assertEqual({f"{CONSTANTS.ACCOUNT_INFO_PATH_URL}/{BOT_ADDRESS}"}, set(accounts))
        self.assertEqual(
            {f"{CONSTANTS.BALANCES_PATH_URL}/{ADDRESS}"}, set(self._account_requests(CONSTANTS.BALANCES_PATH_URL))
        )

    async def test_gas_of_every_accepted_tx_is_counted(self):
        self.exchange.batch_order_create(self._ladder(1))
        await self._settle()

        # The fee of the tx, ceil(gas limit x 0.01) ubze, is paid by the granter's feegrant and counted as before.
        self.assertEqual(1, len(self.node.landed))
        fee = (Decimal(self.node.landed[0][1].gas_limit) * Decimal("0.01")).to_integral_value(rounding="ROUND_CEILING")
        self.assertEqual(fee / Decimal("1000000"), self.exchange.gas_fees_paid())


class BeezeeDelegatedExchangeConfigTests(unittest.TestCase):
    def test_bot_key_equal_to_the_granter_is_refused_when_the_key_is_read(self):
        # The config check skips a bot key that is still encrypted; the connector checks it again once decrypted.
        account = BeezeeDelegatedAccountMode.model_construct(
            name="delegated", private_key=SecretStr(derive_private_key_from_mnemonic(TEST_MNEMONIC)), granter_address=ADDRESS,
        )
        config = BeezeeConfigMap.model_construct(
            connector="beezee",
            network=BeezeeCustomNetworkMode(rest_endpoint=REST_URL, rpc_endpoint="https://rpc.test", chain_id="beezee-1"),
            account_type=account, batch_window_ms=0, max_messages_per_tx=CONSTANTS.DEFAULT_MAX_MESSAGES_PER_TX,
            denom_labels={},
        )

        with self.assertRaisesRegex(ValueError, "is the bot key's own address"):
            BeezeeExchange(connector_configuration=config, trading_pairs=["BZE-USDC"])


class BeezeeDelegatedDataSourceTests(unittest.IsolatedAsyncioTestCase):
    """The data source in delegated mode against mocked REST."""

    def setUp(self) -> None:
        self.bot = BeezeeSigner(derive_private_key_from_mnemonic(BOT_MNEMONIC), "bze")
        self.data_source = BeezeeAPIDataSource(
            api_factory=web_utils.build_api_factory(throttler=AsyncThrottler(CONSTANTS.RATE_LIMITS)),
            rest_endpoint=REST_URL,
            chain_id="beezee-1",
            address_prefix="bze",
            native_denom="ubze",
            gas_price=Decimal("0.01"),
            signer=self.bot,
            gas_adjustment=Decimal("1.3"),
            granter_address=ADDRESS,
        )
        self.market = market_from_data({"base": "ubze", "quote": "uusdc"}, METADATA)
        self.simulate_url = f"{REST_URL}{CONSTANTS.SIMULATE_TX_PATH_URL}"
        self.broadcast_url = f"{REST_URL}{CONSTANTS.BROADCAST_TX_PATH_URL}"

    @staticmethod
    def _creator(message: bytes) -> str:
        return _first(_fields(_first(_fields(message), 2)), 1).decode()

    @staticmethod
    def _request_bodies(mock_api: aioresponses, url: str) -> List[Dict[str, Any]]:
        return [json.loads(call.kwargs["data"]) for call in mock_api.requests.get(("POST", URL(url)), [])]

    def test_bot_key_is_the_bzed_address_of_its_mnemonic(self):
        self.assertEqual(BOT_ADDRESS, self.bot.address)
        self.assertEqual(ADDRESS, self.data_source.account_address)
        self.assertTrue(self.data_source.is_delegated)

    def test_every_message_kind_is_created_by_the_granter(self):
        messages = [
            self.data_source.create_order_message(self.market, "buy", Decimal("2"), Decimal("0.5")),
            self.data_source.cancel_order_message("ubze/uusdc", "sell", "000000000000000000000042"),
            self.data_source.multi_swap_message("ubze_uusdc", "uusdc", 1000, "ubze", 990),
            self.data_source.fill_orders_message(self.market, "sell", [("0.5", 1000)]),
        ]

        self.assertEqual([ADDRESS] * 4, [self._creator(message) for message in messages])

    @aioresponses()
    async def test_simulate_sign_and_broadcast_sends_one_msg_exec_from_the_bot_key(self, mock_api):
        messages = [
            self.data_source.cancel_order_message("ubze/uusdc", "sell", "000000000000000000000042"),
            self.data_source.create_order_message(self.market, "buy", Decimal("2"), Decimal("0.5")),
            self.data_source.multi_swap_message("ubze_uusdc", "uusdc", 1000, "ubze", 990),
            self.data_source.fill_orders_message(self.market, "sell", [("0.5", 1000)]),
        ]
        mock_api.get(
            f"{REST_URL}{CONSTANTS.ACCOUNT_INFO_PATH_URL}/{BOT_ADDRESS}",
            payload={"account": {"account_number": "9", "sequence": "4"}},
        )
        mock_api.post(self.simulate_url, payload={"gas_info": {"gas_used": "100001"}})
        mock_api.post(self.broadcast_url, payload={"tx_response": {"code": 0, "txhash": "AB" * 32}})

        await self.data_source.simulate_sign_and_broadcast(messages, memo="hbot:OID1")

        wrapped = [msg_exec(BOT_ADDRESS, messages)]
        self.assertEqual(
            [build_simulate_request(build_simulation_transaction(self.bot.public_key_bytes, 4, wrapped, memo="hbot:OID1", fee_granter=ADDRESS))],
            self._request_bodies(mock_api, self.simulate_url),
        )
        # gas limit = ceil(100001 * 1.3) = 130002; fee = ceil(130002 * 0.01) = 1301 ubze, paid through the granter.
        expected_tx = build_signed_transaction(
            signer=self.bot, chain_id="beezee-1", account_number=9, sequence=4, messages=wrapped, fee_denom="ubze",
            fee_amount="1301", gas_limit=130002, memo="hbot:OID1", fee_granter=ADDRESS,
        )
        self.assertEqual([build_broadcast_request(expected_tx)], self._request_bodies(mock_api, self.broadcast_url))
        for call in [*mock_api.requests[("POST", URL(self.simulate_url))], *mock_api.requests[("POST", URL(self.broadcast_url))]]:
            sent = decode_tx(call.kwargs)
            self.assertEqual(([MSG_EXEC], BOT_ADDRESS, ADDRESS), (sent.outer_types, sent.exec_grantee, sent.fee_granter))
            self.assertEqual(["cancel", "create", "swap", "fill"], sent.kinds)
            self.assertEqual(4, sent.sequence)
        self.assertEqual(Decimal("0.001301"), self.data_source.gas_fees_paid)

    @aioresponses()
    async def test_balances_dust_and_open_orders_are_read_for_the_granter(self, mock_api):
        mock_api.get(f"{REST_URL}{CONSTANTS.BALANCES_PATH_URL}/{ADDRESS}", payload={"balances": [{"denom": "ubze", "amount": "5"}]})
        mock_api.get(
            re.compile(rf"^{re.escape(REST_URL + CONSTANTS.ALL_USER_DUST_PATH_URL)}\?address={ADDRESS}$"), payload={"list": []}
        )
        mock_api.get(
            re.compile(rf"^{re.escape(REST_URL)}/bze/tradebin/user_market_orders/{ADDRESS}(\?.*)?$"),
            payload={"list": [{"id": "1", "market_id": "ubze/uusdc"}], "pagination": {"next_key": None}},
        )

        self.assertEqual([{"denom": "ubze", "amount": "5"}], await self.data_source.get_balances())
        self.assertEqual([], await self.data_source.get_user_dust())
        self.assertEqual([{"id": "1", "market_id": "ubze/uusdc"}], await self.data_source.get_user_market_orders())
