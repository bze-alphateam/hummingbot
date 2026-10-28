import asyncio
import base64
import json
import re
import unittest
from decimal import Decimal
from typing import Any, Dict, List
from unittest.mock import AsyncMock

from aioresponses import CallbackResult, aioresponses
from yarl import URL

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS, beezee_web_utils as web_utils
from hummingbot.connector.exchange.beezee.beezee_api_data_source import BeezeeAPIDataSource, BeezeeTxRefusedError
from hummingbot.connector.exchange.beezee.beezee_signer import BeezeeSigner
from hummingbot.connector.exchange.beezee.beezee_tx import (
    build_broadcast_request,
    build_signed_transaction,
    build_simulate_request,
    build_simulation_transaction,
    msg_create_order,
)
from hummingbot.connector.exchange.beezee.beezee_utils import derive_private_key_from_mnemonic
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.trade_fee import TokenAmount


class BeezeeAPIDataSourceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.data_source = BeezeeAPIDataSource(
            api_factory=AsyncMock(),
            rest_endpoint="https://rest.getbze.com",
            chain_id="beezee-1",
            address_prefix="bze",
            native_denom="ubze",
            gas_price=Decimal("0.025"),
            account_address="bze1test",
        )

    async def test_get_all_markets_builds_symbol_map_and_market_cache(self):
        self.data_source._paged_request = AsyncMock(return_value=[{"base": "ubze", "quote": "uusdc"}])
        metadata_by_denom = {
            "ubze": {
                "symbol": "BZE",
                "display": "bze",
                "denom_units": [{"denom": "ubze", "exponent": 0}, {"denom": "bze", "exponent": 6}],
            },
            "uusdc": {
                "symbol": "USDC",
                "display": "usdc",
                "denom_units": [{"denom": "uusdc", "exponent": 0}, {"denom": "usdc", "exponent": 6}],
            },
        }
        # Denoms are fetched in set order, which varies with the hash seed.
        self.data_source.get_denom_metadata = AsyncMock(side_effect=lambda denom: metadata_by_denom[denom])

        markets = await self.data_source.get_all_markets(refresh=True)

        self.assertIn("ubze/uusdc", markets)
        self.assertEqual("BZE-USDC", (await self.data_source.get_symbol_map())["ubze/uusdc"])

    async def test_get_last_traded_price_prefers_history(self):
        market = AsyncMock()
        market.market_id = "ubze/uusdc"
        market.chain_price_scaler = Decimal("1")
        self.data_source.get_all_markets = AsyncMock(return_value={"ubze/uusdc": market})
        self.data_source.get_market_history = AsyncMock(return_value=[{"price": "2.5"}])

        price = await self.data_source.get_last_traded_price("ubze/uusdc")

        self.assertEqual(Decimal("2.5"), price)

    async def test_get_last_traded_price_falls_back_to_mid_price(self):
        market = AsyncMock()
        market.market_id = "ubze/uusdc"
        market.chain_price_scaler = Decimal("1")
        self.data_source.get_all_markets = AsyncMock(return_value={"ubze/uusdc": market})
        self.data_source.get_market_history = AsyncMock(return_value=[])
        self.data_source.get_order_book = AsyncMock(
            return_value={"bids": [{"price": "2.0"}], "asks": [{"price": "4.0"}]}
        )

        price = await self.data_source.get_last_traded_price("ubze/uusdc")

        self.assertEqual(Decimal("3"), price)

    async def test_get_last_traded_price_returns_none_for_empty_order_book(self):
        market = AsyncMock()
        market.market_id = "ubze/uusdc"
        market.chain_price_scaler = Decimal("1")
        self.data_source.get_all_markets = AsyncMock(return_value={"ubze/uusdc": market})
        self.data_source.get_market_history = AsyncMock(return_value=[])
        self.data_source.get_order_book = AsyncMock(return_value={"bids": [], "asks": []})

        price = await self.data_source.get_last_traded_price("ubze/uusdc")

        self.assertIsNone(price)

    def test_candidate_order_ids_filters_by_market_and_side(self):
        candidate_ids = self.data_source.candidate_order_ids(
            refs=[
                {"id": "0002", "market_id": "ubze/uusdc", "order_type": "buy"},
                {"id": "0001", "market_id": "ubze/uusdc", "order_type": "buy"},
                {"id": "0003", "market_id": "ubze/uusdc", "order_type": "sell"},
                {"id": "9999", "market_id": "uatom/uusdc", "order_type": "buy"},
            ],
            market_id="ubze/uusdc",
            order_type="buy",
        )

        self.assertEqual(["0002", "0001"], candidate_ids)

    def test_fingerprint_history_trade_is_stable(self):
        trade = {
            "executed_at": "1710000000",
            "market_id": "ubze/uusdc",
            "order_type": "buy",
            "amount": "1000000",
            "price": "2.5",
            "maker": "bze1maker",
            "taker": "bze1taker",
        }

        self.assertEqual(
            self.data_source.fingerprint_history_trade(trade),
            self.data_source.fingerprint_history_trade(dict(trade)),
        )


TEST_MNEMONIC = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"
REST_URL = "https://rest.getbze.com"
BZE_METADATA = {
    "symbol": "BZE",
    "display": "bze",
    "denom_units": [{"denom": "ubze", "exponent": 0}, {"denom": "bze", "exponent": 6}],
}


class BeezeeAPIDataSourceHttpTests(unittest.IsolatedAsyncioTestCase):
    """Request/response tests against mocked REST endpoints."""

    def setUp(self) -> None:
        self.signer = BeezeeSigner(derive_private_key_from_mnemonic(TEST_MNEMONIC), "bze")
        self.data_source = BeezeeAPIDataSource(
            api_factory=web_utils.build_api_factory(throttler=AsyncThrottler(CONSTANTS.RATE_LIMITS)),
            rest_endpoint=REST_URL,
            chain_id="beezee-1",
            address_prefix="bze",
            native_denom="ubze",
            gas_price=Decimal("0.01"),
            signer=self.signer,
            gas_adjustment=Decimal("1.3"),
        )
        self.messages = [msg_create_order(self.signer.address, "buy", "2000000", "0.5", "ubze/uusdc")]
        self.account_url = f"{REST_URL}{CONSTANTS.ACCOUNT_INFO_PATH_URL}/{self.signer.address}"
        self.simulate_url = f"{REST_URL}{CONSTANTS.SIMULATE_TX_PATH_URL}"
        self.broadcast_url = f"{REST_URL}{CONSTANTS.BROADCAST_TX_PATH_URL}"

    def _mock_account(self, mock_api: aioresponses, sequence: int):
        mock_api.get(self.account_url, payload={"account": {"account_number": "7", "sequence": str(sequence)}})

    @staticmethod
    def _request_bodies(mock_api: aioresponses, url: str) -> List[Dict[str, Any]]:
        calls = mock_api.requests.get(("POST", URL(url)), [])
        return [json.loads(call.kwargs["data"]) for call in calls]

    @aioresponses()
    async def test_get_trade_fees_parses_v820_coin_params(self, mock_api):
        mock_api.get(
            f"{REST_URL}{CONSTANTS.TRADEBIN_PARAMS_PATH_URL}",
            payload={"params": {
                "createMarketFee": {"denom": "ubze", "amount": "25000000000"},
                "marketMakerFee": {"denom": "ubze", "amount": "1000"},
                "marketTakerFee": {"denom": "ubze", "amount": "100000"},
                "native_denom": "ubze",
            }},
        )
        mock_api.get(f"{REST_URL}{CONSTANTS.DENOM_METADATA_PATH_URL}/ubze", payload={"metadata": BZE_METADATA})

        fees = await self.data_source.get_trade_fees()

        self.assertEqual(TokenAmount("BZE", Decimal("0.001")), fees["maker"])
        self.assertEqual(TokenAmount("BZE", Decimal("0.1")), fees["taker"])

    @aioresponses()
    async def test_get_trade_fees_accepts_legacy_string_params(self, mock_api):
        mock_api.get(
            f"{REST_URL}{CONSTANTS.TRADEBIN_PARAMS_PATH_URL}",
            payload={"params": {"market_maker_fee": "1000ubze", "market_taker_fee": "100000ubze"}},
        )
        mock_api.get(f"{REST_URL}{CONSTANTS.DENOM_METADATA_PATH_URL}/ubze", payload={"metadata": BZE_METADATA})

        fees = await self.data_source.get_trade_fees()

        self.assertEqual(TokenAmount("BZE", Decimal("0.001")), fees["maker"])
        self.assertEqual(TokenAmount("BZE", Decimal("0.1")), fees["taker"])

    @aioresponses()
    async def test_get_trade_fees_converts_with_the_fee_denom_exponent(self, mock_api):
        mock_api.get(
            f"{REST_URL}{CONSTANTS.TRADEBIN_PARAMS_PATH_URL}",
            payload={"params": {
                "marketMakerFee": {"denom": "factory/bze1x/mill", "amount": "5000"},
                "marketTakerFee": {"denom": "ubze", "amount": "0"},
            }},
        )
        mock_api.get(
            f"{REST_URL}{CONSTANTS.DENOM_METADATA_PATH_URL}/factory%2Fbze1x%2Fmill",
            payload={"metadata": {
                "symbol": "MILL",
                "display": "mill",
                "denom_units": [{"denom": "factory/bze1x/mill", "exponent": 0}, {"denom": "mill", "exponent": 3}],
            }},
        )

        fees = await self.data_source.get_trade_fees()

        self.assertEqual(TokenAmount("MILL", Decimal("5")), fees["maker"])
        self.assertIsNone(fees["taker"])

    @aioresponses()
    async def test_simulate_sign_and_broadcast_uses_simulated_gas_and_fee(self, mock_api):
        self._mock_account(mock_api, sequence=3)
        mock_api.post(self.simulate_url, payload={"gas_info": {"gas_wanted": "0", "gas_used": "100001"}})
        mock_api.post(self.broadcast_url, payload={"tx_response": {"code": 0, "txhash": "AB" * 32}})

        response = await self.data_source.simulate_sign_and_broadcast(self.messages, memo="hbot:OID1")

        self.assertEqual("AB" * 32, response["tx_response"]["txhash"])
        simulated = self._request_bodies(mock_api, self.simulate_url)
        self.assertEqual(
            [build_simulate_request(build_simulation_transaction(self.signer.public_key_bytes, 3, self.messages, memo="hbot:OID1"))],
            simulated,
        )
        # gas limit = ceil(100001 * 1.3) = 130002; fee = ceil(130002 * 0.01) = 1301 ubze
        expected_tx = build_signed_transaction(
            signer=self.signer,
            chain_id="beezee-1",
            account_number=7,
            sequence=3,
            messages=self.messages,
            fee_denom="ubze",
            fee_amount="1301",
            gas_limit=130002,
            memo="hbot:OID1",
        )
        self.assertEqual([build_broadcast_request(expected_tx)], self._request_bodies(mock_api, self.broadcast_url))

    @aioresponses()
    async def test_simulation_error_is_never_broadcast(self, mock_api):
        self._mock_account(mock_api, sequence=3)
        mock_api.post(
            self.simulate_url,
            status=400,
            payload={"code": 2, "message": "failed to execute message; message index: 0: amount should be bigger than: 4: invalid order amount", "details": []},
        )

        with self.assertRaises(IOError) as context:
            await self.data_source.simulate_sign_and_broadcast(self.messages)

        self.assertIn("amount should be bigger than: 4", str(context.exception))
        self.assertEqual([], self._request_bodies(mock_api, self.broadcast_url))

    @aioresponses()
    async def test_simulation_sequence_mismatch_resets_and_retries_once(self, mock_api):
        self._mock_account(mock_api, sequence=3)
        self._mock_account(mock_api, sequence=4)
        mock_api.post(
            self.simulate_url,
            status=400,
            payload={"code": 2, "message": "account sequence mismatch, expected 4, got 3: incorrect account sequence", "details": []},
        )
        mock_api.post(self.simulate_url, payload={"gas_info": {"gas_used": "100000"}})
        mock_api.post(self.broadcast_url, payload={"tx_response": {"code": 0, "txhash": "CD" * 32}})

        response = await self.data_source.simulate_sign_and_broadcast(self.messages)

        self.assertEqual("CD" * 32, response["tx_response"]["txhash"])
        self.assertEqual(2, len(self._request_bodies(mock_api, self.simulate_url)))
        expected_tx = build_signed_transaction(
            signer=self.signer,
            chain_id="beezee-1",
            account_number=7,
            sequence=4,
            messages=self.messages,
            fee_denom="ubze",
            fee_amount="1300",
            gas_limit=130000,
        )
        self.assertEqual([build_broadcast_request(expected_tx)], self._request_bodies(mock_api, self.broadcast_url))

    @aioresponses()
    async def test_second_sequence_mismatch_is_not_retried_again(self, mock_api):
        mismatch = {"code": 2, "message": "account sequence mismatch, expected 9, got 3: incorrect account sequence", "details": []}
        self._mock_account(mock_api, sequence=3)
        self._mock_account(mock_api, sequence=3)
        mock_api.post(self.simulate_url, status=400, payload=mismatch)
        mock_api.post(self.simulate_url, status=400, payload=mismatch)

        with self.assertRaises(IOError):
            await self.data_source.simulate_sign_and_broadcast(self.messages)

        self.assertEqual(2, len(self._request_bodies(mock_api, self.simulate_url)))
        self.assertEqual([], self._request_bodies(mock_api, self.broadcast_url))

    @aioresponses()
    async def test_refused_broadcast_is_flagged_as_denom_halted(self, mock_api):
        self._mock_account(mock_api, sequence=3)
        mock_api.post(self.simulate_url, payload={"gas_info": {"gas_used": "100000"}})
        mock_api.post(
            self.broadcast_url,
            payload={"tx_response": {"code": 4017, "codespace": "tradebin", "raw_log": "market ubze/uusdc is halted: denom is halted", "txhash": ""}},
        )

        with self.assertRaises(BeezeeTxRefusedError) as context:
            await self.data_source.simulate_sign_and_broadcast(self.messages)

        self.assertTrue(context.exception.is_denom_halted)
        self.assertEqual(4017, context.exception.code)

    @aioresponses()
    async def test_get_halted_denoms_reads_all_pages(self, mock_api):
        url = re.compile(rf"^{re.escape(REST_URL + CONSTANTS.HALTED_DENOMS_PATH_URL)}(\?.*)?$")
        mock_api.get(url, payload={"denoms": ["uusdc.n"], "pagination": {"next_key": "AQ==", "total": "0"}})
        mock_api.get(url, payload={"denoms": ["factory/bze1x/halted"], "pagination": {"next_key": None, "total": "0"}})

        halted = await self.data_source.get_halted_denoms()

        self.assertEqual({"uusdc.n", "factory/bze1x/halted"}, halted)

    @aioresponses()
    async def test_concurrent_txs_get_consecutive_sequences(self, mock_api):
        # Node model: the account query returns committed state (sequence 3 until a block), while simulation and
        # CheckTx accept only the next sequence of the node's check state, which a broadcast advances. A bid and an
        # ask placed together must not race: the second may only simulate after the first is broadcast.
        check_state = {"sequence": 3}
        memos = ["hbot:BID", "hbot:ASK"]

        async def simulate(url, **kwargs):
            tx_bytes = base64.b64decode(json.loads(kwargs["data"])["tx_bytes"])
            expected = {
                build_simulation_transaction(self.signer.public_key_bytes, check_state["sequence"], self.messages, memo=memo)
                for memo in memos
            }
            # The node reads its state when the request arrives and answers after a round trip.
            await asyncio.sleep(0.01)
            if tx_bytes in expected:
                return CallbackResult(payload={"gas_info": {"gas_used": "100000"}})
            return CallbackResult(status=400, payload={
                "code": 2,
                "message": f"account sequence mismatch, expected {check_state['sequence']}: incorrect account sequence",
                "details": [],
            })

        def broadcast(url, **kwargs):
            check_state["sequence"] += 1
            return CallbackResult(payload={"tx_response": {"code": 0, "txhash": f"{check_state['sequence']:064d}"}})

        mock_api.get(self.account_url, payload={"account": {"account_number": "7", "sequence": "3"}}, repeat=True)
        mock_api.post(self.simulate_url, callback=simulate, repeat=True)
        mock_api.post(self.broadcast_url, callback=broadcast, repeat=True)

        responses = await asyncio.gather(*[
            self.data_source.simulate_sign_and_broadcast(self.messages, memo=memo) for memo in memos
        ])

        self.assertEqual({f"{4:064d}", f"{5:064d}"}, {response["tx_response"]["txhash"] for response in responses})
        self.assertEqual(2, len(self._request_bodies(mock_api, self.simulate_url)))
        self.assertEqual(1, len(mock_api.requests[("GET", URL(self.account_url))]))

    @aioresponses()
    async def test_non_json_simulation_error_resets_the_sequence(self, mock_api):
        self._mock_account(mock_api, sequence=3)
        self._mock_account(mock_api, sequence=3)
        mock_api.post(self.simulate_url, status=502, body="<html><body>502 Bad Gateway</body></html>", content_type="text/html")
        mock_api.post(self.simulate_url, payload={"gas_info": {"gas_used": "100000"}})
        mock_api.post(self.broadcast_url, payload={"tx_response": {"code": 0, "txhash": "EF" * 32}})

        with self.assertRaises(IOError) as context:
            await self.data_source.simulate_sign_and_broadcast(self.messages)
        await self.data_source.simulate_sign_and_broadcast(self.messages)

        self.assertIn("502 Bad Gateway", str(context.exception))
        expected_tx = build_signed_transaction(
            signer=self.signer,
            chain_id="beezee-1",
            account_number=7,
            sequence=3,
            messages=self.messages,
            fee_denom="ubze",
            fee_amount="1300",
            gas_limit=130000,
        )
        self.assertEqual([build_broadcast_request(expected_tx)], self._request_bodies(mock_api, self.broadcast_url))

    @aioresponses()
    async def test_refused_broadcast_resets_the_sequence(self, mock_api):
        # CheckTx refused the tx (not a sequence mismatch), so sequence 3 was never used on chain.
        self._mock_account(mock_api, sequence=3)
        self._mock_account(mock_api, sequence=3)
        mock_api.post(self.simulate_url, payload={"gas_info": {"gas_used": "100000"}}, repeat=True)
        mock_api.post(self.broadcast_url, payload={"tx_response": {
            "code": 5, "codespace": "sdk", "raw_log": "spendable balance 0ubze is smaller than 1300ubze: insufficient funds", "txhash": "",
        }})
        mock_api.post(self.broadcast_url, payload={"tx_response": {"code": 0, "txhash": "AA" * 32}})

        with self.assertRaises(BeezeeTxRefusedError) as context:
            await self.data_source.simulate_sign_and_broadcast(self.messages)
        await self.data_source.simulate_sign_and_broadcast(self.messages)

        self.assertIn("insufficient funds", str(context.exception))
        self.assertEqual(2, len(mock_api.requests[("GET", URL(self.account_url))]))
        signed_at_3 = build_signed_transaction(
            signer=self.signer,
            chain_id="beezee-1",
            account_number=7,
            sequence=3,
            messages=self.messages,
            fee_denom="ubze",
            fee_amount="1300",
            gas_limit=130000,
        )
        self.assertEqual([build_broadcast_request(signed_at_3)] * 2, self._request_bodies(mock_api, self.broadcast_url))
