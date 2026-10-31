"""Readable labels for BeeZee denoms, the names used in trading pairs, balances, logs and status.

The BeeZee dapps (dex.getbze.com and the others) name a token from its chain-registry asset list first: an IBC token
by the entry's `symbol`, a native or factory token by its `display` unit, both upper-cased, with the decimals of the
display unit. A token missing from the list is named from its on-chain bank metadata. The bot follows the same order:

1. the connector config's `denom_labels` override, if the denom is in it;
2. the asset list snapshot below for the chain id;
3. the denom's bank metadata (its symbol, else its display unit, else the last part of the denom).

Labels from 1 and 2 are verified: they must be unique on the chain, and the connector refuses to start when two
denoms get the same one. A metadata label is chosen by whoever creates the token, so it never takes a verified label
or another denom's label: such a token is labelled `<label>_<4 hex of its denom's sha1>` instead, with a warning.
"""
import hashlib
import logging
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_utils import BeezeeToken, token_from_metadata


@dataclass(frozen=True)
class RegistryAsset:
    label: str
    exponent: int
    name: str


# Snapshot of the chain-registry asset list the dapps are built with: cosmos/chain-registry `beezee/assetlist.json`
# for mainnet, the dapps' own testnet list for testnet. Add a token here when it joins the list; until then the
# `denom_labels` override names it.
ASSET_LISTS: Dict[str, Dict[str, RegistryAsset]] = {
    CONSTANTS.MAINNET_CHAIN_ID: {
        "ubze": RegistryAsset("BZE", 6, "BeeZee"),
        "factory/bze13gzq40che93tgfm9kzmkpjamah5nj0j73pyhqk/uvdl": RegistryAsset("VDL", 6, "Vidulum"),
        "factory/bze15pqjgk4la0mfphwddce00d05n3th3u66n3ptcv/2MARS": RegistryAsset("C2M", 6, "Crypto2Mars"),
        "factory/bze12gyp30f29zg26nuqrwdhl26ej4q066pt572fhm/GGE": RegistryAsset("GGE", 6, "GEEGEE"),
        "factory/bze1972aqfzdg29ugjln74edx0xvcg4ehvysjptk77/CTL": RegistryAsset("CTL", 6, "CryptoTrtl"),
        "ibc/ED07A3391A112B175915CD8FAF43A2DA8E4790EDE12566649D0C2F97716B8518": RegistryAsset("OSMO", 6, "Osmosis"),  # noqa: mock
        "ibc/693DDB2D9B4260D67C8136C22D837F37488E0FBD81857D8E9C6022332EA26E33": RegistryAsset("LMN", 6, "Lumen"),  # noqa: mock
        "ibc/C00D101A3572A5374E23F11944463D1325319E255D7C824B33AF2F927F532348": RegistryAsset("ARCH", 18, "Archway"),  # noqa: mock
        "ibc/6490A7EAB61059BFC1CDDEB05917DD70BDF3A611654162A1A47DB930D40D8AF4": RegistryAsset("USDC.N", 6, "Noble USDC"),  # noqa: mock
        "ibc/08EAEAB525E59C611D5BD8FAC4BE65DF65A69E62874377F6889BBD01A33F385F": RegistryAsset("SPICE", 6, "Spice"),  # noqa: mock
        "ibc/4AA3B163580B4377250CD4486FB6AD098EB27822EE056949EC4A39B09C5E3B4E": RegistryAsset("JKL", 6, "Jackal"),  # noqa: mock
        "ibc/FF39851E73089ACBD0B09BDF62FA3C67FBD77A2CD97CD159DBCE9C770561F8AF": RegistryAsset("FLIX", 6, "Flix"),  # noqa: mock
        "ibc/02EE50AB3A4B7540FA001B24CB75E688016F65547CABE885EA184338440080B2": RegistryAsset("SHERPA", 6, "Sherpa"),  # noqa: mock
        "ibc/B2219CA05421EA988F83E5E824BF69362E80A6A87503EFD18B92C91C9E03763D": RegistryAsset("ATONE", 6, "Atone"),  # noqa: mock
        "ibc/12C0B8B561AFCFDA3C73DEE0F7F84AA2B860D48493C27E8E81A5D14724FAB08B": RegistryAsset("PHMN.LEGACY", 6, "POSTHUMAN"),  # noqa: mock
        "ibc/82931180F0962712BFABB189083A94C77EA90E269ABFC6D310AF922B2B14E011": RegistryAsset("PHOTON", 6, "Photon"),  # noqa: mock
        "ibc/81DA528F4C5546208D1D50F84C2D0B388B45D37AA36FCA622A3CB2D8EF014838": RegistryAsset("USDC.INJ", 6, "Injective USDC"),  # noqa: mock
        "ibc/9D0AC973BAFA293781F9A86B2911069DF9D8ECE86CDD884F0DE4889AF666B28A": RegistryAsset("INJ", 18, "Injective"),  # noqa: mock
        "ibc/5FEB332D2B121921C792F1A0DBF7C3163FF205337B4AFE6E14F69E8E49545F49": RegistryAsset("ATOM", 6, "Cosmos Hub"),  # noqa: mock
        "ibc/062CA1F72787DB36CE8FEA8EC733293215A5554646650A9B92BFA498802A11AA": RegistryAsset("DGN", 6, "Dragon Coin"),  # noqa: mock
    },
    CONSTANTS.TESTNET_CHAIN_ID: {
        "ubze": RegistryAsset("TBZE", 6, "BeeZee"),
        "ibc/9DA252F9F9C86132CC282EA431DFB7DE7729501F6DC9A3E0F50EC8C6EE380CC7": RegistryAsset("LMN", 6, "Lumen"),  # noqa: mock
        "factory/bze1z3mkcr2jz424w6m49frgjmy9uhlrx69phqwg3l/vidulum": RegistryAsset("VDL", 6, "Vidulum"),
    },
}


class DenomLabelError(ValueError):
    pass


def clean_label(label: str) -> str:
    """Upper-cased, without spaces, and with `_` for `-` (Hummingbot splits a pair name on `-`)."""
    return "".join(str(label).split()).upper().replace("-", "_")


class BeezeeDenomLabels:
    """Gives each denom its token (label and decimals); a denom keeps its label for the whole run.

    `asset_list` maps a denom to its registry entry (ASSET_LISTS of the chain id, or nothing); `overrides` maps a denom
    to the label the user wants. Raises DenomLabelError when two of these verified labels are the same.
    """

    def __init__(self, asset_list: Optional[Mapping[str, RegistryAsset]] = None, overrides: Optional[Mapping[str, str]] = None):
        self._asset_list = dict(asset_list or {})
        self._overrides = {denom: clean_label(label) for denom, label in (overrides or {}).items()}
        verified = {denom: asset.label for denom, asset in self._asset_list.items()}
        verified.update(self._overrides)
        denoms_by_label: Dict[str, list] = {}
        for denom, label in verified.items():
            if not label:
                raise DenomLabelError(f"Beezee denom label for '{denom}' is empty. Set a label in `denom_labels`.")
            denoms_by_label.setdefault(label, []).append(denom)
        clashes = {label: sorted(denoms) for label, denoms in denoms_by_label.items() if len(denoms) > 1}
        if clashes:
            details = "; ".join(f"'{label}' for {', '.join(denoms)}" for label, denoms in sorted(clashes.items()))
            raise DenomLabelError(
                f"Beezee denom labels must be unique on the chain, but several denoms got the same label: {details}. "
                f"Give one of them another label under `denom_labels` in the beezee connector config."
            )
        self._verified = verified
        self._denom_by_label: Dict[str, str] = {label: denom for denom, label in verified.items()}
        # Label given to each unverified denom, kept so a denom keeps its label for the whole run.
        self._unverified: Dict[str, str] = {}

    @classmethod
    def logger(cls):
        return logging.getLogger(__name__)

    @classmethod
    def for_chain(cls, chain_id: str, overrides: Optional[Mapping[str, str]] = None) -> "BeezeeDenomLabels":
        return cls(ASSET_LISTS.get(chain_id), overrides)

    def is_verified(self, denom: str) -> bool:
        return denom in self._verified

    def token(self, denom: str, metadata: Optional[Dict[str, Any]]) -> BeezeeToken:
        """The denom's token. `metadata` (its bank metadata, or None) gives the decimals of a denom the asset list
        does not have and the label of a denom that is not verified."""
        token = token_from_metadata(denom, metadata)
        asset = self._asset_list.get(denom)
        if asset is not None:
            token = BeezeeToken(denom=denom, symbol=asset.label, display_name=asset.name,
                                display_denom=token.display_denom, exponent=asset.exponent)
        return BeezeeToken(**{**token.__dict__, "symbol": self.label(denom, token.symbol)})

    def label(self, denom: str, metadata_label: str) -> str:
        if denom in self._verified:
            return self._verified[denom]
        if denom in self._unverified:
            return self._unverified[denom]
        label = clean_label(metadata_label)
        if self._denom_by_label.get(label, denom) != denom:
            taken_by = self._denom_by_label[label]
            label = f"{label}_{hashlib.sha1(denom.encode()).hexdigest()[:4].upper()}"
            self.logger().warning(
                f"Beezee denom '{denom}' has the label of '{taken_by}' in its metadata; it is labelled '{label}' "
                f"instead. Set its label under `denom_labels` in the beezee connector config to change it."
            )
        self._denom_by_label[label] = denom
        self._unverified[denom] = label
        return label
