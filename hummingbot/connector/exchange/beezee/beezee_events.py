import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS


@dataclass(frozen=True)
class TradebinEvent:
    """A typed `bze.tradebin.*` event with its attribute values decoded.

    Typed (protobuf) events JSON-encode their attribute values, so a string field arrives as `"sell"`.
    """

    type: str
    attributes: Dict[str, str]

    def get(self, key: str, default: str = "") -> str:
        return self.attributes.get(key, default)


def _decode_attribute_value(value: Any) -> str:
    if not isinstance(value, str):
        return str(value)
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        return value
    return decoded if isinstance(decoded, str) else value


def tradebin_events(raw_events: Iterable[Dict[str, Any]]) -> List[TradebinEvent]:
    """Returns the tradebin events of a raw ABCI event list, in order."""
    events: List[TradebinEvent] = []
    for raw_event in raw_events or []:
        event_type = raw_event.get("type", "")
        if not event_type.startswith(CONSTANTS.TRADEBIN_EVENT_PREFIX):
            continue
        attributes = {
            attribute.get("key", ""): _decode_attribute_value(attribute.get("value", ""))
            for attribute in raw_event.get("attributes") or []
        }
        events.append(TradebinEvent(type=event_type, attributes=attributes))
    return events


def tx_tradebin_events(tx_response: Dict[str, Any]) -> List[TradebinEvent]:
    """Returns the tradebin events of a `/cosmos/tx/v1beta1/txs/{hash}` tx_response."""
    return tradebin_events(tx_response.get("events") or [])


def block_tradebin_events(block_results: Dict[str, Any]) -> List[TradebinEvent]:
    """Returns the tradebin events of an RPC `/block_results` result: the tx results first, then the
    finalize-block events (where the EndBlock order queue emits its outcomes)."""
    raw_events: List[Dict[str, Any]] = []
    for tx_result in block_results.get("txs_results") or []:
        raw_events.extend(tx_result.get("events") or [])
    raw_events.extend(block_results.get("finalize_block_events") or [])
    return tradebin_events(raw_events)
