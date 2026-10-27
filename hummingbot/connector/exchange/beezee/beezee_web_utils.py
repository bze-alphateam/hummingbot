import time
from typing import Optional

from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str) -> str:
    return f"{domain.rstrip('/')}{path_url}"


def private_rest_url(path_url: str, domain: str) -> str:
    return public_rest_url(path_url=path_url, domain=domain)


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = "",
) -> float:
    return time.time()


def build_api_factory(throttler: Optional[AsyncThrottler] = None, auth=None) -> WebAssistantsFactory:
    return WebAssistantsFactory(throttler=throttler, auth=auth)
