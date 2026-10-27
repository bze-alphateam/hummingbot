from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class BeezeeAuth(AuthBase):
    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request

