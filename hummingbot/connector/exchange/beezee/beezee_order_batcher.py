import asyncio
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, Iterable, List, Optional, Set

from hummingbot.connector.exchange.beezee.beezee_utils import BeezeeMarket


@dataclass
class BatchedOrderMessage:
    """One create or cancel message waiting for its batch tx. `result` resolves to the tx hash, or to the error.

    A fill (MsgFillOrders) is queued as a create: `fill_client_order_ids` are its entries' orders, in entry order,
    and `client_order_id` is the first of them.
    """

    client_order_id: str
    is_cancel: bool
    message: bytes
    market: BeezeeMarket
    memo: str
    result: asyncio.Future = field(default_factory=lambda: asyncio.get_event_loop().create_future())
    fill_client_order_ids: List[str] = field(default_factory=list)

    def resolve(self, tx_hash: str):
        if not self.result.done():
            self.result.set_result(tx_hash)

    def reject(self, error: BaseException):
        if not self.result.done():
            self.result.set_exception(error)


class BeezeeCreateWithdrawnError(IOError):
    """The order was cancelled while its create message still waited for its tx: nothing was sent."""

    def __init__(self, client_order_id: str):
        super().__init__(f"Beezee order {client_order_id} was cancelled before its create was sent.")


BatchSender = Callable[[List[BatchedOrderMessage]], Awaitable[None]]


class BeezeeOrderBatcher:
    """Coalesces create and cancel messages into batches of at most `max_messages` messages, cancels first.

    A batch is sent `window_s` after its first message, at once when it is full (the rest keeps waiting), or at once
    when every order of an explicit batch call (`expect`) has been submitted or given up. Batches are sent one after the other by `send`,
    which must resolve or reject every message it gets.
    """

    def __init__(self, send: BatchSender, sleep: Callable[[float], Awaitable[None]], window_s: float, max_messages: int):
        self._send = send
        self._sleep = sleep
        self._window_s = window_s
        self._max_messages = max_messages
        self._cancels: Dict[str, BatchedOrderMessage] = {}
        self._creates: Dict[str, BatchedOrderMessage] = {}
        self._expected: Set[str] = set()
        self._explicit = False
        self._flush_everything = False
        self._window_task: Optional[asyncio.Task] = None
        self._flush_task: Optional[asyncio.Task] = None

    def expect(self, client_order_ids: Iterable[str]):
        """The orders of an explicit batch call: their batch goes out as soon as all of them are submitted."""
        self._expected.update(client_order_ids)
        self._explicit = bool(self._expected)

    def settle(self, client_order_id: str):
        """An expected order that will not be submitted (refused before signing, or nothing to cancel yet)."""
        if client_order_id in self._expected:
            self._expected.discard(client_order_id)
            self._flush_if_ready()

    async def submit(self, item: BatchedOrderMessage) -> str:
        queue = self._cancels if item.is_cancel else self._creates
        queued = queue.get(item.client_order_id)
        if queued is None:
            queue[item.client_order_id] = item
            self._expected.discard(item.client_order_id)
            self._flush_if_ready()
        else:
            # The same order's message is already waiting: one message, one outcome.
            item = queued
        return await asyncio.shield(item.result)

    def withdraw_create(self, client_order_id: str) -> bool:
        """Takes a create still waiting for its tx out of the queue, failing it with BeezeeCreateWithdrawnError.
        False when there is none: the create was never submitted, or its batch is already being sent."""
        item = self._creates.pop(client_order_id, None)
        if item is None:
            return False
        item.reject(BeezeeCreateWithdrawnError(client_order_id))
        return True

    def stop(self):
        """Fails the messages still waiting for their tx. A batch already being sent is left to finish: its tx may be
        on chain already, and only its own send knows its outcome."""
        if self._window_task is not None:
            self._window_task.cancel()
            self._window_task = None
        error = IOError("Beezee connector stopped before the order message was sent.")
        for item in list(self._cancels.values()) + list(self._creates.values()):
            item.reject(error)
        self._cancels.clear()
        self._creates.clear()
        self._expected.clear()
        self._explicit = False
        self._flush_everything = False

    @property
    def _queued(self) -> int:
        return len(self._cancels) + len(self._creates)

    def _flush_if_ready(self):
        if self._explicit and not self._expected:
            self._explicit = False
            if self._queued:
                self._start_flush(everything=True)
                return
        if self._queued >= self._max_messages:
            # Full txs go out at once; the rest keeps waiting for the window.
            self._start_flush(everything=False)
        if self._queued and self._window_task is None and not self._flush_everything:
            self._window_task = asyncio.ensure_future(self._flush_after_window())

    # Plain tasks: a task cancelled before it starts must not leave a never-awaited coroutine behind, and _flush
    # handles every error itself.
    def _start_flush(self, everything: bool):
        if everything:
            if self._window_task is not None:
                self._window_task.cancel()
                self._window_task = None
            self._flush_everything = True
        if self._flush_task is None:
            self._flush_task = asyncio.ensure_future(self._flush())

    async def _flush_after_window(self):
        await self._sleep(self._window_s)
        self._window_task = None
        self._start_flush(everything=True)

    async def _flush(self):
        try:
            # Messages arriving while everything is being sent go out right after, in the next tx.
            while self._queued >= self._max_messages or self._flush_everything and self._queued:
                batch = self._next_batch()
                try:
                    await self._send(batch)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    for item in batch:
                        item.reject(error)
        finally:
            if self._flush_task is asyncio.current_task():
                self._flush_task = None
                self._flush_everything = False
                self._flush_if_ready()

    def _next_batch(self) -> List[BatchedOrderMessage]:
        batch: List[BatchedOrderMessage] = []
        for queue in (self._cancels, self._creates):
            while queue and len(batch) < self._max_messages:
                batch.append(queue.pop(next(iter(queue))))
        return batch
