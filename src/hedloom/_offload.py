"""Small, bounded daemon lanes owned by one Runtime's controller loop.

Daemon ownership matters here: a wedged filesystem operation must not prevent
the process exiting and its owner-bound children being reclaimed. A lane is not
a general executor API; at most its worker count of calls enter synchronous
work. Waiting requests retain coroutine data, just as waiting Runs do.
"""
from __future__ import annotations

import asyncio
from functools import partial
from heapq import heappop, heappush
from itertools import count
from queue import Queue
from threading import Thread


class Offload:
    def __init__(self, workers: int, name: str):
        self.loop = asyncio.get_running_loop()
        self.workers = workers
        self.busy = 0
        self.waiting = []
        self.sequence = count()
        self.queue = Queue()
        self.closed = False
        self.pending = 0
        self.idle = asyncio.Event()
        self.idle.set()
        self.threads = [Thread(target=self._worker, name=f'hedloom-{name}-{n}',
                               daemon=True) for n in range(workers)]
        for thread in self.threads:
            thread.start()

    async def __call__(self, function, *args, **kwargs):
        return await self.prioritized(0, function, *args, **kwargs)

    async def prioritized(self, priority, function, *args, **kwargs):
        if self.closed:
            raise RuntimeError('offload lane is closed')
        self.pending += 1
        self.idle.clear()
        future = self.loop.create_future()
        heappush(self.waiting, (-priority, next(self.sequence),
                              partial(function, *args, **kwargs), future))
        self._feed()
        # Cancelling an observer cannot free a slot still held by synchronous
        # work. Only delivery from the worker releases execution capacity.
        return await asyncio.shield(future)

    def _feed(self):
        while self.busy < self.workers and self.waiting:
            _, _, function, future = heappop(self.waiting)
            self.busy += 1
            self.queue.put((function, future))

    def _worker(self):
        while True:
            item = self.queue.get()
            if item is None:
                return
            function, future = item
            try:
                value, error = function(), None
            except BaseException as caught:
                value, error = None, caught
            try:
                self.loop.call_soon_threadsafe(self._deliver, future, value, error)
            except RuntimeError:
                return  # The owning process/loop has already exited.

    def _deliver(self, future, value, error):
        self.pending -= 1
        self.busy -= 1
        self._feed()
        if not self.pending:
            self.idle.set()
        if not future.done():
            if error is None:
                future.set_result(value)
            else:
                future.set_exception(error)

    async def close(self):
        self.closed = True
        await self.idle.wait()
        for _ in self.threads:
            self.queue.put(None)
        for thread in self.threads:
            thread.join()
