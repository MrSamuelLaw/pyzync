import time
import asyncio
from typing import (Generator, TypeAlias, Iterable, Literal)

import humanize

from pyzync import logging
from pyzync.interfaces import SnapshotNode

logger = logging.get_logger(__name__)

BytesConsumer: TypeAlias = Generator[None, bytes, None]
ExitStatus: TypeAlias = Literal['SUCCESS', 'FAILURE']


class SnapshotStreamBase:

    def __init__(self):
        self.total_seconds: float = 0.0
        self.total_bytes_transfered: int = 0
        self.exit_status = 'FAILURE'
        self.exit_exception = None

    def sync2async_gen_wrapper(self, callable_, *args, **kwargs):
        try:
            chunk = callable_(*args, **kwargs)
            return chunk
        except StopIteration:
            raise StopAsyncIteration


class SnapshotStreamProducer(SnapshotStreamBase):

    def __init__(self, /, node: SnapshotNode, generator: Iterable[bytes]):
        super().__init__()
        self.node = node
        self.generator = generator
        self.l = logger.bind(generator=generator)

    async def produce_chunk(self) -> bytes | None:
        try:
            start = time.time()
            chunk = await asyncio.to_thread(self.sync2async_gen_wrapper, next, self.generator)
            end = time.time()
            nbytes = 0 if chunk is None else len(chunk)
            nseconds = 0 if chunk is None else end - start
            self.l.debug('Produced bytes', nbytes=humanize.naturalsize(nbytes), nseconds=f'{nseconds:.2f}')
            self.total_bytes_transfered += nbytes
            self.total_seconds += nseconds
            return chunk
        except StopAsyncIteration:
            self.exit_status = 'SUCCESS'
            return
        except Exception as e:
            self.exit_exception = e
            self.l.exception("Failed to produce bytes")
            raise


class SnapshotStreamConsumer(SnapshotStreamBase):

    def __init__(self, /, node: SnapshotNode, generator: BytesConsumer):
        super().__init__()
        self.node = node
        self.generator = generator
        self.l = logger.bind(generator=generator, node=node)
        next(self.generator)  # prime the consumer

    async def consume_chunk(self, chunk: bytes):
        try:
            start = time.time()
            await asyncio.to_thread(self.sync2async_gen_wrapper, self.generator.send, chunk)
            end = time.time()
            nbytes = 0 if chunk is None else len(chunk)
            nseconds = 0 if chunk is None else end - start
            self.l.debug('Consumed bytes', nbytes=humanize.naturalsize(nbytes), nseconds=nseconds.__round__(3))
            self.total_bytes_transfered += nbytes
            self.total_seconds += nseconds
        except StopAsyncIteration:
            self.exit_status = 'SUCCESS'
            return
        except Exception as e:
            self.exit_exception = e
            self.l.exception("Failed to consume bytes")
            raise


class SnapshotStreamManager:

    def __init__(self, producer: SnapshotStreamProducer, consumers: list[SnapshotStreamConsumer]):
        self.producer = producer
        self.consumers = list(consumers)
        self.l = logger.bind()

        if any((c.node != self.producer.node for c in self.consumers)):
            raise ValueError(
                'All SnapshotStreamConsumer SnapshotNodes must match SnapshotStreamProducer SnapshotNodes'
            )

    async def transmit(self):
        chunk = await self.producer.produce_chunk()
        consumers = self.consumers
        while chunk is not None:
            chunk, *exceptions = await asyncio.gather(self.producer.produce_chunk(),
                                                      *[c.consume_chunk(chunk) for c in consumers],
                                                      return_exceptions=True)
            consumers = [c for c, e in zip(consumers, exceptions) if e is None]
        # shut down the consumers
        await asyncio.gather(*[c.consume_chunk(chunk) for c in consumers])

        # publish final statuses
        if self.producer.exit_exception:
            self.l.error("Producer exited exceptionally", producer_generator=self.producer.generator)
        for c in self.consumers:
            if c.exit_exception:
                self.l.error("Consumer completed exceptionally", consumer_generator=c.generator)
