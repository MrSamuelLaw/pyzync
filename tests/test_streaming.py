import asyncio
import unittest
from pyzync.interfaces import SnapshotNode
from pyzync.streaming import SnapshotStreamProducer, SnapshotStreamConsumer, SnapshotStreamManager


class TestAsyncSnapshotStreams(unittest.IsolatedAsyncioTestCase):

    def test_cannot_stream_different_nodes(self):

        def produce():
            for x in b'send':
                yield bytes([x])

        def consume(id_: str):
            while True:
                chunk = yield
                if chunk is None:
                    return

        node0 = SnapshotNode(dt='20250505T120000', dataset_id='tank/foo')
        node1 = SnapshotNode(dt='20250505T120001', dataset_id='tank/foo')

        with self.assertRaisesRegex(
                ValueError,
                'All SnapshotStreamConsumer SnapshotNodes must match SnapshotStreamProducer SnapshotNodes'
        ):
            SnapshotStreamManager(producer=SnapshotStreamProducer(node=node0, generator=produce()),
                                  consumers=[
                                      SnapshotStreamConsumer(node=node1, generator=consume('c00')),
                                      SnapshotStreamConsumer(node=node0, generator=consume('c01'))
                                  ])

    async def test_async_publish_method(self):

        def produce0():
            for x in b'send':
                yield bytes([x])

        def produce1():
            for x in b'send00000':
                yield bytes([x])

        def consume(id_: str, fail: bool = False):
            i = 0
            while True:
                i += 1
                chunk = yield
                if chunk is None:
                    return
                # print(id_, chunk, flush=True)
                if fail and i > 1:
                    raise ValueError('Somekind of error!')

        node = SnapshotNode(dt='20250505T120000', dataset_id='tank/foo')
        managers = [
            SnapshotStreamManager(producer=SnapshotStreamProducer(node=node, generator=produce0()),
                                  consumers=[
                                      SnapshotStreamConsumer(node=node,
                                                             generator=consume('c00', fail=True)),
                                      SnapshotStreamConsumer(node=node, generator=consume('c01'))
                                  ]),
            SnapshotStreamManager(producer=SnapshotStreamProducer(node=node, generator=produce1()),
                                  consumers=[
                                      SnapshotStreamConsumer(node=node, generator=consume('c10')),
                                      SnapshotStreamConsumer(node=node, generator=consume('c11'))
                                  ])
        ]
        await asyncio.gather(*[m.transmit() for m in managers])
        self.assertEqual(managers[0].consumers[0].exit_status, 'FAILURE')
        self.assertEqual(managers[0].consumers[1].exit_status, 'SUCCESS')
        self.assertEqual(managers[1].consumers[0].exit_status, 'SUCCESS')
        self.assertEqual(managers[1].consumers[1].exit_status, 'SUCCESS')
        self.assertEqual(managers[0].consumers[0].total_bytes_transfered, 1)
        self.assertEqual(managers[0].consumers[1].total_bytes_transfered, 4)
        self.assertEqual(managers[1].consumers[0].total_bytes_transfered, 9)
        self.assertEqual(managers[1].consumers[1].total_bytes_transfered, 9)


if __name__ == '__main__':
    unittest.main(verbosity=2)
