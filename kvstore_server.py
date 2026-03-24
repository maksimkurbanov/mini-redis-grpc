import asyncio
from collections import OrderedDict
from datetime import datetime, UTC, timedelta

import grpc.aio

import kvstore_pb2_grpc
import kvstore_pb2


CAPACITY = 10


class KeyValueStoreServicer(kvstore_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, capacity):
        self.capacity = capacity
        self.store = OrderedDict()

    def _is_alive(self, data: dict) -> bool:
        if data["ttl"] == 0:
            return True
        return data["updated_at"] + timedelta(seconds=data["ttl"]) > datetime.now(UTC)

    async def Put(self, request, context):
        self.store[request.key] = {
            "value": request.value,
            "updated_at": datetime.now(UTC),
            "ttl": request.ttl_seconds,
        }
        self.store.move_to_end(request.key)
        if len(self.store) > self.capacity:
            self.store.popitem(last=False)
        return kvstore_pb2.PutResponse()

    async def Get(self, request, context):
        elem = self.store.get(request.key)
        if elem and self._is_alive(elem):
            self.store.move_to_end(request.key)
            return kvstore_pb2.GetResponse(value=elem["value"])
        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details(f"Entry with key {request.key} not found")

    async def Delete(self, request, context):
        del self.store[request.key]
        return kvstore_pb2.DeleteResponse()

    async def List(self, request, context):
        result = []
        for k, v in self.store.items():
            if request.prefix in k and self._is_alive(v):
                result.append(kvstore_pb2.KeyValue(key=k, value=v["value"]))
        return kvstore_pb2.ListResponse(items=result)


async def serve():
    server = grpc.aio.server()
    kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(
        KeyValueStoreServicer(CAPACITY), server
    )
    listen_address = "[::]:8000"
    server.add_insecure_port(listen_address)
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
