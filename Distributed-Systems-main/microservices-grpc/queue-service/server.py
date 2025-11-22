
import grpc
from concurrent import futures
import time
import os
import redis
import queue_pb2
import queue_pb2_grpc


class QueueServiceServicer(queue_pb2_grpc.QueueServiceServicer):
    def __init__(self):
        redis_host = os.environ.get('REDIS_HOST', 'localhost')
        redis_port = int(os.environ.get('REDIS_PORT', 6379))
        self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=False)
        self.queue_key = 'queue'
        self.history_key = 'history'

    def AddTrack(self, request, context):
        # Serialize Track to bytes (now includes duration)
        self.redis.rpush(self.queue_key, request.SerializeToString())
        queue = self._get_queue()
        return queue_pb2.QueueResponse(message="Track added", queue=queue)

    def RemoveTrack(self, request, context):
        # Remove all tracks with matching id
        queue = self._get_queue()
        new_queue = [t for t in queue if t.id != request.id]
        # Clear and re-add
        self.redis.delete(self.queue_key)
        for t in new_queue:
            self.redis.rpush(self.queue_key, t.SerializeToString())
        return queue_pb2.QueueResponse(message="Track removed", queue=new_queue)

    def VoteTrack(self, request, context):
        queue = self._get_queue()
        for t in queue:
            if t.id == request.id:
                t.votes += 1 if request.up else -1
        # Sort by votes descending
        queue.sort(key=lambda x: -x.votes)
        # Save back to Redis
        self.redis.delete(self.queue_key)
        for t in queue:
            self.redis.rpush(self.queue_key, t.SerializeToString())
        return queue_pb2.QueueResponse(message="Vote updated", queue=queue)

    def GetQueue(self, request, context):
        queue = self._get_queue()
        return queue_pb2.QueueList(queue=queue)

    def GetMetadata(self, request, context):
        queue = self._get_queue()
        for t in queue:
            if t.id == request.id:
                return t
        return queue_pb2.Track()  # empty

    def PlayNext(self, request, context):
        # Pop from queue
        data = self.redis.lpop(self.queue_key)
        if not data:
            return queue_pb2.Track()  # empty
        track = queue_pb2.Track()
        track.ParseFromString(data)
        # Add to history
        self.redis.rpush(self.history_key, track.SerializeToString())
        return track

    def GetHistory(self, request, context):
        history = self._get_history()
        return queue_pb2.QueueList(queue=history)

    def _get_queue(self):
        # Get all tracks from Redis queue (now includes duration)
        data_list = self.redis.lrange(self.queue_key, 0, -1)
        queue = []
        for data in data_list:
            t = queue_pb2.Track()
            t.ParseFromString(data)
            queue.append(t)
        return queue

    def _get_history(self):
        data_list = self.redis.lrange(self.history_key, 0, -1)
        history = []
        for data in data_list:
            t = queue_pb2.Track()
            t.ParseFromString(data)
            history.append(t)
        return history

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    queue_pb2_grpc.add_QueueServiceServicer_to_server(QueueServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("gRPC QueueService running on port 50051...")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
