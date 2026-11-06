"""In-process GPU consumer API; not a registered vLLM KVConnector.

The caller owns the GPU allocation and its lifetime. Call from the worker
process, never pass a GPU virtual address to a remote agent process.
"""
import time
from callosum_runtime import Cuda, Log, Mapping, config, require, rpc


class PoolWorker:
    def __init__(self, config_path, node_id, log_path, device=0):
        self.config = config(config_path)
        self.node = next(n for n in self.config['nodes'] if n['id'] == node_id)
        self.mapping = Mapping(self.config, self.node)
        self.cuda = Cuda(device)
        self.log = Log(log_path, node_id)

    def load_into(self, key, gpu_pointer, capacity, request_id, tp_rank=0,
                  chunk_bytes=1024*1024):
        """Return only after GPU completion and checksum. Raise on any failure.

        gpu_pointer must address capacity contiguous bytes on this process's
        selected CUDA device. The caller must not free/reuse those bytes during
        the call. This byte-object API does not infer vLLM's layer/block layout.
        """
        require(type(gpu_pointer) is int and gpu_pointer > 0 and capacity > 0,
                'invalid caller-owned GPU destination')
        start = time.monotonic_ns()
        result = rpc(self.config['master'], 'fetch', dict(key=key,
            requester=self.node['id'], worker=self.node['id'], request_id=request_id,
            tp_rank=tp_rank, chunk_bytes=chunk_bytes), timeout=600)
        require(result['length'] <= capacity, 'GPU destination too small')
        self.mapping.bounds(result['offset'], result['length'])
        require(self.mapping.digest(result['offset'], result['length']) == result['checksum'],
                'consumer mapping cannot observe published object')
        result.update(self.cuda.copy(self.mapping.address + result['offset'],
                      result['length'], result['checksum'], destination=gpu_pointer))
        result['request_to_ready_ns'] = time.monotonic_ns() - start
        self.log.emit('gpu_ready', transfer_id=result['transfer_id'],
            request_id=request_id, tp_rank=tp_rank, bytes=result['length'],
            request_to_ready_ns=result['request_to_ready_ns'])
        return result
