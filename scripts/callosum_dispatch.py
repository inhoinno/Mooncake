"""Bounded FCFS source dispatch; endpoints retain responsibility for DMA drain.

This is an execution adapter for the existing experimental relay. It owns no
object directory, allocator or payload buffers. Admission and completion use
separate locks; a blocked demand does not hold the status lock.
"""
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import threading


def partition_source(plan):
    """Source-side fixed striping of authorized, still-unposted chunks.

    Receiver target ranges and source generation stay unchanged. Candidate IDs
    come from the authority's validated sender groups. This preserves the CXL
    baseline and does not claim multi-object or adaptive placement.
    """
    candidates = sorted(g['sender'] for g in plan['senders'])
    if not candidates or len(candidates) != len(set(candidates)):
        raise RuntimeError('invalid source candidate set')
    writes = sorted((w for g in plan['senders'] for w in g['writes']),
                    key=lambda w: w['source']['offset'])
    source = plan['source']
    cursor = source['offset']
    chunks = set()
    for w in writes:
        extent = w['source']
        if (extent['pool'] != source['pool'] or extent['offset'] != cursor or
                extent['length'] <= 0 or w['chunk'] in chunks):
            raise RuntimeError('source assignment gap, overlap or duplicate')
        cursor += extent['length']
        chunks.add(w['chunk'])
    if cursor != source['offset'] + source['length'] or chunks != set(range(len(writes))):
        raise RuntimeError('incomplete source object assignment')
    groups = [dict(sender=identity, writes=[]) for identity in candidates]
    for index,w in enumerate(writes):
        groups[index % len(groups)]['writes'].append(w)
    return dict(plan,senders=[g for g in groups if g['writes']])


class SourceDispatcher:
    def __init__(self, send, capacity=2, queue_limit=32):
        if type(capacity) is not int or capacity <= 0 or type(queue_limit) is not int or queue_limit <= 0:
            raise ValueError('positive dispatch capacity and queue bound required')
        self.send = send
        self.capacity, self.queue_limit = capacity, queue_limit
        self.condition = threading.Condition()
        self.queue, self.jobs = deque(), {}
        self.active = 0

    def status(self):
        with self.condition:
            return {key: job['state'] for key, job in self.jobs.items()}

    def execute(self, plan):
        identity = plan['transfer_id']
        fingerprint = hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()
        with self.condition:
            old = self.jobs.get(identity)
            if old:
                if old['fingerprint'] != fingerprint:
                    raise RuntimeError('conflicting dispatch replay')
                job = old
                while job['state'] in ('QUEUED', 'DISPATCHED'):
                    self.condition.wait()
                if job['state'] == 'DRAINED':
                    return job['result']
                raise RuntimeError('dispatch remains quarantined; inspect original failure')
            if len(self.queue) >= self.queue_limit:
                raise RuntimeError('source dispatch queue full; no work posted')
            job = dict(state='QUEUED', fingerprint=fingerprint)
            self.jobs[identity] = job
            self.queue.append(identity)
            while self.queue[0] != identity or self.active >= self.capacity:
                self.condition.wait()
            self.queue.popleft()
            self.active += 1
            job['state'] = 'DISPATCHED'
            self.condition.notify_all()
        try:
            # A plan has a bounded member set (two/four workers). Every launched
            # group is joined even when another fails. RPC uncertainty remains
            # quarantine, never physical drain proof.
            groups = plan['senders']
            if not groups or len(groups) > 4:
                raise RuntimeError('invalid source sender groups')
            with ThreadPoolExecutor(max_workers=len(groups)) as executor:
                pending = [executor.submit(self.send, plan, g) for g in groups]
                result, errors = [], []
                for future in pending:
                    try:
                        result.extend(future.result())
                    except Exception as error:
                        errors.append(error)
                if errors:
                    raise errors[0]
            with self.condition:
                job.update(state='DRAINED', result=result)
            return result
        except BaseException:
            with self.condition:
                job['state'] = 'QUARANTINED'
            raise
        finally:
            with self.condition:
                self.active -= 1
                self.condition.notify_all()
