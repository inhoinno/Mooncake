"""Worker-local initiation contract. No Dynamo wire API or serving hook.

The worker supplies exact keys/layouts and retains destination allocations.
The delivery adapter must return only after validation and access completion.
Unknown failures retain the allocation. Cancellation is not DMA revocation.
"""
from dataclasses import dataclass, field
import threading
import time


class CacheMiss(RuntimeError):
    """Authoritative lookup missed before payload submission or target access."""


class Superseded(RuntimeError):
    pass


@dataclass(frozen=True)
class Intent:
    request_id: str
    compatibility: str  # Worker-resolved opaque compatibility identity, not tokens.
    worker: str
    worker_epoch: str
    assignment_generation: int

    def __post_init__(self):
        if (not all(type(v) is str and v for v in
                    (self.request_id, self.compatibility, self.worker, self.worker_epoch))
                or type(self.assignment_generation) is not int
                or self.assignment_generation < 1):
            raise ValueError('invalid assignment identity')


@dataclass(frozen=True)
class Demand:
    consumer_id: str
    key: str
    layout_id: str
    length: int
    tp_rank: int = 0
    source_generation: str = ''  # Optional expected generation; never a source grant.

    def __post_init__(self):
        if (not all(type(v) is str and v for v in
                    (self.consumer_id, self.key, self.layout_id))
                or type(self.length) is not int or not 0 < self.length < 2**64
                or type(self.tp_rank) is not int or self.tp_rank < 0
                or type(self.source_generation) is not str):
            raise ValueError('invalid concrete worker demand')


@dataclass(frozen=True)
class Target:
    address: int
    capacity: int
    allocation_id: str
    owner: object = field(compare=False, repr=False)

    def __post_init__(self):
        if (type(self.address) is not int or type(self.capacity) is not int
                or self.address <= 0 or self.capacity <= 0
                or self.address + self.capacity >= 2**64
                or not isinstance(self.allocation_id, str) or not self.allocation_id
                or self.owner is None):
            raise ValueError('invalid retained destination allocation')


@dataclass(frozen=True)
class Delivery:
    phase: str  # host_validated or gpu_ready; neither means inference consumption.
    result: dict


class WorkerInitiator:
    """Bounded per-process assignment and consumer single flight.

    deliver(intent, demand, target) owns lookup, transport and validation. A
    successful Delivery means all writes/copies into target have completed.
    The caller must not recycle target.owner until release, nor before its
    dependent consumer finishes. One instance covers this worker's allocations.
    The lock is never held during delivery or while waiting for a duplicate.
    """
    def __init__(self, worker, epoch, deliver, max_entries=128):
        if (not worker or not epoch or not callable(deliver)
                or type(max_entries) is not int or max_entries < 1):
            raise ValueError('invalid worker initiation configuration')
        self.worker, self.epoch, self.deliver = worker, epoch, deliver
        self.max_entries = max_entries
        self.lock = threading.RLock()
        self.assignments, self.entries = {}, {}
        self.accepted_at = {}

    def _check(self, intent):
        if intent.worker != self.worker or intent.worker_epoch != self.epoch:
            raise Superseded('wrong worker or worker incarnation')
        if self.assignments.get(intent.request_id) != (intent, True):
            raise Superseded('assignment is absent, cancelled or superseded')

    def accept(self, intent):
        with self.lock:
            if intent.worker != self.worker or intent.worker_epoch != self.epoch:
                raise Superseded('wrong worker or worker incarnation')
            old = self.assignments.get(intent.request_id)
            if old:
                if old[0] == intent:
                    self._check(intent)
                    return  # Router intent and connector notification converge.
                if intent.assignment_generation <= old[0].assignment_generation:
                    raise Superseded('stale or conflicting assignment generation')
                self.cancel(old[0])
            elif len(self.assignments) >= self.max_entries:
                raise RuntimeError('assignment ledger full; refuse new demand')
            self.assignments[intent.request_id] = (intent, True)
            self.accepted_at[intent.request_id] = time.monotonic_ns()

    def cancel(self, intent):
        with self.lock:
            old = self.assignments.get(intent.request_id)
            if not old or old[0] != intent:
                raise Superseded('cannot cancel another assignment')
            self.assignments[intent.request_id] = (intent, False)
            for (assignment, _), entry in self.entries.items():
                if assignment == intent:
                    if entry['state'] == 'LOADING':
                        entry['state'] = 'CANCEL_PENDING'
                    elif entry['state'] == 'READY':
                        entry['state'] = 'CANCELLED_DRAINED'

    def load(self, intent, demand, target):
        identity = (intent, demand.consumer_id)
        with self.lock:
            self._check(intent)
            entry = self.entries.get(identity)
            if entry:
                if entry['state'] == 'RELEASED':
                    raise Superseded('consumer allocation was released')
                if (entry['demand'] != demand or entry['target'] != target
                        or entry['target'].owner is not target.owner):
                    raise ValueError('consumer replay changed demand or allocation')
                first = False
            else:
                if demand.length > target.capacity:
                    raise ValueError('destination is smaller than concrete demand')
                if len(self.entries) >= self.max_entries:
                    raise RuntimeError('consumer ledger full; refuse new demand')
                for previous in self.entries.values():
                    held = previous['target']
                    if held is not None and max(held.address, target.address) < min(
                            held.address + held.capacity, target.address + target.capacity):
                        raise ValueError('destination aliases a retained consumer allocation')
                entry = dict(demand=demand, target=target, state='LOADING',
                             event=threading.Event(), error=None, delivery=None,
                             start_ns=time.monotonic_ns(),
                             accepted_ns=self.accepted_at[intent.request_id])
                self.entries[identity] = entry
                first = True
        if first:
            try:
                delivery = self.deliver(intent, demand, target)
                if (not isinstance(delivery, Delivery)
                        or delivery.phase not in ('host_validated', 'gpu_ready')):
                    raise RuntimeError('delivery lacks validated completion evidence')
                with self.lock:
                    entry['delivery'] = delivery
                    entry['state'] = ('READY' if self.assignments.get(intent.request_id)
                                      == (intent, True) else 'CANCELLED_DRAINED')
            except Exception as error:
                with self.lock:
                    entry['error'] = error
                    entry['state'] = 'MISS' if isinstance(error, CacheMiss) else 'QUARANTINED'
            finally:
                with self.lock:
                    entry['elapsed_ns'] = time.monotonic_ns() - entry['start_ns']
                    entry['intent_to_terminal_ns'] = time.monotonic_ns() - entry['accepted_ns']
                    entry['event'].set()
        else:
            entry['event'].wait()
        with self.lock:
            self._check(intent)  # Late old completion must not authorize consumption.
            if entry['error'] is not None:
                raise entry['error']
            if entry['state'] != 'READY':
                raise Superseded('consumer is no longer ready')
            return entry['delivery']

    def release(self, intent, consumer_id):
        """Caller confirms consumer no longer uses the buffer. Never drains DMA."""
        with self.lock:
            entry = self.entries[(intent, consumer_id)]
            if entry['state'] not in ('READY', 'MISS', 'CANCELLED_DRAINED', 'RELEASED'):
                raise RuntimeError('target access is uncertain; retain allocation')
            entry['target'] = None
            # A cached exception traceback can retain the load frame and owner.
            # Released tombstones reject replay before consulting this field.
            entry['error'] = None
            entry['state'] = 'RELEASED'

    def status(self):
        with self.lock:
            return [dict(request_id=i.request_id, assignment_generation=i.assignment_generation,
                         worker=i.worker, worker_epoch=i.worker_epoch, consumer_id=consumer,
                         state=e['state'], phase=e['delivery'].phase if e['delivery'] else None,
                         elapsed_ns=e.get('elapsed_ns'), retained=e['target'] is not None,
                         intent_to_terminal_ns=e.get('intent_to_terminal_ns'),
                         validation_scope='ADAPTER_ONLY')
                    for (i, consumer), e in self.entries.items()]
