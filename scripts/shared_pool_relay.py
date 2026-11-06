#!/usr/bin/env python3
"""Executable T0 protocol model for multi-sender/multi-receiver KV relay.

Pool bytes, MR registration and WRITE completion are simulated. This module
does not connect to Mooncake Master, verbs, DAX or CUDA. The protocol uses
pool offsets; each receiver projects its assigned range into its own mapping.
"""

from dataclasses import dataclass, field
import hashlib
import time


def checksum(data):
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class Location:
    pool: str
    offset: int
    length: int


@dataclass(frozen=True)
class Member:
    node: str
    pool: str
    mapping_base: int
    can_send: bool = False
    can_receive: bool = False


@dataclass(frozen=True)
class ObjectRecord:
    location: Location
    digest: str
    generation: int


@dataclass(frozen=True)
class ReceiverMR:
    mr_id: str
    receiver: str
    location: Location
    address: int


@dataclass(frozen=True)
class WriteRequest:
    transfer_id: str
    chunk_id: int
    sender: str
    source: Location
    destination: Location
    mr_id: str
    receiver: str
    remote_address: int


@dataclass
class Transfer:
    transfer_id: str
    key: str
    worker: str
    requester: str
    source: ObjectRecord
    reservation: Location
    requests: list = field(default_factory=list)
    completed: set = field(default_factory=set)
    state: str = "RESERVED"


class SharedPoolRelay:
    """Small coordinator and transport model, with explicit phase boundaries."""

    def __init__(self):
        self.pools = {}
        self.next_offset = {}
        self.members = {}
        self.objects = {}  # (key, pool) -> published ObjectRecord
        self.mrs = {}  # transfer ID -> receiver registrations visible to Master
        self.transfers = {}
        self.source_pins = {}
        self.events = []

    def event(self, transfer, event, **fields):
        self.events.append(dict(transfer_id=transfer.transfer_id, event=event,
                                worker=transfer.worker,
                                timestamp_ns=time.monotonic_ns(), **fields))

    def add_pool(self, pool, capacity):
        if pool in self.pools or capacity <= 0:
            raise ValueError("pool must be unique with positive capacity")
        self.pools[pool] = bytearray(capacity)
        self.next_offset[pool] = 0

    def add_member(self, member):
        if member.pool not in self.pools or member.node in self.members:
            raise ValueError("member needs an existing pool and unique node ID")
        self.members[member.node] = member

    def reserve(self, pool, length):
        start = self.next_offset[pool]
        if length <= 0 or start + length > len(self.pools[pool]):
            raise ValueError("reservation does not fit")
        self.next_offset[pool] += length
        return Location(pool, start, length)

    def read(self, location):
        return bytes(self.pools[location.pool][
            location.offset:location.offset + location.length])

    def seed(self, key, pool, payload, generation=1):
        location = self.reserve(pool, len(payload))
        self.pools[pool][location.offset:location.offset + len(payload)] = payload
        self.objects[key, pool] = ObjectRecord(location, checksum(payload), generation)
        return location

    def lookup(self, key, pool):
        record = self.objects[key, pool]
        senders = sorted(member.node for member in self.members.values()
                         if member.pool == pool and member.can_send)
        return record, senders

    def prepare(self, key, source_pool, requester, worker, chunk_bytes):
        """Target requests KV, reserves one extent and advertises receiver MRs."""
        source, senders = self.lookup(key, source_pool)
        target_pool = self.members[requester].pool
        receivers = sorted(member.node for member in self.members.values()
                           if member.pool == target_pool and member.can_receive)
        if not senders or not receivers or chunk_bytes <= 0:
            raise ValueError("sender, receiver and positive chunk size required")
        if source_pool == target_pool or (key, target_pool) in self.objects:
            raise ValueError("relay requires an unmaterialized destination pool")
        reservation = self.reserve(target_pool, source.location.length)
        tid = f"relay-{len(self.transfers) + 1}"
        transfer = Transfer(tid, key, worker, requester, source, reservation)
        self.transfers[tid] = transfer
        self.source_pins[key, source_pool] = self.source_pins.get((key, source_pool), 0) + 1
        self.event(transfer, "master_lookup", senders=senders,
                   source_pool=source_pool, generation=source.generation)
        self.event(transfer, "destination_reserved", pool=target_pool,
                   offset=reservation.offset, bytes=reservation.length)

        # Receiver MRs partition ONE reservation; they are not duplicate objects.
        receiver_count = min(len(receivers), reservation.length)
        registrations = []
        for index, receiver in enumerate(receivers[:receiver_count]):
            begin = reservation.length * index // receiver_count
            end = reservation.length * (index + 1) // receiver_count
            region = Location(target_pool, reservation.offset + begin, end - begin)
            mr = ReceiverMR(f"{tid}-mr-{index}", receiver, region,
                            self.members[receiver].mapping_base + region.offset)
            registrations.append(mr)
            self.event(transfer, "receiver_mr_ready", receiver=receiver,
                       mr_id=mr.mr_id, offset=region.offset, bytes=region.length)
        self.mrs[tid] = registrations
        self.event(transfer, "master_targets_published", receivers=receivers[:receiver_count])

        for mr in registrations:
            for relative in range(0, mr.location.length, chunk_bytes):
                length = min(chunk_bytes, mr.location.length - relative)
                destination = Location(target_pool, mr.location.offset + relative, length)
                object_offset = destination.offset - reservation.offset
                source_chunk = Location(source_pool, source.location.offset + object_offset, length)
                index = len(transfer.requests)
                transfer.requests.append(WriteRequest(
                    tid, index, senders[index % len(senders)], source_chunk,
                    destination, mr.mr_id, mr.receiver, mr.address + relative))
        transfer.state = "DISPATCHED"
        self.event(transfer, "requests_dispatched", chunks=len(transfer.requests))
        return transfer

    def complete_write(self, request):
        """Simulate one RNIC WRITE and its completion, in any dispatch order."""
        transfer = self.transfers[request.transfer_id]
        if transfer.state != "DISPATCHED" or request not in transfer.requests:
            raise ValueError("request is not outstanding")
        if request.chunk_id in transfer.completed:
            raise ValueError("duplicate completion")
        mr = next(mr for mr in self.mrs[request.transfer_id] if mr.mr_id == request.mr_id)
        receiver = self.members[mr.receiver]
        # Resolve through the receiver mapping, not the requester's address space.
        offset = request.remote_address - receiver.mapping_base
        assert offset == request.destination.offset
        assert mr.location.offset <= offset
        assert offset + request.destination.length <= mr.location.offset + mr.location.length
        assert self.members[request.sender].pool == request.source.pool
        self.pools[receiver.pool][offset:offset + request.source.length] = self.read(request.source)
        transfer.completed.add(request.chunk_id)
        self.event(transfer, "write_complete", sender=request.sender,
                   receiver=request.receiver, chunk_id=request.chunk_id,
                   bytes=request.source.length)

    def publish(self, transfer):
        if transfer.state != "DISPATCHED" or len(transfer.completed) != len(transfer.requests):
            raise ValueError("all writes must complete before publication")
        if checksum(self.read(transfer.reservation)) != transfer.source.digest:
            raise ValueError("destination checksum mismatch")
        self.event(transfer, "checksum_verified")
        self.objects[transfer.key, transfer.reservation.pool] = ObjectRecord(
            transfer.reservation, transfer.source.digest, transfer.source.generation)
        transfer.state = "COMPLETE"
        self.event(transfer, "destination_published")
        pin = (transfer.key, transfer.source.location.pool)
        self.source_pins[pin] -= 1
        del self.mrs[transfer.transfer_id]
        self.event(transfer, "transfer_resources_released")

    def worker_read(self, transfer):
        """Final worker reads committed bytes locally; no GPU execution implied."""
        record = self.objects[transfer.key, self.members[transfer.requester].pool]
        payload = self.read(record.location)
        if checksum(payload) != record.digest:
            raise ValueError("worker checksum mismatch")
        self.event(transfer, "worker_read_complete", bytes=len(payload),
                   source="local_shared_pool")
        return payload
