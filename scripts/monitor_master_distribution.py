#!/usr/bin/env python3
"""Poll Mooncake Master metrics and summarize per-segment distribution.

Population is a live gauge. GET evidence is the delta of Master metadata
advertisements; it is deliberately not reported as data-plane RDMA/GDR bytes.
"""

import argparse
import json
import math
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


_SAMPLE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([-+0-9.eE]+)$')
_LABEL = re.compile(r'(\w+)="((?:\\.|[^"])*)"(?:,|$)')


def parse_prometheus(text):
    samples = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = _SAMPLE.match(line)
        if not match:
            continue
        labels = {}
        for key, value in _LABEL.findall(match.group(2) or ""):
            labels[key] = bytes(value, "utf-8").decode("unicode_escape")
        samples.append((match.group(1), labels, float(match.group(3))))
    return samples


def _values(samples, metric, value_label="segment", protocol=None):
    result = {}
    for name, labels, value in samples:
        if name != metric or value_label not in labels:
            continue
        if protocol and labels.get("protocol") != protocol:
            continue
        key = labels[value_label]
        result[key] = result.get(key, 0.0) + value
    return result


def _protocols(samples, metric):
    protocols = {}
    for name, labels, value in samples:
        if name != metric or "protocol" not in labels:
            continue
        protocol = labels["protocol"]
        protocols[protocol] = protocols.get(protocol, 0.0) + value
    return protocols


def _distribution(values):
    total = sum(values.values())
    positive = [value for value in values.values() if value > 0]
    mean = total / len(values) if values else 0.0
    variance = (sum((value - mean) ** 2 for value in values.values()) /
                len(values)) if values else 0.0
    return {
        "total": total,
        "segments": len(values),
        "active_segments": len(positive),
        "coefficient_of_variation": math.sqrt(variance) / mean if mean else 0.0,
        "max_to_min_active": max(positive) / min(positive) if positive else 0.0,
        "by_segment": {
            key: {"value": value, "share": value / total if total else 0.0}
            for key, value in sorted(values.items())
        },
    }


def build_snapshot(samples, baseline=None, expected_segments=0):
    allocated = _values(samples, "segment_allocated_bytes")
    capacity = _values(samples, "segment_total_capacity_bytes")
    objects = _values(samples, "master_get_advertised_replicas_total")
    get_bytes = _values(samples, "master_get_advertised_bytes_total")
    baseline = baseline or {"objects": {}, "bytes": {}}
    object_delta = {key: value - baseline["objects"].get(key, 0.0)
                    for key, value in objects.items()}
    byte_delta = {key: value - baseline["bytes"].get(key, 0.0)
                  for key, value in get_bytes.items()}
    observed = set(capacity) | set(allocated) | set(objects)
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "evidence_scope": {
            "population": "master live allocated bytes",
            "get_distribution": "master metadata replicas advertised",
            "data_plane_bytes_observed": False,
        },
        "registered_segments": sorted(observed),
        "get_advertised_objects_by_protocol": _protocols(
            samples, "master_get_advertised_replicas_total"),
        "expected_segments": expected_segments,
        "expected_segments_ready": not expected_segments or len(capacity) >= expected_segments,
        "capacity_bytes": _distribution(capacity),
        "population_bytes": _distribution(allocated),
        "get_advertised_objects_delta": _distribution(object_delta),
        "get_advertised_bytes_delta": _distribution(byte_delta),
    }


def fetch(url, timeout):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics-url", required=True)
    parser.add_argument("--jsonl", type=Path, required=True)
    parser.add_argument("--summary-json", type=Path, required=True)
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument("--duration", type=float, default=0.0,
                        help="0 means run until interrupted")
    parser.add_argument("--expected-segments", type=int, default=0)
    args = parser.parse_args()
    args.jsonl.parent.mkdir(parents=True, exist_ok=True)
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)

    started = time.monotonic()
    baseline = None
    while not args.duration or time.monotonic() - started < args.duration:
        try:
            samples = parse_prometheus(fetch(args.metrics_url, args.timeout))
            objects = _values(samples, "master_get_advertised_replicas_total")
            get_bytes = _values(samples, "master_get_advertised_bytes_total")
            if baseline is None:
                baseline = {"objects": objects, "bytes": get_bytes}
            snapshot = build_snapshot(samples, baseline, args.expected_segments)
            line = json.dumps(snapshot, sort_keys=True)
            with args.jsonl.open("a", encoding="utf-8") as stream:
                stream.write(line + "\n")
            temporary = args.summary_json.with_suffix(
                args.summary_json.suffix + ".tmp")
            temporary.write_text(line + "\n", encoding="utf-8")
            temporary.replace(args.summary_json)
        except (OSError, urllib.error.URLError, ValueError) as error:
            event = {"timestamp": datetime.now(timezone.utc).isoformat(),
                     "status": "WAITING", "error": str(error)}
            with args.jsonl.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(event, sort_keys=True) + "\n")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
