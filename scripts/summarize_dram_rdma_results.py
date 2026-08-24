#!/usr/bin/env python3
"""Create a compact single-vs-batch report from TODO Extra result JSON."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text())
    if data.get("status") != "PASS":
        raise ValueError(f"failed result in {path}: {data.get('error', data)}")
    return data


def _metric(data: dict[str, Any], name: str) -> dict[str, Any]:
    value = data.get(name)
    if not isinstance(value, dict):
        raise ValueError(f"missing metric {name}")
    return value


def build_summary(out_dir: Path) -> dict[str, Any]:
    single = _load(out_dir / "gpu-single-staged.json")
    batch = _load(out_dir / "gpu-batch-staged.json")
    dram_path = out_dir / "dram.json"
    dram = _load(dram_path) if dram_path.exists() else None

    single_e2e = _metric(single, "to_gpu_single_staged")
    batch_e2e = _metric(batch, "to_gpu_batch_staged")
    single_split = _metric(single, "staged_breakdown")
    batch_split = _metric(batch, "staged_breakdown")

    if single.get("source_endpoints") != batch.get("source_endpoints"):
        raise ValueError("single and batch source endpoints differ")
    if single.get("source_protocols") != ["rdma"] or \
       batch.get("source_protocols") != ["rdma"]:
        raise ValueError("result is not an RDMA-only comparison")
    if single.get("gpu_path_selected") != "rdma_host_staged" or \
       batch.get("gpu_path_selected") != "rdma_host_staged":
        raise ValueError("result is not the staged RDMA-to-GPU path")

    def compact(e2e: dict[str, Any], split: dict[str, Any]) -> dict[str, Any]:
        calls_per_iteration = e2e.get("api_calls_per_iteration")
        if calls_per_iteration is None:
            calls_per_iteration = e2e["api_calls"] // e2e["iterations"]
        return {
            "api_calls": e2e["api_calls"],
            "api_calls_per_iteration": calls_per_iteration,
            "objects_per_call": e2e["objects_per_call"],
            "elapsed_sec": e2e["sec"],
            "end_to_end_GBps": e2e["GBps"],
            "rdma_to_host_sec": split["rdma_to_host"]["sec"],
            "rdma_to_host_GBps": split["rdma_to_host"]["GBps"],
            "host_to_gpu_sec": split["host_to_gpu"]["sec"],
            "host_to_gpu_GBps": split["host_to_gpu"]["GBps"],
            "unattributed_sec": split["unattributed_sec"],
        }

    summary = {
        "status": "PASS",
        "transport": "rdma",
        "gpu_path": "rdma_host_staged",
        "object_count": single["object_count"],
        "block_bytes": single["block_bytes"],
        "bytes_per_iteration": single["object_count"] * single["block_bytes"],
        "iterations": single_e2e["iterations"],
        "source_segment_count": single["source_segment_count"],
        "source_endpoints": single["source_endpoints"],
        "single": compact(single_e2e, single_split),
        "batch": compact(batch_e2e, batch_split),
        "batch_throughput_speedup": round(
            batch_e2e["GBps"] / single_e2e["GBps"], 4),
        "batch_elapsed_reduction_pct": round(
            (1.0 - batch_e2e["sec"] / single_e2e["sec"]) * 100.0, 3),
    }
    if dram is not None:
        host = _metric(dram, "to_local_dram_sequential_single")
        summary["host_dram_reference"] = {
            "elapsed_sec": host["sec"], "GBps": host["GBps"]}
    return summary


def render(summary: dict[str, Any]) -> str:
    single, batch = summary["single"], summary["batch"]
    lines = [
        "TODO Extra RDMA -> host -> GPU comparison",
        f"status=PASS transport={summary['transport']} "
        f"gpu_path={summary['gpu_path']}",
        f"objects={summary['object_count']} block_bytes={summary['block_bytes']} "
        f"bytes/iteration={summary['bytes_per_iteration']} "
        f"iterations={summary['iterations']} sources={summary['source_segment_count']}",
        "",
        "metric                         single          batch",
        f"API calls (total)             {single['api_calls']:>10}     "
        f"{batch['api_calls']:>10}",
        f"API calls / iteration         {single['api_calls_per_iteration']:>10}     "
        f"{batch['api_calls_per_iteration']:>10}",
        f"objects / API call            {single['objects_per_call']:>10}     "
        f"{batch['objects_per_call']:>10}",
        f"end-to-end seconds            {single['elapsed_sec']:>10.6f}     "
        f"{batch['elapsed_sec']:>10.6f}",
        f"end-to-end GB/s               {single['end_to_end_GBps']:>10.3f}     "
        f"{batch['end_to_end_GBps']:>10.3f}",
        f"RDMA -> host GB/s             {single['rdma_to_host_GBps']:>10.3f}     "
        f"{batch['rdma_to_host_GBps']:>10.3f}",
        f"host -> GPU GB/s              {single['host_to_gpu_GBps']:>10.3f}     "
        f"{batch['host_to_gpu_GBps']:>10.3f}",
        f"unattributed seconds          {single['unattributed_sec']:>10.6f}     "
        f"{batch['unattributed_sec']:>10.6f}",
        "",
        f"batch throughput speedup={summary['batch_throughput_speedup']:.4f}x",
        f"batch elapsed reduction={summary['batch_elapsed_reduction_pct']:.3f}%",
        "endpoints=" + ",".join(summary["source_endpoints"]),
    ]
    if "host_dram_reference" in summary:
        ref = summary["host_dram_reference"]
        lines.append(f"host DRAM reference={ref['GBps']:.3f} GB/s")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    summary = build_summary(args.out_dir)
    text = render(summary)
    (args.out_dir / "comparison-summary.json").write_text(
        json.dumps(summary, indent=2) + "\n")
    (args.out_dir / "comparison-summary.txt").write_text(text)
    print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
