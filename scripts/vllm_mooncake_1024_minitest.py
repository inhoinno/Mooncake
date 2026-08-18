#!/usr/bin/env python3
"""Two-phase vLLM + MooncakeStoreConnector 1024-token minitest.

Run ``produce`` once against a CXL-backed vLLM instance and once against an
RDMA-backed instance. After stopping both producers, run ``consume`` against a
fresh vLLM process whose Transfer Engine has both transports installed. The
consumer submits both saved prompts concurrently so vLLM can batch their KV
loads from different Store source protocols.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import pathlib
import time
import urllib.error
import urllib.request
from typing import Any, Sequence


class MiniTestFailure(RuntimeError):
    pass


def _completion(base_url: str, model: str, prompt: list[int], timeout: float) -> dict[str, Any]:
    body = json.dumps(
        {
            "model": model,
            "prompt": prompt,
            "max_tokens": 1,
            "temperature": 0,
            "stream": False,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        base_url.rstrip("/") + "/v1/completions",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")[:1000]
        raise MiniTestFailure(f"vLLM returned HTTP {error.code}: {detail}") from error
    payload["_elapsed_ms"] = round((time.monotonic() - started) * 1000.0, 3)
    prompt_tokens = payload.get("usage", {}).get("prompt_tokens")
    if prompt_tokens != len(prompt):
        raise MiniTestFailure(
            f"vLLM reported {prompt_tokens!r} prompt tokens, expected {len(prompt)}"
        )
    return payload


def _prompt_digest(prompt: list[int]) -> str:
    encoded = json.dumps(prompt, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _write_state(path: pathlib.Path, state: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(state, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _produce(args: argparse.Namespace) -> dict[str, Any]:
    prompt = [args.token_id] * args.token_count
    response = _completion(args.base_url, args.model, prompt, args.timeout)
    state = {
        "model": args.model,
        "prompt": prompt,
        "prompt_sha256": _prompt_digest(prompt),
        "source": args.source,
        "token_count": args.token_count,
    }
    _write_state(args.state[0], state)
    return {
        "phase": "produce",
        "source": args.source,
        "status": "PASS",
        "prompt_tokens": args.token_count,
        "elapsed_ms": response["_elapsed_ms"],
        "state": str(args.state[0]),
    }


def _load_state(path: pathlib.Path) -> dict[str, Any]:
    state = json.loads(path.read_text(encoding="utf-8"))
    prompt = state.get("prompt")
    if not isinstance(prompt, list) or not all(isinstance(item, int) for item in prompt):
        raise MiniTestFailure(f"invalid prompt in {path}")
    if state.get("prompt_sha256") != _prompt_digest(prompt):
        raise MiniTestFailure(f"prompt digest mismatch in {path}")
    return state


def _consume(args: argparse.Namespace) -> dict[str, Any]:
    states = [_load_state(path) for path in args.state]
    sources = {state.get("source") for state in states}
    if sources != {"cxl", "rdma"}:
        raise MiniTestFailure(
            f"consume requires one cxl and one rdma state, got {sorted(sources)!r}"
        )
    if any(state.get("model") != args.model for state in states):
        raise MiniTestFailure("producer and consumer model names differ")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                _completion,
                args.base_url,
                args.model,
                state["prompt"],
                args.timeout,
            )
            for state in states
        ]
        responses = [future.result() for future in futures]
    return {
        "phase": "consume",
        "status": "PASS",
        "sources": sorted(sources),
        "prompt_tokens": [state["token_count"] for state in states],
        "elapsed_ms": [response["_elapsed_ms"] for response in responses],
        "required_store_paths": ["cxl_cuda_copy", "rdma_host_staged"],
    }


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("produce", "consume"))
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--model", required=True)
    parser.add_argument("--state", action="append", type=pathlib.Path, required=True)
    parser.add_argument("--source", choices=("cxl", "rdma"))
    parser.add_argument("--token-id", type=int, default=42)
    parser.add_argument("--token-count", type=int, default=1024)
    parser.add_argument("--timeout", type=float, default=180.0)
    args = parser.parse_args(argv)
    if args.phase == "produce" and (args.source is None or len(args.state) != 1):
        parser.error("produce requires --source and exactly one --state")
    if args.phase == "consume" and len(args.state) != 2:
        parser.error("consume requires two --state arguments")
    if args.token_count <= 0 or args.token_id < 0 or args.timeout <= 0:
        parser.error("token count, token id, and timeout must be valid positive values")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        summary = _produce(args) if args.phase == "produce" else _consume(args)
        code = 0
    except Exception as error:
        summary = {
            "phase": args.phase,
            "status": "FAIL",
            "failure_type": type(error).__name__,
            "message": str(error)[:1000],
        }
        code = 1
    print(json.dumps(summary, sort_keys=True), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
