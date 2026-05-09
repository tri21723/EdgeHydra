import argparse
import asyncio
import json
import math
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from common.fault_simulator import FaultConfig, maybe_delay, should_drop
from common.metrics import record_cloud_to_edge
from common.transport import Frame, request_response


@dataclass(frozen=True)
class TargetEdge:
    edge_id: str
    host: str
    port: int


def _parse_edges(s: str) -> List[TargetEdge]:
    out: List[TargetEdge] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        edge_id, addr = part.split("@", 1)
        host, port_s = addr.rsplit(":", 1)
        out.append(TargetEdge(edge_id=edge_id, host=host, port=int(port_s)))
    if not out:
        raise ValueError("no edges specified")
    return out


def _slice_blocks(data: bytes, n: int) -> List[bytes]:
    if n <= 0:
        raise ValueError("n must be > 0")
    block_bytes = int(math.ceil(len(data) / n)) if data else 0
    padded = data + (b"\x00" * (block_bytes * n - len(data)))
    return [padded[i * block_bytes : (i + 1) * block_bytes] for i in range(n)]


def _append_protocol_event(event: str, **extra: object) -> None:
    sink = os.getenv("METRICS_SINK", "").strip()
    if not sink:
        return
    Path(sink).parent.mkdir(parents=True, exist_ok=True)
    evt = {
        "event": event,
        "event_kind": "protocol",
        "ts_ms": int(time.time() * 1000),
    }
    evt.update(extra)
    with open(sink, "a", encoding="utf-8") as f:
        f.write(json.dumps(evt) + "\n")


def _append_infra_event(event: str, **extra: object) -> None:
    sink = os.getenv("METRICS_SINK", "").strip()
    if not sink:
        return
    Path(sink).parent.mkdir(parents=True, exist_ok=True)
    evt = {
        "event": event,
        "event_kind": "infrastructure",
        "ts_ms": int(time.time() * 1000),
    }
    evt.update(extra)
    with open(sink, "a", encoding="utf-8") as f:
        f.write(json.dumps(evt) + "\n")


async def distribute_edgedis(
    *,
    input_file: str,
    work_dir: str,
    edges: List[TargetEdge],
    timeout_s: float = 10.0,
) -> Dict[int, int]:
    """
    EdgeDis baseline Stage 1 (cloud):
      - slice file into n data blocks (no EC)
      - send block i to entry edge i via mesbd_edgedis
      - wait mesbdc_edgedis and update blockStatus[i]=1
    """
    n = len(edges)
    inp = Path(input_file)
    file_id = inp.stem
    data = inp.read_bytes()
    blocks = _slice_blocks(data, n)

    work = Path(work_dir) / file_id
    work.mkdir(parents=True, exist_ok=True)
    meta = {
        "file_id": file_id,
        "filename": inp.name,
        "n": n,
        "orig_bytes": len(data),
        "block_bytes": len(blocks[0]) if blocks else 0,
    }
    (work / "meta.json").write_text(json.dumps(meta, indent=2))

    fault = FaultConfig.from_env("C2E_FAULT")
    block_status: Dict[int, int] = {i: 0 for i in range(n)}

    async def _send_one(i: int, edge: TargetEdge) -> Tuple[int, bool]:
        if should_drop(fault):
            return i, False
        await maybe_delay(fault)
        record_cloud_to_edge(len(blocks[i]))
        req = Frame(
            header={
                "type": "mesbd_edgedis",
                "file_id": file_id,
                "filename": inp.name,
                "block_id": i,
                "meta": meta,
            },
            payload=blocks[i],
        )
        resp = await request_response(edge.host, edge.port, req, timeout_s=timeout_s)
        ok = resp.header.get("type") == "mesbdc_edgedis"
        _append_protocol_event("block_sent", file_id=file_id, block_id=i, edge_id=edge.edge_id, ok=ok)
        return i, ok

    results = await asyncio.gather(*[_send_one(i, e) for i, e in enumerate(edges)], return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            continue
        i, ok = r
        if ok:
            block_status[i] = 1

    (work / "cloud_block_status.json").write_text(json.dumps(block_status, indent=2))
    return block_status


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Cloud EdgeDis distributor (slice + mesbd fan-out)")
    parser.add_argument("--file", required=True)
    parser.add_argument("--work-dir", default="experiments/data/cloud_work_edgedis")
    parser.add_argument("--edges", required=True, help="e0@host:port,e1@host:port,...")
    parser.add_argument("--timeout-s", type=float, default=10.0)
    args = parser.parse_args(argv)

    edges = _parse_edges(args.edges)
    _append_infra_event("container_start", component="edgedis_cloud")
    try:
        _append_infra_event("container_ready", component="edgedis_cloud")
        asyncio.run(distribute_edgedis(input_file=args.file, work_dir=args.work_dir, edges=edges, timeout_s=args.timeout_s))
    finally:
        _append_infra_event("container_exit", component="edgedis_cloud")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

