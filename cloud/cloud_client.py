import argparse
import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from common.fault_simulator import FaultConfig, maybe_delay, should_drop
from common.erasure_coding import ECInfo, encode_file
from common.metrics import record_cloud_to_edge
from common.transport import Frame, request_response


@dataclass(frozen=True)
class TargetEdge:
    edge_id: str
    host: str
    port: int


def _parse_edges(s: str) -> List[TargetEdge]:
    """
    Format: "e1@127.0.0.1:9001,e2@127.0.0.1:9002"
    """
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


async def stage1_distribute(
    *,
    input_file: str,
    work_dir: str,
    edges: List[TargetEdge],
    k: int,
    m: int,
    codec: str,
    timeout_s: float = 10.0,
) -> Dict[int, int]:
    """
    Stage 1:
      - encode file into m coded blocks (k data, m-k parity)
      - send mesbd(block_i) to edge_i in parallel
      - wait mesbdc and update blockStatus[i]=1
    Returns blockStatus dict {block_id: 0|1}
    """
    if len(edges) != m:
        raise ValueError(f"Stage1 requires exactly m={m} edges, got {len(edges)}")

    inp = Path(input_file)
    file_id = inp.stem
    begin_id = 0
    end_id = m - 1

    work = Path(work_dir) / file_id
    shards_dir = work / "shards"
    shards_dir.mkdir(parents=True, exist_ok=True)

    info: ECInfo = encode_file(
        str(inp),
        str(shards_dir),
        k=k,
        m=m,
        codec=codec,
        prefix=inp.name,
    )

    block_status: Dict[int, int] = {i: 0 for i in range(m)}
    fault = FaultConfig.from_env("C2E_FAULT")

    async def _send_one(i: int, edge: TargetEdge) -> Tuple[int, bool]:
        if should_drop(fault):
            return i, False
        await maybe_delay(fault)
        shard_path = shards_dir / f"{inp.name}.shard.{i}.bin"
        payload = shard_path.read_bytes()
        record_cloud_to_edge(len(payload))
        req = Frame(
            header={
                "type": "mesbd",
                "file_id": file_id,
                "filename": inp.name,
                "block_id": i,
                "ecinfo": {
                    **info.to_json(),
                    "begin_id": begin_id,
                    "end_id": end_id,
                    "filename": inp.name,
                    # Stage 3 request cooldown default for simulation
                    "block_req_timeout_ms": int(timeout_s * 1000),
                },
            },
            payload=payload,
        )
        resp = await request_response(edge.host, edge.port, req, timeout_s=timeout_s)
        if resp.header.get("type") != "mesbdc":
            return i, False
        return i, True

    results = await asyncio.gather(*[_send_one(i, e) for i, e in enumerate(edges)], return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            continue
        i, ok = r
        if ok:
            block_status[i] = 1

    (work / "stage1_block_status.json").write_text(json.dumps(block_status, indent=2))
    return block_status


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Cloud Stage-1 distributor (encode + mesbd fan-out)")
    parser.add_argument("--file", required=True, help="Input raw file path")
    parser.add_argument("--work-dir", default="experiments/data/cloud_work")
    parser.add_argument("--edges", required=True, help="e1@host:port,e2@host:port,... (must equal m)")
    parser.add_argument("--k", type=int, required=True)
    parser.add_argument("--m", type=int, required=True)
    parser.add_argument("--codec", default="zfec", choices=["zfec", "pyeclib"])
    parser.add_argument("--timeout-s", type=float, default=10.0)

    args = parser.parse_args(argv)
    edges = _parse_edges(args.edges)
    asyncio.run(
        stage1_distribute(
            input_file=args.file,
            work_dir=args.work_dir,
            edges=edges,
            k=args.k,
            m=args.m,
            codec=args.codec,
            timeout_s=args.timeout_s,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

