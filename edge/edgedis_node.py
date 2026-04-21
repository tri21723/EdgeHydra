import argparse
import asyncio
import json
import math
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from common.fault_simulator import FaultConfig, maybe_delay, should_drop
from common.metrics import record_edge_to_edge
from common.transport import Frame, read_frame, request_response, write_frame


def _now_ms() -> int:
    return int(time.time() * 1000)


class EdgeDisNode:
    def __init__(
        self,
        *,
        server_id: str,
        cache_dir: str,
        peers: List[Tuple[str, str, int]],
        coordinator_id: str,
        coordinator_poll_ms: int = 200,
    ):
        self.server_id = server_id
        self.cache_root = Path(cache_dir)
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.peers = peers  # (peer_id, host, tcp_port)
        self.coordinator_id = coordinator_id
        self.coordinator_poll_ms = coordinator_poll_ms

        self._states: Dict[str, Dict[str, Any]] = {}  # file_id -> state

        # Fault injection for edge-to-edge traffic
        self.fault = FaultConfig.from_env("E2E_FAULT")
        self.crashed = False
        # crash_after_s is scheduled in run_node() when an event loop is running.

    def _crash(self) -> None:
        self.crashed = True

    def _file_dir(self, file_id: str) -> Path:
        return (self.cache_root / self.server_id) / file_id

    def _state_path(self, file_id: str) -> Path:
        return self._file_dir(file_id) / "state.json"

    def _load_state(self, file_id: str) -> Dict[str, Any]:
        if file_id in self._states:
            return self._states[file_id]
        p = self._state_path(file_id)
        if p.exists():
            st = json.loads(p.read_text())
        else:
            st = {
                "file_id": file_id,
                "server_id": self.server_id,
                "meta": None,  # {n, orig_bytes, block_bytes, filename}
                "received_blocks": [],
                "updated_at_ms": _now_ms(),
                "reconstructed": False,
            }
        self._states[file_id] = st
        return st

    def _save_state(self, file_id: str) -> None:
        st = self._states.get(file_id)
        if st is None:
            return
        st["updated_at_ms"] = _now_ms()
        p = self._state_path(file_id)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(st, indent=2))

    def _mark_received(self, file_id: str, block_id: int) -> None:
        st = self._load_state(file_id)
        r = set(int(x) for x in st.get("received_blocks", []))
        r.add(int(block_id))
        st["received_blocks"] = sorted(r)
        self._save_state(file_id)

    def _block_path(self, file_id: str, filename: str, block_id: int) -> Path:
        return self._file_dir(file_id) / f"{filename}.block.{block_id}.bin"

    def _has_block(self, file_id: str, filename: str, block_id: int) -> bool:
        return self._block_path(file_id, filename, block_id).exists()

    def _write_block(self, file_id: str, filename: str, block_id: int, payload: bytes) -> None:
        p = self._block_path(file_id, filename, block_id)
        p.parent.mkdir(parents=True, exist_ok=True)
        if not p.exists():
            p.write_bytes(payload)

    def _missing_blocks(self, meta: Dict[str, Any], received: List[int]) -> List[int]:
        n = int(meta["n"])
        have = set(int(x) for x in received)
        return [i for i in range(n) if i not in have]

    async def _forward_block(self, *, file_id: str, filename: str, block_id: int, meta: Dict[str, Any], payload: bytes) -> None:
        # Forward this block to all peers (best effort).
        async def _one(peer_id: str, host: str, port: int) -> None:
            if should_drop(self.fault):
                return
            await maybe_delay(self.fault)
            record_edge_to_edge(len(payload))
            try:
                await request_response(
                    host,
                    port,
                    Frame(
                        header={
                            "type": "mesbt_edgedis",
                            "sender_id": self.server_id,
                            "file_id": file_id,
                            "filename": filename,
                            "block_id": block_id,
                            "meta": meta,
                        },
                        payload=payload,
                    ),
                    timeout_s=5.0,
                )
            except Exception:
                return

        await asyncio.gather(*[_one(pid, h, p) for (pid, h, p) in self.peers], return_exceptions=True)

    def _try_reconstruct(self, file_id: str) -> None:
        st = self._load_state(file_id)
        if st.get("reconstructed"):
            return
        meta = st.get("meta")
        if not meta:
            return
        filename = str(meta["filename"])
        n = int(meta["n"])
        orig_bytes = int(meta["orig_bytes"])

        received = [int(x) for x in st.get("received_blocks", [])]
        if len(received) < n:
            return

        # Reconstruct by concatenating blocks 0..n-1, then truncate to orig_bytes.
        blocks: List[bytes] = []
        for i in range(n):
            p = self._block_path(file_id, filename, i)
            if not p.exists():
                return
            blocks.append(p.read_bytes())
        data = b"".join(blocks)[:orig_bytes]

        out_dir = Path("experiments/data/recovered_edgedis") / self.server_id
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / filename).write_bytes(data)

        st["reconstructed"] = True
        self._save_state(file_id)

    async def handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            if self.crashed:
                return
            frame = await read_frame(reader)
            t = frame.header.get("type")

            if t == "mesbd_edgedis":
                # Cloud -> entry edge: receive assigned block, cache, then forward to peers.
                file_id = str(frame.header["file_id"])
                filename = str(frame.header["filename"])
                block_id = int(frame.header["block_id"])
                meta = dict(frame.header["meta"])

                st = self._load_state(file_id)
                st["meta"] = meta
                self._save_state(file_id)

                self._write_block(file_id, filename, block_id, frame.payload)
                self._mark_received(file_id, block_id)
                asyncio.create_task(self._forward_block(file_id=file_id, filename=filename, block_id=block_id, meta=meta, payload=frame.payload))

                await write_frame(
                    writer,
                    Frame(header={"type": "mesbdc_edgedis", "server_id": self.server_id, "file_id": file_id, "block_id": block_id}),
                )
                return

            if t == "mesbt_edgedis":
                # Edge -> edge: receive forwarded block
                file_id = str(frame.header["file_id"])
                filename = str(frame.header["filename"])
                block_id = int(frame.header["block_id"])
                meta = dict(frame.header["meta"])

                st = self._load_state(file_id)
                if st.get("meta") is None:
                    st["meta"] = meta
                self._save_state(file_id)

                self._write_block(file_id, filename, block_id, frame.payload)
                self._mark_received(file_id, block_id)
                self._try_reconstruct(file_id)

                await write_frame(writer, Frame(header={"type": "mesbrc_edgedis", "server_id": self.server_id, "file_id": file_id, "block_id": block_id}))
                return

            if t == "status_req_edgedis":
                file_id = str(frame.header["file_id"])
                st = self._load_state(file_id)
                await write_frame(
                    writer,
                    Frame(
                        header={
                            "type": "status_resp_edgedis",
                            "server_id": self.server_id,
                            "file_id": file_id,
                            "received_blocks": st.get("received_blocks", []),
                            "meta": st.get("meta"),
                        }
                    ),
                )
                return

            if t == "supplement_edgedis":
                # Coordinator -> edge: supply missing block
                file_id = str(frame.header["file_id"])
                filename = str(frame.header["filename"])
                block_id = int(frame.header["block_id"])
                meta = dict(frame.header["meta"])

                st = self._load_state(file_id)
                if st.get("meta") is None:
                    st["meta"] = meta
                self._save_state(file_id)

                self._write_block(file_id, filename, block_id, frame.payload)
                self._mark_received(file_id, block_id)
                self._try_reconstruct(file_id)

                await write_frame(writer, Frame(header={"type": "supplement_ack_edgedis", "server_id": self.server_id, "file_id": file_id, "block_id": block_id}))
                return

            if t == "get_block_edgedis":
                file_id = str(frame.header["file_id"])
                filename = str(frame.header["filename"])
                block_id = int(frame.header["block_id"])
                p = self._block_path(file_id, filename, block_id)
                if not p.exists():
                    await write_frame(writer, Frame(header={"type": "error", "error": "block_not_found"}))
                    return
                record_edge_to_edge(p.stat().st_size)
                await write_frame(
                    writer,
                    Frame(
                        header={"type": "get_block_resp_edgedis", "server_id": self.server_id, "file_id": file_id, "block_id": block_id},
                        payload=p.read_bytes(),
                    ),
                )
                return

            await write_frame(writer, Frame(header={"type": "error", "error": f"unknown type={t}"}))
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def coordinator_loop(self) -> None:
        """
        Coordinator periodically queries each node's received blocks and supplies missing blocks
        by copying blocks from any node that has them.
        """
        if self.server_id != self.coordinator_id:
            return
        while True:
            try:
                # For each known file in local state, try to complete others
                for file_id, st in list(self._states.items()):
                    meta = st.get("meta")
                    if not meta:
                        continue
                    filename = str(meta["filename"])
                    n = int(meta["n"])

                    # Ask all peers (and self) for status
                    statuses: Dict[str, Dict[str, Any]] = {self.server_id: {"received_blocks": st.get("received_blocks", []), "meta": meta}}
                    for pid, host, port in self.peers:
                        try:
                            resp = await request_response(
                                host,
                                port,
                                Frame(header={"type": "status_req_edgedis", "file_id": file_id}),
                                timeout_s=2.0,
                            )
                            if resp.header.get("type") == "status_resp_edgedis":
                                statuses[pid] = resp.header
                        except Exception:
                            continue

                    # Build an index: block_id -> (holder_id)
                    holders: Dict[int, str] = {}
                    for sid, sresp in statuses.items():
                        for bid in sresp.get("received_blocks", []):
                            bid = int(bid)
                            holders.setdefault(bid, sid)

                    # For each node, supply missing blocks we can find holders for
                    for sid, sresp in statuses.items():
                        received = [int(x) for x in sresp.get("received_blocks", [])]
                        missing = [i for i in range(n) if i not in set(received)]
                        if not missing:
                            continue
                        # Supply a few missing blocks per tick to avoid bursts
                        random.shuffle(missing)
                        for bid in missing[:2]:
                            holder_id = holders.get(bid)
                            if holder_id is None:
                                continue
                            # Read block bytes from holder's cache (if holder is self or peer)
                            payload: Optional[bytes] = None
                            if holder_id == self.server_id:
                                p = self._block_path(file_id, filename, bid)
                                if p.exists():
                                    payload = p.read_bytes()
                            else:
                                holder = next(((pid, host, port) for (pid, host, port) in self.peers if pid == holder_id), None)
                                if holder:
                                    _, hh, hp = holder
                                    try:
                                        r = await request_response(
                                            hh,
                                            hp,
                                            Frame(header={"type": "get_block_edgedis", "file_id": file_id, "filename": filename, "block_id": bid}),
                                            timeout_s=2.0,
                                        )
                                        if r.header.get("type") == "get_block_resp_edgedis":
                                            payload = r.payload
                                    except Exception:
                                        payload = None
                            if payload is None:
                                continue

                            if sid == self.server_id:
                                # already handled locally
                                continue
                            # Find target addr
                            target = next(((pid, host, port) for (pid, host, port) in self.peers if pid == sid), None)
                            if not target:
                                continue
                            _, host, port = target
                            try:
                                if payload is not None:
                                    record_edge_to_edge(len(payload))
                                await request_response(
                                    host,
                                    port,
                                    Frame(
                                        header={
                                            "type": "supplement_edgedis",
                                            "file_id": file_id,
                                            "filename": filename,
                                            "block_id": bid,
                                            "meta": meta,
                                        },
                                        payload=payload,
                                    ),
                                    timeout_s=3.0,
                                )
                            except Exception:
                                continue
            finally:
                await asyncio.sleep(self.coordinator_poll_ms / 1000.0)


async def run_node(host: str, port: int, *, node: EdgeDisNode) -> None:
    loop = asyncio.get_running_loop()
    if node.fault.enabled and node.fault.crash_after_s and node.fault.crash_after_s > 0:
        loop.call_later(node.fault.crash_after_s, node._crash)
    srv = await asyncio.start_server(node.handle_conn, host, port)
    coord_task = asyncio.create_task(node.coordinator_loop())
    try:
        async with srv:
            await srv.serve_forever()
    finally:
        coord_task.cancel()


def main() -> int:
    parser = argparse.ArgumentParser(description="EdgeDis node (baseline)")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--id", dest="server_id", required=True)
    parser.add_argument("--cache-dir", default="experiments/data/edge_cache_edgedis")
    parser.add_argument("--coordinator-id", required=True)
    parser.add_argument(
        "--peers",
        default="",
        help="Comma-separated peers: e1@127.0.0.1:9101,e2@127.0.0.1:9102 ... (excluding self)",
    )
    args = parser.parse_args()

    peers: List[Tuple[str, str, int]] = []
    if args.peers.strip():
        for part in args.peers.split(","):
            part = part.strip()
            if not part:
                continue
            pid, addr = part.split("@", 1)
            h, ps = addr.rsplit(":", 1)
            peers.append((pid, h, int(ps)))

    node = EdgeDisNode(
        server_id=args.server_id,
        cache_dir=args.cache_dir,
        peers=peers,
        coordinator_id=args.coordinator_id,
    )
    asyncio.run(run_node(args.host, args.port, node=node))
    return 0


if __name__ == "__main__":
    import contextlib

    raise SystemExit(main())

