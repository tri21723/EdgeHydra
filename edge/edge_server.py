import argparse
import asyncio
import contextlib
import json
import os
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from common.fault_simulator import FaultConfig, maybe_delay, should_drop
from common.erasure_coding import decode_blocks
from common.metrics import record_edge_to_edge
from common.network_monitor import NetworkStateMonitor
from common.transport import Frame, read_frame, write_frame
from common.transport import request_response


def _now_ms() -> int:
    return int(time.time() * 1000)


class EdgeServer:
    def __init__(
        self,
        *,
        server_id: str,
        cache_dir: str,
        tcp_port: int,
        udp_port: int,
        peers: List[Tuple[str, str, int, int]],
        drop_mesbt_block_ids: Optional[Set[int]] = None,
    ):
        self.server_id = server_id
        self.cache_root = Path(cache_dir)
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.peers = peers  # (peer_id, host, tcp_port, udp_port)
        self._file_states: Dict[str, Dict[str, Any]] = {}
        self._cooldowns: Dict[Tuple[str, int], int] = {}  # (file_id, block_id) -> next_allowed_ms
        self._udp_transport: Optional[asyncio.DatagramTransport] = None
        self.drop_mesbt_block_ids: Set[int] = drop_mesbt_block_ids or set()
        self._no_send_to: Dict[str, Set[str]] = {}  # file_id -> peer_ids that don't need more blocks
        self._peer_blocks: Dict[str, Dict[str, Set[int]]] = {}  # file_id -> peer_id -> known block ids
        # Stage 2 dedup ledger: file_id -> block_id -> peer_ids already sent at least once.
        self._stage2_sent_ledger: Dict[str, Dict[int, Set[str]]] = {}
        # Stage 3 dedup ledger: file_id -> block_id -> peer_ids already supplied via mesbs.
        self._supplement_sent_ledger: Dict[str, Dict[int, Set[str]]] = {}
        # Fix 4 (Stage 3): suppress duplicate supplement requests in-flight.
        # file_id -> block_ids that have already been requested in current request window.
        self._pending_requests: Dict[str, Set[int]] = {}
        self._dedup_stats: Dict[str, int] = {
            "stage2_skipped_reconstructed_peer": 0,
            "stage2_skipped_already_sent": 0,
            "stage2_skipped_peer_has_block": 0,
            "stage3_skipped_duplicate_supply": 0,
            "stage3_supplies_sent": 0,
            "stage3_skipped_duplicate_request": 0,
        }
        self.fault_e2e = FaultConfig.from_env("E2E_FAULT")
        self.crashed = False
        # crash_after_s is scheduled in run_server() when an event loop is running.
        self.netmon = NetworkStateMonitor(
            self_id=self.server_id,
            peers=[(pid, host, tcp) for (pid, host, tcp, _udp) in self.peers],
        )
        self._netmon_export_path = self._build_netmon_export_path()
        self._state_export_path = self._build_state_export_path()

    def _build_netmon_export_path(self) -> Optional[Path]:
        sink = os.getenv("METRICS_SINK", "").strip()
        if not sink:
            return None
        p = Path(sink)
        out = p.parent / f"netmon_{self.server_id}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        return out

    def _build_state_export_path(self) -> Optional[Path]:
        sink = os.getenv("METRICS_SINK", "").strip()
        if not sink:
            return None
        p = Path(sink)
        out = p.parent / f"node_state_{self.server_id}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        return out

    async def _export_netmon_loop(self, interval_s: float = 0.5) -> None:
        if self._netmon_export_path is None:
            return
        while True:
            try:
                snap = self.netmon.snapshot()
                tmp = self._netmon_export_path.with_suffix(".tmp")
                tmp.write_text(json.dumps(snap, ensure_ascii=False))
                tmp.replace(self._netmon_export_path)
            except Exception:
                pass
            await asyncio.sleep(interval_s)

    async def _export_state_loop(self, interval_s: float = 0.4) -> None:
        if self._state_export_path is None:
            return
        while True:
            try:
                latest: Optional[Dict[str, Any]] = None
                for st in self._file_states.values():
                    if latest is None or int(st.get("updated_at_ms", 0)) > int(latest.get("updated_at_ms", 0)):
                        latest = st
                payload: Dict[str, Any] = {
                    "server_id": self.server_id,
                    "file_id": None,
                    "received_blocks": [],
                    "received_block_ids": [],
                    "received_count": 0,
                    "reconstructed": False,
                    "crashed": bool(self.crashed),
                    "dedup_stats": dict(self._dedup_stats),
                    "total_duplicate_sends_avoided": 0,
                    "updated_at_ms": _now_ms(),
                }
                if latest is not None:
                    rb = [int(x) for x in latest.get("received_blocks", [])]
                    payload.update(
                        {
                            "file_id": str(latest.get("file_id", "")),
                            "received_blocks": rb,
                            "received_block_ids": rb,
                            "received_count": len(rb),
                            "reconstructed": bool(latest.get("reconstructed", False)),
                            "crashed": bool(self.crashed),
                            "dedup_stats": dict(self._dedup_stats),
                            "total_duplicate_sends_avoided": int(
                                self._dedup_stats.get("stage2_skipped_reconstructed_peer", 0)
                                + self._dedup_stats.get("stage2_skipped_already_sent", 0)
                                + self._dedup_stats.get("stage2_skipped_peer_has_block", 0)
                                + self._dedup_stats.get("stage3_skipped_duplicate_supply", 0)
                            ),
                            "updated_at_ms": int(latest.get("updated_at_ms", _now_ms())),
                        }
                    )
                tmp = self._state_export_path.with_suffix(".tmp")
                tmp.write_text(json.dumps(payload, ensure_ascii=False))
                tmp.replace(self._state_export_path)
            except Exception:
                pass
            await asyncio.sleep(interval_s)

    def _crash(self) -> None:
        self.crashed = True

    def _file_dir(self, file_id: str) -> Path:
        # Separate cache per edge server to avoid collisions when running locally.
        return (self.cache_root / self.server_id) / file_id

    def _state_path(self, file_id: str) -> Path:
        return self._file_dir(file_id) / "state.json"

    def _load_state(self, file_id: str) -> Dict[str, Any]:
        if file_id in self._file_states:
            return self._file_states[file_id]
        p = self._state_path(file_id)
        if p.exists():
            st = json.loads(p.read_text())
        else:
            st = {
                "file_id": file_id,
                "server_id": self.server_id,
                "received_blocks": [],
                "reconstructed": False,
                "updated_at_ms": _now_ms(),
                "tran_status": {},  # block_id -> peer_id -> 0|1
            }
        self._file_states[file_id] = st
        return st

    def _save_state(self, file_id: str) -> None:
        st = self._file_states.get(file_id)
        if st is None:
            return
        st["updated_at_ms"] = _now_ms()
        p = self._state_path(file_id)
        p.write_text(json.dumps(st, indent=2))

    def _mark_received(self, file_id: str, block_id: int) -> None:
        st = self._load_state(file_id)
        r: Set[int] = set(int(x) for x in st.get("received_blocks", []))
        r.add(int(block_id))
        st["received_blocks"] = sorted(r)
        # Fix 4 (Stage 3): once block is received, allow future request cycles
        # for this block id (if needed by other control flow).
        self._pending_requests.setdefault(file_id, set()).discard(int(block_id))
        self._save_state(file_id)

    def _clear_pending_request(self, file_id: str, block_id: int) -> None:
        self._pending_requests.setdefault(file_id, set()).discard(int(block_id))

    def _maybe_reconstruct(self, file_id: str, *, ecinfo: Dict[str, Any]) -> bool:
        st = self._load_state(file_id)
        if st.get("reconstructed"):
            return True
        k = int(ecinfo.get("k"))
        received = st.get("received_blocks", [])
        if len(received) >= k:
            st["reconstructed"] = True
            self._save_state(file_id)
            asyncio.create_task(self._on_reconstructed(file_id=file_id, ecinfo=ecinfo))
            return True
        return False

    async def _on_reconstructed(self, *, file_id: str, ecinfo: Dict[str, Any]) -> None:
        """
        Stage 4: reconstruct file locally and broadcast mesdr to stop redundant traffic.
        """
        try:
            await self._reconstruct_file(file_id=file_id, ecinfo=ecinfo)
        finally:
            await self._broadcast_mesdr(file_id=file_id, ecinfo=ecinfo)

    async def _reconstruct_file(self, *, file_id: str, ecinfo: Dict[str, Any]) -> None:
        k = int(ecinfo["k"])
        m = int(ecinfo["m"])
        codec = str(ecinfo.get("codec", "zfec"))
        orig_bytes = int(ecinfo.get("orig_bytes"))
        filename = str(ecinfo.get("filename", f"{file_id}.bin"))

        st = self._load_state(file_id)
        have = [int(x) for x in st.get("received_blocks", [])][:k]
        idxs: List[int] = []
        datas: List[bytes] = []
        for bid in have:
            if self._has_block(file_id, filename=filename, block_id=bid):
                idxs.append(bid)
                datas.append(self._read_block(file_id, filename=filename, block_id=bid))
            if len(datas) >= k:
                break
        if len(datas) < k:
            return

        recovered = decode_blocks(k=k, m=m, idxs=idxs, datas=datas, codec=codec, orig_bytes=orig_bytes)
        out_dir = Path("experiments/data/recovered") / self.server_id
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / filename
        out_path.write_bytes(recovered)

    async def _broadcast_mesdr(self, *, file_id: str, ecinfo: Dict[str, Any]) -> None:
        """
        Stage 4: tell peers (entry servers) to stop sending any more blocks for this file to me.
        """
        for peer_id, host, tcp_port, _ in self.peers:
            try:
                await request_response(
                    host,
                    tcp_port,
                    Frame(
                        header={
                            "type": "mesdr_bcast",
                            "server_id": self.server_id,
                            "file_id": file_id,
                            "ecinfo": ecinfo,
                        }
                    ),
                    timeout_s=3.0,
                )
            except Exception:
                continue

    def _load_ecinfo(self, file_id: str) -> Optional[Dict[str, Any]]:
        p = self._file_dir(file_id) / "ecinfo.json"
        try:
            if not p.exists():
                return None
        except OSError:
            # WSL/OneDrive can throw EIO on stat during concurrent deletes.
            return None
        try:
            return json.loads(p.read_text())
        except (FileNotFoundError, OSError):
            # Race: file may disappear if a test script clears cache while heartbeat tasks run.
            return None

    def _has_block(self, file_id: str, *, filename: str, block_id: int) -> bool:
        shard_path = self._file_dir(file_id) / f"{filename}.block.{block_id}.bin"
        return shard_path.exists()

    def _read_block(self, file_id: str, *, filename: str, block_id: int) -> bytes:
        shard_path = self._file_dir(file_id) / f"{filename}.block.{block_id}.bin"
        return shard_path.read_bytes()

    def _missing_blocks(self, file_id: str, *, ecinfo: Dict[str, Any]) -> List[int]:
        begin_id = int(ecinfo.get("begin_id", 0))
        end_id = int(ecinfo.get("end_id", int(ecinfo["m"]) - 1))
        st = self._load_state(file_id)
        have = set(int(x) for x in st.get("received_blocks", []))
        return [i for i in range(begin_id, end_id + 1) if i not in have]

    def _cooldown_allows(self, file_id: str, block_id: int, *, cooldown_ms: int) -> bool:
        now = _now_ms()
        key = (file_id, int(block_id))
        nxt = self._cooldowns.get(key, 0)
        if now < nxt:
            return False
        self._cooldowns[key] = now + cooldown_ms
        return True

    def _ledger_has(self, ledger: Dict[str, Dict[int, Set[str]]], file_id: str, block_id: int, peer_id: str) -> bool:
        return peer_id in ledger.get(file_id, {}).get(int(block_id), set())

    def _ledger_mark(self, ledger: Dict[str, Dict[int, Set[str]]], file_id: str, block_id: int, peer_id: str) -> None:
        ledger.setdefault(file_id, {}).setdefault(int(block_id), set()).add(peer_id)

    async def _send_mesbt(
        self,
        *,
        peer_host: str,
        peer_port: int,
        file_id: str,
        filename: str,
        block_id: int,
        ecinfo: Dict[str, Any],
        payload: bytes,
        timeout_s: float = 10.0,
    ) -> Frame:
        if self.crashed:
            raise RuntimeError("node crashed")
        if should_drop(self.fault_e2e):
            raise RuntimeError("simulated_drop_e2e")
        await maybe_delay(self.fault_e2e)
        record_edge_to_edge(len(payload))
        return await request_response(
            peer_host,
            peer_port,
            Frame(
                header={
                    "type": "mesbt",
                    "sender_id": self.server_id,
                    "file_id": file_id,
                    "filename": filename,
                    "block_id": block_id,
                    "ecinfo": ecinfo,
                },
                payload=payload,
            ),
            timeout_s=timeout_s,
        )

    async def handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            if self.crashed:
                return
            frame = await read_frame(reader)
            msg_type = frame.header.get("type")
            if msg_type == "mesping":
                await write_frame(
                    writer,
                    Frame(
                        header={
                            "type": "mespong",
                            "server_id": self.server_id,
                            "echo_ts_ms": frame.header.get("ts_ms"),
                        }
                    ),
                )
                return
            if msg_type == "mesbd":
                # Stage 1: receive coded block from Cloud
                file_id = str(frame.header["file_id"])
                block_id = int(frame.header["block_id"])
                filename = str(frame.header.get("filename", "unknown"))
                ecinfo = frame.header.get("ecinfo", {})

                out_dir = self._file_dir(file_id)
                out_dir.mkdir(parents=True, exist_ok=True)
                shard_path = out_dir / f"{filename}.block.{block_id}.bin"
                shard_path.write_bytes(frame.payload)

                info_path = out_dir / "ecinfo.json"
                if not info_path.exists():
                    info_path.write_text(json.dumps(ecinfo, indent=2))

                self._mark_received(file_id, block_id)
                # Stage 3 pacing: allow Stage 2 forwarding a short head-start window
                # before supplement requests begin, reducing unnecessary mesbrq/mesbs in normal runs.
                st = self._load_state(file_id)
                if int(st.get("supplement_after_ms", 0)) <= 0:
                    st["supplement_after_ms"] = _now_ms() + int(ecinfo.get("block_req_timeout_ms", 600))
                    self._save_state(file_id)

                # Stage 2: forward to peers asynchronously (fire-and-forget)
                asyncio.create_task(
                    self._forward_to_peers(
                        file_id=file_id,
                        filename=filename,
                        block_id=block_id,
                        ecinfo=ecinfo,
                        payload=frame.payload,
                    )
                )

                await write_frame(
                    writer,
                    Frame(
                        header={
                            "type": "mesbdc",
                            "server_id": self.server_id,
                            "file_id": file_id,
                            "block_id": block_id,
                            "cached_path": str(shard_path),
                        }
                    ),
                )
                return

            if msg_type == "mesbt":
                # Stage 2: receive forwarded coded block from an entry server
                file_id = str(frame.header["file_id"])
                block_id = int(frame.header["block_id"])
                filename = str(frame.header.get("filename", "unknown"))
                sender_id = str(frame.header.get("sender_id", "unknown"))
                ecinfo = frame.header.get("ecinfo", {})

                if block_id in self.drop_mesbt_block_ids:
                    # Simulate a transmission failure on selected blocks (used to demo Stage 3 supplement).
                    await write_frame(
                        writer,
                        Frame(
                            header={
                                "type": "error",
                                "server_id": self.server_id,
                                "file_id": file_id,
                                "block_id": block_id,
                                "error": "simulated_drop_mesbt",
                                "to_sender": sender_id,
                            }
                        ),
                    )
                    return

                out_dir = self._file_dir(file_id)
                out_dir.mkdir(parents=True, exist_ok=True)
                shard_path = out_dir / f"{filename}.block.{block_id}.bin"
                if not shard_path.exists():
                    shard_path.write_bytes(frame.payload)

                info_path = out_dir / "ecinfo.json"
                if not info_path.exists():
                    info_path.write_text(json.dumps(ecinfo, indent=2))

                self._mark_received(file_id, block_id)

                # Decide response: mesdr if already has enough blocks, else mesbrc
                reconstructed = self._maybe_reconstruct(file_id, ecinfo=ecinfo)
                if reconstructed:
                    await write_frame(
                        writer,
                        Frame(
                            header={
                                "type": "mesdr",
                                "server_id": self.server_id,
                                "file_id": file_id,
                                "note": f"reconstructed (k={ecinfo.get('k')})",
                                "to_sender": sender_id,
                            }
                        ),
                    )
                else:
                    await write_frame(
                        writer,
                        Frame(
                            header={
                                "type": "mesbrc",
                                "server_id": self.server_id,
                                "file_id": file_id,
                                "block_id": block_id,
                                "to_sender": sender_id,
                            }
                        ),
                    )
                return

            if msg_type == "mesbrq":
                # Stage 3: TCP block request (request missing coded block)
                file_id = str(frame.header["file_id"])
                block_id = int(frame.header["block_id"])
                filename = str(frame.header["filename"])
                requester_id = str(frame.header.get("requester_id", "unknown"))
                ecinfo = frame.header.get("ecinfo", {})

                if self.crashed:
                    return
                if not self._has_block(file_id, filename=filename, block_id=block_id):
                    await write_frame(
                        writer,
                        Frame(
                            header={
                                "type": "error",
                                "server_id": self.server_id,
                                "error": f"block not found: {file_id} {filename} {block_id}",
                                "to_requester": requester_id,
                            }
                        ),
                    )
                    return
                # Dedup check for Stage 3 supplement:
                # if this exact (block_id, requester_id) was already supplied before, skip retransmission.
                if self._ledger_has(self._supplement_sent_ledger, file_id, block_id, requester_id):
                    self._dedup_stats["stage3_skipped_duplicate_supply"] += 1
                    await write_frame(
                        writer,
                        Frame(
                            header={
                                "type": "mesbs_skip",
                                "server_id": self.server_id,
                                "file_id": file_id,
                                "block_id": block_id,
                                "note": "duplicate_supply_skipped",
                                "to_requester": requester_id,
                            }
                        ),
                    )
                    return

                payload = self._read_block(file_id, filename=filename, block_id=block_id)
                record_edge_to_edge(len(payload))
                self._ledger_mark(self._supplement_sent_ledger, file_id, block_id, requester_id)
                self._dedup_stats["stage3_supplies_sent"] += 1
                await write_frame(
                    writer,
                    Frame(
                        header={
                            "type": "mesbs",
                            "server_id": self.server_id,
                            "file_id": file_id,
                            "filename": filename,
                            "block_id": block_id,
                            "ecinfo": ecinfo,
                            "to_requester": requester_id,
                        },
                        payload=payload,
                    ),
                )
                return

            if msg_type == "mesdr_bcast":
                # Stage 4: receiver confirms it reconstructed; stop sending blocks to it.
                file_id = str(frame.header["file_id"])
                reconstructed_id = str(frame.header.get("server_id", "unknown"))
                st = self._load_state(file_id)
                st["updated_at_ms"] = _now_ms()
                self._no_send_to.setdefault(file_id, set()).add(reconstructed_id)
                self._save_state(file_id)
                await write_frame(
                    writer,
                    Frame(
                        header={
                            "type": "mesdr_ack",
                            "server_id": self.server_id,
                            "file_id": file_id,
                            "stopped_sending_to": reconstructed_id,
                        }
                    ),
                )
                return

            await write_frame(
                writer,
                Frame(
                    header={
                        "type": "error",
                        "server_id": self.server_id,
                        "error": f"unexpected type={msg_type}",
                    }
                ),
            )
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _forward_to_peers(
        self,
        *,
        file_id: str,
        filename: str,
        block_id: int,
        ecinfo: Dict[str, Any],
        payload: bytes,
    ) -> None:
        st = self._load_state(file_id)
        ts = st.setdefault("tran_status", {})
        ts.setdefault(str(block_id), {})
        known_peer_blocks = self._peer_blocks.get(file_id, {})

        # Adaptive controlled fanout: don't blast all peers.
        # For (n=4,k=3), this yields fanout=2, reducing duplicate traffic.
        k = int(ecinfo.get("k", 1))
        m = int(ecinfo.get("m", len(self.peers) + 1))
        target_fanout = max(1, min(2, max(1, m - k + 1)))

        async def _one(peer_id: str, host: str, tcp_port: int) -> None:
            # Dedup check 1:
            # do not forward to peers that already reconstructed and broadcast mesdr_bcast.
            if peer_id in self._no_send_to.get(file_id, set()):
                self._dedup_stats["stage2_skipped_reconstructed_peer"] += 1
                ts[str(block_id)][peer_id] = 1
                self._save_state(file_id)
                return
            # Dedup check 2:
            # if this block was already sent once to this peer in Stage 2, skip further sends.
            if self._ledger_has(self._stage2_sent_ledger, file_id, block_id, peer_id):
                self._dedup_stats["stage2_skipped_already_sent"] += 1
                return
            # Skip peers that already acknowledged this block before.
            if int(ts[str(block_id)].get(peer_id, 0)) == 1:
                self._dedup_stats["stage2_skipped_already_sent"] += 1
                return
            # Skip peers that heartbeat says already have this block.
            if block_id in known_peer_blocks.get(peer_id, set()):
                self._dedup_stats["stage2_skipped_peer_has_block"] += 1
                ts[str(block_id)][peer_id] = 1
                self._save_state(file_id)
                return
            # Skip if already reconstructed locally; still forward in paper, but we allow it.
            try:
                self._ledger_mark(self._stage2_sent_ledger, file_id, block_id, peer_id)
                resp = await self._send_mesbt(
                    peer_host=host,
                    peer_port=tcp_port,
                    file_id=file_id,
                    filename=filename,
                    block_id=block_id,
                    ecinfo=ecinfo,
                    payload=payload,
                )
                if resp.header.get("type") in ("mesbrc", "mesdr"):
                    ts[str(block_id)][peer_id] = 1
                else:
                    ts[str(block_id)][peer_id] = 0
            except Exception:
                ts[str(block_id)][peer_id] = 0
            finally:
                self._save_state(file_id)

        # Adaptive scheduling: prioritize links with lower RTT / higher success.
        peer_order = self.netmon.rank_peers([pid for (pid, _h, _tcp, _udp) in self.peers])
        peer_map = {pid: (h, tcp) for (pid, h, tcp, _udp) in self.peers}
        ordered = [(pid, *peer_map[pid]) for pid in peer_order if pid in peer_map]
        ordered = ordered[:target_fanout]

        sem = asyncio.Semaphore(3)

        async def _wrapped(pid: str, host: str, tcp: int) -> None:
            async with sem:
                await _one(pid, host, tcp)

        await asyncio.gather(*[_wrapped(pid, host, tcp) for (pid, host, tcp) in ordered], return_exceptions=True)

    async def heartbeat_loop(self, *, interval_s: float = 0.1) -> None:
        """
        Stage 3 Step 1: broadcast heartbeat meshb every 100ms over UDP.
        """
        if self._udp_transport is None:
            return
        while True:
            try:
                # For each known file, broadcast received blocks.
                for file_id, st in list(self._file_states.items()):
                    hb = {
                        "type": "meshb",
                        "server_id": self.server_id,
                        "tcp_port": self.tcp_port,
                        "udp_port": self.udp_port,
                        "file_id": file_id,
                        "received_block_ids": st.get("received_blocks", []),
                    }
                    data = json.dumps(hb, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
                    for _, host, _, udp_port in self.peers:
                        self._udp_transport.sendto(data, (host, udp_port))
            finally:
                await asyncio.sleep(interval_s)

    async def handle_heartbeat(self, hb: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """
        Stage 3 Step 2: on heartbeat, request missing blocks from a peer that has them.
        We keep a per-block cooldown to avoid request storms.
        """
        try:
            if self.crashed:
                return
            if hb.get("type") != "meshb":
                return
            peer_id = str(hb.get("server_id"))
            peer_host = addr[0]
            peer_tcp_port = int(hb.get("tcp_port"))
            file_id = str(hb.get("file_id"))
            peer_blocks = set(int(x) for x in hb.get("received_block_ids", []))
            self._peer_blocks.setdefault(file_id, {})[peer_id] = peer_blocks

            ecinfo = self._load_ecinfo(file_id)
            if ecinfo is None:
                return
            if self._load_state(file_id).get("reconstructed"):
                return
            st = self._load_state(file_id)
            if _now_ms() < int(st.get("supplement_after_ms", 0)):
                return

            missing = self._missing_blocks(file_id, ecinfo=ecinfo)
            if not missing:
                return
            # Fix 4 (Stage 3): avoid over-requesting while there are enough
            # already-received + in-flight blocks to satisfy k for reconstruction.
            k_needed = int(ecinfo.get("k", 1))
            st = self._load_state(file_id)
            have_cnt = len(set(int(x) for x in st.get("received_blocks", [])))
            pending_cnt = len(self._pending_requests.setdefault(file_id, set()))
            if have_cnt + pending_cnt >= k_needed:
                return

            # Pick one missing block that the peer claims to have.
            candidates = [b for b in missing if b in peer_blocks]
            if not candidates:
                return

            block_id = random.choice(candidates)
            cooldown_ms = int(ecinfo.get("block_req_timeout_ms", 600))  # reasonable default for simulation
            if not self._cooldown_allows(file_id, block_id, cooldown_ms=cooldown_ms):
                return
            # Fix 4 (Stage 3): suppress duplicate mesbrq for same block in this request window.
            if block_id in self._pending_requests.setdefault(file_id, set()):
                self._dedup_stats["stage3_skipped_duplicate_request"] += 1
                return
            self._pending_requests.setdefault(file_id, set()).add(block_id)
            # Clear pending flag when this request window ends (new supplement round),
            # so a later retry is possible if no payload arrives.
            asyncio.get_running_loop().call_later(
                max(0.05, cooldown_ms / 1000.0),
                self._clear_pending_request,
                file_id,
                block_id,
            )

            filename = str(ecinfo.get("filename", "unknown"))
            if filename == "unknown":
                filename = f"{file_id}.bin"

            resp = await request_response(
                peer_host,
                peer_tcp_port,
                Frame(
                    header={
                        "type": "mesbrq",
                        "requester_id": self.server_id,
                        "file_id": file_id,
                        "filename": filename,
                        "block_id": block_id,
                        "ecinfo": ecinfo,
                    }
                ),
                timeout_s=5.0,
            )
            if resp.header.get("type") != "mesbs":
                # Non-mesbs means request did not yield a block now; unlock early for retry.
                self._clear_pending_request(file_id, block_id)
                return

            # Store supplemented block
            out_dir = self._file_dir(file_id)
            out_dir.mkdir(parents=True, exist_ok=True)
            shard_path = out_dir / f"{filename}.block.{block_id}.bin"
            if not shard_path.exists():
                shard_path.write_bytes(resp.payload)
            self._mark_received(file_id, block_id)
            self._maybe_reconstruct(file_id, ecinfo=ecinfo)
        except OSError:
            # Ignore filesystem EIO during concurrent cleanup.
            return
        except Exception:
            return


class _UDPProtocol(asyncio.DatagramProtocol):
    def __init__(self, server: EdgeServer):
        self.server = server

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.server._udp_transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        try:
            msg = json.loads(data.decode("utf-8"))
        except Exception:
            return
        asyncio.create_task(self.server.handle_heartbeat(msg, addr))


async def run_server(host: str, tcp_port: int, udp_port: int, *, server: EdgeServer) -> None:
    loop = asyncio.get_running_loop()
    if server.fault_e2e.enabled and server.fault_e2e.crash_after_s and server.fault_e2e.crash_after_s > 0:
        loop.call_later(server.fault_e2e.crash_after_s, server._crash)
    await loop.create_datagram_endpoint(lambda: _UDPProtocol(server), local_addr=(host, udp_port))
    srv = await asyncio.start_server(server.handle_conn, host, tcp_port)
    hb_task = asyncio.create_task(server.heartbeat_loop(interval_s=0.1))
    netmon_export_task = asyncio.create_task(server._export_netmon_loop(interval_s=0.5))
    state_export_task = asyncio.create_task(server._export_state_loop(interval_s=0.4))
    server.netmon.start()
    try:
        async with srv:
            await srv.serve_forever()
    finally:
        hb_task.cancel()
        netmon_export_task.cancel()
        state_export_task.cancel()
        server.netmon.stop()
        print(
            f"[edge_server:{server.server_id}] dedup_stats="
            f"{json.dumps(server._dedup_stats, ensure_ascii=False)}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Edge node receiver (Stage 1+2+3)")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, required=True, help="TCP port")
    parser.add_argument("--udp-port", type=int, required=True, help="UDP port for heartbeats")
    parser.add_argument("--id", dest="server_id", required=True)
    parser.add_argument("--cache-dir", default="experiments/data/edge_cache")
    parser.add_argument(
        "--drop-mesbt-block-ids",
        default="",
        help="Comma-separated block IDs to drop on mesbt receive (demo/testing). Example: 2,3",
    )
    parser.add_argument(
        "--peers",
        default="",
        help="Comma-separated peers: e1@127.0.0.1:tcp:udp,e2@127.0.0.1:tcp:udp ... (excluding self)",
    )
    args = parser.parse_args()

    peers: List[Tuple[str, str, int, int]] = []
    if args.peers.strip():
        for part in args.peers.split(","):
            part = part.strip()
            if not part:
                continue
            peer_id, addr = part.split("@", 1)
            host, tcp_s, udp_s = addr.split(":")
            peers.append((peer_id, host, int(tcp_s), int(udp_s)))

    drop_ids: Set[int] = set()
    if args.drop_mesbt_block_ids.strip():
        drop_ids = {int(x.strip()) for x in args.drop_mesbt_block_ids.split(",") if x.strip()}

    server = EdgeServer(
        server_id=args.server_id,
        cache_dir=args.cache_dir,
        tcp_port=args.port,
        udp_port=args.udp_port,
        peers=peers,
        drop_mesbt_block_ids=drop_ids,
    )
    asyncio.run(run_server(args.host, args.port, args.udp_port, server=server))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

