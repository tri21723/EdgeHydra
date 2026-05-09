import asyncio
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from common.transport import Frame, request_response


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class LinkStats:
    peer_id: str
    ewma_rtt_ms: float = 9999.0
    sent: int = 0
    ok: int = 0
    last_ok_ms: int = 0

    @property
    def ok_rate(self) -> float:
        if self.sent <= 0:
            return 0.0
        return float(self.ok) / float(self.sent)


class NetworkStateMonitor:
    """
    Very lightweight monitor:
      - Probes peers via TCP (mesping/mespong).
      - Maintains EWMA RTT and success rate (as packet-loss proxy).
    """

    def __init__(
        self,
        *,
        self_id: str,
        peers: List[Tuple[str, str, int]],
        probe_interval_s: float = 0.5,
        probe_timeout_s: float = 0.25,
        ewma_alpha: float = 0.25,
    ):
        self.self_id = self_id
        self.peers = peers  # (peer_id, host, tcp_port)
        self.probe_interval_s = probe_interval_s
        self.probe_timeout_s = probe_timeout_s
        self.ewma_alpha = ewma_alpha
        self._stats: Dict[str, LinkStats] = {pid: LinkStats(peer_id=pid) for (pid, _, _) in peers}
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self.run())

    def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None

    def stats(self, peer_id: str) -> LinkStats:
        return self._stats.setdefault(peer_id, LinkStats(peer_id=peer_id))

    def rank_peers(self, peer_ids: List[str]) -> List[str]:
        """
        Lower score is better.
        Score combines RTT (primary) and failure rate (secondary).
        """

        def score(pid: str) -> float:
            st = self._stats.get(pid)
            if st is None:
                return 1e9
            # Penalize loss heavily; ok_rate in [0,1]
            loss_penalty = (1.0 - st.ok_rate) * 5000.0
            return float(st.ewma_rtt_ms) + loss_penalty

        return sorted(peer_ids, key=score)

    def snapshot(self) -> Dict[str, object]:
        peers: List[Dict[str, object]] = []
        ids = [pid for (pid, _h, _p) in self.peers]
        ranked = self.rank_peers(ids)
        for pid in ids:
            st = self._stats.get(pid) or LinkStats(peer_id=pid)
            peers.append(
                {
                    "peer_id": pid,
                    "ewma_rtt_ms": float(st.ewma_rtt_ms),
                    "ok_rate": float(st.ok_rate),
                    "sent": int(st.sent),
                    "ok": int(st.ok),
                    "last_ok_ms": int(st.last_ok_ms),
                }
            )
        return {
            "self_id": self.self_id,
            "probe_interval_s": float(self.probe_interval_s),
            "probe_timeout_s": float(self.probe_timeout_s),
            "ewma_alpha": float(self.ewma_alpha),
            "ranked_peers": ranked,
            "peers": peers,
            "ts_ms": _now_ms(),
        }

    async def _probe_one(self, peer_id: str, host: str, port: int) -> None:
        st = self.stats(peer_id)
        st.sent += 1
        t0 = time.perf_counter()
        try:
            resp = await request_response(
                host,
                port,
                Frame(header={"type": "mesping", "from": self.self_id, "ts_ms": _now_ms()}),
                timeout_s=self.probe_timeout_s,
            )
            if resp.header.get("type") != "mespong":
                return
            rtt_ms = (time.perf_counter() - t0) * 1000.0
            st.ok += 1
            st.last_ok_ms = _now_ms()
            # EWMA update
            if st.ewma_rtt_ms >= 9999.0:
                st.ewma_rtt_ms = rtt_ms
            else:
                st.ewma_rtt_ms = (1.0 - self.ewma_alpha) * st.ewma_rtt_ms + self.ewma_alpha * rtt_ms
        except Exception:
            return

    async def run(self) -> None:
        while True:
            await asyncio.gather(
                *[self._probe_one(pid, host, port) for (pid, host, port) in self.peers],
                return_exceptions=True,
            )
            await asyncio.sleep(self.probe_interval_s)

