import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional


MiB = 1024 * 1024


@dataclass
class Metrics:
    scenario: str
    method: str  # "edgehydra" | "edgedis"
    n: int
    k: Optional[int] = None
    m: Optional[int] = None
    file_path: str = ""
    file_bytes: int = 0

    started_at_ms: int = 0
    ended_at_ms: int = 0

    cloud_to_edge_bytes: int = 0
    edge_to_edge_bytes: int = 0

    extra: Dict[str, Any] = field(default_factory=dict)

    def start(self) -> None:
        self.started_at_ms = int(time.time() * 1000)

    def stop(self) -> None:
        self.ended_at_ms = int(time.time() * 1000)

    @property
    def distribution_time_s(self) -> float:
        if not self.started_at_ms or not self.ended_at_ms:
            return 0.0
        return max(0.0, (self.ended_at_ms - self.started_at_ms) / 1000.0)

    def distribution_cost_units(self, *, cloud_unit_per_mib: float = 1.0) -> float:
        """
        Cost model from paper:
          - Cloud->Edge: x per MiB
          - Edge->Edge: x/9 per MiB
        """
        c2e = (self.cloud_to_edge_bytes / MiB) * cloud_unit_per_mib
        e2e = (self.edge_to_edge_bytes / MiB) * (cloud_unit_per_mib / 9.0)
        return c2e + e2e

    def to_json(self) -> Dict[str, Any]:
        return {
            "scenario": self.scenario,
            "method": self.method,
            "n": self.n,
            "k": self.k,
            "m": self.m,
            "file_path": self.file_path,
            "file_bytes": self.file_bytes,
            "started_at_ms": self.started_at_ms,
            "ended_at_ms": self.ended_at_ms,
            "distribution_time_s": self.distribution_time_s,
            "cloud_to_edge_bytes": self.cloud_to_edge_bytes,
            "edge_to_edge_bytes": self.edge_to_edge_bytes,
            "distribution_cost_units": self.distribution_cost_units(),
            "extra": self.extra,
        }


class MetricsRecorder:
    def __init__(self, metrics: Metrics):
        self.metrics = metrics

    def add_cloud_to_edge(self, payload_bytes: int) -> None:
        self.metrics.cloud_to_edge_bytes += int(payload_bytes)

    def add_edge_to_edge(self, payload_bytes: int) -> None:
        self.metrics.edge_to_edge_bytes += int(payload_bytes)

    def write_json(self, out_path: str) -> None:
        p = Path(out_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(self.metrics.to_json(), indent=2))


_ACTIVE: Optional[MetricsRecorder] = None


def set_active(rec: Optional[MetricsRecorder]) -> None:
    global _ACTIVE
    _ACTIVE = rec


def active() -> Optional[MetricsRecorder]:
    return _ACTIVE


def _sink_path() -> Optional[str]:
    p = os.getenv("METRICS_SINK", "").strip()
    return p or None


def _append_event(direction: str, payload_bytes: int) -> None:
    """
    Cross-process metric sink (jsonl). Safe enough for our simulation.
    Env:
      METRICS_SINK=/tmp/metrics-run123/edge_e1.jsonl
    """
    sink = _sink_path()
    if sink is None:
        return
    Path(sink).parent.mkdir(parents=True, exist_ok=True)
    evt = {
        "direction": direction,
        "bytes": int(payload_bytes),
        "ts_ms": int(time.time() * 1000),
        "event": "payload_transfer",
        "event_kind": "protocol",
    }
    with open(sink, "a", encoding="utf-8") as f:
        f.write(json.dumps(evt) + "\n")


def record_cloud_to_edge(payload_bytes: int) -> None:
    rec = active()
    if rec is not None:
        rec.add_cloud_to_edge(payload_bytes)
    else:
        _append_event("cloud_to_edge", payload_bytes)


def record_edge_to_edge(payload_bytes: int) -> None:
    rec = active()
    if rec is not None:
        rec.add_edge_to_edge(payload_bytes)
    else:
        _append_event("edge_to_edge", payload_bytes)

