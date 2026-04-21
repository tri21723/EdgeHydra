import asyncio
import os
import random
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class FaultConfig:
    """
    Fault injection config.
    - delay: adds latency before send/handle
    - drop: randomly drops sends (simulated packet loss)
    """

    enabled: bool = False
    delay_min_ms: int = 0
    delay_max_ms: int = 0
    drop_prob: float = 0.0
    crash_after_s: float = -1.0  # <0 means never

    @staticmethod
    def from_env(prefix: str) -> "FaultConfig":
        """
        Read config from env:
          <prefix>_ENABLED=1
          <prefix>_DELAY_MIN_MS=20
          <prefix>_DELAY_MAX_MS=40
          <prefix>_DROP_PROB=0.03
        """
        enabled = os.getenv(f"{prefix}_ENABLED", "0") in ("1", "true", "True")
        return FaultConfig(
            enabled=enabled,
            delay_min_ms=int(os.getenv(f"{prefix}_DELAY_MIN_MS", "0")),
            delay_max_ms=int(os.getenv(f"{prefix}_DELAY_MAX_MS", "0")),
            drop_prob=float(os.getenv(f"{prefix}_DROP_PROB", "0.0")),
            crash_after_s=float(os.getenv(f"{prefix}_CRASH_AFTER_S", "-1")),
        )


async def maybe_delay(cfg: FaultConfig) -> None:
    if not cfg.enabled:
        return
    if cfg.delay_max_ms <= 0:
        return
    lo = max(0, cfg.delay_min_ms)
    hi = max(lo, cfg.delay_max_ms)
    await asyncio.sleep(random.uniform(lo, hi) / 1000.0)


def should_drop(cfg: FaultConfig) -> bool:
    if not cfg.enabled:
        return False
    if cfg.drop_prob <= 0:
        return False
    return random.random() < cfg.drop_prob

