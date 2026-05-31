# EdgeHydra: A Fault-Tolerant Edge Data Distribution Protocol

**IS211 – Cơ sở dữ liệu phân tán | Experiment Report**

**Date:** May 3, 2026  

**Design:** *n* = 3 repeated runs per configuration · **4 scenarios** (normal, slow_servers, failed_servers, variable_file_sizes) · **2 protocols** compared (EdgeHydra vs. EdgeDis)

---

## 1. Cover

This section states the document metadata above: **title**, **course / report type**, **date (May 3, 2026)**, **n = 3** independent repeated runs, **four** evaluation scenarios, and comparison of **two** protocols (EdgeHydra, EdgeDis).

---

## 2. Introduction

Distributing large objects to resource-constrained edge nodes must stay **reliable under faults** (slow or failed peers) while limiting latency and traffic. **EdgeHydra** addresses this with **erasure coding** using **zfec with *k* = 3, *m* = 4**: any **3 of 4** coded shards suffice to reconstruct the object, so the system **tolerates one missing or failed node** while the cloud still sends only a modest expansion (*m*/*k* = 4/3) over the raw file. **EdgeDis** is a **naive slicing baseline**: the file is split across four edges and delivery assumes **all four slices** participate, with no erasure recovery path. We report two metrics. **(1) Distribution time** — `distribution_time_s` = (*max* − *min*) `ts_ms` over all JSONL metric events, capturing the active transfer window rather than compose startup noise. **(2) Distribution cost** — `distribution_cost` = C2E in MiB × **1.0** + E2E in MiB × **(1/9)**, reflecting cloud egress as primary and edge-to-edge relay as cheaper.

---

## 3. Experimental Setup

- **Topology:** *n* = **4** nodes (1 cloud + 3 edges). EdgeHydra: **k = 3**, **m = 4** (zfec). EdgeDis: four-way slice; aggregator uses *k* = *m* = 0 in recorded metadata for the baseline run.
- **Scenarios:** **normal**, **slow_servers** (injected E2E delay on three edges), **failed_servers** (one edge crash after 1 s), **variable_file_sizes** (sweep of file sizes).
- **File sizes:** **1, 24, 48, 72, 96, 120, 144 MiB** for the variable sweep; fault scenarios use a **1 MiB** file.
- **Repetitions:** Each configuration **repeated *n* = 3** times; tables report **mean ± std** for time where applicable.
- **Environment:** **Docker Compose** deployment, Python **asyncio** control plane, **TCP** bulk data plane, **UDP** heartbeat/signaling between edges.

---

## 4. Results — Fault Scenarios (1 MiB, *n* = 3)

| Scenario | EdgeHydra time | EdgeHydra cost | EdgeDis time | EdgeDis cost | Speedup | Cost overhead | Trade-off score |
|----------|----------------|----------------|---------------|--------------|---------|---------------|----------------|
| normal | **0.096 ± 0.021** s | 1.630 | **1.766 ± 0.187** s | 1.444 | **18.4×** | **1.13×** | **16.3** |
| slow_servers | **0.133 ± 0.014** s | 1.630 | **2.022 ± 0.635** s | 1.519 | **15.2×** | **1.07×** | **14.1** |
| failed_servers | **0.095 ± 0.022** s | 1.580 | **1.937 ± 0.910** s | 1.389 | **20.5×** | **1.14×** | **18.0** |

**EdgeHydra is fastest in all three scenarios.** Under **failed_servers**, speedup peaks at **20.5×** because EdgeHydra only needs **k = 3** shards for reconstruction even when **one edge has crashed**, whereas EdgeDis must rely on timeouts/retries for a missing slice. Under **slow_servers**, **15.2×** speedup is consistent with **adaptive scheduling** (EWMA-based RTT ranking) steering work away from slow nodes. Across these runs, **cost overhead stays modest — 1.07×–1.14×** — a small price for **order-of-magnitude** latency reductions.

---

## 5. Results — Variable File Sizes (*n* = 3 each)

| File size | EdgeHydra time | EdgeHydra cost | EdgeDis time | EdgeDis cost | Speedup | Cost overhead | Score |
|-----------|----------------|----------------|---------------|--------------|---------|----------------|-------|
| 1 MiB | **0.124 ± 0.009** s | **1.6** | **1.534 ± 0.188** s | **1.4** | **12.4×** | **1.15×** | **10.8** |
| 24 MiB | **0.239 ± 0.064** s | **39.1** | **2.493 ± 0.380** s | **34.7** | **10.4×** | **1.13×** | **9.3** |
| 48 MiB | **0.363 ± 0.077** s | **78.2** | **3.775 ± 0.942** s | **64.4** | **10.4×** | **1.21×** | **8.6** |
| 72 MiB | **0.463 ± 0.092** s | **117.3** | **2.419 ± 0.258** s | **97.3** | **5.2×** | **1.21×** | **4.3** |
| 96 MiB | **0.566 ± 0.101** s | **156.4** | **2.785 ± 0.166** s | **130.7** | **4.9×** | **1.20×** | **4.1** |
| 120 MiB | **0.985 ± 0.462** s | **195.6** | **3.345 ± 0.690** s | **163.3** | **3.4×** | **1.20×** | **2.8** |
| 144 MiB | **0.879 ± 0.397** s | **234.7** | **4.108 ± 0.831** s | **196.0** | **4.7×** | **1.20×** | **3.9** |

Speedup is **highest on small files** (**12.4×** at **1 MiB**), where fixed protocol and orchestration overhead **amortizes** less for EdgeDis than for the coded path. EdgeHydra’s wall-clock time scales **sub-linearly** in this sweep (**1 MiB → 144 MiB**: **0.124 s → 0.879 s**, roughly **×7×** duration for **×144×** bytes), while EdgeDis grows **super-linearly** (**1.534 s → 4.108 s**, **×2.7×** for the same ratio). At **144 MiB**, EdgeHydra remains **4.7× faster** despite **1.20× higher cost**. **EdgeDis shows much higher std than EdgeHydra** — for example **48 MiB**: **EdgeDis 0.942 s** vs. **EdgeHydra 0.077 s** — indicating **greater run-to-run jitter** under the naive slicing policy.

---

## 6. Results — E2E Traffic Overhead (E2E / C2E)

| Protocol | E2E / C2E ratio |
|----------|-----------------|
| **EdgeHydra** | **×2.00** (fixed across all file sizes) |
| **EdgeDis** | **×3.08 – ×4.00** (varies by configuration) |

**EdgeHydra’s ratio is exactly ×2.00** for every file size — **deterministic edge relaying**: each coded shard is forwarded to **exactly one** peer under the evaluated topology, yielding a stable amplification factor. **EdgeDis spans ×3.08–×4.00**, reflecting **variable multi-hop forwarding** and less regular traffic patterns. **Lower E2E/C2E** implies **less inter-edge traffic** for the same cloud delivery, which matters on **bandwidth-limited** edge backhaul.

---

## 7. Results — Time–Cost Trade-off (variable sizes)

**Definitions:** *Speedup* = EdgeDis mean time / EdgeHydra mean time. *Cost overhead* = EdgeHydra mean cost / EdgeDis mean cost. **Trade-off score** = speedup / cost overhead.

| File | Speedup | Cost overhead | Trade-off score |
|------|--------:|--------------:|----------------:|
| 1 MiB | **12.4×** | **1.15×** | **10.8** |
| 24 MiB | **10.4×** | **1.13×** | **9.3** |
| 48 MiB | **10.4×** | **1.21×** | **8.6** |
| 72 MiB | **5.2×** | **1.21×** | **4.3** |
| 96 MiB | **4.9×** | **1.20×** | **4.1** |
| 120 MiB | **3.4×** | **1.20×** | **2.8** |
| 144 MiB | **4.7×** | **1.20×** | **3.9** |

**Average trade-off score = 6.3:** conceptually, **each 1 unit of relative cost increase buys about 6.3 units of speedup** under the score definition above. The **best trade-offs** appear at **1 MiB (10.8)** and **24 MiB (9.3)**. Even at **120 MiB**, where the score is lowest (**2.8**), EdgeHydra still delivers **3.4× speedup** for **1.20× cost** — a **favorable** trade for **latency-sensitive** workloads.

**Summary statistics (as reported):** **Average speedup 7.4×** (range **3.4×–12.4×**). **Average cost overhead 1.18×**. **Slow servers speedup: 15.2×**. **Failed servers speedup: 20.5×**.

---

## 8. Discussion

**Why EdgeHydra is faster.** Erasure coding lets the system finish as soon as any **k = 3** of **m = 4** shards are available. In **failed_servers**, EdgeHydra can still **reconstruct** after **one crashed edge**, while EdgeDis must **wait** for recovery paths or suffer long tails. In **slow_servers**, **adaptive scheduling** (EWMA RTT ranking) **deprioritizes slow nodes**, keeping the critical path off stragglers.

**Why cost is higher.** Cloud-to-edge volume for EdgeHydra scales with coding expansion **m/k = 4/3** vs. **×1.0** raw bytes for slicing. Observed **E2E/C2E** is **×2.00** for EdgeHydra vs. **×3.08–×4.00** for EdgeDis, so relay cost is actually **better controlled** under EdgeHydra. **Net measured cost overhead is ~1.18×** — an acceptable premium given **speedups** in the teens to twenties on faults and multi-fold gains on typical file sizes.

**Consistency.** EdgeHydra’s **standard deviation** stays **narrower** than EdgeDis throughout the variable sweep, especially on **large** files. Predictable tails matter for **SLAs** more than averages alone.

**Limitations.** **Docker Compose** on a single host **smooths WAN effects** and may **underestimate** real wide-area latency.**n = 3** runs establish **directional** trends but not tight confidence intervals. The **cost model** (**E2E** weighted **1/9** of **C2E**) is a **design choice**; different weights could flip **`cost_winner`** in marginal cases without changing timed delivery superiority.

---

## 9. Conclusion

EdgeHydra achieves **3.4×–20.5× speedup** over EdgeDis across all tested configurations at **only 1.07×–1.21× cost overhead**. **Average trade-off score 6.3** confirms that **erasure-coded delivery plus adaptive scheduling** buys **substantial latency reduction** for a **small** relative cost increase. Together with **deterministic ×2.00 E2E/C2E** and **lower timing variance**, these results support EdgeHydra as a **practical protocol** for **fault-tolerant, latency-sensitive edge data distribution**.

---

*End of report.*
