# Experiment report — tables only (IS211)

## Table 1 — Fault scenarios (1 MiB, *n* = 3)

| Scenario | EdgeHydra time | EdgeHydra cost | EdgeDis time | EdgeDis cost | Speedup | Cost overhead | Trade-off score |
|----------|----------------|----------------|---------------|--------------|---------|---------------|----------------|
| normal | 0.096 ± 0.021 s | 1.630 | 1.766 ± 0.187 s | 1.444 | 18.4× | 1.13× | 16.3 |
| slow_servers | 0.133 ± 0.014 s | 1.630 | 2.022 ± 0.635 s | 1.519 | 15.2× | 1.07× | 14.1 |
| failed_servers | 0.095 ± 0.022 s | 1.580 | 1.937 ± 0.910 s | 1.389 | 20.5× | 1.14× | 18.0 |

## Table 2 — Variable file sizes (*n* = 3 each)

| File size | EdgeHydra time | EdgeHydra cost | EdgeDis time | EdgeDis cost | Speedup | Cost overhead | Score |
|-----------|----------------|----------------|---------------|--------------|---------|----------------|-------|
| 1 MiB | 0.124 ± 0.009 s | 1.6 | 1.534 ± 0.188 s | 1.4 | 12.4× | 1.15× | 10.8 |
| 24 MiB | 0.239 ± 0.064 s | 39.1 | 2.493 ± 0.380 s | 34.7 | 10.4× | 1.13× | 9.3 |
| 48 MiB | 0.363 ± 0.077 s | 78.2 | 3.775 ± 0.942 s | 64.4 | 10.4× | 1.21× | 8.6 |
| 72 MiB | 0.463 ± 0.092 s | 117.3 | 2.419 ± 0.258 s | 97.3 | 5.2× | 1.21× | 4.3 |
| 96 MiB | 0.566 ± 0.101 s | 156.4 | 2.785 ± 0.166 s | 130.7 | 4.9× | 1.20× | 4.1 |
| 120 MiB | 0.985 ± 0.462 s | 195.6 | 3.345 ± 0.690 s | 163.3 | 3.4× | 1.20× | 2.8 |
| 144 MiB | 0.879 ± 0.397 s | 234.7 | 4.108 ± 0.831 s | 196.0 | 4.7× | 1.20× | 3.9 |

## Table 3 — E2E / C2E overhead

| Protocol | E2E / C2E ratio |
|----------|-----------------|
| EdgeHydra | ×2.00 (fixed across all file sizes) |
| EdgeDis | ×3.08 – ×4.00 (varying) |

## Table 4 — Time–cost trade-off (variable sizes)

Formula: trade-off score = speedup / cost overhead

| File | Speedup | Cost overhead | Trade-off score |
|------|--------:|--------------:|----------------:|
| 1 MiB | 12.4× | 1.15× | 10.8 |
| 24 MiB | 10.4× | 1.13× | 9.3 |
| 48 MiB | 10.4× | 1.21× | 8.6 |
| 72 MiB | 5.2× | 1.21× | 4.3 |
| 96 MiB | 4.9× | 1.20× | 4.1 |
| 120 MiB | 3.4× | 1.20× | 2.8 |
| 144 MiB | 4.7× | 1.20× | 3.9 |
