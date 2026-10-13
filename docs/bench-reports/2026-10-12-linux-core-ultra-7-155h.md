# Benchmark: Linux laptop, Intel Core Ultra 7 155H (2026-10-12/13)

The first measurements of the POC: 4 validators on one machine, 800 ms block pacing, APT
transfers through the MoveVM. Run with [the benchmarking workflow](../benchmarking.md); use this
as the baseline when testing other machines.

## Summary

| | Result |
|---|---|
| Sustained TPS (all committed, p50 finality under ~3 s) | **~1,000** (1.6-1.8 s p50, 2.5 s p99, blocks every ~830 ms) |
| Peak committed TPS | **~2,200** at 3,000 offered; every transaction committed, but p50 finality 14 s, so a queue was building |
| Above ~3,000 offered | Throughput falls (1,400-1,700), timeouts rise and the queue never drains |
| Main limit | CPU: every validator executes every block, and all 4 share one 22-thread laptop CPU |

Between 1,000 and ~3,000 offered, the chain commits ~1,300-2,200 TPS, but finality grows from
seconds to tens of seconds as the mempool backs up.

## Machine and software

| | |
|---|---|
| CPU | Intel Core Ultra 7 155H, 22 logical CPUs: 6 performance cores with hyper-threading (CPUs 0-11, 4.5-4.8 GHz), 8 efficiency cores (12-19, 3.8 GHz), 2 low-power cores (20-21, 2.5 GHz) |
| Memory, disk | 30 GB, NVMe |
| OS | Ubuntu 22.04, Linux 6.8 |
| Also running | VS Code, GNOME, a security agent (wazuh, ~30% of one CPU) |
| chain | `e5c3baa`, release build (`debug = 1`, `overflow-checks = true`), Rust 1.98.1 |
| aptos-core | `b63273ec49` (main, 2026-10-11) |
| Network | 4 validators on 127.0.0.1, QUIC; client = `chain-bench remote` through RPC |
| Workload | APT transfers between random accounts, 4 x tps funded accounts (minimum 20,000) |

## 1. Execution ceiling

`chain-bench exec`, MoveVM, RocksDB, 20,000 accounts; no consensus or networking.

One validator alone:

| Threads | Block size | Execute TPS | Execute + commit TPS |
|---|---|---|---|
| 4 | 5,000 | 5,568 | 5,277 |
| 8 | 5,000 | 8,046 | 7,331 |
| 4 | 1,000 | | 5,149 |
| 4 | 3,000 | | 5,132 |

Block size barely matters between 1,000 and 5,000 transactions.

Four copies at once (what 4 co-located validators do), 1,000-transaction blocks,
`RAYON_NUM_THREADS=2`:

| Threads each | Execute + commit TPS, each |
|---|---|
| 2 | 1,610-1,760 |
| 3 | 2,130-2,310 |
| 4 | 2,110-2,120 |
| 5 | 2,300-2,390 |

Sharing the CPU four ways cuts each validator from ~5,100 to ~2,300 TPS. The chain cannot exceed
that, since every validator executes every block.

## 2. End to end, CPUs pinned (`scripts/bench-local.sh` defaults)

Each validator was pinned to 5 CPUs (3 performance-core threads plus 2 efficiency cores) with 4
Block-STM threads. The load generator ran on CPUs 20-21, with `RAYON_NUM_THREADS=2`. Each run
lasted 60 s with the first 15 s excluded (the 1,000 rows: 20 s, first 5 s excluded).

| Offered TPS | Committed TPS | p50 finality ms | p99 finality ms | Committed / sent | Timeouts | Block gap p50 ms | Blocks/s | Txns/block |
|---|---|---|---|---|---|---|---|---|
| 1000 | 1005-1060 | 1550-1780 | 2520-2550 | 20000 / 20000 | 0-2 | 830-834 | 1.20 | 770-800 |
| 2000 | 1283 | 16920 | 25152 | 112420 / 120000 | 3 | 515 | 1.64 | 878 |
| 3000 | 1848 | 21299 | 31733 | 178553 / 180000 | 6 | 688 | 1.17 | 1935 |
| 4000 | 1486 | 38472 | 48169 | 146601 / 240000 | 5 | 880 | 0.93 | 2053 |
| 5000 | 1540 | 37703 | 42298 | 211088 / 300000 | 3 | 970 | 0.94 | 2912 |

## 3. End to end, unpinned

Each validator ran 5 Block-STM threads with `RAYON_NUM_THREADS=3`, scheduled freely across all 22
CPUs. Each run lasted 60 s with the first 15 s excluded.

| Offered TPS | Committed TPS | p50 finality ms | p99 finality ms | Committed / sent | Timeouts | Block gap p50 ms | Blocks/s | Txns/block |
|---|---|---|---|---|---|---|---|---|
| 1000 (20 s run) | 1053 | 1727 | 2519 | 20000 / 20000 | 0 | 824 | 1.20 | 800 |
| 1500 | 1320 | 5990 | 18747 | 90000 / 90000 | 4 | 689 | 1.41 | 891 |
| 2000 | 1311 | 18122 | 28011 | 107177 / 120000 | 4 | 563 | 1.44 | 944 |
| 2500 | 1317 | 24006 | 37308 | 125337 / 150000 | 5 | 774 | 1.05 | 1547 |
| 3000 (run 1) | 3403* | 6239 | 22811 | 180000 / 180000 | 5 | 962 | 0.90 | 3051 |
| 3000 (run 2) | 2246 | 14153 | 33276 | 180000 / 180000 | 7 | 826 | 0.90 | 2769 |
| 4000 | 1712 | 29546 | 44783 | 219996 / 240000 | 10 | 936 | 0.82 | 3422 |
| 5000 | 1648 | 44305 | 52440 | 156650 / 300000 | 6 | 1136 | 0.66 | 3012 |
| 6000 | 1415 | 36929 | 61524 | 125843 / 360000 | 16 | 1850 | 0.48 | 3251 |

\* Above the offered rate: a backlog drained inside the measured window. Not a capacity figure.

Earlier unpinned runs with the binary's defaults (4 Block-STM threads, rayon pool = all 22 CPUs
per validator) committed only 920-1,280 TPS at 2,000 offered. The oversized rayon pools (88
threads across 4 validators) starve Block-STM; `RAYON_NUM_THREADS=2` raised that to 1,193.

Pinning gives steadier but not higher numbers than unpinned on this CPU, because it caps each
validator at 5 CPUs.

## 4. Findings

1. **Co-location is the dominant limit.** Each validator re-executes every block, so 4
   validators on one CPU do 4x the execution work. Per-validator execution drops from ~5,100
   to ~2,300 TPS (section 1), and end-to-end peak sits just below that.
2. **Blocks stay small at moderate load and come early.** The adaptive block size
   (`BlockSizer` in `crates/node/src/validator.rs`) caps proposals at the measured execution rate
   x 800 ms, about 800-950 transactions here. A leader proposes before 800 ms whenever a
   full block is waiting (`has_full_block`). So at 1,500-2,500 offered, blocks come every
   515-775 ms, commits plateau at ~1,300 TPS and the mempool grows. The 800 ms cadence only holds
   while the chain keeps up.
3. **Overload makes throughput fall, not plateau.** Past ~3,000 offered, commits drop, timeouts
   rise (up to 16 per run), blocks stretch to 1-1.9 s apart and finality reaches 40-60 s. Intake
   is only bounded by the 200,000-transaction mempool cap.
4. **Thread pools default to the whole machine.** Each validator's rayon pool (signature
   checks) sizes itself to all CPUs, which oversubscribes the machine when validators share it.
   The script sets `RAYON_NUM_THREADS=2`.
5. **Large run-to-run variance.** The same 3,000 TPS level gave 3,403, 2,246 and 1,848 committed
   TPS on three runs (laptop thermals, background load). Repeat runs before comparing.
6. **Benchmark overheads (not in the results, but they cost wall time):**
   - Genesis installs in ~40 s for 20,000 accounts, per validator, per run.
   - The client signs every transaction up front on one thread (46 s for 300,000 on a 2.5 GHz
     core).
   - The client polls validator 0 every 50 ms, adding up to 50 ms to measured finality.

## 5. What to look at on other machines

- **Execution ceiling (section 1):** if a Mac executes much faster alone (4a) and with 4 copies
  (4b), end-to-end peak should rise roughly with the 4-copy number.
- **Sustained TPS** (finality under ~3 s) is the number to compare, not peak committed TPS.
- **Block gap p50 near 800 ms** at a given load means the chain keeps up at that load.
- **Thread counts:** try `--exec-threads` 2-5. The best value depends on the number of
  performance cores.
