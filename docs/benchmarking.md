# Benchmarking on one machine (Linux or macOS)

How to build the chain, run 4 validators locally with 800 ms blocks, and measure throughput and
finality, so results from different machines can be compared with the baseline in
[bench-reports/](bench-reports/).

Everything below runs from the repository root.

## 1. Prerequisites

| | macOS (Apple Silicon or Intel) | Linux |
|---|---|---|
| Compiler toolchain | `xcode-select --install` | `sudo apt-get install -y build-essential clang libclang-dev pkg-config libssl-dev` |
| Rust | [rustup](https://rustup.rs) (`rust-toolchain.toml` pins 1.98.1; rustup fetches it) | same |
| Other | git, curl, python3 (ships with the command line tools) | git, curl, python3 |
| Disk | ~20 GB free (15 GB build output, 0.5 GB aptos-core, run data) | same |
| Memory | 16 GB or more (each validator uses ~0.5 GB; the build needs more) | same |

No OpenSSL install is needed on macOS (TLS uses the system framework there).

## 2. Set up and build

```bash
scripts/setup.sh
```

This clones aptos-core at the commit `Cargo.lock` was resolved against into
`../vendor/aptos-core` (the workspace expects it there) and builds `target/release/chain` and
`target/release/chain-bench`. The first build takes 15-30 minutes.

## 3. Prepare the machine

Results swing a lot with background load and thermals (see the baseline report), so:

- Close other heavy apps (IDEs, browsers, Docker, indexing).
- Laptops: plug in power. macOS: turn Low Power Mode off and wrap runs in `caffeinate -i` so
  the machine does not sleep.
- macOS may ask whether `chain` may accept incoming connections (validators listen on QUIC
  ports 7000-7003). Allow it, or the validators cannot reach each other.
- Ports 7000-7003 (QUIC) and 8080-8083 (RPC) must be free.

## 4. Run the standard suite

Run all three steps on every machine; together they separate "execution is faster" from "the
network or the client is the limit".

### 4a. Execution ceiling, one validator alone

```bash
for t in 4 8; do
  target/release/chain-bench exec --backend move-vm --store rocksdb --threads $t \
    --accounts 20000 --block-size 5000 --blocks 6 | grep summary
done
```

Record `execute+commit` tps. This is the most one validator can execute with the whole machine.

### 4b. Execution ceiling with 4 validators sharing the machine

```bash
T=4   # Block-STM threads per validator; repeat with 2, 3, 5
for i in 1 2 3 4; do
  RAYON_NUM_THREADS=2 target/release/chain-bench exec --backend move-vm --store rocksdb \
    --threads $T --accounts 20000 --block-size 1000 --blocks 30 | grep summary &
done; wait
```

Every validator executes every block, so with 4 validators on one machine the chain cannot go
faster than one of these four numbers.

### 4c. End to end: 4 validators under load

```bash
caffeinate -i scripts/bench-local.sh --tps 1000,1500,2000,3000,4000,5000 \
  --label "MacBook Pro M3 Max, 16 cores, on power"   # Linux: drop `caffeinate -i`
```

Each load level starts a fresh 4-validator network (800 ms block pacing), offers that many
transfers per second for 60 s, and ignores the first 15 s. One level takes about 2.5 minutes;
the 6 levels above take about 15. Then repeat the best level once more (`--tps 3000`) to see the
run-to-run spread.

Useful options (`scripts/bench-local.sh -h` lists all):

| Option | Default | Notes |
|---|---|---|
| `--tps LIST` | 3000 | Offered loads, comma-separated |
| `--duration`, `--warmup` | 60, 15 | Seconds |
| `--exec-threads N` | CPUs per validator (minus 1 when pinned) | Worth sweeping, e.g. 2-5 |
| `--cores-per-validator N`, `--bench-cores N` | even split, 2 | CPU budget per validator and for the load generator |
| `--rayon-threads N` | 2 | Signature-verification pool per validator |
| `--no-pin` | pinned on Linux | macOS cannot pin CPUs, so there it only sizes thread pools |
| `--block-interval-ms` | 800 | Keep at 800 for comparable results |

Results land in `target/bench-local/<timestamp>/`:

- `env.txt`: machine, commits, and settings.
- `results.md` / `results.csv`: one row per load level.
- `tps-<N>/`: the load generator's output, block stats, and each validator's `node.log`.

## 5. Read the results

| Column | Meaning |
|---|---|
| Committed TPS | Transactions the client saw committed during the measured window, per second. A backlog draining inside the window can push this above the offered rate. |
| Committed / sent | Whether the chain kept up overall. Fewer committed than sent means a queue built up and was never drained. |
| p50 / p99 finality | From each transaction's scheduled send time to the client seeing it committed. Under 2-3 s means the chain keeps up; tens of seconds means a growing queue. |
| Timeouts | Consensus rounds that timed out (a leader's proposal did not certify in time). |
| Block gap p50 | Time between blocks carrying transactions. About 800 ms when the chain keeps up; lower when blocks fill early; higher when overloaded. |

Report two numbers per machine:

- **Sustained TPS:** the highest offered rate where all transactions committed and p50 finality
  stays under ~3 s.
- **Peak committed TPS:** the highest Committed TPS seen at any level, with its finality next to
  it.

## 6. Record and compare

Copy [bench-reports/TEMPLATE.md](bench-reports/TEMPLATE.md) to
`bench-reports/<date>-<machine>.md`, paste in `env.txt`, `results.md` and the 4a/4b numbers,
then compare against the baseline,
[2026-10-12-linux-core-ultra-7-155h.md](bench-reports/2026-10-12-linux-core-ultra-7-155h.md).

## Troubleshooting

| Symptom | Fix |
|---|---|
| `no release binaries` | Run `scripts/setup.sh`, or pass `--build` |
| `something already serves http://127.0.0.1:8080` | Stop the leftover validators: `pkill -x chain` |
| `validator N exited during startup` | Read `tps-<N>/validator-N/node.log`; on macOS check `ulimit -n` (the script raises it, but a low hard limit can stop it) |
| Validators stuck on `waiting for a quorum` | On macOS, allow incoming connections for `chain` in the firewall prompt |
| `tps-<N> exists; pick another --out` | Each run needs a fresh output directory; omit `--out` to get a timestamped one |
| Very different numbers between repeats | Background load or thermal throttling; close apps, plug in, repeat |
