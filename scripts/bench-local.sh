#!/usr/bin/env bash
# Runs N validators on this machine and benchmarks them with `chain-bench remote` at one or
# more offered loads. Works on Linux and macOS (bash 3.2+).
#
# CPUs: on Linux each validator is pinned (taskset) to its own CPUs and the load generator to
# the ones left over. macOS has no CPU pinning, so there the same split only sizes each
# validator's thread pools and the scheduler places the threads.
#
# Every load level gets a fresh network: the load generator's accounts start at sequence
# number 0, and validators cannot restart yet.
#
#   scripts/bench-local.sh --tps 3000,5000
#   scripts/bench-local.sh --tps 2000,3000,4000 --cores-per-validator 4 --bench-cores 4
#   scripts/bench-local.sh --validator-cpus '0-4;5-9;10-14;15-19' --bench-cpus 20,21 --tps 3000
#
# Results (results.csv, results.md, env.txt, per-run logs) go to --out.

set -euo pipefail

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OS=$(uname -s)

usage() {
    cat <<EOF
Usage: $(basename "$0") [options]

Load:
  --tps LIST                 Offered loads, comma-separated (default: 3000)
  --duration S               Seconds of load per run (default: 60)
  --warmup S                 Seconds at the start excluded from results (default: 15)
  --accounts N               Funded accounts (default: max(20000, 4 x tps), per run)

Validators:
  --validators N             Number of validators (default: 4)
  --block-interval-ms MS     Block pacing (default: 800)
  --max-block-txns N         Upper bound on block size (default: 100000)
  --exec-threads N           Block-STM threads per validator (default: CPUs per validator,
                             minus one when pinned)
  --rayon-threads N          Signature-verification pool per validator (default: 2)
  --base-port P              QUIC ports are P + i (default: 7000)
  --rpc-base-port P          RPC ports are P + i (default: 8080)

CPU split:
  --bench-cores N            CPUs kept for the load generator (default: 2; on Linux the
                             slowest N)
  --cores-per-validator N    CPUs per validator (default: the rest, split evenly)
  --no-pin                   Do not pin (always the case on macOS)
  --layout spread|packed     Linux pinning: spread deals CPUs round-robin, fastest first, so
                             each validator gets the same mix of fast and slow cores
                             (default); packed gives contiguous blocks
  --validator-cpus LISTS     Linux: explicit taskset lists, one per validator, ';'-separated
                             (e.g. '0-4;5-9;10-14;15-19'); overrides the split above
  --bench-cpus LIST          Linux: explicit taskset list for the load generator

Other:
  --build                    cargo build --release first
  --label TEXT               Free-form note recorded in env.txt (e.g. "M3 Max, on power")
  --out DIR                  Results directory (default: target/bench-local/<timestamp>)
  -h, --help
EOF
}

TPS_LIST=3000
DURATION=60
WARMUP=15
ACCOUNTS=
VALIDATORS=4
BLOCK_INTERVAL_MS=800
MAX_BLOCK_TXNS=100000
EXEC_THREADS=
RAYON_THREADS=2
BASE_PORT=7000
RPC_BASE_PORT=8080
BENCH_CORES=2
CORES_PER_VALIDATOR=
PIN=1
LAYOUT=spread
VALIDATOR_CPUS=
BENCH_CPUS=
BUILD=0
LABEL=
OUT=

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tps) TPS_LIST=$2; shift 2 ;;
        --duration) DURATION=$2; shift 2 ;;
        --warmup) WARMUP=$2; shift 2 ;;
        --accounts) ACCOUNTS=$2; shift 2 ;;
        --validators) VALIDATORS=$2; shift 2 ;;
        --block-interval-ms) BLOCK_INTERVAL_MS=$2; shift 2 ;;
        --max-block-txns) MAX_BLOCK_TXNS=$2; shift 2 ;;
        --exec-threads) EXEC_THREADS=$2; shift 2 ;;
        --rayon-threads) RAYON_THREADS=$2; shift 2 ;;
        --base-port) BASE_PORT=$2; shift 2 ;;
        --rpc-base-port) RPC_BASE_PORT=$2; shift 2 ;;
        --bench-cores) BENCH_CORES=$2; shift 2 ;;
        --cores-per-validator) CORES_PER_VALIDATOR=$2; shift 2 ;;
        --no-pin) PIN=0; shift ;;
        --layout) LAYOUT=$2; shift 2 ;;
        --validator-cpus) VALIDATOR_CPUS=$2; shift 2 ;;
        --bench-cpus) BENCH_CPUS=$2; shift 2 ;;
        --build) BUILD=1; shift ;;
        --label) LABEL=$2; shift 2 ;;
        --out) OUT=$2; shift 2 ;;
        -h | --help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

die() { echo "error: $*" >&2; exit 1; }
log() { echo "[$(date +%H:%M:%S)] $*"; }
join() { local IFS=$1; shift; echo "$*"; }

[[ $LAYOUT == spread || $LAYOUT == packed ]] || die "--layout must be spread or packed"
(( WARMUP < DURATION )) || die "--warmup must be shorter than --duration"
for cmd in curl python3; do
    command -v "$cmd" >/dev/null || die "$cmd is required"
done
if (( PIN )) && ! command -v taskset >/dev/null; then
    [[ $OS == Darwin ]] || log "taskset not found; running without CPU pinning"
    PIN=0
fi
if [[ -n $VALIDATOR_CPUS ]] && (( ! PIN )); then
    die "--validator-cpus needs CPU pinning (taskset; macOS has none)"
fi

BIN=$REPO/target/release
if (( BUILD )); then
    log "building release binaries"
    (cd "$REPO" && source env.sh && cargo build --release --locked -p chain-node -p chain-bench)
fi
[[ -x $BIN/chain && -x $BIN/chain-bench ]] || die "no release binaries in $BIN; run scripts/setup.sh or pass --build"

# RocksDB keeps many files open per validator; macOS defaults to 256 per process.
for limit in 65536 24576 10240; do
    ulimit -n "$limit" 2>/dev/null && break
done
(( $(ulimit -n) >= 4096 )) || log "warning: open-file limit is $(ulimit -n); RocksDB may fail"

# --- Machine ---------------------------------------------------------------------------------

if [[ $OS == Darwin ]]; then
    TOTAL_CPUS=$(sysctl -n hw.ncpu)
    CPU_MODEL=$(sysctl -n machdep.cpu.brand_string)
    P_CPUS=$(sysctl -n hw.perflevel0.logicalcpu 2>/dev/null || echo "?")
    E_CPUS=$(sysctl -n hw.perflevel1.logicalcpu 2>/dev/null || echo 0)
    CPU_DETAIL="$P_CPUS performance + $E_CPUS efficiency"
    MEM_GB=$(( $(sysctl -n hw.memsize) / 1073741824 ))
else
    TOTAL_CPUS=$(getconf _NPROCESSORS_ONLN)
    CPU_MODEL=$(lscpu 2>/dev/null | sed -n 's/^Model name: *//p' | head -1)
    CPU_DETAIL="$(lscpu 2>/dev/null | sed -n 's/^Core(s) per socket: *//p' | head -1) cores/socket, $(lscpu 2>/dev/null | sed -n 's/^Thread(s) per core: *//p' | head -1) threads/core"
    MEM_GB=$(( $(sed -n 's/^MemTotal: *\([0-9]*\).*/\1/p' /proc/meminfo) / 1048576 ))
fi

# --- CPU split -------------------------------------------------------------------------------

# Logical CPUs, fastest first (ties keep CPU order); Linux only, for pinning.
CPUS=()
if (( PIN )); then
    while read -r cpu; do
        CPUS+=("$cpu")
    done < <(lscpu -p=CPU,MAXMHZ 2>/dev/null | grep -v '^#' |
        awk -F, '{ print ($2 == "" ? 0 : $2), $1 }' | sort -s -k1,1nr | awk '{ print $2 }')
    (( ${#CPUS[@]} > 0 )) || for (( c = 0; c < TOTAL_CPUS; c++ )); do CPUS+=("$c"); done
fi

VCPUS=()
if [[ -n $VALIDATOR_CPUS ]]; then
    IFS=';' read -r -a VCPUS <<< "$VALIDATOR_CPUS"
    (( ${#VCPUS[@]} == VALIDATORS )) || die "--validator-cpus has ${#VCPUS[@]} lists for $VALIDATORS validators"
    [[ -n $BENCH_CPUS ]] || die "--validator-cpus needs --bench-cpus too"
else
    available=$(( TOTAL_CPUS - BENCH_CORES ))
    (( available >= VALIDATORS )) || die "$TOTAL_CPUS CPUs cannot fit $VALIDATORS validators plus $BENCH_CORES bench CPUs"
    PER=${CORES_PER_VALIDATOR:-$(( available / VALIDATORS ))}
    (( PER >= 1 && PER * VALIDATORS <= available )) ||
        die "$VALIDATORS x $PER CPUs plus $BENCH_CORES bench CPUs exceeds the $TOTAL_CPUS CPUs here"
    if (( PIN )); then
        for (( i = 0; i < VALIDATORS; i++ )); do
            list=()
            for (( j = 0; j < PER; j++ )); do
                if [[ $LAYOUT == spread ]]; then
                    list+=("${CPUS[j * VALIDATORS + i]}")
                else
                    list+=("${CPUS[i * PER + j]}")
                fi
            done
            VCPUS[i]=$(join , "${list[@]}")
        done
        # The load generator gets the slowest CPUs (all of them, shared, with --bench-cores 0).
        if [[ -z $BENCH_CPUS ]]; then
            from=$(( BENCH_CORES > 0 ? TOTAL_CPUS - BENCH_CORES : 0 ))
            BENCH_CPUS=$(join , "${CPUS[@]:from}")
        fi
    fi
fi

cpu_count() {
    taskset -c "$1" true 2>/dev/null || die "bad CPU list '$1'"
    python3 -c 'import sys
print(sum(int(b or a) - int(a) + 1 for a, _, b in (p.partition("-") for p in sys.argv[1].split(","))))' "$1"
}

VEXEC=()
for (( i = 0; i < VALIDATORS; i++ )); do
    if (( PIN )); then
        n=$(cpu_count "${VCPUS[i]}")
        # One pinned CPU is left for consensus, networking, RPC and the commit thread.
        VEXEC[i]=${EXEC_THREADS:-$(( n > 1 ? n - 1 : 1 ))}
    else
        VEXEC[i]=${EXEC_THREADS:-$PER}
    fi
done
(( PIN )) && cpu_count "$BENCH_CPUS" >/dev/null

# --- Run setup -------------------------------------------------------------------------------

OUT=${OUT:-$REPO/target/bench-local/$(date +%Y%m%d-%H%M%S)}
mkdir -p "$OUT"
RESULTS=$OUT/results.csv
echo "offered_tps,committed_tps,finality_p50_ms,finality_p95_ms,finality_p99_ms,submitted,committed,timeouts,block_gap_p50_ms,block_gap_mean_ms,block_gap_p95_ms,blocks_per_s,txns_per_block" > "$RESULTS"

{
    echo "label: ${LABEL:--}"
    echo "date: $(date '+%Y-%m-%d %H:%M %Z')"
    echo "host: $OS $(uname -r) $(uname -m)"
    echo "cpu: $CPU_MODEL ($TOTAL_CPUS logical; $CPU_DETAIL)"
    echo "memory: ${MEM_GB} GB"
    echo "chain commit: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo ?)$(git -C "$REPO" diff --quiet 2>/dev/null || echo ' (modified)')"
    echo "aptos-core commit: $(git -C "$REPO/../vendor/aptos-core" rev-parse --short HEAD 2>/dev/null || echo ?)"
    echo "validators: $VALIDATORS  block_interval_ms: $BLOCK_INTERVAL_MS  max_block_txns: $MAX_BLOCK_TXNS  rayon_threads: $RAYON_THREADS"
    echo "load: tps $TPS_LIST  duration ${DURATION}s  warmup ${WARMUP}s"
    for (( i = 0; i < VALIDATORS; i++ )); do
        if (( PIN )); then
            echo "validator $i: cpus ${VCPUS[i]}  exec_threads ${VEXEC[i]}"
        else
            echo "validator $i: unpinned  exec_threads ${VEXEC[i]}"
        fi
    done
    if (( PIN )); then
        echo "load generator: cpus $BENCH_CPUS"
    else
        echo "load generator: unpinned"
    fi
} | tee "$OUT/env.txt"

PIDS=()
stop_validators() {
    (( ${#PIDS[@]} )) || return 0
    kill "${PIDS[@]}" 2>/dev/null || true
    wait "${PIDS[@]}" 2>/dev/null || true
    PIDS=()
}
trap stop_validators EXIT
trap 'echo; log "interrupted"; exit 130' INT TERM

rpc_url() { echo "http://127.0.0.1:$(( RPC_BASE_PORT + $1 ))"; }
# pinned <cpu-list> <command...>: runs the command on those CPUs (when pinning). It execs, so
# call it only in a subshell (`&` or a pipeline); then `$!` is the command itself.
pinned() {
    local cpus=$1; shift
    if (( PIN )); then exec taskset -c "$cpus" "$@"; else exec "$@"; fi
}

for (( i = 0; i < VALIDATORS; i++ )); do
    if curl -sf -m 1 "$(rpc_url "$i")/v1/status" >/dev/null; then
        die "something already serves $(rpc_url "$i"); stop it first"
    fi
done

# --- One load level --------------------------------------------------------------------------

run_one() {
    local tps=$1 dir=$OUT/tps-$1 accounts i pub h0
    accounts=${ACCOUNTS:-$(( tps * 4 > 20000 ? tps * 4 : 20000 ))}
    # chain-bench remote needs each account to send at most once every 3 s.
    (( accounts / tps >= 3 )) || die "--accounts $accounts is too few for $tps tps (need $(( tps * 3 )))"
    mkdir -p "$dir"

    log "tps $tps: genesis with $accounts accounts"
    local specs=() urls=()
    for (( i = 0; i < VALIDATORS; i++ )); do
        mkdir -p "$dir/validator-$i"
        pub=$("$BIN/chain" keygen --out "$dir/validator-$i/key" 2>>"$dir/genesis.log")
        specs+=(--validator "$pub@127.0.0.1:$(( BASE_PORT + i ))")
        urls+=("$(rpc_url "$i")")
    done
    "$BIN/chain" genesis "${specs[@]}" --accounts "$accounts" --out "$dir/genesis.json" 2>>"$dir/genesis.log"

    for (( i = 0; i < VALIDATORS; i++ )); do
        pinned "${VCPUS[i]:-}" env RAYON_NUM_THREADS="$RAYON_THREADS" "$BIN/chain" run \
            --genesis "$dir/genesis.json" --key "$dir/validator-$i/key" \
            --data-dir "$dir/validator-$i/data" --rpc "127.0.0.1:$(( RPC_BASE_PORT + i ))" \
            --exec-threads "${VEXEC[i]}" --block-interval-ms "$BLOCK_INTERVAL_MS" \
            --max-block-txns "$MAX_BLOCK_TXNS" \
            > "$dir/validator-$i/node.log" 2>&1 &
        PIDS+=($!)
    done

    log "tps $tps: waiting for validators (installing genesis)"
    for (( i = 0; i < VALIDATORS; i++ )); do
        until curl -sf -m 1 "${urls[i]}/v1/status" >/dev/null; do
            if ! kill -0 "${PIDS[i]}" 2>/dev/null; then
                tail -20 "$dir/validator-$i/node.log" >&2
                die "validator $i exited during startup (log: $dir/validator-$i/node.log)"
            fi
            sleep 1
        done
    done

    h0=$(curl -sf "${urls[0]}/v1/status" | python3 -c 'import sys, json; print(json.load(sys.stdin)["committed_height"])')
    log "tps $tps: load for ${DURATION}s"
    pinned "$BENCH_CPUS" "$BIN/chain-bench" remote \
        --rpc "$(join , "${urls[@]}")" --tps "$tps" --duration "$DURATION" \
        --warmup "$WARMUP" --accounts "$accounts" 2>&1 | tee "$dir/bench.txt"

    # Block cadence while loaded, from validator 0's commit history.
    python3 - "${urls[0]}" "$h0" <<'EOF' | tee "$dir/blocks.txt"
import json, sys, urllib.request
url, nxt = sys.argv[1], int(sys.argv[2]) + 1
blocks = []
while True:
    page = json.load(urllib.request.urlopen(f"{url}/v1/commits?from={nxt}"))
    if not page:
        break
    blocks += page
    nxt = page[-1]["height"] + 1
loaded = [b for b in blocks if b["txn_hashes"]]
if len(loaded) < 2:
    print("block_gap_p50_ms=nan block_gap_mean_ms=nan block_gap_p95_ms=nan blocks_per_s=nan txns_per_block=nan")
    sys.exit()
t = [b["committed_unix_ms"] for b in loaded]
gaps = sorted(b - a for a, b in zip(t, t[1:]))
pct = lambda p: gaps[min(int(len(gaps) * p), len(gaps) - 1)]
print(f"block_gap_p50_ms={pct(.5)} block_gap_mean_ms={sum(gaps) / len(gaps):.0f} "
      f"block_gap_p95_ms={pct(.95)} blocks_per_s={(len(loaded) - 1) / ((t[-1] - t[0]) / 1e3):.2f} "
      f"txns_per_block={sum(len(b['txn_hashes']) for b in loaded) / len(loaded):.0f}")
EOF
    grep -h FAILED "$dir"/validator-*/node.log || true
    stop_validators
    # Data directories are large and single-use; keep keys, genesis and logs.
    rm -rf "$dir"/validator-*/data

    local b=$dir/bench.txt k=$dir/blocks.txt
    field() { grep -o "$1=[0-9.na]*" "$2" | head -1 | cut -d= -f2; }
    echo "$tps,$(field committed_tps "$b"),$(field p50 "$b"),$(field p95 "$b"),$(field p99 "$b"),$(field submitted "$b"),$(field committed "$b"),$(field timeouts "$b"),$(field block_gap_p50_ms "$k"),$(field block_gap_mean_ms "$k"),$(field block_gap_p95_ms "$k"),$(field blocks_per_s "$k"),$(field txns_per_block "$k")" >> "$RESULTS"
}

IFS=',' read -r -a LOADS <<< "$TPS_LIST"
for tps in "${LOADS[@]}"; do
    [[ ! -e $OUT/tps-$tps ]] || die "$OUT/tps-$tps exists; pick another --out"
done
for tps in "${LOADS[@]}"; do
    run_one "$tps"
done

# --- Summary ---------------------------------------------------------------------------------

echo
python3 - "$RESULTS" "$OUT/results.md" <<'EOF'
import csv, sys
rows = list(csv.DictReader(open(sys.argv[1])))
cols = [("Offered TPS", "offered_tps"), ("Committed TPS", "committed_tps"),
        ("p50 finality ms", "finality_p50_ms"), ("p99 finality ms", "finality_p99_ms"),
        ("Committed / sent", None), ("Timeouts", "timeouts"),
        ("Block gap p50 ms", "block_gap_p50_ms"), ("Blocks/s", "blocks_per_s"),
        ("Txns/block", "txns_per_block")]
cell = lambda r, k: r[k] if k else f"{r['committed']} / {r['submitted']}"
lines = ["| " + " | ".join(h for h, _ in cols) + " |", "|" + "---|" * len(cols)]
lines += ["| " + " | ".join(cell(r, k) for _, k in cols) + " |" for r in rows]
open(sys.argv[2], "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
EOF
log "results: $OUT (results.csv, results.md, env.txt)"
