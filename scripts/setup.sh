#!/usr/bin/env bash
# One-time setup on a fresh Linux or macOS machine: checks prerequisites, fetches the vendored
# aptos-core checkout the workspace builds against (../vendor/aptos-core, pinned below), and
# builds the release binaries `chain` and `chain-bench`.
#
#   scripts/setup.sh            # everything
#   scripts/setup.sh --no-build # only prerequisites and aptos-core

set -euo pipefail

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
VENDOR=$(cd "$REPO/.." && pwd)/vendor/aptos-core
# aptos-core commit whose crates match Cargo.lock (main, 2026-10-11).
APTOS_CORE_REV=b63273ec49e26c4f71d32be08033bc760f55a4b5
APTOS_CORE_URL=https://github.com/aptos-labs/aptos-core.git

BUILD=1
[[ ${1:-} == --no-build ]] && BUILD=0

die() { echo "error: $*" >&2; exit 1; }
log() { echo "[setup] $*"; }

# --- Prerequisites ---------------------------------------------------------------------------

missing=()
for cmd in git curl python3 rustup; do
    command -v "$cmd" >/dev/null || missing+=("$cmd")
done
if [[ $(uname -s) == Darwin ]]; then
    # Clang and libclang (RocksDB's bindgen) come with the command line tools.
    xcode-select -p >/dev/null 2>&1 || die "install the Xcode command line tools: xcode-select --install"
fi
if (( ${#missing[@]} )); then
    echo "missing: ${missing[*]}" >&2
    [[ " ${missing[*]} " == *" rustup "* ]] &&
        echo "  rustup: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh" >&2
    [[ $(uname -s) == Linux ]] &&
        echo "  Linux build tools: sudo apt-get install -y build-essential clang libclang-dev pkg-config libssl-dev" >&2
    exit 1
fi

# --- aptos-core ------------------------------------------------------------------------------

if [[ -d $VENDOR/.git ]]; then
    head=$(git -C "$VENDOR" rev-parse HEAD)
    if [[ $head != "$APTOS_CORE_REV" ]]; then
        [[ -z $(git -C "$VENDOR" status --porcelain) ]] ||
            die "$VENDOR has local changes and is not at $APTOS_CORE_REV; fix it by hand"
        log "moving $VENDOR from ${head:0:10} to ${APTOS_CORE_REV:0:10}"
        git -C "$VENDOR" fetch -q origin "$APTOS_CORE_REV"
        git -C "$VENDOR" checkout -q "$APTOS_CORE_REV"
    fi
else
    log "cloning aptos-core into $VENDOR (blobless, ~500 MB checkout)"
    mkdir -p "$(dirname "$VENDOR")"
    git clone -q --filter=blob:none --no-checkout "$APTOS_CORE_URL" "$VENDOR"
    git -C "$VENDOR" checkout -q "$APTOS_CORE_REV"
fi
log "aptos-core at $(git -C "$VENDOR" log -1 --format='%h %cs %s')"

# --- Build -----------------------------------------------------------------------------------

if (( BUILD )); then
    log "building release binaries (first build takes 15-30 minutes and ~15 GB of target/)"
    cd "$REPO"
    # shellcheck source=/dev/null
    source env.sh
    cargo build --release --locked -p chain-node -p chain-bench
    log "done: $REPO/target/release/chain and chain-bench"
fi
