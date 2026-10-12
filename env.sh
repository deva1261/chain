# Source before building: `source env.sh`.
# This machine has several Rust installs (/usr/local/bin, apt, snap rustup) and the snap rustup
# does not put the pinned toolchain on PATH, so do it explicitly. aptos-core requires this version.
_chain_toolchain="$(sed -n 's/^channel = "\(.*\)"/\1/p' "$(dirname "${BASH_SOURCE[0]}")/rust-toolchain.toml")"
export PATH="$(rustup show home)/toolchains/${_chain_toolchain}-x86_64-unknown-linux-gnu/bin:$PATH"
unset _chain_toolchain
