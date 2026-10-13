# Source before building: `source env.sh`.
# This machine has several Rust installs (/usr/local/bin, apt, snap rustup) and the snap rustup
# does not put the pinned toolchain on PATH, so do it explicitly. aptos-core requires this version.
_chain_toolchain="$(sed -n 's/^channel = "\(.*\)"/\1/p' "$(dirname "${BASH_SOURCE[0]}")/rust-toolchain.toml")"
case "$(uname -s)-$(uname -m)" in
    Darwin-arm64) _chain_host=aarch64-apple-darwin ;;
    Darwin-x86_64) _chain_host=x86_64-apple-darwin ;;
    Linux-aarch64) _chain_host=aarch64-unknown-linux-gnu ;;
    *) _chain_host=x86_64-unknown-linux-gnu ;;
esac
export PATH="$(rustup show home)/toolchains/${_chain_toolchain}-${_chain_host}/bin:$PATH"
unset _chain_toolchain _chain_host
