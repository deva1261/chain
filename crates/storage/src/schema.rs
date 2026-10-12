//! RocksDB layout (design doc 6.2). Heights are big-endian so keys sort in block order.
//! Column families for blocks, QCs and the transaction index arrive with consensus.

use anyhow::{Context, Result};

/// Flat state: `StateKey::encoded()` -> BCS `StateValue`. Latest value only.
pub const STATE: &str = "state";
/// Jellyfish Merkle Tree nodes: `NodeKey::encode()` -> `Node::encode()`.
pub const JMT_NODES: &str = "jmt_nodes";
/// Index of tree nodes made stale by later versions, for pruning:
/// `stale_since_height` (u64 BE) ++ `NodeKey::encode()` -> empty.
pub const STALE_NODES: &str = "stale_nodes";
/// Height -> state root (32 bytes) once the tree has hashed that block.
pub const STATE_ROOTS: &str = "state_roots";
/// Height -> BCS net change set of that block, kept until the tree has hashed it, so hashing
/// can resume after a crash.
pub const PENDING_CHANGES: &str = "pending_changes";
/// Static keys below.
pub const META: &str = "meta";

pub const ALL: [&str; 6] = [STATE, JMT_NODES, STALE_NODES, STATE_ROOTS, PENDING_CHANGES, META];

/// Height of the last committed block (u64 BE).
pub const META_COMMITTED_HEIGHT: &[u8] = b"committed_height";
/// Id of the last committed block (32 bytes).
pub const META_COMMITTED_BLOCK: &[u8] = b"committed_block";
/// Number of transactions committed so far (u64 BE); Aptos' state version.
pub const META_NEXT_VERSION: &[u8] = b"next_version";
/// Latest tree version that has nodes (u64 BE). Blocks without state changes reuse their
/// parent's root and create no tree version, so this can be below the hashed height.
pub const META_TREE_VERSION: &[u8] = b"tree_version";
/// Height of the last block whose state root is computed (u64 BE).
pub const META_HASHED_HEIGHT: &[u8] = b"hashed_height";

pub fn encode_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

pub fn decode_u64(bytes: &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(
        bytes.try_into().context("expected an 8-byte integer")?,
    ))
}
