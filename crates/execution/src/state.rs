use crate::BlockId;
use anyhow::{Result, ensure};
use aptos_crypto::HashValue;
use aptos_types::{
    state_store::{
        StateView, StateViewResult, TStateView, state_key::StateKey, state_slot::StateSlot,
        state_storage_usage::StateStorageUsage, state_value::StateValue,
    },
    transaction::{
        ExecutionStatus, TransactionAuxiliaryData, TransactionOutput, TransactionStatus, Version,
    },
    write_set::{WriteOp, WriteSet, WriteSetMut},
};
use std::collections::{BTreeMap, HashMap};

/// A committed state that blocks execute on top of. Implemented by [`InMemoryState`] and by the
/// RocksDB store in `chain-storage`, so callers (benchmark, node) can use either.
pub trait StateStore: StateView + Sync {
    /// Last block committed into this state; the parent of the next block to execute.
    fn committed_block(&self) -> HashValue;

    /// Commits a block's outputs in order. Kept transactions are applied even when they failed
    /// (they still pay gas, design doc D3); discarded ones never touch state. Returns how many
    /// were kept. `block` must have been executed on top of this state.
    fn apply_block(&mut self, block: BlockId, outputs: &[TransactionOutput]) -> Result<usize>;

    /// The [`BlockId`] for executing block `id` on top of this state.
    fn next_block(&self, id: HashValue) -> BlockId {
        BlockId {
            parent: self.committed_block(),
            id,
        }
    }
}

/// The net state change of a committed block: each key's final value (`None` = deleted), after
/// applying kept outputs in order (design doc D3). This is what storage persists and what the
/// Merkle tree hashes. Also returns how many outputs were kept.
pub fn block_changes(
    outputs: &[TransactionOutput],
) -> (BTreeMap<StateKey, Option<StateValue>>, usize) {
    let mut changes = BTreeMap::new();
    let mut kept = 0;
    for output in outputs {
        match output.status() {
            TransactionStatus::Keep(_) => {
                for (key, op) in output.write_set().write_op_iter() {
                    changes.insert(key.clone(), op.as_state_value_opt().cloned());
                }
                kept += 1;
            },
            TransactionStatus::Discard(_) | TransactionStatus::Retry => {},
        }
    }
    (changes, kept)
}

/// Flat key-value state held in memory, for tests and benchmarks. `chain-storage` provides the
/// persistent RocksDB equivalent.
#[derive(Clone, Default)]
pub struct InMemoryState {
    values: HashMap<StateKey, StateValue>,
    /// Number of transactions applied so far; Aptos' notion of state version.
    next_version: Version,
    /// Last block committed into this state (zero before the first block).
    committed_block: HashValue,
}

impl InMemoryState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies one transaction's write set. Call in block order, once per transaction.
    pub fn apply(&mut self, write_set: &WriteSet) {
        for (key, op) in write_set.write_op_iter() {
            match op.as_state_value_opt() {
                Some(value) => self.values.insert(key.clone(), value.clone()),
                None => self.values.remove(key),
            };
        }
        self.next_version += 1;
    }

    /// The whole state as one output that creates every value, so a fresh store can start from
    /// this state with a single commit (e.g. every validator installing the same genesis).
    pub fn snapshot_output(&self) -> anyhow::Result<TransactionOutput> {
        let ops = self.values.iter().map(|(key, value)| {
            (
                key.clone(),
                WriteOp::creation(value.bytes().clone(), value.metadata().clone()),
            )
        });
        Ok(TransactionOutput::new(
            WriteSetMut::new(ops).freeze()?,
            vec![],
            0,
            TransactionStatus::Keep(ExecutionStatus::Success),
            TransactionAuxiliaryData::None,
        ))
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl StateStore for InMemoryState {
    fn committed_block(&self) -> HashValue {
        self.committed_block
    }

    fn apply_block(&mut self, block: BlockId, outputs: &[TransactionOutput]) -> Result<usize> {
        ensure!(
            block.parent == self.committed_block,
            "block {} has parent {}, but the committed block is {}",
            block.id,
            block.parent,
            self.committed_block
        );
        let mut kept = 0;
        for output in outputs {
            match output.status() {
                TransactionStatus::Keep(_) => {
                    self.apply(output.write_set());
                    kept += 1;
                },
                TransactionStatus::Discard(_) | TransactionStatus::Retry => {},
            }
        }
        self.committed_block = block.id;
        Ok(kept)
    }
}

impl TStateView for InMemoryState {
    type Key = StateKey;

    fn get_usage(&self) -> StateViewResult<StateStorageUsage> {
        Ok(StateStorageUsage::new_untracked())
    }

    fn next_version(&self) -> Version {
        self.next_version
    }

    fn get_state_slot(&self, key: &StateKey) -> StateViewResult<StateSlot> {
        let value = self.values.get(key).map(|v| (0, v.clone()));
        Ok(StateSlot::from_db_get(key.clone(), value))
    }

    fn get_state_value(&self, key: &StateKey) -> StateViewResult<Option<StateValue>> {
        Ok(self.values.get(key).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExecutionBackend, ExecutorConfig, MoveVmBackend};

    #[test]
    fn empty_block_on_empty_state() {
        let state = InMemoryState::new();
        let block = state.next_block(HashValue::sha3_256_of(b"block 1"));
        let outputs = MoveVmBackend::new(ExecutorConfig::new(1))
            .execute_block(block, vec![], &state)
            .unwrap();
        assert!(outputs.is_empty());
    }
}
