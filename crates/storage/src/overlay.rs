//! Speculative state for commit pipelining: block N+1 executes on top of block N's changes
//! while N is still being written to RocksDB (Aptos keeps a similar speculative state).
//!
//! Layers are pushed when a block finishes executing and popped only after its write batch has
//! landed, so a reader always finds a key's latest value either in a layer or in the database.

use crate::{Changes, StoreReader};
use aptos_types::{
    state_store::{
        StateViewResult, TStateView, state_key::StateKey, state_slot::StateSlot,
        state_storage_usage::StateStorageUsage, state_value::StateValue,
    },
    transaction::Version,
};
use std::{
    collections::VecDeque,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

/// Shared between the executing thread (reads, pushes) and the committing thread (pops).
#[derive(Clone)]
pub struct OverlayState {
    base: StoreReader,
    /// Executed, not yet persisted blocks, oldest first, tagged with their store height.
    layers: Arc<RwLock<VecDeque<(u64, Arc<Changes>)>>>,
    /// Transactions executed so far, persisted or not (Aptos' state version).
    next_version: Arc<AtomicU64>,
}

impl OverlayState {
    /// `next_version` is the base store's version.
    pub fn new(base: StoreReader, next_version: Version) -> Self {
        Self {
            base,
            layers: Arc::default(),
            next_version: Arc::new(AtomicU64::new(next_version)),
        }
    }

    /// Makes an executed block's changes (from `kept` transactions) visible to the next block.
    pub fn push(&self, height: u64, changes: Arc<Changes>, kept: u64) {
        self.layers.write().unwrap().push_back((height, changes));
        self.next_version.fetch_add(kept, Ordering::SeqCst);
    }

    /// Drops the layer of `height` once that block is persisted.
    pub fn persisted(&self, height: u64) {
        let mut layers = self.layers.write().unwrap();
        if layers.front().is_some_and(|(h, _)| *h == height) {
            layers.pop_front();
        }
    }

    /// Blocks executed but not yet persisted.
    pub fn depth(&self) -> usize {
        self.layers.read().unwrap().len()
    }
}

impl TStateView for OverlayState {
    type Key = StateKey;

    fn get_usage(&self) -> StateViewResult<StateStorageUsage> {
        Ok(StateStorageUsage::new_untracked())
    }

    fn next_version(&self) -> Version {
        self.next_version.load(Ordering::SeqCst)
    }

    fn get_state_slot(&self, key: &StateKey) -> StateViewResult<StateSlot> {
        let value = self.get_state_value(key)?.map(|v| (0, v));
        Ok(StateSlot::from_db_get(key.clone(), value))
    }

    fn get_state_value(&self, key: &StateKey) -> StateViewResult<Option<StateValue>> {
        {
            let layers = self.layers.read().unwrap();
            for (_, changes) in layers.iter().rev() {
                if let Some(value) = changes.get(key) {
                    return Ok(value.clone());
                }
            }
        }
        self.base.get(key)
    }
}
