//! Persistent chain state (design doc D4, 6.2): flat key-value state in RocksDB for O(1)
//! execution reads, one atomic write batch per block, and a Jellyfish Merkle Tree root
//! computed asynchronously behind the commit path.

mod jmt;
mod overlay;
pub mod schema;

pub use jmt::Changes;
pub use overlay::OverlayState;

use anyhow::{Context, Result, ensure};
use aptos_crypto::HashValue;
use aptos_types::{
    state_store::{
        StateViewResult, TStateView, errors::StateViewError, state_key::StateKey,
        state_slot::StateSlot, state_storage_usage::StateStorageUsage, state_value::StateValue,
    },
    transaction::{TransactionOutput, Version},
};
use chain_execution::{BlockId, StateStore, block_changes};
use jmt::{RootUpdater, cf};
use rocksdb::{BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, Options, WriteBatch};
use schema::{decode_u64, encode_u64};
use std::{
    path::Path,
    sync::{Arc, mpsc},
};

/// RocksDB block cache shared by all column families.
const BLOCK_CACHE_BYTES: usize = 512 << 20;

#[derive(Clone, Copy, Debug)]
pub struct StoreConfig {
    /// State-tree versions (block heights) kept readable; older tree nodes are pruned.
    pub keep_versions: u64,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            keep_versions: 1_000,
        }
    }
}

pub struct ChainStore {
    db: Arc<DB>,
    committed_height: u64,
    committed_block: HashValue,
    next_version: Version,
    roots: RootUpdater,
}

impl ChainStore {
    /// Opens or creates the store at `path` with the default [`StoreConfig`].
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, StoreConfig::default())
    }

    /// Opens or creates the store at `path`. Blocks that were committed but not yet hashed
    /// (e.g. after a crash) are queued for hashing again.
    pub fn open_with(path: impl AsRef<Path>, config: StoreConfig) -> Result<Self> {
        let cache = Cache::new_lru_cache(BLOCK_CACHE_BYTES);
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&cache);
        table.set_bloom_filter(10.0, false);
        let mut cf_opts = Options::default();
        cf_opts.set_block_based_table_factory(&table);
        cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.increase_parallelism(num_threads());
        let descriptors = schema::ALL
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, cf_opts.clone()));
        let db = Arc::new(
            DB::open_cf_descriptors(&db_opts, path.as_ref(), descriptors)
                .with_context(|| format!("opening {}", path.as_ref().display()))?,
        );

        let meta_u64 = |key: &[u8]| -> Result<u64> {
            db.get_cf(cf(&db, schema::META)?, key)?
                .map_or(Ok(0), |bytes| decode_u64(&bytes))
        };
        let committed_height = meta_u64(schema::META_COMMITTED_HEIGHT)?;
        let next_version = meta_u64(schema::META_NEXT_VERSION)?;
        let hashed_height = meta_u64(schema::META_HASHED_HEIGHT)?;
        let committed_block = match db.get_cf(cf(&db, schema::META)?, schema::META_COMMITTED_BLOCK)? {
            Some(bytes) => HashValue::from_slice(&bytes)?,
            None => HashValue::zero(),
        };

        let roots = RootUpdater::start(db.clone(), hashed_height, config.keep_versions);
        for height in hashed_height + 1..=committed_height {
            let bytes = db
                .get_cf(cf(&db, schema::PENDING_CHANGES)?, encode_u64(height))?
                .with_context(|| format!("pending changes for block {height} are missing"))?;
            let changes: Vec<(StateKey, Option<StateValue>)> = bcs::from_bytes(&bytes)?;
            roots.submit(height, changes.into_iter().collect())?;
        }
        Ok(Self {
            db,
            committed_height,
            committed_block,
            next_version,
            roots,
        })
    }

    /// A read-only handle on the flat state that other threads can hold while this store
    /// keeps committing.
    pub fn reader(&self) -> StoreReader {
        StoreReader {
            db: self.db.clone(),
        }
    }

    /// Sends `(height, root)` for every state root computed from now on.
    pub fn set_root_listener(&self, listener: mpsc::Sender<(u64, HashValue)>) {
        self.roots.set_listener(listener);
    }

    /// Height of the last committed block (0 = empty store).
    pub fn committed_height(&self) -> u64 {
        self.committed_height
    }

    /// Height of the last block whose state root is computed. Lags `committed_height`.
    pub fn hashed_height(&self) -> u64 {
        self.roots.hashed_height()
    }

    /// Blocks until the root of `height` is computed, then returns it.
    pub fn wait_for_root(&self, height: u64) -> Result<HashValue> {
        ensure!(
            height <= self.committed_height,
            "block {height} is not committed (committed height {})",
            self.committed_height
        );
        self.roots.wait_for(height)?;
        self.state_root(height)?
            .with_context(|| format!("root of block {height} missing after hashing"))
    }

    /// Number of stored state-tree nodes, for diagnostics and pruning tests. Scans the column.
    pub fn tree_node_count(&self) -> Result<usize> {
        let nodes = cf(&self.db, schema::JMT_NODES)?;
        let mut count = 0;
        for entry in self.db.iterator_cf(nodes, rocksdb::IteratorMode::Start) {
            entry?;
            count += 1;
        }
        Ok(count)
    }

    /// The state root after block `height`, if already computed.
    pub fn state_root(&self, height: u64) -> Result<Option<HashValue>> {
        self.db
            .get_cf(cf(&self.db, schema::STATE_ROOTS)?, encode_u64(height))?
            .map(|bytes| HashValue::from_slice(&bytes).map_err(Into::into))
            .transpose()
    }
}

impl StateStore for ChainStore {
    fn committed_block(&self) -> HashValue {
        self.committed_block
    }

    /// Writes the block's net changes, its pending change set and the new commit point in one
    /// atomic batch, then hands the changes to the root updater.
    fn apply_block(&mut self, block: BlockId, outputs: &[TransactionOutput]) -> Result<usize> {
        ensure!(
            block.parent == self.committed_block,
            "block {} has parent {}, but the committed block is {}",
            block.id,
            block.parent,
            self.committed_block
        );
        let (changes, kept) = block_changes(outputs);
        let height = self.committed_height + 1;
        let next_version = self.next_version + kept as u64;

        let state = cf(&self.db, schema::STATE)?;
        let meta = cf(&self.db, schema::META)?;
        let mut batch = WriteBatch::default();
        for (key, value) in &changes {
            match value {
                Some(value) => batch.put_cf(state, key.encoded(), bcs::to_bytes(value)?),
                None => batch.delete_cf(state, key.encoded()),
            }
        }
        let pending: Vec<(&StateKey, &Option<StateValue>)> = changes.iter().collect();
        batch.put_cf(
            cf(&self.db, schema::PENDING_CHANGES)?,
            encode_u64(height),
            bcs::to_bytes(&pending)?,
        );
        batch.put_cf(meta, schema::META_COMMITTED_HEIGHT, encode_u64(height));
        batch.put_cf(meta, schema::META_COMMITTED_BLOCK, block.id.to_vec());
        batch.put_cf(meta, schema::META_NEXT_VERSION, encode_u64(next_version));
        self.db.write(batch)?;

        self.committed_height = height;
        self.committed_block = block.id;
        self.next_version = next_version;
        self.roots.submit(height, changes)?;
        Ok(kept)
    }
}

impl TStateView for ChainStore {
    type Key = StateKey;

    fn get_usage(&self) -> StateViewResult<StateStorageUsage> {
        Ok(StateStorageUsage::new_untracked())
    }

    fn next_version(&self) -> Version {
        self.next_version
    }

    fn get_state_slot(&self, key: &StateKey) -> StateViewResult<StateSlot> {
        let value = self.get_state_value(key)?.map(|v| (0, v));
        Ok(StateSlot::from_db_get(key.clone(), value))
    }

    fn get_state_value(&self, key: &StateKey) -> StateViewResult<Option<StateValue>> {
        read_state(&self.db, key)
    }
}

/// Read-only view of the committed flat state; see [`ChainStore::reader`].
#[derive(Clone)]
pub struct StoreReader {
    db: Arc<DB>,
}

impl StoreReader {
    pub fn get(&self, key: &StateKey) -> StateViewResult<Option<StateValue>> {
        read_state(&self.db, key)
    }
}

fn read_state(db: &DB, key: &StateKey) -> StateViewResult<Option<StateValue>> {
    let read = || -> Result<Option<StateValue>> {
        let bytes = db.get_pinned_cf(cf(db, schema::STATE)?, key.encoded())?;
        bytes.map(|b| bcs::from_bytes(&b).map_err(Into::into)).transpose()
    };
    read().map_err(|e| StateViewError::Other(format!("{e:#}")))
}

fn num_threads() -> i32 {
    std::thread::available_parallelism().map_or(4, |n| n.get() as i32)
}
