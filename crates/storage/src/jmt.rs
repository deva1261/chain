//! Asynchronous state root (design doc D4): a dedicated thread hashes each committed block's
//! changes into a Jellyfish Merkle Tree, versioned by block height, off the commit path.
//! The tree's 16 top-level shards are updated in parallel. Tree versions older than the
//! configured window are pruned as new blocks are hashed (design doc 6.2).

use crate::schema::{self, encode_u64};
use anyhow::{Context, Result, anyhow};
use aptos_crypto::{HashValue, hash::CryptoHash};
use aptos_jellyfish_merkle::{
    JellyfishMerkleTree, TreeReader, TreeUpdateBatch,
    node_type::{LeafNode, Node, NodeKey},
};
use aptos_storage_interface::{AptosDbError, Result as DbResult};
use aptos_types::{
    state_store::{state_key::StateKey, state_value::StateValue},
    transaction::Version,
};
use rayon::prelude::*;
use rocksdb::{DB, Direction, IteratorMode, WriteBatch};
use std::{
    collections::BTreeMap,
    sync::{Arc, Condvar, Mutex, mpsc},
    thread::JoinHandle,
};

pub type Changes = BTreeMap<StateKey, Option<StateValue>>;

/// Hashing progress shared between the updater thread and readers.
#[derive(Default)]
struct Progress {
    hashed_height: u64,
    error: Option<String>,
    /// Told about every root as soon as it is computed.
    listener: Option<mpsc::Sender<(u64, HashValue)>>,
}

pub struct RootUpdater {
    sender: Option<mpsc::Sender<(u64, Changes)>>,
    progress: Arc<(Mutex<Progress>, Condvar)>,
    thread: Option<JoinHandle<()>>,
}

impl RootUpdater {
    /// Starts the updater at `hashed_height` (the last height with a computed root), keeping
    /// the trees of the last `keep_versions` heights readable.
    pub fn start(db: Arc<DB>, hashed_height: u64, keep_versions: u64) -> Self {
        let progress = Arc::new((
            Mutex::new(Progress {
                hashed_height,
                error: None,
                listener: None,
            }),
            Condvar::new(),
        ));
        let (sender, receiver) = mpsc::channel::<(u64, Changes)>();
        let thread_progress = progress.clone();
        let thread = std::thread::Builder::new()
            .name("jmt-updater".into())
            .spawn(move || {
                let mut hasher = match Hasher::load(db, hashed_height, keep_versions) {
                    Ok(hasher) => hasher,
                    Err(e) => {
                        let (lock, cvar) = &*thread_progress;
                        lock.lock().unwrap().error = Some(format!("loading tree state: {e:#}"));
                        cvar.notify_all();
                        return;
                    },
                };
                for (height, changes) in receiver {
                    let result = hasher.hash_block(height, &changes);
                    let (lock, cvar) = &*thread_progress;
                    let mut progress = lock.lock().unwrap();
                    match result {
                        Ok(root) => {
                            progress.hashed_height = height;
                            if let Some(listener) = &progress.listener
                                && listener.send((height, root)).is_err()
                            {
                                progress.listener = None;
                            }
                        },
                        Err(e) => {
                            progress.error = Some(format!("hashing block {height}: {e:#}"));
                            cvar.notify_all();
                            return;
                        },
                    }
                    cvar.notify_all();
                }
            })
            .expect("spawning the JMT updater thread");
        Self {
            sender: Some(sender),
            progress,
            thread: Some(thread),
        }
    }

    /// Queues a committed block for hashing. Heights must be submitted in order.
    pub fn submit(&self, height: u64, changes: Changes) -> Result<()> {
        self.sender
            .as_ref()
            .expect("sender lives until drop")
            .send((height, changes))
            .map_err(|_| anyhow!("JMT updater stopped: {:?}", self.error()))
    }

    pub fn hashed_height(&self) -> u64 {
        self.progress.0.lock().unwrap().hashed_height
    }

    /// Sends `(height, root)` to `listener` for every root computed from now on.
    pub fn set_listener(&self, listener: mpsc::Sender<(u64, HashValue)>) {
        self.progress.0.lock().unwrap().listener = Some(listener);
    }

    fn error(&self) -> Option<String> {
        self.progress.0.lock().unwrap().error.clone()
    }

    /// Blocks until the root for `height` is computed (design doc 5.5 uses this as
    /// backpressure: no vote until the root k blocks back is known).
    pub fn wait_for(&self, height: u64) -> Result<()> {
        let (lock, cvar) = &*self.progress;
        let mut progress = lock.lock().unwrap();
        while progress.hashed_height < height {
            if let Some(e) = &progress.error {
                return Err(anyhow!("{e}"));
            }
            progress = cvar.wait(progress).unwrap();
        }
        Ok(())
    }
}

impl Drop for RootUpdater {
    /// Finishes hashing everything already submitted, then stops the thread.
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(thread) = self.thread.take() {
            thread.join().ok();
        }
    }
}

/// Hashing state carried from block to block.
struct Hasher {
    db: Arc<DB>,
    keep_versions: u64,
    /// Latest tree version with nodes; `None` for an empty tree.
    tree_version: Option<Version>,
    /// Root after the last hashed block.
    last_root: Option<HashValue>,
    /// Stale-node index entries below this height are already pruned. Seeking past them
    /// avoids rescanning RocksDB tombstones of earlier deletions on every block.
    prune_cursor: Version,
}

impl Hasher {
    fn load(db: Arc<DB>, hashed_height: u64, keep_versions: u64) -> Result<Self> {
        let meta = cf(&db, schema::META)?;
        let tree_version = match db.get_cf(meta, schema::META_TREE_VERSION)? {
            Some(bytes) => Some(schema::decode_u64(&bytes)?),
            // Stores written before this key existed created a version for every block.
            None => (hashed_height > 0).then_some(hashed_height),
        };
        let last_root = match db.get_cf(cf(&db, schema::STATE_ROOTS)?, encode_u64(hashed_height))? {
            Some(bytes) => Some(HashValue::from_slice(&bytes)?),
            None => None,
        };
        Ok(Self {
            db,
            keep_versions,
            tree_version,
            last_root,
            prune_cursor: 0,
        })
    }

    /// Computes the root after block `height` and persists it with the new tree nodes and the
    /// hashing progress in one atomic batch, dropping the block's pending change set and pruning
    /// tree versions outside the window. A block without state changes keeps its parent's root
    /// and creates no tree version.
    fn hash_block(&mut self, height: u64, changes: &Changes) -> Result<HashValue> {
        let db = &*self.db;
        let mut write = WriteBatch::default();
        let root = match self.last_root {
            Some(root) if changes.is_empty() => root,
            _ => {
                let root = self.update_tree(height, changes, &mut write)?;
                self.tree_version = Some(height);
                write.put_cf(cf(db, schema::META)?, schema::META_TREE_VERSION, encode_u64(height));
                root
            },
        };

        // Versions >= oldest_kept stay readable. A node stale since version v is unreachable
        // from every root at v or later, so it can go once v <= oldest_kept.
        let oldest_kept = (height + 1).saturating_sub(self.keep_versions.max(1));
        if oldest_kept >= self.prune_cursor {
            prune_stale_nodes(db, self.prune_cursor, oldest_kept, &mut write)?;
            self.prune_cursor = oldest_kept + 1;
        }

        write.put_cf(cf(db, schema::STATE_ROOTS)?, encode_u64(height), root.to_vec());
        write.delete_cf(cf(db, schema::PENDING_CHANGES)?, encode_u64(height));
        write.put_cf(cf(db, schema::META)?, schema::META_HASHED_HEIGHT, encode_u64(height));
        db.write(write)?;
        self.last_root = Some(root);
        Ok(root)
    }

    /// Writes tree version `height` on top of the latest one; returns its root.
    fn update_tree(&self, height: u64, changes: &Changes, write: &mut WriteBatch) -> Result<HashValue> {
        let db = &*self.db;
        let reader = JmtStore { db };
        let tree = JellyfishMerkleTree::<_, StateKey>::new(&reader);
        let base_version = self.tree_version;

        let updates: Vec<(HashValue, Option<(HashValue, StateKey)>)> = changes
            .iter()
            .map(|(key, value)| {
                (
                    *key.crypto_hash_ref(),
                    value.as_ref().map(|v| (CryptoHash::hash(v), key.clone())),
                )
            })
            .collect();

        let shards = (0..16u8)
            .into_par_iter()
            .map(|shard| {
                let shard_updates = updates
                    .iter()
                    .filter(|(key_hash, _)| key_hash.nibble(0) == shard)
                    .map(|(key_hash, value)| (*key_hash, value.as_ref()))
                    .collect();
                tree.batch_put_value_set_for_shard(shard, shard_updates, None, base_version, height)
            })
            .collect::<DbResult<Vec<_>>>()?;
        let mut batch = TreeUpdateBatch::new();
        let mut shard_roots = Vec::with_capacity(16);
        for (shard_root, shard_batch) in shards {
            shard_roots.push(shard_root);
            batch.combine(shard_batch);
        }
        let (root, _leaf_count, top_batch) =
            tree.put_top_levels_nodes(shard_roots, base_version, height)?;
        batch.combine(top_batch);

        let nodes = cf(db, schema::JMT_NODES)?;
        let stale = cf(db, schema::STALE_NODES)?;
        for (node_key, node) in batch.node_batch.iter().flatten() {
            write.put_cf(nodes, node_key.encode()?, node.encode()?);
        }
        for index in batch.stale_node_index_batch.iter().flatten() {
            write.put_cf(stale, stale_key(index.stale_since_version, &index.node_key)?, []);
        }
        Ok(root)
    }
}

fn stale_key(stale_since: Version, node_key: &NodeKey) -> Result<Vec<u8>> {
    let mut key = encode_u64(stale_since).to_vec();
    key.extend(node_key.encode()?);
    Ok(key)
}

/// Adds deletions for every node stale since a version in `from..=up_to`, and for their index
/// entries.
fn prune_stale_nodes(db: &DB, from: Version, up_to: Version, write: &mut WriteBatch) -> Result<()> {
    let stale = cf(db, schema::STALE_NODES)?;
    let nodes = cf(db, schema::JMT_NODES)?;
    let start = encode_u64(from);
    for entry in db.iterator_cf(stale, IteratorMode::From(&start, Direction::Forward)) {
        let (key, _) = entry?;
        if schema::decode_u64(&key[..8])? > up_to {
            break;
        }
        write.delete_cf(nodes, &key[8..]);
        write.delete_cf(stale, &key);
    }
    Ok(())
}

pub(crate) fn cf<'a>(db: &'a DB, name: &str) -> Result<&'a rocksdb::ColumnFamily> {
    db.cf_handle(name)
        .with_context(|| format!("missing column family {name}"))
}

/// Reads tree nodes for the JMT library.
struct JmtStore<'a> {
    db: &'a DB,
}

impl TreeReader<StateKey> for JmtStore<'_> {
    fn get_node_option(&self, node_key: &NodeKey, _tag: &str) -> DbResult<Option<Node<StateKey>>> {
        let column = cf(self.db, schema::JMT_NODES).map_err(db_error)?;
        let key = node_key.encode().map_err(db_error)?;
        match self.db.get_cf(column, key).map_err(db_error)? {
            Some(bytes) => Ok(Some(Node::decode(&bytes).map_err(db_error)?)),
            None => Ok(None),
        }
    }

    fn get_rightmost_leaf(&self, _version: Version) -> DbResult<Option<(NodeKey, LeafNode<StateKey>)>> {
        Err(AptosDbError::Other(
            "tree restore is not supported".to_string(),
        ))
    }
}

fn db_error(e: impl std::fmt::Display) -> AptosDbError {
    AptosDbError::Other(e.to_string())
}
