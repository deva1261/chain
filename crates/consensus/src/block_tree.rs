//! Blocks between the last committed block (the root) and the tips, plus orphans waiting for
//! their parent. Committing a block emits it with every uncommitted ancestor, oldest first,
//! and drops forks that can no longer commit.

use anyhow::{Result, bail};
use aptos_crypto::{HashValue, hash::CryptoHash};
use chain_types::{Block, BlockHeader, Round};
use std::collections::HashMap;

pub struct BlockTree {
    root_hash: HashValue,
    root_round: Round,
    root_height: u64,
    root_timestamp_ms: u64,
    blocks: HashMap<HashValue, Block>,
    /// Blocks whose parent is unknown, by parent hash.
    orphans: HashMap<HashValue, Vec<Block>>,
}

pub enum Inserted {
    /// The block and any orphans it unblocked, parents before children.
    Added(Vec<Block>),
    /// Parent unknown; the block is parked until `parent` arrives.
    Orphan { parent: HashValue },
    /// Already known, or too old to matter.
    Ignored,
}

impl BlockTree {
    pub fn new(root: &BlockHeader) -> Self {
        Self {
            root_hash: root.hash(),
            root_round: root.round,
            root_height: root.height,
            root_timestamp_ms: root.timestamp_ms,
            blocks: HashMap::new(),
            orphans: HashMap::new(),
        }
    }

    pub fn root_hash(&self) -> HashValue {
        self.root_hash
    }

    pub fn root_round(&self) -> Round {
        self.root_round
    }

    pub fn root_height(&self) -> u64 {
        self.root_height
    }

    pub fn get(&self, hash: &HashValue) -> Option<&Block> {
        self.blocks.get(hash)
    }

    /// Height and timestamp of a known block (the root included).
    pub fn height_and_time(&self, hash: &HashValue) -> Option<(u64, u64)> {
        if *hash == self.root_hash {
            return Some((self.root_height, self.root_timestamp_ms));
        }
        self.blocks
            .get(hash)
            .map(|b| (b.header.height, b.header.timestamp_ms))
    }

    /// Inserts a verified block. Heights must increase by one along parent links.
    pub fn insert(&mut self, block: Block) -> Result<Inserted> {
        let hash = block.hash();
        if block.round() <= self.root_round || self.blocks.contains_key(&hash) {
            return Ok(Inserted::Ignored);
        }
        let parent = block.header.parent_hash;
        let Some((parent_height, _)) = self.height_and_time(&parent) else {
            let siblings = self.orphans.entry(parent).or_default();
            if !siblings.iter().any(|b| b.hash() == hash) {
                siblings.push(block);
            }
            return Ok(Inserted::Orphan { parent });
        };
        if block.header.height != parent_height + 1 {
            bail!("block height {} does not follow parent height {parent_height}", block.header.height);
        }
        let mut added = vec![];
        let mut queue = vec![block];
        while let Some(block) = queue.pop() {
            let hash = block.hash();
            if let Some(children) = self.orphans.remove(&hash) {
                queue.extend(
                    children
                        .into_iter()
                        .filter(|c| c.header.height == block.header.height + 1),
                );
            }
            self.blocks.insert(hash, block.clone());
            added.push(block);
        }
        Ok(Inserted::Added(added))
    }

    /// The first unknown block on the path from `hash` down to the root, if any.
    pub fn missing_ancestor(&self, hash: HashValue) -> Option<HashValue> {
        let mut cursor = hash;
        while cursor != self.root_hash {
            match self.blocks.get(&cursor) {
                Some(b) => cursor = b.header.parent_hash,
                None => return Some(cursor),
            }
        }
        None
    }

    /// Commits `hash` and its uncommitted ancestors. `Ok(None)` if a block on the way is still
    /// missing; an error if `hash` conflicts with what is already committed.
    pub fn commit(&mut self, hash: HashValue) -> Result<Option<Vec<Block>>> {
        if hash == self.root_hash {
            return Ok(Some(vec![]));
        }
        let mut path = vec![];
        let mut cursor = hash;
        while cursor != self.root_hash {
            let Some(block) = self.blocks.get(&cursor) else {
                return Ok(None);
            };
            if block.round() <= self.root_round {
                bail!(
                    "safety violation: block {hash} does not extend committed block {}",
                    self.root_hash
                );
            }
            path.push(block.clone());
            cursor = block.header.parent_hash;
        }
        path.reverse();
        let new_root = path.last().expect("non-empty path").header.clone();
        self.root_hash = hash;
        self.root_round = new_root.round;
        self.root_height = new_root.height;
        self.root_timestamp_ms = new_root.timestamp_ms;
        // Anything at or below the new root's round is committed or a dead fork.
        let root_round = self.root_round;
        self.blocks.retain(|_, b| b.round() > root_round);
        self.orphans
            .retain(|_, children| children.iter().any(|b| b.round() > root_round));
        Ok(Some(path))
    }
}
