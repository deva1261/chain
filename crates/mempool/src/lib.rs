//! Transaction intake and batching (design doc 4.1 steps 1-4, D6, D9).
//!
//! Clients submit to any node. Stateless checks run on intake; signatures are verified in
//! parallel when pending transactions are packed into a batch (every few ms). The node forwards
//! each batch straight to the next few HRW leaders instead of gossiping it, and a leader pulls
//! batches into its proposal. Once any block includes a batch, every pool drops it; a batch
//! proposed in a block that never commits is offered again.
//!
//! Not checked yet: sequence numbers and balances against state (the cheap stateful precheck of
//! step 3). Bad transactions are ordered and then fail during execution, which is safe (D3).

use anyhow::{Result, ensure};
use aptos_crypto::HashValue;
use aptos_types::{
    chain_id::ChainId,
    transaction::{SignedTransaction, Transaction},
};
use chain_types::{Batch, Block, Round, ValidatorIndex};
use rayon::prelude::*;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
};

/// Digests of batches whose every signature this validator has verified itself, so execution
/// can skip checking them again. Only local verification goes in: a peer's word never does, so
/// each validator still checks every transaction at least once. A digest commits to the batch's
/// full contents (signatures included), so a matching digest means identical bytes.
///
/// Bounded: the oldest digests are forgotten first, which only costs a re-verification.
pub struct VerifiedBatches {
    inner: Mutex<(HashSet<HashValue>, VecDeque<HashValue>)>,
    capacity: usize,
}

impl VerifiedBatches {
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new((HashSet::new(), VecDeque::new())),
            capacity,
        })
    }

    fn insert(&self, digest: HashValue) {
        let mut inner = self.inner.lock().unwrap();
        let (set, order) = &mut *inner;
        if set.insert(digest) {
            order.push_back(digest);
            while order.len() > self.capacity {
                if let Some(old) = order.pop_front() {
                    set.remove(&old);
                }
            }
        }
    }

    /// Whether this validator verified the batch with `digest` (forgetting it: a batch
    /// executes once).
    pub fn take(&self, digest: &HashValue) -> bool {
        self.inner.lock().unwrap().0.remove(digest)
    }
}

#[derive(Clone, Debug)]
pub struct MempoolConfig {
    pub chain_id: ChainId,
    pub max_batch_txns: usize,
    pub max_block_txns: usize,
    /// Pending plus ready transactions above which intake is refused (backpressure).
    pub max_pool_txns: usize,
    /// Upcoming leaders each batch is forwarded to (design doc D6).
    pub forward_to: usize,
    /// Rounds after which a batch proposed in an uncommitted block is offered again.
    pub reproposal_rounds: u64,
}

impl MempoolConfig {
    pub fn new(chain_id: ChainId) -> Self {
        Self {
            chain_id,
            max_batch_txns: 500,
            max_block_txns: 10_000,
            max_pool_txns: 200_000,
            forward_to: 2,
            reproposal_rounds: 10,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct MempoolStats {
    pub accepted: u64,
    pub rejected: u64,
    pub bad_signatures: u64,
    pub batches_made: u64,
    pub batches_received: u64,
}

pub struct Mempool {
    me: ValidatorIndex,
    config: MempoolConfig,
    /// Submitted here, not yet batched.
    pending: Vec<SignedTransaction>,
    /// Batches this node may propose, oldest first.
    ready: VecDeque<Batch>,
    /// Transactions in `ready`.
    ready_txns: usize,
    /// Batches this node proposed, by digest, with the round, until they commit.
    in_flight: HashMap<HashValue, (Batch, Round)>,
    /// Digests of batches already seen in some block, with the round seen.
    included: HashMap<HashValue, Round>,
    /// Hashes of transactions currently in this pool (dedup).
    known: HashSet<HashValue>,
    next_batch_id: u64,
    /// Current proposal size limit; adapted to execution speed by the node.
    block_limit: usize,
    verified: Option<Arc<VerifiedBatches>>,
    stats: MempoolStats,
}

impl Mempool {
    pub fn new(me: ValidatorIndex, config: MempoolConfig) -> Self {
        Self {
            me,
            block_limit: config.max_block_txns,
            verified: None,
            config,
            pending: vec![],
            ready: VecDeque::new(),
            ready_txns: 0,
            in_flight: HashMap::new(),
            included: HashMap::new(),
            known: HashSet::new(),
            next_batch_id: 0,
            stats: MempoolStats::default(),
        }
    }

    /// Records every batch this pool verifies in `verified` (see [`VerifiedBatches`]).
    pub fn with_verified(mut self, verified: Arc<VerifiedBatches>) -> Self {
        self.verified = Some(verified);
        self
    }

    pub fn config(&self) -> &MempoolConfig {
        &self.config
    }

    pub fn stats(&self) -> &MempoolStats {
        &self.stats
    }

    /// Transactions waiting to be proposed (pending and in ready batches).
    pub fn len(&self) -> usize {
        self.pending.len() + self.ready_txns
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Caps proposals at `limit` transactions (never above the configured maximum). The node
    /// lowers it when execution cannot keep up (adaptive block size).
    pub fn set_block_limit(&mut self, limit: usize) {
        self.block_limit = limit.clamp(1, self.config.max_block_txns);
    }

    pub fn block_limit(&self) -> usize {
        self.block_limit
    }

    /// Whether enough batches are ready to fill a block.
    pub fn has_full_block(&self) -> bool {
        self.ready_txns >= self.block_limit
    }

    /// Batches proposed by this node that have not committed yet.
    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    /// Stateless intake checks (design doc 4.1 step 2); signatures are checked when batching.
    pub fn submit(&mut self, txn: SignedTransaction) -> Result<()> {
        let result = self.check(&txn);
        match &result {
            Ok(hash) => {
                self.known.insert(*hash);
                self.pending.push(txn);
                self.stats.accepted += 1;
            },
            Err(_) => self.stats.rejected += 1,
        }
        result.map(|_| ())
    }

    fn check(&self, txn: &SignedTransaction) -> Result<HashValue> {
        ensure!(txn.chain_id() == self.config.chain_id, "wrong chain id");
        ensure!(self.len() < self.config.max_pool_txns, "mempool is full");
        let hash = txn.committed_hash();
        ensure!(!self.known.contains(&hash), "duplicate transaction");
        Ok(hash)
    }

    /// Packs pending transactions into a batch, dropping bad signatures. The node forwards it to
    /// [`MempoolConfig::forward_to`] upcoming leaders (including itself, via
    /// [`Mempool::add_batch`], if it is one).
    pub fn make_batch(&mut self) -> Option<Batch> {
        if self.pending.is_empty() {
            return None;
        }
        let take = self.pending.len().min(self.config.max_batch_txns);
        let txns: Vec<_> = self.pending.drain(..take).collect();
        let (txns, bad) = verified(txns);
        self.stats.bad_signatures += bad.len() as u64;
        for txn in &bad {
            self.known.remove(&txn.committed_hash());
        }
        if txns.is_empty() {
            return None;
        }
        self.next_batch_id += 1;
        self.stats.batches_made += 1;
        let batch = Batch {
            author: self.me,
            batch_id: self.next_batch_id,
            txns,
        };
        if let Some(verified) = &self.verified {
            verified.insert(batch.digest());
        }
        Some(batch)
    }

    /// A batch for this node to propose: one of its own, or forwarded by a peer (whose
    /// signatures are re-verified; a batch with any bad signature is dropped).
    pub fn add_batch(&mut self, batch: Batch, from_peer: bool) {
        if self.included.contains_key(&batch.digest()) {
            return;
        }
        if from_peer {
            self.stats.batches_received += 1;
            let (_, bad) = verified(batch.txns.clone());
            if !bad.is_empty() {
                self.stats.bad_signatures += bad.len() as u64;
                return;
            }
            if let Some(verified) = &self.verified {
                verified.insert(batch.digest());
            }
        }
        self.ready_txns += batch.txns.len();
        self.ready.push_back(batch);
    }

    /// Batches for a proposal in `round`, up to the block limit. Batches proposed long ago in
    /// blocks that never committed go first.
    pub fn pull(&mut self, round: Round) -> Vec<Batch> {
        let stale: Vec<HashValue> = self
            .in_flight
            .iter()
            .filter(|(_, (_, r))| *r + self.config.reproposal_rounds < round)
            .map(|(d, _)| *d)
            .collect();
        for digest in stale {
            let (batch, _) = self.in_flight.remove(&digest).expect("listed");
            self.included.remove(&digest);
            self.ready_txns += batch.txns.len();
            self.ready.push_front(batch);
        }

        let mut payload = vec![];
        let mut txns = 0;
        while let Some(batch) = self.ready.front() {
            if !payload.is_empty() && txns + batch.txns.len() > self.block_limit {
                break;
            }
            let batch = self.ready.pop_front().expect("peeked");
            self.ready_txns -= batch.txns.len();
            let digest = batch.digest();
            if self.included.contains_key(&digest) {
                continue;
            }
            txns += batch.txns.len();
            self.in_flight.insert(digest, (batch.clone(), round));
            payload.push(batch);
        }
        payload
    }

    /// Any block seen (proposed by anyone): its batches must not be proposed again here.
    pub fn on_block(&mut self, block: &Block) {
        let round = block.round();
        for batch in &block.payload {
            self.included.insert(batch.digest(), round);
        }
        if !block.payload.is_empty() {
            let included = &self.included;
            self.ready.retain(|b| !included.contains_key(&b.digest()));
            self.ready_txns = self.ready.iter().map(|b| b.txns.len()).sum();
        }
    }

    /// Committed blocks: their batches are done for good.
    pub fn on_commit(&mut self, blocks: &[Block]) {
        let Some(last) = blocks.last() else { return };
        for block in blocks {
            for batch in &block.payload {
                let digest = batch.digest();
                self.in_flight.remove(&digest);
                self.included.insert(digest, block.round());
                for txn in &batch.txns {
                    self.known.remove(&txn.committed_hash());
                }
            }
        }
        // Forget inclusions long past; their batches cannot come back.
        let horizon = last.round().saturating_sub(10 * self.config.reproposal_rounds);
        self.included.retain(|_, round| *round >= horizon);
    }
}

/// Splits transactions into (valid, bad signature), checking in parallel.
fn verified(txns: Vec<SignedTransaction>) -> (Vec<SignedTransaction>, Vec<SignedTransaction>) {
    txns.into_par_iter()
        .partition(|t| t.verify_signature().is_ok())
}

/// Committed block payloads as Aptos transactions, in order.
pub fn block_transactions(block: &Block) -> Vec<Transaction> {
    block
        .payload
        .iter()
        .flat_map(|b| b.txns.iter().cloned().map(Transaction::UserTransaction))
        .collect()
}
