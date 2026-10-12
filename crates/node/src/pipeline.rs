//! Execution behind consensus (design doc D3, 4.3), pipelined: the execute thread runs block
//! N+1 on top of block N's changes (an overlay) while the commit thread is still writing N to
//! RocksDB. State roots are computed after that by the store's Merkle updater.
//!
//! Heights: consensus block h is store block `base + h`, where `base` is the store height of
//! the genesis state. Every committed consensus block, empty or not, becomes one store block.

use anyhow::{Context, Result};
use aptos_crypto::HashValue;
use aptos_types::{
    state_store::TStateView,
    transaction::{
        ExecutionStatus, Transaction, TransactionOutput, TransactionStatus,
        signature_verified_transaction::SignatureVerifiedTransaction,
    },
};
use chain_execution::{
    BlockId, ExecutionBackend, MoveVmBackend, StateStore, block_changes, verify_signatures,
};
use chain_mempool::VerifiedBatches;
use chain_storage::{ChainStore, OverlayState};
use chain_types::Block;
use std::{
    sync::{Arc, mpsc},
    thread::JoinHandle,
    time::Instant,
};

#[derive(Clone, Debug)]
pub enum PipelineEvent {
    /// Block `height` executed and persisted.
    Executed {
        height: u64,
        block: HashValue,
        txns: usize,
        succeeded: usize,
        /// Signatures checked here (the rest were verified earlier by this validator).
        signatures_checked: usize,
        execute_ms: f64,
    },
    /// State root after block `height` (0 = genesis state).
    StateRoot { height: u64, root: HashValue },
    /// Execution or storage failed; the node cannot continue.
    Failed(String),
}

pub struct ExecutionPipeline {
    blocks: Option<mpsc::Sender<Block>>,
    threads: Vec<JoinHandle<()>>,
}

impl ExecutionPipeline {
    /// Takes over `store` (which holds the genesis state and nothing after it) and reports
    /// into `events` through `wrap`. The genesis state root is reported first, as height 0.
    pub fn start<T: Send + 'static>(
        store: ChainStore,
        backend: Arc<MoveVmBackend>,
        verified: Arc<VerifiedBatches>,
        events: mpsc::Sender<T>,
        wrap: fn(PipelineEvent) -> T,
    ) -> Result<Self> {
        let base = store.committed_height();
        let genesis_root = store.wait_for_root(base)?;
        events
            .send(wrap(PipelineEvent::StateRoot {
                height: 0,
                root: genesis_root,
            }))
            .ok();

        let (root_tx, root_rx) = mpsc::channel();
        store.set_root_listener(root_tx);
        let root_events = events.clone();
        let roots = std::thread::Builder::new()
            .name("roots".into())
            .spawn(move || {
                for (store_height, root) in root_rx {
                    let height = store_height - base;
                    if root_events.send(wrap(PipelineEvent::StateRoot { height, root })).is_err() {
                        return;
                    }
                }
            })?;

        let overlay = OverlayState::new(store.reader(), store.next_version());
        let (block_tx, block_rx) = mpsc::channel::<Block>();
        let (commit_tx, commit_rx) = mpsc::channel::<ToCommit>();

        let exec_overlay = overlay.clone();
        let exec_events = events.clone();
        let mut parent = store.committed_block();
        let execute = std::thread::Builder::new()
            .name("execute".into())
            .spawn(move || {
                for block in block_rx {
                    let result =
                        execute(&backend, &exec_overlay, &verified, base, &mut parent, &block);
                    match result {
                        Ok(job) => {
                            if commit_tx.send(job).is_err() {
                                return;
                            }
                        },
                        Err(e) => {
                            exec_events.send(wrap(PipelineEvent::Failed(format!("{e:#}")))).ok();
                            return;
                        },
                    }
                }
            })?;

        let commit = std::thread::Builder::new()
            .name("commit".into())
            .spawn(move || {
                let mut store = store;
                for job in commit_rx {
                    if let Err(e) = store.apply_block(job.id, &job.outputs) {
                        events.send(wrap(PipelineEvent::Failed(format!("{e:#}")))).ok();
                        return;
                    }
                    overlay.persisted(job.store_height);
                    let event = PipelineEvent::Executed {
                        height: job.store_height - base,
                        block: job.id.id,
                        txns: job.txns,
                        succeeded: job.succeeded,
                        signatures_checked: job.signatures_checked,
                        execute_ms: job.execute_ms,
                    };
                    if events.send(wrap(event)).is_err() {
                        return;
                    }
                }
                // Dropping the store finishes outstanding root computations.
            })?;

        Ok(Self {
            blocks: Some(block_tx),
            threads: vec![execute, commit, roots],
        })
    }

    /// Queues committed blocks, in commit order.
    pub fn submit(&self, blocks: &[Block]) -> Result<()> {
        let sender = self.blocks.as_ref().context("pipeline stopped")?;
        for block in blocks {
            sender.send(block.clone()).context("execution thread stopped")?;
        }
        Ok(())
    }
}

impl Drop for ExecutionPipeline {
    fn drop(&mut self) {
        drop(self.blocks.take());
        for thread in self.threads.drain(..) {
            thread.join().ok();
        }
    }
}

struct ToCommit {
    id: BlockId,
    store_height: u64,
    outputs: Vec<TransactionOutput>,
    txns: usize,
    succeeded: usize,
    signatures_checked: usize,
    execute_ms: f64,
}

fn execute(
    backend: &MoveVmBackend,
    overlay: &OverlayState,
    verified: &VerifiedBatches,
    base: u64,
    parent: &mut HashValue,
    block: &Block,
) -> Result<ToCommit> {
    let start = Instant::now();
    // Every transaction is checked by this validator exactly once: here, unless this validator
    // already verified the identical batch itself when it was created or forwarded here.
    let mut txns = Vec::new();
    let mut signatures_checked = 0;
    for batch in &block.payload {
        let user_txns = batch.txns.iter().cloned().map(Transaction::UserTransaction);
        if verified.take(&batch.digest()) {
            txns.extend(user_txns.map(SignatureVerifiedTransaction::Valid));
        } else {
            signatures_checked += batch.txns.len();
            txns.extend(verify_signatures(user_txns.collect()));
        }
    }
    let count = txns.len();
    let id = BlockId {
        parent: *parent,
        id: block.hash(),
    };
    let outputs = backend.execute_block(id, txns, overlay)?;
    let succeeded = outputs[..count]
        .iter()
        .filter(|o| *o.status() == TransactionStatus::Keep(ExecutionStatus::Success))
        .count();
    let (changes, kept) = block_changes(&outputs);
    let store_height = base + block.header.height;
    overlay.push(store_height, Arc::new(changes), kept as u64);
    *parent = id.id;
    Ok(ToCommit {
        id,
        store_height,
        outputs,
        txns: count,
        succeeded,
        signatures_checked,
        execute_ms: start.elapsed().as_secs_f64() * 1e3,
    })
}
