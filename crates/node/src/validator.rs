//! One validator: a single-threaded event loop over its inbox (network messages, client
//! transactions, execution events) and its timers, driving consensus and the mempool and
//! handing committed blocks to the execution pipeline.

use crate::{
    network::{Delivery, NetMessage, Network},
    pipeline::{ExecutionPipeline, PipelineEvent},
};
use aptos_crypto::HashValue;
use aptos_types::transaction::SignedTransaction;
use chain_consensus::{
    Action, Consensus, ConsensusMessage, InMemoryBlockArchive, LeaderSchedule, PayloadSource,
    SafetyRules, SafetyStorage,
};
use chain_mempool::Mempool;
use chain_types::{Batch, BlockHeader, Round, ValidatorIndex};
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    sync::{
        Arc, Mutex,
        mpsc::{self, RecvTimeoutError},
    },
    time::{Duration, Instant},
};

pub enum Input {
    Net(Delivery),
    Submit(SignedTransaction),
    Pipeline(PipelineEvent),
    Stop,
}

/// What a validator reports to observers (benchmarks, tests).
#[derive(Clone, Debug)]
pub enum NodeEvent {
    /// Blocks committed by consensus (final). Transaction hashes only if requested.
    Committed {
        node: ValidatorIndex,
        height: u64,
        txns: usize,
        txn_hashes: Vec<HashValue>,
        at: Instant,
    },
    Executed {
        node: ValidatorIndex,
        height: u64,
        txns: usize,
        succeeded: usize,
        signatures_checked: usize,
        execute_ms: f64,
        at: Instant,
    },
    StateRoot {
        node: ValidatorIndex,
        height: u64,
        root: HashValue,
    },
    Failed {
        node: ValidatorIndex,
        error: String,
    },
    /// Periodic snapshot of the validator's progress.
    Status {
        node: ValidatorIndex,
        round: Round,
        committed_height: u64,
        consensus: chain_consensus::Stats,
        mempool: chain_mempool::MempoolStats,
        pool_txns: usize,
        in_flight_batches: usize,
        block_limit: usize,
    },
}

const STATUS_INTERVAL_MS: u64 = 1_000;

/// Adaptive block size: proposals carry about what this validator's execution finishes in one
/// block interval (at least `MIN_BLOCK_EXEC_MS`), shrinking further while committed blocks wait
/// for execution, so consensus slows down smoothly instead of stalling on backpressure.
struct BlockSizer {
    /// Smoothed execution throughput, transactions per second.
    rate_tps: Option<f64>,
    committed_height: u64,
    executed_height: u64,
    min_txns: usize,
    /// Execution time one block may take: the block interval, but at least `MIN_BLOCK_EXEC_MS`.
    target_ms: f64,
}

const MIN_BLOCK_EXEC_MS: f64 = 300.0;
/// Blocks smaller than this say little about throughput.
const MIN_SAMPLE_TXNS: usize = 50;

impl BlockSizer {
    fn on_executed(&mut self, height: u64, txns: usize, execute_ms: f64) {
        self.executed_height = self.executed_height.max(height);
        if txns >= MIN_SAMPLE_TXNS && execute_ms > 0.0 {
            let sample = txns as f64 / (execute_ms / 1e3);
            self.rate_tps = Some(match self.rate_tps {
                Some(rate) => 0.8 * rate + 0.2 * sample,
                None => sample,
            });
        }
    }

    /// The proposal limit to use now, or `None` before the first measurement.
    fn limit(&self) -> Option<usize> {
        let rate = self.rate_tps?;
        let backlog = self.committed_height.saturating_sub(self.executed_height);
        let mut limit = rate * self.target_ms / 1e3;
        if backlog > 2 {
            limit /= (backlog - 1) as f64;
        }
        Some((limit as usize).max(self.min_txns))
    }
}

/// Lets consensus pull proposal payloads from the mempool the loop also uses.
struct SharedPool(Arc<Mutex<Mempool>>);

impl PayloadSource for SharedPool {
    fn pull(&mut self, round: Round) -> Vec<Batch> {
        self.0.lock().unwrap().pull(round)
    }

    fn has_full_block(&self) -> bool {
        self.0.lock().unwrap().has_full_block()
    }
}

/// Timer kinds in the validator loop.
const ROUND_TIMEOUT: u8 = 0;
const PROPOSAL: u8 = 1;

pub struct ValidatorParts<S> {
    pub consensus_config: chain_consensus::ConsensusConfig,
    pub schedule: LeaderSchedule,
    pub safety: SafetyRules<S>,
    pub mempool: Mempool,
    pub genesis: BlockHeader,
    pub pipeline: ExecutionPipeline,
    pub network: Box<dyn Network>,
    pub batch_interval: Duration,
    pub observer: Option<mpsc::Sender<NodeEvent>>,
    /// Report committed transaction hashes (for latency measurement; costs a hash per txn).
    pub report_txn_hashes: bool,
}

/// Runs the validator until `Input::Stop` or a fatal execution error.
pub fn run<S: SafetyStorage>(parts: ValidatorParts<S>, inbox: mpsc::Receiver<Input>) {
    let ValidatorParts {
        consensus_config,
        schedule,
        safety,
        mempool,
        genesis,
        pipeline,
        network,
        batch_interval,
        observer,
        report_txn_hashes,
    } = parts;
    let me = safety.signer().index();
    let block_interval_ms = consensus_config.min_block_interval_ms;
    let validators = schedule.committee().len() as ValidatorIndex;
    let forward_to = mempool.config().forward_to;
    let pool = Arc::new(Mutex::new(mempool));
    let mut consensus = Consensus::new(
        consensus_config,
        schedule.clone(),
        safety,
        SharedPool(pool.clone()),
        Box::new(InMemoryBlockArchive::default()),
        &genesis,
    );
    let notify = |event: NodeEvent| {
        if let Some(observer) = &observer {
            observer.send(event).ok();
        }
    };

    let start = Instant::now();
    let now_ms = || start.elapsed().as_millis() as u64;
    let mut timers: BinaryHeap<Reverse<(u64, Round, u8)>> = BinaryHeap::new();
    let mut next_batch_ms = now_ms();
    let mut next_status_ms = now_ms() + STATUS_INTERVAL_MS;
    let mut sizer = BlockSizer {
        rate_tps: None,
        committed_height: 0,
        executed_height: 0,
        min_txns: 100,
        target_ms: MIN_BLOCK_EXEC_MS.max(block_interval_ms as f64),
    };
    let mut actions = consensus.start(now_ms());

    loop {
        // Carry out what consensus asked for.
        for action in actions.drain(..) {
            match action {
                Action::Broadcast(msg) => {
                    if let ConsensusMessage::Proposal(block) = &msg {
                        pool.lock().unwrap().on_block(block);
                    }
                    for to in (0..validators).filter(|&to| to != me) {
                        network.send(to, NetMessage::Consensus(msg.clone()));
                    }
                },
                Action::Send(to, msg) => network.send(to, NetMessage::Consensus(msg)),
                Action::ScheduleTimeout { round, at_ms } => {
                    timers.push(Reverse((at_ms, round, ROUND_TIMEOUT)))
                },
                Action::ScheduleProposal { round, at_ms } => {
                    timers.push(Reverse((at_ms, round, PROPOSAL)))
                },
                Action::Commit(blocks) => {
                    if let Some(last) = blocks.last() {
                        sizer.committed_height = last.header.height;
                    }
                    let mut locked = pool.lock().unwrap();
                    locked.on_commit(&blocks);
                    if let Some(limit) = sizer.limit() {
                        locked.set_block_limit(limit);
                    }
                    drop(locked);
                    if let Err(e) = pipeline.submit(&blocks) {
                        notify(NodeEvent::Failed {
                            node: me,
                            error: format!("{e:#}"),
                        });
                        return;
                    }
                    for block in &blocks {
                        let txns = block.payload.iter().map(|b| b.txns.len()).sum();
                        let txn_hashes = if report_txn_hashes {
                            block
                                .payload
                                .iter()
                                .flat_map(|b| b.txns.iter().map(|t| t.committed_hash()))
                                .collect()
                        } else {
                            vec![]
                        };
                        notify(NodeEvent::Committed {
                            node: me,
                            height: block.header.height,
                            txns,
                            txn_hashes,
                            at: Instant::now(),
                        });
                    }
                },
            }
        }

        // Wait for input until the next timer or batch tick.
        let next_timer = timers.peek().map_or(u64::MAX, |Reverse((at, _, _))| *at);
        let wake = next_timer.min(next_batch_ms);
        let wait = Duration::from_millis(wake.saturating_sub(now_ms()));
        match inbox.recv_timeout(wait) {
            Ok(Input::Net(Delivery { from, msg })) => match msg {
                NetMessage::Consensus(msg) => {
                    if let ConsensusMessage::Proposal(block) | ConsensusMessage::BlockResponse(block) =
                        &msg
                    {
                        pool.lock().unwrap().on_block(block);
                    }
                    actions = consensus.handle(now_ms(), from, msg);
                },
                NetMessage::Batch(batch) => pool.lock().unwrap().add_batch(batch, true),
            },
            Ok(Input::Submit(txn)) => {
                // Rejections (duplicates, full pool) are counted in the mempool's stats.
                pool.lock().unwrap().submit(txn).ok();
            },
            Ok(Input::Pipeline(event)) => match event {
                PipelineEvent::StateRoot { height, root } => {
                    notify(NodeEvent::StateRoot {
                        node: me,
                        height,
                        root,
                    });
                    actions = consensus.on_state_root(now_ms(), height, root);
                },
                PipelineEvent::Executed {
                    height,
                    txns,
                    succeeded,
                    signatures_checked,
                    execute_ms,
                    ..
                } => {
                    sizer.on_executed(height, txns, execute_ms);
                    if let Some(limit) = sizer.limit() {
                        pool.lock().unwrap().set_block_limit(limit);
                    }
                    notify(NodeEvent::Executed {
                        node: me,
                        height,
                        txns,
                        succeeded,
                        signatures_checked,
                        execute_ms,
                        at: Instant::now(),
                    })
                },
                PipelineEvent::Failed(error) => {
                    notify(NodeEvent::Failed { node: me, error });
                    return;
                },
            },
            Ok(Input::Stop) | Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => {},
        }

        // Fire due timers.
        let now = now_ms();
        while let Some(Reverse((at, round, kind))) = timers.peek().copied() {
            if at > now {
                break;
            }
            timers.pop();
            actions.extend(match kind {
                PROPOSAL => consensus.on_proposal_timer(now, round),
                _ => consensus.on_timer(now, round),
            });
        }

        if now >= next_status_ms {
            next_status_ms = now + STATUS_INTERVAL_MS;
            let pool = pool.lock().unwrap();
            notify(NodeEvent::Status {
                node: me,
                round: consensus.round(),
                committed_height: consensus.committed_height(),
                consensus: consensus.stats().clone(),
                mempool: pool.stats().clone(),
                pool_txns: pool.len(),
                in_flight_batches: pool.in_flight(),
                block_limit: pool.block_limit(),
            });
        }

        // Batch pending transactions and forward them to the upcoming leaders (design doc D6).
        if now >= next_batch_ms {
            next_batch_ms = now + batch_interval.as_millis().max(1) as u64;
            let targets = schedule.upcoming_leaders(consensus.round() + 1, forward_to);
            let mut pool = pool.lock().unwrap();
            while let Some(batch) = pool.make_batch() {
                for &to in &targets {
                    if to == me {
                        pool.add_batch(batch.clone(), false);
                    } else {
                        network.send(to, NetMessage::Batch(batch.clone()));
                    }
                }
            }
        }
    }
}
