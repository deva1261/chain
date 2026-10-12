//! Consensus in a deterministic simulated network: virtual time, random latency, message
//! loss, crashes, restarts and an equivocating leader. Every scenario checks safety (all
//! committed chains are prefixes of one another) and liveness (blocks keep committing).

use aptos_crypto::{HashValue, PrivateKey, ed25519::Ed25519PrivateKey};
use aptos_keygen::KeyGen;
use aptos_types::chain_id::ChainId;
use chain_consensus::{
    Action, Consensus, ConsensusConfig, ConsensusMessage, InMemoryBlockArchive, LeaderSchedule,
    SafetyData, SafetyRules, SafetyStorage,
};
use chain_types::{Batch, Block, Committee, Round, ValidatorIndex, ValidatorInfo, ValidatorSigner};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    sync::{Arc, Mutex},
};

const CHAIN: u8 = 42;

/// Safety storage that outlives a simulated crash.
#[derive(Clone, Default)]
struct SharedStorage(Arc<Mutex<Option<SafetyData>>>);

impl SafetyStorage for SharedStorage {
    fn load(&self) -> anyhow::Result<Option<SafetyData>> {
        Ok(self.0.lock().unwrap().clone())
    }
    fn store(&mut self, data: &SafetyData) -> anyhow::Result<()> {
        *self.0.lock().unwrap() = Some(data.clone());
        Ok(())
    }
}

type Node = Consensus<SharedStorage, fn(Round) -> Vec<Batch>>;

fn empty_payload(_: Round) -> Vec<Batch> {
    vec![]
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Event {
    Deliver {
        to: ValidatorIndex,
        from: ValidatorIndex,
        msg: Vec<u8>,
    },
    Timer {
        node: ValidatorIndex,
        round: Round,
    },
    ProposalTimer {
        node: ValidatorIndex,
        round: Round,
    },
    /// Simulated execution finished block `height` on `node`.
    Executed {
        node: ValidatorIndex,
        height: u64,
        root: HashValue,
    },
}

/// Simulated execution: each node executes committed blocks one at a time, `block_ms` each, and
/// reports `hash(height)` as the state root (`divergent` nodes report something else).
struct ExecModel {
    lag: u64,
    block_ms: u64,
    busy_until: Vec<u64>,
    divergent: Vec<ValidatorIndex>,
}

fn sim_root(height: u64) -> HashValue {
    HashValue::sha3_256_of(&height.to_be_bytes())
}

struct Sim {
    committee: Committee,
    keys: Vec<Ed25519PrivateKey>,
    storages: Vec<SharedStorage>,
    nodes: Vec<Option<Node>>,
    committed: Vec<Vec<HashValue>>,
    queue: BinaryHeap<Reverse<(u64, u64, Event)>>,
    seq: u64,
    now: u64,
    rng: StdRng,
    latency_ms: (u64, u64),
    drop_pct: u32,
    /// Leader that sends conflicting proposals to the two halves of the network.
    equivocator: Option<ValidatorIndex>,
    exec: Option<ExecModel>,
    /// Block pacing for every node (0 = responsive).
    block_interval_ms: u64,
    genesis: chain_types::BlockHeader,
}

impl Sim {
    fn new(n: usize, seed: u64) -> Self {
        Self::build(n, seed, None, 0)
    }

    fn paced(n: usize, seed: u64, block_interval_ms: u64) -> Self {
        Self::build(n, seed, None, block_interval_ms)
    }

    /// Consensus coupled to simulated execution with lag `lag` and `block_ms` per block.
    fn with_execution(n: usize, seed: u64, lag: u64, block_ms: u64, divergent: Vec<ValidatorIndex>) -> Self {
        let exec = ExecModel {
            lag,
            block_ms,
            busy_until: vec![0; n],
            divergent,
        };
        Self::build(n, seed, Some(exec), 0)
    }

    fn build(n: usize, seed: u64, exec: Option<ExecModel>, block_interval_ms: u64) -> Self {
        let mut keygen = KeyGen::from_seed([9; 32]);
        let mut keys: Vec<_> = (0..n)
            .map(|_| keygen.generate_ed25519_private_key())
            .collect();
        let committee = Committee::new(
            1,
            keys.iter()
                .map(|k| ValidatorInfo {
                    public_key: k.public_key(),
                    network_address: String::new(),
                    stake: 1,
                })
                .collect(),
        )
        .unwrap();
        // Order keys by committee index.
        keys.sort_by_key(|k| committee.index_of(&k.public_key()).unwrap());
        let mut sim = Self {
            committee,
            keys,
            storages: (0..n).map(|_| SharedStorage::default()).collect(),
            nodes: (0..n).map(|_| None).collect(),
            committed: vec![vec![]; n],
            queue: BinaryHeap::new(),
            seq: 0,
            now: 0,
            rng: StdRng::seed_from_u64(seed),
            latency_ms: (5, 25),
            drop_pct: 0,
            equivocator: None,
            exec,
            block_interval_ms,
            genesis: Block::genesis(ChainId::new(CHAIN), 1),
        };
        for i in 0..n {
            sim.start_node(i as ValidatorIndex);
        }
        sim
    }

    fn signer(&self, i: ValidatorIndex) -> ValidatorSigner {
        let key = Ed25519PrivateKey::try_from(self.keys[i as usize].to_bytes().as_slice()).unwrap();
        ValidatorSigner::new(i, key)
    }

    /// Starts (or restarts, keeping only safety storage) a node.
    fn start_node(&mut self, i: ValidatorIndex) {
        let safety = SafetyRules::new(self.signer(i), self.storages[i as usize].clone(), 1).unwrap();
        let schedule = LeaderSchedule::new(self.committee.clone(), HashValue::sha3_256_of(b"seed"), 1);
        let config = ConsensusConfig {
            chain_id: ChainId::new(CHAIN),
            base_timeout_ms: 200,
            max_timeout_ms: 5_000,
            exec_lag: self.exec.as_ref().map(|e| e.lag),
            min_block_interval_ms: self.block_interval_ms,
        };
        // A restarted node keeps its safety data but, here, nothing else.
        let archive = Box::new(InMemoryBlockArchive::default());
        let mut node: Node =
            Consensus::new(config, schedule, safety, empty_payload, archive, &self.genesis);
        let mut actions = vec![];
        if self.exec.is_some() {
            // The genesis state is known to everyone before round 1.
            actions.extend(node.on_state_root(self.now, 0, self.root_of(i, 0)));
            self.exec.as_mut().unwrap().busy_until[i as usize] = self.now;
        }
        actions.extend(node.start(self.now));
        self.nodes[i as usize] = Some(node);
        self.committed[i as usize].clear();
        self.apply(i, actions);
    }

    fn root_of(&self, node: ValidatorIndex, height: u64) -> HashValue {
        match &self.exec {
            Some(e) if e.divergent.contains(&node) && height > 0 => {
                HashValue::sha3_256_of(format!("wrong {height}").as_bytes())
            },
            _ => sim_root(height),
        }
    }

    fn crash(&mut self, i: ValidatorIndex) {
        self.nodes[i as usize] = None;
    }

    fn push(&mut self, at: u64, event: Event) {
        self.seq += 1;
        self.queue.push(Reverse((at, self.seq, event)));
    }

    fn send(&mut self, from: ValidatorIndex, to: ValidatorIndex, msg: &ConsensusMessage) {
        if self.rng.gen_range(0, 100) < self.drop_pct {
            return;
        }
        let delay = self.rng.gen_range(self.latency_ms.0, self.latency_ms.1 + 1);
        let bytes = bcs::to_bytes(msg).unwrap();
        self.push(self.now + delay, Event::Deliver { to, from, msg: bytes });
    }

    fn apply(&mut self, from: ValidatorIndex, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Broadcast(ConsensusMessage::Proposal(block))
                    if self.equivocator == Some(from) =>
                {
                    self.equivocate(from, *block)
                },
                Action::Broadcast(msg) => {
                    for to in 0..self.committee.len() as ValidatorIndex {
                        if to != from {
                            self.send(from, to, &msg);
                        }
                    }
                },
                Action::Send(to, msg) => self.send(from, to, &msg),
                Action::ScheduleTimeout { round, at_ms } => {
                    self.push(at_ms, Event::Timer { node: from, round })
                },
                Action::ScheduleProposal { round, at_ms } => {
                    self.push(at_ms, Event::ProposalTimer { node: from, round })
                },
                Action::Commit(blocks) => {
                    self.committed[from as usize].extend(blocks.iter().map(Block::hash));
                    if self.exec.is_some() {
                        for block in &blocks {
                            let root = self.root_of(from, block.header.height);
                            let exec = self.exec.as_mut().unwrap();
                            let done = exec.busy_until[from as usize].max(self.now) + exec.block_ms;
                            exec.busy_until[from as usize] = done;
                            let height = block.header.height;
                            self.push(done, Event::Executed { node: from, height, root });
                        }
                    }
                },
            }
        }
    }

    /// Sends the real block to one half and a conflicting twin to the other.
    fn equivocate(&mut self, from: ValidatorIndex, block: Block) {
        let mut header = block.header.clone();
        header.timestamp_ms += 1;
        let twin = Block::new(header, block.qc.clone(), block.tc.clone(), vec![], &self.signer(from))
            .unwrap();
        for to in 0..self.committee.len() as ValidatorIndex {
            if to == from {
                continue;
            }
            let b = if to % 2 == 0 { &block } else { &twin };
            self.send(from, to, &ConsensusMessage::Proposal(Box::new(b.clone())));
        }
    }

    /// Runs until every live node committed `height` blocks or `deadline_ms` passes.
    fn run(&mut self, height: usize, deadline_ms: u64) {
        while let Some(Reverse((at, _, event))) = self.queue.pop() {
            if at > deadline_ms {
                break;
            }
            self.now = at;
            let (node, actions) = match event {
                Event::Deliver { to, from, msg } => {
                    let Some(node) = self.nodes[to as usize].as_mut() else { continue };
                    let msg = bcs::from_bytes(&msg).unwrap();
                    (to, node.handle(at, from, msg))
                },
                Event::Timer { node, round } => {
                    let Some(n) = self.nodes[node as usize].as_mut() else { continue };
                    (node, n.on_timer(at, round))
                },
                Event::ProposalTimer { node, round } => {
                    let Some(n) = self.nodes[node as usize].as_mut() else { continue };
                    (node, n.on_proposal_timer(at, round))
                },
                Event::Executed { node, height, root } => {
                    let Some(n) = self.nodes[node as usize].as_mut() else { continue };
                    (node, n.on_state_root(at, height, root))
                },
            };
            self.apply(node, actions);
            let done = self
                .nodes
                .iter()
                .zip(&self.committed)
                .all(|(n, c)| n.is_none() || c.len() >= height);
            if done {
                return;
            }
        }
    }

    fn assert_safe(&self) {
        for a in &self.committed {
            for b in &self.committed {
                let n = a.len().min(b.len());
                assert_eq!(a[..n], b[..n], "committed chains diverge");
            }
        }
    }

    fn min_committed(&self) -> usize {
        self.nodes
            .iter()
            .zip(&self.committed)
            .filter(|(n, _)| n.is_some())
            .map(|(_, c)| c.len())
            .min()
            .unwrap()
    }

    fn total_tcs(&self) -> u64 {
        self.nodes.iter().flatten().map(|n| n.stats().tcs_formed).sum()
    }
}

#[test]
fn happy_path_commits_every_round_without_timeouts() {
    let mut sim = Sim::new(4, 1);
    sim.run(50, 60_000);
    sim.assert_safe();
    assert!(sim.min_committed() >= 50, "committed {}", sim.min_committed());
    assert_eq!(sim.total_tcs(), 0, "no round should time out");
    // Responsive: each round takes about two message delays, far below the 200 ms timeout.
    assert!(sim.now < 50 * 120, "took {} ms", sim.now);
}

#[test]
fn survives_a_crashed_validator() {
    let mut sim = Sim::new(4, 2);
    sim.crash(3);
    sim.run(30, 120_000);
    sim.assert_safe();
    assert!(sim.min_committed() >= 30, "committed {}", sim.min_committed());
    assert!(sim.total_tcs() > 0, "the crashed leader's rounds must time out");
}

#[test]
fn survives_message_loss_and_jitter() {
    let mut sim = Sim::new(7, 3);
    sim.drop_pct = 10;
    sim.latency_ms = (1, 80);
    sim.run(40, 300_000);
    sim.assert_safe();
    assert!(sim.min_committed() >= 40, "committed {}", sim.min_committed());
}

#[test]
fn equivocating_leader_cannot_fork() {
    for seed in 0..5 {
        let mut sim = Sim::new(4, 10 + seed);
        sim.equivocator = Some(0);
        sim.run(30, 120_000);
        sim.assert_safe();
        assert!(sim.min_committed() >= 30, "seed {seed}: committed {}", sim.min_committed());
    }
}

#[test]
fn crashed_validator_restarts_and_catches_up() {
    let mut sim = Sim::new(4, 4);
    sim.run(15, 60_000);
    sim.crash(2);
    let target = sim.min_committed() + 15;
    sim.run(target, 120_000);
    sim.start_node(2);
    let target = sim.min_committed().max(sim.committed[0].len()) + 10;
    sim.run(target, 300_000);
    sim.assert_safe();
    assert!(
        sim.committed[2].len() >= target,
        "restarted node committed {}",
        sim.committed[2].len()
    );
}


#[test]
fn tolerates_f_crashes_plus_loss_across_seeds() {
    // n = 7 tolerates f = 2 crashed validators.
    for seed in 0..10 {
        let mut sim = Sim::new(7, 100 + seed);
        sim.crash(1);
        sim.crash(4);
        sim.drop_pct = 5;
        sim.latency_ms = (2, 40);
        sim.run(20, 600_000);
        sim.assert_safe();
        assert!(sim.min_committed() >= 20, "seed {seed}: committed {}", sim.min_committed());
    }
}

#[test]
fn execution_coupled_consensus_commits() {
    let mut sim = Sim::with_execution(4, 20, 4, 10, vec![]);
    sim.run(40, 120_000);
    sim.assert_safe();
    assert!(sim.min_committed() >= 40, "committed {}", sim.min_committed());
    assert_eq!(sim.total_tcs(), 0);
}

#[test]
fn slow_execution_throttles_consensus() {
    // Executing a block takes 100 ms, far longer than a consensus round (~40 ms).
    let mut sim = Sim::with_execution(4, 21, 4, 100, vec![]);
    sim.run(1_000, 5_000);
    sim.assert_safe();
    let committed = sim.committed.iter().map(Vec::len).max().unwrap() as u64;
    // Consensus may run at most k blocks (plus the commit pipeline) ahead of execution.
    assert!(committed <= 5_000 / 100 + 4 + 3, "committed {committed}");
    assert!(sim.min_committed() >= 30, "still makes progress: {}", sim.min_committed());
    let waited: u64 = sim.nodes.iter().flatten().map(|n| n.stats().waited_for_execution).sum();
    assert!(waited > 0);
}

#[test]
fn validator_with_divergent_state_is_ignored() {
    let mut sim = Sim::with_execution(4, 22, 2, 5, vec![3]);
    sim.run(40, 300_000);
    sim.assert_safe();
    let honest = (0..3).map(|i| sim.committed[i].len()).min().unwrap();
    assert!(honest >= 40, "honest validators committed {honest}");
    let bad = sim.nodes[3].as_ref().unwrap().stats();
    assert!(bad.rejected_messages > 0, "the divergent node rejects honest blocks");
}

/// Regression: with the doc's literal "root of height h - k" rule, timeouts leave an
/// uncommitted tail that reaches k and consensus deadlocks.
#[test]
fn execution_coupling_survives_repeated_timeouts() {
    for seed in 0..5 {
        let mut sim = Sim::with_execution(4, 30 + seed, 2, 10, vec![]);
        sim.crash(3);
        sim.run(40, 300_000);
        sim.assert_safe();
        assert!(sim.min_committed() >= 40, "seed {seed}: committed {}", sim.min_committed());
        assert!(sim.total_tcs() > 0);
    }
}

#[test]
fn paced_blocks_keep_their_interval_without_timeouts() {
    let mut sim = Sim::paced(4, 41, 800);
    sim.run(20, 120_000);
    sim.assert_safe();
    assert!(sim.min_committed() >= 20);
    assert_eq!(sim.total_tcs(), 0, "pacing must not cause timeouts");
    // 20 committed blocks need about 22 rounds of >= 800 ms each.
    assert!(sim.now >= 20 * 800, "blocks came faster than the interval: {} ms", sim.now);
    assert!(sim.now <= 24 * 1_000, "pacing slower than expected: {} ms", sim.now);
}

#[test]
fn paced_consensus_survives_a_crashed_leader() {
    let mut sim = Sim::paced(4, 42, 300);
    sim.crash(2);
    sim.run(20, 300_000);
    sim.assert_safe();
    assert!(sim.min_committed() >= 20, "committed {}", sim.min_committed());
}
