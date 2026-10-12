//! The consensus state machine (design doc 4.1, 4.2, 5.3, 5.4), free of I/O: callers feed it
//! messages and timer expiries with the current time, and carry out the actions it returns.
//!
//! Round flow: the leader of round r proposes as soon as it holds QC(r-1) or TC(r-1)
//! (responsive, D7); validators vote and send the vote to the leader of r+1, which forms QC(r)
//! and proposes block r+1 carrying it. If a round stalls, everyone broadcasts a timeout; a
//! quorum of timeouts forms TC(r) and the leader of r+1 proposes with it. A block commits with
//! its ancestors once a QC certifies its child in the next round (2-chain rule).

use crate::{
    LeaderSchedule,
    block_tree::{BlockTree, Inserted},
    safety::{SafetyRules, SafetyStorage},
};
use anyhow::{Context, Result, ensure};
use aptos_crypto::{HashValue, ed25519::Ed25519Signature, hash::CryptoHash};
use aptos_types::chain_id::ChainId;
use chain_types::{
    Batch, Block, BlockHeader, QuorumCert, Round, Timeout, TimeoutCert, ValidatorIndex, Vote,
    VoteData, payload_digest,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsensusMessage {
    Proposal(Box<Block>),
    Vote(Vote),
    /// `last_tc` is the TC through which the sender entered the round, so lagging validators
    /// can catch up to it.
    Timeout {
        timeout: Timeout,
        last_tc: Option<TimeoutCert>,
    },
    BlockRequest(HashValue),
    BlockResponse(Box<Block>),
    /// A QC sent to everyone as soon as the next leader forms it, so all validators enter the
    /// next round (and commit by the 2-chain rule) without waiting for the next proposal.
    QuorumCert(Box<QuorumCert>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// To every other validator.
    Broadcast(ConsensusMessage),
    Send(ValidatorIndex, ConsensusMessage),
    /// Call [`Consensus::on_timer`] with `round` at `at_ms` (if still relevant).
    ScheduleTimeout { round: Round, at_ms: u64 },
    /// Call [`Consensus::on_proposal_timer`] with `round` at `at_ms` (block pacing).
    ScheduleProposal { round: Round, at_ms: u64 },
    /// Newly committed blocks, oldest first, to hand to execution.
    Commit(Vec<Block>),
}

/// Committed blocks, so peers that fall behind (or restart) can fetch history. The node backs
/// this with its block store; consensus writes every block it commits.
pub trait BlockArchive: Send {
    fn get(&self, hash: &HashValue) -> Option<Block>;
    fn put(&mut self, blocks: &[Block]);
}

#[derive(Default)]
pub struct InMemoryBlockArchive(HashMap<HashValue, Block>);

impl BlockArchive for InMemoryBlockArchive {
    fn get(&self, hash: &HashValue) -> Option<Block> {
        self.0.get(hash).cloned()
    }

    fn put(&mut self, blocks: &[Block]) {
        self.0.extend(blocks.iter().map(|b| (b.hash(), b.clone())));
    }
}

/// State roots kept for checking proposals whose execution lags this node's.
const ROOT_HISTORY: u64 = 256;

/// Missing blocks re-requested per round timer, at most.
const MAX_REREQUESTS: usize = 16;

/// Supplies block payloads; the mempool implements this later.
pub trait PayloadSource {
    fn pull(&mut self, round: Round) -> Vec<Batch>;

    /// Whether a full block is already waiting, so a paced leader need not wait.
    fn has_full_block(&self) -> bool {
        false
    }
}

impl<F: FnMut(Round) -> Vec<Batch>> PayloadSource for F {
    fn pull(&mut self, round: Round) -> Vec<Batch> {
        self(round)
    }
}

#[derive(Clone, Debug)]
pub struct ConsensusConfig {
    pub chain_id: ChainId,
    /// Round timeout before any consecutive timeouts (design doc 5.4: ~3x round-trip time).
    pub base_timeout_ms: u64,
    pub max_timeout_ms: u64,
    /// k (design doc D3, 5.5). Each block carries the proposer's latest executed
    /// (height, state root); validators vote only on a root they computed too, and stop voting
    /// and proposing while more than k committed blocks await execution. `None` decouples
    /// consensus from execution (roots are not checked).
    ///
    /// The doc's literal rule ("block h carries the root of h - k") deadlocks: execution follows
    /// commits, and after timeouts the certified-but-uncommitted tail can reach k blocks, so the
    /// next proposal would need a root that can only appear after more proposals.
    pub exec_lag: Option<u64>,
    /// Block pacing: a leader proposes this long after entering its round (sooner if a full
    /// block is waiting), so blocks gather more transactions. 0 = propose immediately (D7).
    /// Only timing changes, never safety; round timeouts are extended by the same amount.
    pub min_block_interval_ms: u64,
}

#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub proposals: u64,
    pub votes_sent: u64,
    pub qcs_formed: u64,
    pub tcs_formed: u64,
    pub timeouts_sent: u64,
    pub rejected_messages: u64,
    pub last_rejection: Option<String>,
    /// Votes or proposals delayed because execution had not produced the needed root yet.
    pub waited_for_execution: u64,
}

pub struct Consensus<S, P> {
    config: ConsensusConfig,
    schedule: LeaderSchedule,
    safety: SafetyRules<S>,
    payload: P,
    me: ValidatorIndex,
    genesis_hash: HashValue,
    tree: BlockTree,
    archive: Box<dyn BlockArchive>,
    /// Blocks requested but not yet received; re-requested on each round timer.
    wanted: BTreeSet<HashValue>,

    now_ms: u64,
    round: Round,
    high_qc: QuorumCert,
    /// TC for `round - 1` if the round was entered through a timeout.
    last_tc: Option<TimeoutCert>,
    consecutive_timeouts: u32,
    proposed_round: Round,
    /// When this node entered the current round (its own clock).
    round_started_ms: u64,
    /// Round for which a paced proposal timer is already scheduled.
    proposal_timer_round: Round,
    /// Highest round for which a proposal waits on its parent block.
    pending_proposal: Option<Round>,
    /// Commit waiting for missing ancestors.
    pending_commit: Option<HashValue>,
    /// Current-round block waiting for its execution root before this node can vote.
    pending_vote: Option<HashValue>,
    /// State roots by block height, as reported by execution.
    state_roots: BTreeMap<u64, HashValue>,

    /// Votes per round: who voted, and signatures per distinct vote.
    votes: BTreeMap<Round, (Vec<ValidatorIndex>, HashMap<HashValue, (VoteData, BTreeMap<ValidatorIndex, Ed25519Signature>)>)>,
    timeouts: BTreeMap<Round, BTreeMap<ValidatorIndex, Timeout>>,

    outbox: Vec<Action>,
    loopback: VecDeque<ConsensusMessage>,
    stats: Stats,
}

impl<S: SafetyStorage, P: PayloadSource> Consensus<S, P> {
    pub fn new(
        config: ConsensusConfig,
        schedule: LeaderSchedule,
        safety: SafetyRules<S>,
        payload: P,
        archive: Box<dyn BlockArchive>,
        genesis: &BlockHeader,
    ) -> Self {
        let genesis_hash = genesis.hash();
        let epoch = schedule.committee().epoch();
        Self {
            me: safety.signer().index(),
            config,
            schedule,
            safety,
            payload,
            genesis_hash,
            tree: BlockTree::new(genesis),
            archive,
            wanted: BTreeSet::new(),
            now_ms: 0,
            round: 0,
            high_qc: QuorumCert::genesis(epoch, genesis_hash),
            last_tc: None,
            consecutive_timeouts: 0,
            proposed_round: 0,
            round_started_ms: 0,
            proposal_timer_round: 0,
            pending_proposal: None,
            pending_commit: None,
            pending_vote: None,
            state_roots: BTreeMap::new(),
            votes: BTreeMap::new(),
            timeouts: BTreeMap::new(),
            outbox: vec![],
            loopback: VecDeque::new(),
            stats: Stats::default(),
        }
    }

    pub fn round(&self) -> Round {
        self.round
    }

    pub fn committed_round(&self) -> Round {
        self.tree.root_round()
    }

    pub fn committed_height(&self) -> u64 {
        self.tree.root_height()
    }

    pub fn high_qc(&self) -> &QuorumCert {
        &self.high_qc
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Enters round 1.
    pub fn start(&mut self, now_ms: u64) -> Vec<Action> {
        self.now_ms = now_ms;
        if self.round == 0 {
            self.advance_round(1, None);
        }
        self.flush()
    }

    pub fn handle(&mut self, now_ms: u64, from: ValidatorIndex, msg: ConsensusMessage) -> Vec<Action> {
        self.now_ms = now_ms;
        self.process(from, msg);
        self.flush()
    }

    /// Execution computed the state root after block `height` (height 0 = the epoch's genesis
    /// state). Unblocks proposals and votes waiting on it.
    pub fn on_state_root(&mut self, now_ms: u64, height: u64, root: HashValue) -> Vec<Action> {
        self.now_ms = now_ms;
        self.state_roots.insert(height, root);
        // Keep enough history to check proposers that are somewhat behind.
        let keep_from = height.saturating_sub(ROOT_HISTORY);
        self.state_roots = self.state_roots.split_off(&keep_from);
        if let Some(hash) = self.pending_vote.take()
            && let Some(block) = self.tree.get(&hash).cloned()
            && let Err(e) = self.maybe_vote(&block)
        {
            self.reject(e);
        }
        if self.pending_proposal == Some(self.round) {
            self.pending_proposal = None;
            if let Err(e) = self.propose() {
                self.reject(e);
            }
        }
        self.flush()
    }

    /// Latest (height, root) this node has executed; (0, zero) without execution coupling.
    fn latest_executed(&self) -> Option<(u64, HashValue)> {
        match self.config.exec_lag {
            None => Some((0, HashValue::zero())),
            Some(_) => self.state_roots.last_key_value().map(|(h, r)| (*h, *r)),
        }
    }

    /// Whether more than k committed blocks await execution here (design doc 5.5).
    fn execution_behind(&self) -> bool {
        match (self.config.exec_lag, self.latest_executed()) {
            (None, _) => false,
            (Some(_), None) => true,
            (Some(lag), Some((executed, _))) => self.tree.root_height() > executed + lag,
        }
    }

    /// Checks a block's execution claim. `Ok(false)`: cannot decide until this node executes
    /// further. Errors on a mismatching root (state divergence) or a regressing height.
    fn check_execution(&self, block: &Block) -> Result<bool> {
        if self.config.exec_lag.is_none() {
            return Ok(true);
        }
        let h = &block.header;
        ensure!(h.executed_height < h.height, "claims to have executed itself");
        if let Some(parent) = self.tree.get(&h.parent_hash) {
            ensure!(
                h.executed_height >= parent.header.executed_height,
                "executed height went backwards"
            );
        }
        match self.state_roots.get(&h.executed_height) {
            Some(root) => {
                ensure!(
                    *root == h.executed_state_root,
                    "state root mismatch after block {}: proposer has {}, we computed {root}",
                    h.executed_height,
                    h.executed_state_root
                );
                Ok(true)
            },
            None if self
                .latest_executed()
                .is_none_or(|(latest, _)| h.executed_height > latest) =>
            {
                Ok(false)
            },
            None => anyhow::bail!("root of block {} is too old to check", h.executed_height),
        }
    }

    /// A paced proposal timer scheduled with [`Action::ScheduleProposal`] fired.
    pub fn on_proposal_timer(&mut self, now_ms: u64, round: Round) -> Vec<Action> {
        self.now_ms = now_ms;
        if round == self.round
            && self.schedule.leader(round) == self.me
            && let Err(e) = self.propose()
        {
            self.reject(e);
        }
        self.flush()
    }

    /// A timer scheduled with [`Action::ScheduleTimeout`] fired.
    pub fn on_timer(&mut self, now_ms: u64, round: Round) -> Vec<Action> {
        self.now_ms = now_ms;
        if round == self.round {
            if let Err(e) = self.local_timeout() {
                self.reject(e);
            }
            // Requests or replies may have been lost: ask everyone again.
            let wanted: Vec<_> = self.wanted.iter().take(MAX_REREQUESTS).copied().collect();
            for hash in wanted {
                self.outbox
                    .push(Action::Broadcast(ConsensusMessage::BlockRequest(hash)));
            }
        }
        self.flush()
    }

    fn flush(&mut self) -> Vec<Action> {
        while let Some(msg) = self.loopback.pop_front() {
            self.process(self.me, msg);
        }
        std::mem::take(&mut self.outbox)
    }

    fn reject(&mut self, e: anyhow::Error) {
        self.stats.rejected_messages += 1;
        self.stats.last_rejection = Some(format!("{e:#}"));
    }

    /// Sends to everyone, including this node (through the loopback).
    fn broadcast(&mut self, msg: ConsensusMessage) {
        self.outbox.push(Action::Broadcast(msg.clone()));
        self.loopback.push_back(msg);
    }

    fn send(&mut self, to: ValidatorIndex, msg: ConsensusMessage) {
        if to == self.me {
            self.loopback.push_back(msg);
        } else {
            self.outbox.push(Action::Send(to, msg));
        }
    }

    fn process(&mut self, from: ValidatorIndex, msg: ConsensusMessage) {
        let result = match msg {
            ConsensusMessage::Proposal(block) => self.on_proposal(from, *block, true),
            ConsensusMessage::BlockResponse(block) => self.on_proposal(from, *block, false),
            ConsensusMessage::Vote(vote) => self.on_vote(vote),
            ConsensusMessage::Timeout { timeout, last_tc } => self.on_timeout_msg(timeout, last_tc),
            ConsensusMessage::QuorumCert(qc) => self.on_quorum_cert(from, *qc),
            ConsensusMessage::BlockRequest(hash) => {
                let block = self.tree.get(&hash).cloned().or_else(|| self.archive.get(&hash));
                if let Some(block) = block {
                    self.send(from, ConsensusMessage::BlockResponse(Box::new(block)));
                }
                Ok(())
            },
        };
        if let Err(e) = result {
            self.reject(e);
        }
    }

    fn verify_block(&self, block: &Block) -> Result<()> {
        if block.round() <= self.tree.root_round() {
            return Ok(()); // Ignored by the tree; no need to spend signature checks.
        }
        block.verify(self.schedule.committee(), self.genesis_hash)?;
        ensure!(
            block.header.chain_id == self.config.chain_id,
            "block for another chain"
        );
        ensure!(
            block.header.proposer == self.schedule.leader(block.round()),
            "proposer {} is not the leader of round {}",
            block.header.proposer,
            block.round()
        );
        Ok(())
    }

    fn on_proposal(&mut self, from: ValidatorIndex, block: Block, direct: bool) -> Result<()> {
        if from != self.me {
            self.verify_block(&block)?;
        }
        if direct {
            ensure!(from == block.header.proposer, "proposal relayed by a non-proposer");
        }
        self.process_qc(&block.qc)?;
        if let Some(tc) = &block.tc {
            self.process_tc(tc.clone());
        }
        let hash = block.hash();
        match self.tree.insert(block)? {
            Inserted::Added(blocks) => {
                for block in &blocks {
                    self.wanted.remove(&block.hash());
                    // QCs inside unblocked orphans can advance and commit too.
                    self.process_qc(&block.qc)?;
                    self.maybe_vote(block)?;
                }
                self.retry_pending()?;
            },
            Inserted::Orphan { parent } => {
                // Ask once; repeats come only from the round timer, so duplicate replies cannot
                // snowball into duplicate requests.
                if self.wanted.insert(parent) {
                    self.send(from, ConsensusMessage::BlockRequest(parent));
                }
            },
            Inserted::Ignored => {
                self.wanted.remove(&hash);
            },
        }
        Ok(())
    }

    fn maybe_vote(&mut self, block: &Block) -> Result<()> {
        if block.round() != self.round {
            return Ok(());
        }
        // Execution backpressure (design doc 5.5): vote only on a state root computed here too,
        // and not while execution is more than k committed blocks behind.
        if self.execution_behind() || !self.check_execution(block)? {
            self.pending_vote = Some(block.hash());
            self.stats.waited_for_execution += 1;
            return Ok(());
        }
        let vote = self.safety.vote(block)?;
        self.stats.votes_sent += 1;
        let next_leader = self.schedule.leader(self.round + 1);
        self.send(next_leader, ConsensusMessage::Vote(vote));
        Ok(())
    }

    fn on_vote(&mut self, vote: Vote) -> Result<()> {
        let round = vote.data.round;
        if round + 1 < self.round || self.schedule.leader(round + 1) != self.me {
            return Ok(());
        }
        vote.verify(self.schedule.committee())?;
        let (voters, by_data) = self.votes.entry(round).or_default();
        if voters.contains(&vote.voter) {
            return Ok(()); // One vote per validator per round; later ones are ignored.
        }
        voters.push(vote.voter);
        let key = vote.data.hash();
        let (data, signatures) = by_data
            .entry(key)
            .or_insert_with(|| (vote.data.clone(), BTreeMap::new()));
        signatures.insert(vote.voter, vote.signature);
        let committee = self.schedule.committee();
        let stake = committee.stake_of(signatures.keys().copied())?;
        if !committee.is_quorum(stake) || self.high_qc.round() >= round {
            return Ok(());
        }
        let qc = QuorumCert {
            data: data.clone(),
            signatures: signatures.iter().map(|(i, s)| (*i, s.clone())).collect(),
        };
        self.stats.qcs_formed += 1;
        self.outbox
            .push(Action::Broadcast(ConsensusMessage::QuorumCert(Box::new(qc.clone()))));
        self.process_qc(&qc)
    }

    fn on_quorum_cert(&mut self, from: ValidatorIndex, qc: QuorumCert) -> Result<()> {
        if qc.round() <= self.high_qc.round() {
            return Ok(());
        }
        if from != self.me {
            qc.verify(self.schedule.committee(), self.genesis_hash)?;
        }
        self.process_qc(&qc)
    }

    fn on_timeout_msg(&mut self, timeout: Timeout, last_tc: Option<TimeoutCert>) -> Result<()> {
        let committee = self.schedule.committee().clone();
        if timeout.voter != self.me {
            timeout.verify(&committee, self.genesis_hash)?;
            if let Some(tc) = &last_tc {
                tc.verify(&committee)?;
            }
        }
        if let Some(tc) = last_tc {
            self.process_tc(tc);
        }
        self.process_qc(&timeout.high_qc)?;
        let round = timeout.data.round;
        if round < self.round {
            return Ok(());
        }
        let collected = self.timeouts.entry(round).or_default();
        collected.entry(timeout.voter).or_insert(timeout);
        let signers: Vec<_> = collected.keys().copied().collect();
        let stake = committee.stake_of(signers.iter().copied())?;

        // More than 1/3 timed out: at least one honest validator gave up, so join (Bracha).
        let joined = self.timeouts[&round].contains_key(&self.me);
        if round == self.round
            && !joined
            && stake > committee.total_stake() - committee.quorum_stake()
        {
            self.local_timeout()?;
        }
        if committee.is_quorum(stake) && round >= self.round {
            let tc = TimeoutCert {
                epoch: committee.epoch(),
                round,
                timeouts: self.timeouts[&round]
                    .iter()
                    .map(|(i, t)| (*i, t.data.high_qc_round, t.signature.clone()))
                    .collect(),
            };
            self.stats.tcs_formed += 1;
            self.process_tc(tc);
        }
        Ok(())
    }

    /// Learns a (verified) QC: raises the high QC, commits by the 2-chain rule, and enters the
    /// next round if the QC is for the current one or later.
    fn process_qc(&mut self, qc: &QuorumCert) -> Result<()> {
        if qc.round() > self.high_qc.round() {
            self.high_qc = qc.clone();
        }
        if let Some((round, hash)) = qc.commits()
            && round > self.tree.root_round()
        {
            self.try_commit(hash)?;
        }
        if qc.round() >= self.round {
            self.advance_round(qc.round() + 1, None);
        }
        Ok(())
    }

    fn process_tc(&mut self, tc: TimeoutCert) {
        if tc.round >= self.round {
            self.advance_round(tc.round + 1, Some(tc));
        }
    }

    fn try_commit(&mut self, hash: HashValue) -> Result<()> {
        match self.tree.commit(hash)? {
            Some(blocks) => {
                if !blocks.is_empty() {
                    self.archive.put(&blocks);
                    self.outbox.push(Action::Commit(blocks));
                }
                if self.pending_commit.is_some_and(|p| self.tree.get(&p).is_none()) {
                    self.pending_commit = None;
                }
            },
            None => {
                self.pending_commit = Some(hash);
                if let Some(missing) = self.tree.missing_ancestor(hash)
                    && self.wanted.insert(missing)
                {
                    self.outbox
                        .push(Action::Broadcast(ConsensusMessage::BlockRequest(missing)));
                }
            },
        }
        Ok(())
    }

    fn retry_pending(&mut self) -> Result<()> {
        if let Some(hash) = self.pending_commit.take() {
            self.try_commit(hash)?;
        }
        if self.pending_proposal == Some(self.round) {
            self.pending_proposal = None;
            self.propose()?;
        }
        Ok(())
    }

    fn advance_round(&mut self, round: Round, tc: Option<TimeoutCert>) {
        if round <= self.round {
            return;
        }
        self.round = round;
        self.round_started_ms = self.now_ms;
        self.consecutive_timeouts = if tc.is_some() {
            self.consecutive_timeouts + 1
        } else {
            0
        };
        self.last_tc = tc;
        self.votes = self.votes.split_off(&round.saturating_sub(1));
        self.timeouts = self.timeouts.split_off(&round);
        let at_ms = self.now_ms + self.round_timeout_ms();
        self.outbox.push(Action::ScheduleTimeout { round, at_ms });
        if self.schedule.leader(round) == self.me
            && let Err(e) = self.propose()
        {
            self.reject(e);
        }
    }

    /// Block interval + base x 1.5^(consecutive timeouts), capped (design doc 4.2, 5.4).
    fn round_timeout_ms(&self) -> u64 {
        let factor = 1.5f64.powi(self.consecutive_timeouts.min(30) as i32);
        let backoff =
            ((self.config.base_timeout_ms as f64 * factor) as u64).min(self.config.max_timeout_ms);
        self.config.min_block_interval_ms + backoff
    }

    fn propose(&mut self) -> Result<()> {
        let round = self.round;
        if self.proposed_round >= round {
            return Ok(());
        }
        // Block pacing: wait out the interval unless a full block is ready.
        let due_ms = self.round_started_ms + self.config.min_block_interval_ms;
        if self.now_ms < due_ms && !self.payload.has_full_block() {
            if self.proposal_timer_round < round {
                self.proposal_timer_round = round;
                self.outbox.push(Action::ScheduleProposal { round, at_ms: due_ms });
            }
            return Ok(());
        }
        let parent = self.high_qc.block_hash();
        let Some((parent_height, parent_time)) = self.tree.height_and_time(&parent) else {
            // Certified but not yet received: fetch it, propose when it arrives.
            self.pending_proposal = Some(round);
            if self.wanted.insert(parent) {
                self.outbox
                    .push(Action::Broadcast(ConsensusMessage::BlockRequest(parent)));
            }
            return Ok(());
        };
        let tc = if self.high_qc.round() + 1 == round {
            None
        } else {
            let tc = self
                .last_tc
                .clone()
                .context("round entered without QC or TC")?;
            ensure!(
                self.high_qc.round() >= tc.max_high_qc_round(),
                "missing the TC's highest QC"
            );
            Some(tc)
        };
        let latest = self.latest_executed();
        let Some((executed_height, executed_state_root)) =
            latest.filter(|_| !self.execution_behind())
        else {
            // Execution is behind: propose once it catches up (design doc 5.5).
            self.pending_proposal = Some(round);
            self.stats.waited_for_execution += 1;
            return Ok(());
        };
        let payload = self.payload.pull(round);
        let header = BlockHeader {
            chain_id: self.config.chain_id,
            epoch: self.schedule.committee().epoch(),
            round,
            height: parent_height + 1,
            parent_hash: parent,
            proposer: self.me,
            timestamp_ms: self.now_ms.max(parent_time + 1),
            payload_digest: payload_digest(&payload),
            executed_height,
            executed_state_root,
        };
        let block = Block::new(header, self.high_qc.clone(), tc, payload, self.safety.signer())?;
        self.proposed_round = round;
        self.stats.proposals += 1;
        self.outbox
            .push(Action::Broadcast(ConsensusMessage::Proposal(Box::new(block.clone()))));
        self.loopback
            .push_back(ConsensusMessage::Proposal(Box::new(block)));
        Ok(())
    }

    fn local_timeout(&mut self) -> Result<()> {
        let round = self.round;
        let timeout = self
            .safety
            .timeout(round, &self.high_qc, self.last_tc.as_ref())?;
        self.stats.timeouts_sent += 1;
        let msg = ConsensusMessage::Timeout {
            timeout,
            last_tc: self.last_tc.clone(),
        };
        self.broadcast(msg);
        // Keep re-broadcasting until the round ends, in case messages were lost.
        let at_ms = self.now_ms + self.round_timeout_ms();
        self.outbox.push(Action::ScheduleTimeout { round, at_ms });
        Ok(())
    }
}
