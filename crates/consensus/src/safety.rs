//! Safety rules (design doc 5.3): the only code that signs votes and timeouts. Follows Aptos'
//! 2-chain safety rules (`consensus/safety-rules`, `safety_rules_2chain.rs`):
//!
//! - Vote for block B in round r only if r > last_voted_round, B.qc.round >= preferred_round,
//!   and either B.qc.round == r - 1 or B carries TC(r - 1) with B.qc.round >= the TC's highest
//!   QC round. Voting observes B.qc: one_chain_round = max QC round, preferred_round = max QC
//!   parent round (the 2-chain lock).
//! - Time out in round r only if r >= last_voted_round, the reported high QC is at least
//!   one_chain_round, and r follows that QC or the previous round's TC. Afterwards never vote
//!   in r.
//!
//! State is persisted (fsync) before any signature is returned. Inputs must already be
//! verified (signatures, certificates); these rules only decide whether signing is safe.

use anyhow::{Context, Result, bail, ensure};
use chain_types::{
    Block, QuorumCert, Round, Timeout, TimeoutCert, ValidatorSigner, Vote, VoteData,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::Write,
    path::PathBuf,
};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SafetyData {
    pub epoch: u64,
    pub last_voted_round: Round,
    /// 2-chain lock: highest parent round of any QC observed when voting.
    pub preferred_round: Round,
    /// Highest QC round observed when voting.
    pub one_chain_round: Round,
    pub highest_timeout_round: Round,
    /// Re-sent unchanged if asked to vote in the same round again (e.g. after a restart).
    pub last_vote: Option<Vote>,
}

pub trait SafetyStorage: Send {
    fn load(&self) -> Result<Option<SafetyData>>;
    /// Must be durable when it returns.
    fn store(&mut self, data: &SafetyData) -> Result<()>;
}

/// Writes to a temporary file, fsyncs it, then renames it over the old one and fsyncs the
/// directory, so a crash leaves either the old or the new state, never a torn one.
pub struct FileSafetyStorage {
    path: PathBuf,
}

impl FileSafetyStorage {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl SafetyStorage for FileSafetyStorage {
    fn load(&self) -> Result<Option<SafetyData>> {
        match fs::read(&self.path) {
            Ok(bytes) => Ok(Some(bcs::from_bytes(&bytes).context("corrupt safety data")?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("reading {}", self.path.display())),
        }
    }

    fn store(&mut self, data: &SafetyData) -> Result<()> {
        let tmp = self.path.with_extension("tmp");
        let mut file = File::create(&tmp)?;
        file.write_all(&bcs::to_bytes(data)?)?;
        file.sync_all()?;
        fs::rename(&tmp, &self.path)?;
        if let Some(dir) = self.path.parent() {
            File::open(dir)?.sync_all()?;
        }
        Ok(())
    }
}

/// Volatile storage for tests and simulations.
#[derive(Default)]
pub struct InMemorySafetyStorage(Option<SafetyData>);

impl SafetyStorage for InMemorySafetyStorage {
    fn load(&self) -> Result<Option<SafetyData>> {
        Ok(self.0.clone())
    }

    fn store(&mut self, data: &SafetyData) -> Result<()> {
        self.0 = Some(data.clone());
        Ok(())
    }
}

pub struct SafetyRules<S> {
    signer: ValidatorSigner,
    data: SafetyData,
    storage: S,
}

impl<S: SafetyStorage> SafetyRules<S> {
    /// Loads persisted state; a new epoch starts from fresh state.
    pub fn new(signer: ValidatorSigner, storage: S, epoch: u64) -> Result<Self> {
        let data = match storage.load()? {
            Some(data) if data.epoch == epoch => data,
            Some(data) if data.epoch > epoch => {
                bail!("safety data is from epoch {}, ahead of {epoch}", data.epoch)
            },
            _ => SafetyData {
                epoch,
                ..SafetyData::default()
            },
        };
        let mut rules = Self {
            signer,
            data,
            storage,
        };
        let data = rules.data.clone();
        rules.storage.store(&data)?;
        Ok(rules)
    }

    pub fn data(&self) -> &SafetyData {
        &self.data
    }

    pub fn signer(&self) -> &ValidatorSigner {
        &self.signer
    }

    /// Signs a vote for `block` if that is safe.
    pub fn vote(&mut self, block: &Block) -> Result<Vote> {
        let round = block.round();
        ensure!(block.header.epoch == self.data.epoch, "block from another epoch");
        if let Some(vote) = &self.data.last_vote
            && vote.data.round == round
        {
            // Already voted in this round: repeat that vote, never sign a second one.
            return Ok(vote.clone());
        }
        ensure!(
            round > self.data.last_voted_round,
            "already voted or timed out in round {} (asked {round})",
            self.data.last_voted_round
        );
        let qc = &block.qc;
        ensure!(
            qc.round() >= self.data.preferred_round,
            "block extends QC round {} below the locked round {}",
            qc.round(),
            self.data.preferred_round
        );
        safe_to_vote(round, qc, block.tc.as_ref())?;

        let mut data = self.data.clone();
        observe_qc(&mut data, qc);
        let vote = Vote::new(VoteData::for_block(block), &self.signer)?;
        data.last_voted_round = round;
        data.last_vote = Some(vote.clone());
        self.persist(data)?;
        Ok(vote)
    }

    /// Signs a timeout for `round` reporting `high_qc`, if that is safe. `prev_tc` is the TC
    /// through which this round was entered, if any.
    pub fn timeout(
        &mut self,
        round: Round,
        high_qc: &QuorumCert,
        prev_tc: Option<&TimeoutCert>,
    ) -> Result<Timeout> {
        ensure!(
            round >= self.data.last_voted_round,
            "cannot time out round {round} after round {}",
            self.data.last_voted_round
        );
        ensure!(
            high_qc.round() >= self.data.one_chain_round,
            "high QC round {} is below the highest QC observed, {}",
            high_qc.round(),
            self.data.one_chain_round
        );
        let follows_qc = high_qc.round() + 1 == round;
        let follows_tc = prev_tc.is_some_and(|tc| tc.round + 1 == round);
        ensure!(
            follows_qc || follows_tc,
            "round {round} does not follow the high QC or the previous TC"
        );

        let timeout = Timeout::new(self.data.epoch, round, high_qc.clone(), &self.signer)?;
        let mut data = self.data.clone();
        data.last_voted_round = round;
        data.highest_timeout_round = data.highest_timeout_round.max(round);
        self.persist(data)?;
        Ok(timeout)
    }

    fn persist(&mut self, data: SafetyData) -> Result<()> {
        self.storage.store(&data)?;
        self.data = data;
        Ok(())
    }
}

fn safe_to_vote(round: Round, qc: &QuorumCert, tc: Option<&TimeoutCert>) -> Result<()> {
    if qc.round() + 1 == round {
        return Ok(());
    }
    match tc {
        Some(tc) if tc.round + 1 == round && qc.round() >= tc.max_high_qc_round() => Ok(()),
        _ => bail!(
            "round {round} neither extends QC round {} directly nor follows a valid TC",
            qc.round()
        ),
    }
}

fn observe_qc(data: &mut SafetyData, qc: &QuorumCert) {
    data.one_chain_round = data.one_chain_round.max(qc.round());
    data.preferred_round = data.preferred_round.max(qc.parent_round());
}
