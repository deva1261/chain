//! Votes, timeouts and the certificates formed from them (design doc 5.3, section 9).
//! Each signed payload is a distinct type with its own hashing domain, so a signature can never
//! be replayed as a different kind of message.

use crate::{Committee, ValidatorIndex, ValidatorSigner};
use anyhow::{Context, Result, ensure};
use aptos_crypto::{HashValue, Signature, ed25519::Ed25519Signature};
use aptos_crypto_derive::{BCSCryptoHash, CryptoHasher};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

pub type Round = u64;

/// What a validator signs when voting: "block `block_hash` is valid in `round`", plus the block's
/// parent (the block its QC certifies). The parent makes every QC a two-block chain, which the
/// locking rule and the 2-chain commit rule need (design doc 5.3; same as Aptos `VoteData`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, CryptoHasher, BCSCryptoHash)]
pub struct VoteData {
    pub epoch: u64,
    pub round: Round,
    pub block_hash: HashValue,
    pub parent_round: Round,
    pub parent_hash: HashValue,
}

impl VoteData {
    /// The vote for `block`, which must already be verified.
    pub fn for_block(block: &crate::Block) -> Self {
        Self {
            epoch: block.header.epoch,
            round: block.round(),
            block_hash: block.hash(),
            parent_round: block.qc.round(),
            parent_hash: block.qc.block_hash(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vote {
    pub data: VoteData,
    pub voter: ValidatorIndex,
    pub signature: Ed25519Signature,
}

impl Vote {
    pub fn new(data: VoteData, signer: &ValidatorSigner) -> Result<Self> {
        Ok(Self {
            signature: signer.sign(&data)?,
            voter: signer.index(),
            data,
        })
    }

    pub fn verify(&self, committee: &Committee) -> Result<()> {
        ensure!(self.data.epoch == committee.epoch(), "vote from another epoch");
        let voter = committee
            .validator(self.voter)
            .context("vote from unknown validator")?;
        self.signature.verify(&self.data, &voter.public_key)?;
        Ok(())
    }
}

/// Votes from more than 2/3 of stake for one block in one round. The genesis certificate
/// (round 0) has no signatures and certifies only the epoch's root block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuorumCert {
    pub data: VoteData,
    /// Sorted by validator index, no repeats. (Post-POC: one aggregated BLS signature.)
    pub signatures: Vec<(ValidatorIndex, Ed25519Signature)>,
}

impl QuorumCert {
    pub fn genesis(epoch: u64, root_block: HashValue) -> Self {
        Self {
            data: VoteData {
                epoch,
                round: 0,
                block_hash: root_block,
                parent_round: 0,
                parent_hash: HashValue::zero(),
            },
            signatures: vec![],
        }
    }

    pub fn round(&self) -> Round {
        self.data.round
    }

    pub fn block_hash(&self) -> HashValue {
        self.data.block_hash
    }

    /// Round of the certified block's parent (the "two-chain" round).
    pub fn parent_round(&self) -> Round {
        self.data.parent_round
    }

    /// The block this QC commits under the 2-chain rule, if any: the certified block's parent,
    /// when the two are in consecutive rounds (design doc 5.3).
    pub fn commits(&self) -> Option<(Round, HashValue)> {
        (self.data.round > 0 && self.data.parent_round + 1 == self.data.round)
            .then_some((self.data.parent_round, self.data.parent_hash))
    }

    /// Checks quorum stake and every signature. `root_block` is the only block a round-0
    /// certificate may certify.
    pub fn verify(&self, committee: &Committee, root_block: HashValue) -> Result<()> {
        ensure!(self.data.epoch == committee.epoch(), "QC from another epoch");
        ensure!(
            self.data.round == 0 || self.data.parent_round < self.data.round,
            "QC parent is not older than the certified block"
        );
        if self.data.round == 0 {
            ensure!(
                self.data.block_hash == root_block && self.signatures.is_empty(),
                "invalid genesis QC"
            );
            return Ok(());
        }
        ensure!(
            self.signatures.windows(2).all(|w| w[0].0 < w[1].0),
            "QC signers are not sorted and unique"
        );
        let stake = committee.stake_of(self.signatures.iter().map(|(i, _)| *i))?;
        ensure!(committee.is_quorum(stake), "QC lacks quorum stake");
        self.signatures.par_iter().try_for_each(|(index, signature)| {
            let key = &committee.validator(*index).context("unknown signer")?.public_key;
            signature
                .verify(&self.data, key)
                .with_context(|| format!("bad QC signature from validator {index}"))
        })
    }
}

/// What a validator signs when giving up on a round: its round and the round of its highest QC.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, CryptoHasher, BCSCryptoHash)]
pub struct TimeoutData {
    pub epoch: u64,
    pub round: Round,
    pub high_qc_round: Round,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Timeout {
    pub data: TimeoutData,
    /// The QC whose round is `data.high_qc_round`, so the next leader can extend it.
    pub high_qc: QuorumCert,
    pub voter: ValidatorIndex,
    pub signature: Ed25519Signature,
}

impl Timeout {
    pub fn new(epoch: u64, round: Round, high_qc: QuorumCert, signer: &ValidatorSigner) -> Result<Self> {
        let data = TimeoutData {
            epoch,
            round,
            high_qc_round: high_qc.round(),
        };
        Ok(Self {
            signature: signer.sign(&data)?,
            voter: signer.index(),
            data,
            high_qc,
        })
    }

    pub fn verify(&self, committee: &Committee, root_block: HashValue) -> Result<()> {
        ensure!(self.data.epoch == committee.epoch(), "timeout from another epoch");
        ensure!(
            self.data.high_qc_round == self.high_qc.round() && self.high_qc.round() < self.data.round,
            "timeout's high QC does not match"
        );
        let voter = committee
            .validator(self.voter)
            .context("timeout from unknown validator")?;
        self.signature.verify(&self.data, &voter.public_key)?;
        self.high_qc.verify(committee, root_block)
    }
}

/// Timeouts from more than 2/3 of stake for one round (design doc 4.2). Each signer signed its
/// own `high_qc_round`, so the certificate keeps them all.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeoutCert {
    pub epoch: u64,
    pub round: Round,
    /// (signer, its high QC round, its signature), sorted by signer, no repeats.
    pub timeouts: Vec<(ValidatorIndex, Round, Ed25519Signature)>,
}

impl TimeoutCert {
    /// Highest QC round any signer reported; a proposal after this TC must extend a QC at least
    /// this high (design doc 5.3 vote rule 3).
    pub fn max_high_qc_round(&self) -> Round {
        self.timeouts.iter().map(|(_, r, _)| *r).max().unwrap_or(0)
    }

    pub fn verify(&self, committee: &Committee) -> Result<()> {
        ensure!(self.epoch == committee.epoch(), "TC from another epoch");
        ensure!(
            self.timeouts.windows(2).all(|w| w[0].0 < w[1].0),
            "TC signers are not sorted and unique"
        );
        let stake = committee.stake_of(self.timeouts.iter().map(|(i, _, _)| *i))?;
        ensure!(committee.is_quorum(stake), "TC lacks quorum stake");
        self.timeouts.par_iter().try_for_each(|(index, high_qc_round, signature)| {
            let key = &committee.validator(*index).context("unknown signer")?.public_key;
            let data = TimeoutData {
                epoch: self.epoch,
                round: self.round,
                high_qc_round: *high_qc_round,
            };
            signature
                .verify(&data, key)
                .with_context(|| format!("bad TC signature from validator {index}"))
        })
    }
}
