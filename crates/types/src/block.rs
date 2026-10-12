//! Blocks and their payload (design doc D9, section 9). A block's payload is a list of batches
//! from day one, so batch dissemination (Quorum Store / Narwhal style) can later replace full
//! batches with digests without touching consensus.

use crate::{Committee, QuorumCert, Round, TimeoutCert, ValidatorIndex, ValidatorSigner};
use anyhow::{Context, Result, ensure};
use aptos_crypto::{HashValue, Signature, ed25519::Ed25519Signature, hash::CryptoHash};
use aptos_crypto_derive::{BCSCryptoHash, CryptoHasher};
use aptos_types::{chain_id::ChainId, transaction::SignedTransaction};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Batch {
    pub author: ValidatorIndex,
    pub batch_id: u64,
    pub txns: Vec<SignedTransaction>,
}

impl Batch {
    /// Content digest; what a digest-only proposal would carry instead of the batch.
    pub fn digest(&self) -> HashValue {
        HashValue::sha3_256_of(&bcs::to_bytes(self).expect("batches serialize"))
    }
}

/// Digest of a payload: the hash of its batch digests in order.
pub fn payload_digest(payload: &[Batch]) -> HashValue {
    let digests: Vec<HashValue> = payload.iter().map(Batch::digest).collect();
    HashValue::sha3_256_of(&bcs::to_bytes(&digests).expect("digests serialize"))
}

/// Signed by the proposer; its hash is the block's identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, CryptoHasher, BCSCryptoHash)]
pub struct BlockHeader {
    pub chain_id: ChainId,
    pub epoch: u64,
    pub round: Round,
    pub height: u64,
    pub parent_hash: HashValue,
    pub proposer: ValidatorIndex,
    pub timestamp_ms: u64,
    pub payload_digest: HashValue,
    /// Execution lags consensus (design doc D3): the state root after block `executed_height`
    /// (= height - k), as computed by the proposer.
    pub executed_height: u64,
    pub executed_state_root: HashValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Block {
    pub header: BlockHeader,
    /// Certifies the parent.
    pub qc: QuorumCert,
    /// Present when the previous round ended in a timeout.
    pub tc: Option<TimeoutCert>,
    pub payload: Vec<Batch>,
    pub signature: Ed25519Signature,
}

impl Block {
    /// The epoch's root block: round 0, no parent, no payload, unsigned.
    pub fn genesis(chain_id: ChainId, epoch: u64) -> BlockHeader {
        BlockHeader {
            chain_id,
            epoch,
            round: 0,
            height: 0,
            parent_hash: HashValue::zero(),
            proposer: 0,
            timestamp_ms: 0,
            payload_digest: payload_digest(&[]),
            executed_height: 0,
            executed_state_root: HashValue::zero(),
        }
    }

    pub fn new(
        header: BlockHeader,
        qc: QuorumCert,
        tc: Option<TimeoutCert>,
        payload: Vec<Batch>,
        signer: &ValidatorSigner,
    ) -> Result<Self> {
        Ok(Self {
            signature: signer.sign(&header)?,
            header,
            qc,
            tc,
            payload,
        })
    }

    pub fn hash(&self) -> HashValue {
        self.header.hash()
    }

    pub fn round(&self) -> Round {
        self.header.round
    }

    /// Structural and cryptographic checks that need no other block: signatures, certificates,
    /// payload digest, and that the QC/TC justify this round. Leader identity, parent existence
    /// and state roots are checked by consensus, which has that context.
    pub fn verify(&self, committee: &Committee, root_block: HashValue) -> Result<()> {
        let h = &self.header;
        ensure!(h.epoch == committee.epoch(), "block from another epoch");
        ensure!(
            payload_digest(&self.payload) == h.payload_digest,
            "payload does not match its digest"
        );
        ensure!(
            self.qc.block_hash() == h.parent_hash,
            "QC does not certify the parent"
        );
        ensure!(self.qc.round() < h.round, "QC is not older than the block");
        match &self.tc {
            None => ensure!(
                self.qc.round() + 1 == h.round,
                "block skips rounds without a timeout certificate"
            ),
            Some(tc) => {
                ensure!(tc.round + 1 == h.round, "TC is not for the previous round");
                ensure!(
                    self.qc.round() >= tc.max_high_qc_round(),
                    "block extends a QC older than the TC's highest"
                );
                tc.verify(committee)?;
            },
        }
        let proposer = committee
            .validator(h.proposer)
            .context("unknown proposer")?;
        self.signature.verify(&self.header, &proposer.public_key)?;
        self.qc.verify(committee, root_block)
    }
}
