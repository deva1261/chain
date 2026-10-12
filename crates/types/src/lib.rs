//! Consensus data types (design doc section 9), serialized with BCS (D11).

mod block;
mod cert;
mod committee;

pub use block::{Batch, Block, BlockHeader, payload_digest};
pub use cert::{QuorumCert, Round, Timeout, TimeoutCert, TimeoutData, Vote, VoteData};
pub use committee::{Committee, ValidatorIndex, ValidatorInfo};

use anyhow::Result;
use aptos_crypto::{
    PrivateKey, SigningKey,
    ed25519::{Ed25519PrivateKey, Ed25519PublicKey, Ed25519Signature},
    hash::CryptoHash,
};
use serde::Serialize;

/// A validator's signing identity within one committee.
pub struct ValidatorSigner {
    index: ValidatorIndex,
    key: Ed25519PrivateKey,
}

impl ValidatorSigner {
    pub fn new(index: ValidatorIndex, key: Ed25519PrivateKey) -> Self {
        Self { index, key }
    }

    /// Finds this key's index in `committee`; `None` if it is not a member.
    pub fn for_committee(key: Ed25519PrivateKey, committee: &Committee) -> Option<Self> {
        let index = committee.index_of(&key.public_key())?;
        Some(Self { index, key })
    }

    pub fn index(&self) -> ValidatorIndex {
        self.index
    }

    pub fn public_key(&self) -> Ed25519PublicKey {
        self.key.public_key()
    }

    pub fn sign<T: CryptoHash + Serialize>(&self, message: &T) -> Result<Ed25519Signature> {
        Ok(self.key.sign(message)?)
    }
}
