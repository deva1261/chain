use anyhow::{Result, bail, ensure};
use aptos_crypto::ed25519::Ed25519PublicKey;
use serde::{Deserialize, Serialize};

/// Position of a validator in its epoch's committee.
pub type ValidatorIndex = u16;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatorInfo {
    pub public_key: Ed25519PublicKey,
    /// `host:port` of the validator's QUIC endpoint.
    pub network_address: String,
    pub stake: u64,
}

/// The validator set of one epoch (design doc 5.1). Validators are ordered by public key, so
/// every node assigns the same [`ValidatorIndex`] to each validator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Committee {
    epoch: u64,
    validators: Vec<ValidatorInfo>,
    total_stake: u64,
}

impl Committee {
    pub fn new(epoch: u64, mut validators: Vec<ValidatorInfo>) -> Result<Self> {
        ensure!(!validators.is_empty(), "committee is empty");
        ensure!(
            validators.len() <= ValidatorIndex::MAX as usize,
            "too many validators"
        );
        validators.sort_by(|a, b| a.public_key.to_bytes().cmp(&b.public_key.to_bytes()));
        let mut total_stake: u64 = 0;
        for (i, v) in validators.iter().enumerate() {
            ensure!(v.stake > 0, "validator {i} has zero stake");
            if i > 0 && validators[i - 1].public_key == v.public_key {
                bail!("duplicate validator key");
            }
            total_stake = total_stake
                .checked_add(v.stake)
                .ok_or_else(|| anyhow::anyhow!("total stake overflows"))?;
        }
        Ok(Self {
            epoch,
            validators,
            total_stake,
        })
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn validators(&self) -> &[ValidatorInfo] {
        &self.validators
    }

    pub fn len(&self) -> usize {
        self.validators.len()
    }

    pub fn is_empty(&self) -> bool {
        self.validators.is_empty()
    }

    pub fn total_stake(&self) -> u64 {
        self.total_stake
    }

    /// Smallest stake that is more than 2/3 of the total.
    pub fn quorum_stake(&self) -> u64 {
        (self.total_stake as u128 * 2 / 3) as u64 + 1
    }

    pub fn validator(&self, index: ValidatorIndex) -> Option<&ValidatorInfo> {
        self.validators.get(index as usize)
    }

    pub fn index_of(&self, key: &Ed25519PublicKey) -> Option<ValidatorIndex> {
        self.validators
            .iter()
            .position(|v| &v.public_key == key)
            .map(|i| i as ValidatorIndex)
    }

    /// Total stake of `signers`; errors on unknown or repeated indices.
    pub fn stake_of(&self, signers: impl IntoIterator<Item = ValidatorIndex>) -> Result<u64> {
        let mut seen = vec![false; self.validators.len()];
        let mut stake = 0u64;
        for index in signers {
            let v = self
                .validator(index)
                .ok_or_else(|| anyhow::anyhow!("unknown validator index {index}"))?;
            ensure!(!seen[index as usize], "validator {index} counted twice");
            seen[index as usize] = true;
            stake += v.stake;
        }
        Ok(stake)
    }

    pub fn is_quorum(&self, stake: u64) -> bool {
        stake >= self.quorum_stake()
    }
}
