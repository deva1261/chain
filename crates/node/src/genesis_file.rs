//! `genesis.json` (design doc 8.1): chain id, epoch seed, the initial validators and the funded
//! accounts. Built from public information only; every validator must load an identical file.

use anyhow::{Context, Result};
use aptos_crypto::{HashValue, ed25519::Ed25519PublicKey};
use aptos_types::{account_address::AccountAddress, chain_id::ChainId};
use chain_genesis::{Genesis, GenesisAccount};
use chain_types::{Committee, ValidatorInfo};
use serde::{Deserialize, Serialize};
use std::{path::Path, str::FromStr};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GenesisFile {
    pub chain_id: u8,
    pub epoch: u64,
    /// Hex; seeds HRW leader election for the epoch (design doc 5.2).
    pub epoch_seed: String,
    pub validators: Vec<GenesisValidator>,
    pub accounts: Vec<GenesisBalance>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GenesisValidator {
    /// Hex ed25519 public key.
    pub public_key: String,
    /// `host:port` of the validator's QUIC endpoint.
    pub network_address: String,
    pub stake: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GenesisBalance {
    pub address: String,
    /// Octas.
    pub balance: u64,
}

impl GenesisFile {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        Ok(serde_json::from_str(&text)?)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        std::fs::write(path, serde_json::to_string_pretty(self)?)?;
        Ok(())
    }

    pub fn chain_id(&self) -> ChainId {
        ChainId::new(self.chain_id)
    }

    /// Identifies the network: nodes with a different genesis are different chains.
    pub fn hash(&self) -> HashValue {
        HashValue::sha3_256_of(&serde_json::to_vec(self).expect("serializable"))
    }

    pub fn epoch_seed(&self) -> Result<HashValue> {
        Ok(HashValue::from_slice(hex::decode(&self.epoch_seed)?)?)
    }

    pub fn committee(&self) -> Result<Committee> {
        let validators = self
            .validators
            .iter()
            .map(|v| {
                Ok(ValidatorInfo {
                    public_key: Ed25519PublicKey::try_from(hex::decode(&v.public_key)?.as_slice())?,
                    network_address: v.network_address.clone(),
                    stake: v.stake,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Committee::new(self.epoch, validators)
    }

    pub fn genesis(&self) -> Result<Genesis> {
        Ok(Genesis {
            chain_id: self.chain_id(),
            accounts: self
                .accounts
                .iter()
                .map(|a| {
                    Ok(GenesisAccount {
                        address: AccountAddress::from_str(&a.address)?,
                        balance: a.balance,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        })
    }

    pub fn validator_entry(key: &Ed25519PublicKey, network_address: String, stake: u64) -> GenesisValidator {
        GenesisValidator {
            public_key: hex::encode(key.to_bytes()),
            network_address,
            stake,
        }
    }
}
