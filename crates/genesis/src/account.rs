use aptos_crypto::{PrivateKey, ed25519::Ed25519PrivateKey};
use aptos_keygen::KeyGen;
use aptos_types::{
    account_address::AccountAddress,
    account_config::aptos_test_root_address,
    chain_id::ChainId,
    transaction::{
        RawTransaction, Transaction, TransactionPayload, authenticator::AuthenticationKey,
    },
};
use aptos_vm_genesis::GENESIS_KEYPAIR;

/// Gas limit and price for transactions signed here. Price is the Aptos minimum.
pub const MAX_GAS_AMOUNT: u64 = 200_000;
pub const GAS_UNIT_PRICE: u64 = 100;
/// Expiry far beyond any block timestamp a POC chain will reach (year ~2286).
const EXPIRATION_TIMESTAMP_SECS: u64 = 10_000_000_000;

/// An account whose private key we hold, tracking its next sequence number locally.
/// Used by genesis, tests and (later) the load generator.
pub struct LocalAccount {
    address: AccountAddress,
    key: Ed25519PrivateKey,
    sequence_number: u64,
}

impl LocalAccount {
    /// An account whose address is derived from its ed25519 key, as on Aptos.
    pub fn new(key: Ed25519PrivateKey) -> Self {
        let address = AuthenticationKey::ed25519(&key.public_key()).account_address();
        Self {
            address,
            key,
            sequence_number: 0,
        }
    }

    /// Deterministic accounts: the same seed always yields the same `count` accounts.
    pub fn generate(seed: [u8; 32], count: usize) -> Vec<Self> {
        let mut keygen = KeyGen::from_seed(seed);
        (0..count)
            .map(|_| Self::new(keygen.generate_ed25519_private_key()))
            .collect()
    }

    /// The genesis "core resources" account, which holds the APT mint capability on
    /// test chains.
    pub fn core_resources() -> Self {
        let key = Ed25519PrivateKey::try_from(GENESIS_KEYPAIR.0.to_bytes().as_slice())
            .expect("genesis key bytes are a valid ed25519 key");
        Self {
            address: aptos_test_root_address(),
            key,
            sequence_number: 0,
        }
    }

    pub fn address(&self) -> AccountAddress {
        self.address
    }

    pub fn sequence_number(&self) -> u64 {
        self.sequence_number
    }

    /// Signs `payload` with the next sequence number and advances it.
    pub fn sign(&mut self, payload: TransactionPayload, chain_id: ChainId) -> Transaction {
        let raw = RawTransaction::new(
            self.address,
            self.sequence_number,
            payload,
            MAX_GAS_AMOUNT,
            GAS_UNIT_PRICE,
            EXPIRATION_TIMESTAMP_SECS,
            chain_id,
        );
        self.sequence_number += 1;
        let signed = raw
            .sign(&self.key, self.key.public_key())
            .expect("signing a raw transaction cannot fail")
            .into_inner();
        Transaction::UserTransaction(signed)
    }

    /// An `aptos_account::transfer` of `amount` octas to `to` (creates `to` if needed).
    pub fn transfer(&mut self, to: AccountAddress, amount: u64, chain_id: ChainId) -> Transaction {
        self.sign(
            aptos_cached_packages::aptos_stdlib::aptos_account_transfer(to, amount),
            chain_id,
        )
    }
}
