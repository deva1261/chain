//! Genesis: the state a chain starts from (design doc 8.1).
//!
//! The Aptos framework is installed with Aptos's own genesis builder, so all state follows
//! the Aptos layout and both execution backends can run on it. Initial balances are then
//! minted by the core resources account through ordinary signed transactions executed by
//! our own backend, which also exercises the execution path end to end.
//!
//! Our consensus committee (validators, epoch seed) will be added to [`Genesis`] with the
//! consensus crate. The single Aptos validator below only satisfies the framework's stake
//! module and plays no part in our consensus.

mod account;

pub use account::{GAS_UNIT_PRICE, LocalAccount, MAX_GAS_AMOUNT};

use anyhow::{Result, ensure};
use aptos_crypto::HashValue;
use aptos_types::{
    account_address::AccountAddress,
    chain_id::ChainId,
    on_chain_config::{OnChainConsensusConfig, OnChainExecutionConfig},
    transaction::{
        ChangeSet, ExecutionStatus, TransactionAuxiliaryData, TransactionOutput, TransactionStatus,
    },
};
use aptos_vm_genesis::{GENESIS_KEYPAIR, GenesisConfiguration, TestValidator};
use chain_execution::{ExecutionBackend, InMemoryState, StateStore, verify_signatures};

/// Mint transactions per block while funding genesis accounts.
const MINTS_PER_BLOCK: usize = 1_000;

#[derive(Clone, Debug)]
pub struct GenesisAccount {
    pub address: AccountAddress,
    /// Initial APT balance in octas (1 APT = 10^8 octas).
    pub balance: u64,
}

#[derive(Clone, Debug)]
pub struct Genesis {
    pub chain_id: ChainId,
    pub accounts: Vec<GenesisAccount>,
}

impl Genesis {
    /// Builds the genesis state in memory.
    pub fn build(&self, backend: &impl ExecutionBackend) -> Result<InMemoryState> {
        let mut state = InMemoryState::new();
        self.build_into(backend, &mut state)?;
        Ok(state)
    }

    /// Commits genesis into an empty `state`: block 1 is the Aptos framework write set (as a
    /// single output, so it takes the normal commit path), then blocks of mints follow.
    pub fn build_into(&self, backend: &impl ExecutionBackend, state: &mut impl StateStore) -> Result<()> {
        let framework = TransactionOutput::new(
            framework_change_set(self.chain_id).into_inner().0,
            vec![],
            0,
            TransactionStatus::Keep(ExecutionStatus::Success),
            TransactionAuxiliaryData::None,
        );
        let block = state.next_block(HashValue::sha3_256_of(b"genesis framework"));
        state.apply_block(block, &[framework])?;

        let mut minter = LocalAccount::core_resources();
        for (i, chunk) in self.accounts.chunks(MINTS_PER_BLOCK).enumerate() {
            let txns = chunk
                .iter()
                .map(|account| {
                    minter.sign(
                        aptos_cached_packages::aptos_stdlib::aptos_coin_mint(
                            account.address,
                            account.balance,
                        ),
                        self.chain_id,
                    )
                })
                .collect();
            let block = state.next_block(HashValue::sha3_256_of(format!("genesis {i}").as_bytes()));
            let outputs = backend.execute_block(block, verify_signatures(txns), &*state)?;
            for (account, output) in chunk.iter().zip(&outputs) {
                ensure!(
                    *output.status() == TransactionStatus::Keep(ExecutionStatus::Success),
                    "minting to {} failed: {:?}",
                    account.address,
                    output.status(),
                );
            }
            state.apply_block(block, &outputs)?;
        }
        Ok(())
    }
}

/// The Aptos framework write set for `chain_id`, in test mode so the core resources account
/// can mint. Parameters mirror `aptos_vm_genesis::generate_test_genesis`.
fn framework_change_set(chain_id: ChainId) -> ChangeSet {
    let validators: Vec<_> = TestValidator::new_test_set(Some(1), Some(100_000_000))
        .into_iter()
        .map(|v| v.data)
        .collect();
    aptos_vm_genesis::encode_genesis_change_set(
        &GENESIS_KEYPAIR.1,
        &validators,
        aptos_cached_packages::head_release_bundle(),
        chain_id,
        &GenesisConfiguration {
            allow_new_validators: true,
            epoch_duration_secs: 3600,
            is_test: true,
            min_stake: 0,
            min_voting_threshold: 0,
            max_stake: 100_000_000_000_000,
            recurring_lockup_duration_secs: 7200,
            required_proposer_stake: 0,
            rewards_apy_percentage: 10,
            voting_duration_secs: 3600,
            voting_power_increase_limit: 50,
            employee_vesting_start: 1663456089,
            employee_vesting_period_duration: 5 * 60,
            initial_features_override: None,
            randomness_config_override: None,
            chunky_dkg_config_override: None,
            jwk_consensus_config_override: None,
            initial_jwks: vec![],
            keyless_groth16_vk: None,
            initialize_decryption_at_genesis: true,
        },
        &OnChainConsensusConfig::default_for_genesis(),
        &OnChainExecutionConfig::default_for_genesis(),
        &aptos_vm_genesis::default_gas_schedule(),
    )
}
