//! Native-transfer backend (design doc step E1): Block-STM over APT transfers executed in Rust
//! instead of MoveVM. It reads and writes the same Aptos state layout, so it runs on the same
//! genesis, but charges a flat fee and skips VM-level checks (see `task.rs`). Use it to find
//! the execution ceiling and as the basis of a transfer fast path.

mod task;
mod transaction;

use crate::{BlockId, ExecutionBackend};
use anyhow::{Result, anyhow, bail};
use aptos_block_executor::{
    code_cache_global_manager::AptosModuleCacheManager,
    txn_commit_hook::NoOpTransactionCommitHook, txn_provider::default::DefaultTxnProvider,
};
use aptos_types::{
    block_executor::{
        config::{BlockExecutorConfig, BlockExecutorConfigFromOnchain, BlockExecutorLocalConfig},
        transaction_slice_metadata::TransactionSliceMetadata,
    },
    state_store::StateView,
    transaction::{
        AuxiliaryInfo, BlockOutput, TransactionOutput,
        signature_verified_transaction::SignatureVerifiedTransaction,
    },
};
use aptos_vm::block_executor::AptosBlockExecutorWrapper;
use move_core_types::vm_status::VMStatus;
use task::NativeVMExecutorTask;
use transaction::NativeTransaction;

/// Flat gas charged per transaction by the native path, in gas units (at 100 octas each).
pub const NATIVE_GAS_UNITS: u64 = 4;

pub struct NativeBackend {
    config: BlockExecutorLocalConfig,
    /// Kept across blocks so consecutive blocks reuse cached state (see [`BlockId`]).
    module_cache: AptosModuleCacheManager,
}

impl NativeBackend {
    /// Unlike the MoveVM backend, the thread count is per backend, not per process.
    pub fn new(concurrency_level: usize) -> Self {
        let concurrency_level = concurrency_level.clamp(1, num_cpus::get());
        let config = BlockExecutorLocalConfig {
            enable_pre_write: false,
            ..BlockExecutorLocalConfig::default_with_concurrency_level(concurrency_level)
        };
        Self {
            module_cache: AptosModuleCacheManager::new(config.clone()),
            config,
        }
    }

    pub fn concurrency_level(&self) -> usize {
        self.config.concurrency_level
    }

    /// Whether every transaction is one the native path can execute.
    pub fn supports(txns: &[SignatureVerifiedTransaction]) -> bool {
        txns.iter().all(|t| NativeTransaction::parse(t).is_some())
    }
}

impl ExecutionBackend for NativeBackend {
    fn name(&self) -> &'static str {
        "native"
    }

    fn execute_block(
        &self,
        block: BlockId,
        txns: Vec<SignatureVerifiedTransaction>,
        state: &(impl StateView + Sync),
    ) -> Result<Vec<TransactionOutput>> {
        if !Self::supports(&txns) {
            bail!("native backend only executes APT transfers with valid signatures");
        }
        let aux = (0..txns.len() as u32)
            .map(AuxiliaryInfo::new_timestamp_not_yet_assigned)
            .collect();
        let provider = DefaultTxnProvider::new(txns, aux);
        let output = AptosBlockExecutorWrapper::<NativeVMExecutorTask>::execute_block::<
            _,
            NoOpTransactionCommitHook<VMStatus>,
            _,
        >(
            &provider,
            state,
            &self.module_cache,
            BlockExecutorConfig {
                local: self.config.clone(),
                onchain: BlockExecutorConfigFromOnchain::new_no_block_limit(),
            },
            TransactionSliceMetadata::block(block.parent, block.id),
            None,
        )
        .map_err(|e| anyhow!("native block execution failed: {e:?}"))?;
        Ok(BlockOutput::into_transaction_outputs_forced(output))
    }
}
