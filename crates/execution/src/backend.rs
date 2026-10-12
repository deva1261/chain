use anyhow::{Result, anyhow};
use aptos_crypto::HashValue;
use aptos_types::{
    block_executor::{
        config::BlockExecutorConfigFromOnchain,
        transaction_slice_metadata::TransactionSliceMetadata,
    },
    state_store::StateView,
    transaction::{
        AuxiliaryInfo, BlockOutput, Transaction, TransactionOutput,
        signature_verified_transaction::SignatureVerifiedTransaction,
    },
};
use rayon::prelude::*;
use aptos_block_executor::txn_provider::default::DefaultTxnProvider;
use aptos_vm::{
    AptosVM, VMBlockExecutor,
    aptos_vm::AptosVMBlockExecutor,
};

/// Which block is being executed and on top of which parent. Consensus supplies these; Aptos
/// uses them to keep its module cache warm across consecutive blocks (an unknown or
/// non-consecutive block flushes the whole cache).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockId {
    pub parent: HashValue,
    pub id: HashValue,
}

/// A pluggable block executor. Consensus depends only on this trait, so the native-transfer
/// fast path (E1) and the full MoveVM (E2) are interchangeable.
pub trait ExecutionBackend: Send + Sync {
    fn name(&self) -> &'static str;

    /// Executes `txns` in the given order on top of `state` and returns one output per
    /// transaction, in order, possibly followed by one block-epilogue output that Aptos appends
    /// (block-end info and fee distribution); commit it with the rest. Must be deterministic:
    /// every validator gets identical outputs.
    /// Signatures are already checked (see [`verify_signatures`]); invalid ones are discarded.
    fn execute_block(
        &self,
        block: BlockId,
        txns: Vec<SignatureVerifiedTransaction>,
        state: &(impl StateView + Sync),
    ) -> Result<Vec<TransactionOutput>>;
}

/// Block-STM settings. Aptos keeps them in process-wide `OnceCell`s, so the first backend
/// created in a process decides them; later values are ignored.
#[derive(Clone, Copy, Debug)]
pub struct ExecutorConfig {
    /// Worker threads; 1 means sequential execution.
    pub concurrency_level: usize,
    /// Use the Block-STM v2 scheduler instead of v1.
    pub blockstm_v2: bool,
}

impl ExecutorConfig {
    pub fn new(concurrency_level: usize) -> Self {
        Self {
            concurrency_level,
            blockstm_v2: false,
        }
    }

    /// Settings actually in effect for this process.
    pub fn effective() -> Self {
        Self {
            concurrency_level: AptosVM::get_concurrency_level(),
            blockstm_v2: AptosVM::get_blockstm_v2_enabled(),
        }
    }
}

/// Runs transactions through the real Aptos VM (MoveVM) on Block-STM.
pub struct MoveVmBackend {
    inner: AptosVMBlockExecutor,
}

impl MoveVmBackend {
    pub fn new(config: ExecutorConfig) -> Self {
        AptosVM::set_concurrency_level_once(config.concurrency_level.max(1));
        AptosVM::set_blockstm_v2_enabled_once(config.blockstm_v2);
        Self {
            inner: AptosVMBlockExecutor::new(),
        }
    }
}

impl ExecutionBackend for MoveVmBackend {
    fn name(&self) -> &'static str {
        "move-vm"
    }

    fn execute_block(
        &self,
        block: BlockId,
        txns: Vec<SignatureVerifiedTransaction>,
        state: &(impl StateView + Sync),
    ) -> Result<Vec<TransactionOutput>> {
        let provider = txn_provider(txns);
        let output = self
            .inner
            .execute_block(
                &provider,
                state,
                BlockExecutorConfigFromOnchain::new_no_block_limit(),
                TransactionSliceMetadata::block(block.parent, block.id),
            )
            .map_err(|e| anyhow!("block execution failed: {e:?}"))?;
        Ok(BlockOutput::into_transaction_outputs_forced(output))
    }
}

/// Checks signatures in parallel on the global rayon pool. This belongs at intake (design doc
/// 4.1 step 2), off the execution path: verifying inside execution would serialize it.
pub fn verify_signatures(txns: Vec<Transaction>) -> Vec<SignatureVerifiedTransaction> {
    txns.into_par_iter().map(Into::into).collect()
}

/// Wraps an ordered, signature-checked block for Block-STM.
fn txn_provider(
    txns: Vec<SignatureVerifiedTransaction>,
) -> DefaultTxnProvider<SignatureVerifiedTransaction, AuxiliaryInfo> {
    let aux = (0..txns.len() as u32)
        .map(AuxiliaryInfo::new_timestamp_not_yet_assigned)
        .collect();
    DefaultTxnProvider::new(txns, aux)
}
