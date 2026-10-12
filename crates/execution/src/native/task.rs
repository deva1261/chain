//! Block-STM task that executes APT transfers natively, without MoveVM. Ported from
//! aptos-core `execution/executor-benchmark/src/native/native_vm.rs`, which stays the
//! reference; changes: no logging on speculative failures (they are routine in Block-STM),
//! struct tags and constructors inlined from `db_access.rs`, transactions parsed by
//! `NativeTransaction::parse` (callers guarantee every transaction parses).
//!
//! Semantics differ from MoveVM: a flat 4 gas units per transaction, no chain-id or expiry
//! checks. It is a throughput ceiling and fast-path prototype, not a drop-in replacement.

use super::transaction::{NativeTransaction, compute_deltas_for_batch};
use aptos_aggregator::{
    bounded_math::SignedU128,
    delayed_change::{DelayedApplyChange, DelayedChange},
    delta_change_set::DeltaWithMax,
};
use aptos_block_executor::task::{ExecutionStatus, ExecutorTask};
use aptos_mvhashmap::types::TxnIndex;
use aptos_types::{
    account_address::AccountAddress,
    account_config::{
        AccountResource, ConcurrentSupplyResource, DepositFAEvent, FungibleStoreResource,
        ObjectCoreResource, ObjectGroupResource, WithdrawFAEvent, primary_apt_store,
    },
    contract_event::ContractEvent,
    error::PanicError,
    event::{EventHandle, EventKey},
    fee_statement::FeeStatement,
    move_utils::move_event_v2::MoveEventV2Type,
    state_store::{StateView, state_key::StateKey, state_value::StateValueMetadata},
    transaction::{
        AuxiliaryInfo, TransactionStatus, signature_verified_transaction::SignatureVerifiedTransaction,
    },
    write_set::WriteOp,
};
use aptos_vm::block_executor::AptosTransactionOutput;
use aptos_vm_environment::environment::AptosEnvironment;
use aptos_vm_types::{
    abstract_write_op::{
        AbstractResourceWriteOp, GroupWrite, ResourceGroupInPlaceDelayedFieldChangeOp,
    },
    change_set::VMChangeSet,
    module_write_set::ModuleWriteSet,
    output::VMOutput,
    resolver::{ExecutorView, ResourceGroupView},
    resource_group_adapter::group_size_as_sum,
};
use bytes::Bytes;
use move_core_types::{
    language_storage::StructTag,
    move_resource::MoveStructType,
    value::{IdentifierMappingKind, MoveStructLayout, MoveTypeLayout},
};
use move_vm_types::delayed_values::delayed_field_id::DelayedFieldID;
use serde::{Serialize, de::DeserializeOwned};
use std::{collections::BTreeMap, fmt::Debug};

/// Struct tags the task reads and writes (from aptos-core `db_access.rs`).
pub(crate) struct CommonStructTags {
    pub account: StructTag,
    pub object_group: StructTag,
    pub object_core: StructTag,
    pub fungible_store: StructTag,
    pub concurrent_supply: StructTag,
}

pub(crate) struct DbAccessUtil {
    pub common: CommonStructTags,
}

impl DbAccessUtil {
    pub fn new() -> Self {
        Self {
            common: CommonStructTags {
                account: AccountResource::struct_tag(),
                object_group: ObjectGroupResource::struct_tag(),
                object_core: ObjectCoreResource::struct_tag(),
                fungible_store: FungibleStoreResource::struct_tag(),
                concurrent_supply: ConcurrentSupplyResource::struct_tag(),
            },
        }
    }

    pub fn new_state_key_account(&self, address: &AccountAddress) -> StateKey {
        StateKey::resource(address, &self.common.account).unwrap()
    }

    pub fn new_state_key_object_resource_group(&self, address: &AccountAddress) -> StateKey {
        StateKey::resource_group(address, &self.common.object_group)
    }

    pub fn new_account_resource(address: AccountAddress) -> AccountResource {
        AccountResource::new(
            0,
            address.to_vec(),
            EventHandle::new(EventKey::new(1, address), 0),
            EventHandle::new(EventKey::new(2, address), 0),
        )
    }

    pub fn new_object_core(address: AccountAddress, owner: AccountAddress) -> ObjectCoreResource {
        ObjectCoreResource::new(owner, false, EventHandle::new(EventKey::new(1, address), 0))
    }
}

pub(crate) struct NativeVMExecutorTask {
    db_util: DbAccessUtil,
}

impl ExecutorTask for NativeVMExecutorTask {
    type AuxiliaryInfo = AuxiliaryInfo;
    type Output = AptosTransactionOutput;
    type Txn = SignatureVerifiedTransaction;

    fn init(
        _env: &AptosEnvironment,
        _state_view: &impl StateView,
        _async_runtime_checks_enabled: bool,
    ) -> Self {
        Self {
            db_util: DbAccessUtil::new(),
        }
    }

    // This function is called by the BlockExecutor for each transaction it intends
    // to execute (via the ExecutorTask trait). It can be as a part of sequential
    // execution, or speculatively as a part of a parallel execution.
    fn execute_transaction(
        &self,
        executor_with_group_view: &(impl ExecutorView + ResourceGroupView),
        txn: &SignatureVerifiedTransaction,
        _auxiliary_info: &AuxiliaryInfo,
        _txn_idx: TxnIndex,
    ) -> Result<ExecutionStatus<AptosTransactionOutput>, PanicError> {
        Ok(
            match self.execute_transaction_impl(executor_with_group_view, txn) {
                Ok((change_set, gas_units)) => ExecutionStatus::Executed {
                    output: AptosTransactionOutput::new(VMOutput::new(
                        change_set,
                        ModuleWriteSet::empty(),
                        FeeStatement::builder()
                            .total_charge_gas_units(gas_units)
                            .execution_gas_units(gas_units)
                            .io_gas_units(0)
                            .storage_fee_octas(0)
                            .storage_fee_refund_octas(0)
                            .build(),
                        TransactionStatus::Keep(aptos_types::transaction::ExecutionStatus::Success),
                    )),
                    skips_rest: false,
                },
                Err(_) => ExecutionStatus::SpeculativeFailure,
            },
        )
    }
}

impl NativeVMExecutorTask {
    fn execute_transaction_impl(
        &self,
        view: &(impl ExecutorView + ResourceGroupView),
        txn: &SignatureVerifiedTransaction,
    ) -> Result<(VMChangeSet, u64), ()> {
        let parsed = NativeTransaction::parse(txn).ok_or(())?;
        // System transactions change nothing and pay nothing. (Upstream charged the flat fee
        // before looking at the transaction type, burning supply on block epilogues.)
        if matches!(
            parsed,
            NativeTransaction::BlockEpilogue
                | NativeTransaction::BlockMetadata
                | NativeTransaction::StateCheckpoint
        ) {
            return Ok((VMChangeSet::empty(), 0));
        }
        // A transfer to oneself only pays the fee. Deposits read the pre-transaction balance, so
        // withdrawing and then depositing into the same store would drop the withdrawal and
        // mint the amount plus fee (upstream bug; batch transfers already net self-transfers).
        let parsed = match parsed {
            NativeTransaction::Transfer {
                sender,
                sequence_number,
                recipient,
                ..
            }
            | NativeTransaction::FaTransfer {
                sender,
                sequence_number,
                recipient,
                ..
            } if sender == recipient => NativeTransaction::Nop {
                sender,
                sequence_number,
            },
            other => other,
        };
        let gas_units = super::NATIVE_GAS_UNITS;
        let gas = gas_units * 100;

        let mut resource_write_set = BTreeMap::new();
        let mut events = Vec::new();
        let mut delayed_field_change_set = BTreeMap::new();

        self.reduce_apt_supply(
            gas,
            view,
            &mut resource_write_set,
            &mut delayed_field_change_set,
        )
        .unwrap();

        match parsed {
            NativeTransaction::Nop {
                sender,
                sequence_number,
            } => {
                self.check_and_set_sequence_number(
                    sender,
                    sequence_number,
                    view,
                    &mut resource_write_set,
                )?;
                self.withdraw_apt(sender, 0, view, gas, &mut resource_write_set, &mut events)?;
            },
            NativeTransaction::FaTransfer {
                sender,
                sequence_number,
                recipient,
                amount,
            } => {
                self.check_and_set_sequence_number(
                    sender,
                    sequence_number,
                    view,
                    &mut resource_write_set,
                )?;
                self.withdraw_fa_apt_from_signer(
                    sender,
                    amount,
                    view,
                    gas,
                    &mut resource_write_set,
                    &mut events,
                )?;
                if amount > 0 {
                    self.deposit_fa_apt(
                        recipient,
                        amount,
                        view,
                        &mut resource_write_set,
                        &mut events,
                    )?;
                }
            },
            NativeTransaction::Transfer {
                sender,
                sequence_number,
                recipient,
                amount,
                fail_on_recipient_account_existing: fail_on_account_existing,
                fail_on_recipient_account_missing: fail_on_account_missing,
            } => {
                self.check_and_set_sequence_number(
                    sender,
                    sequence_number,
                    view,
                    &mut resource_write_set,
                )?;

                self.withdraw_apt(
                    sender,
                    amount,
                    view,
                    gas,
                    &mut resource_write_set,
                    &mut events,
                )?;

                let exists = self.deposit_apt(
                    recipient,
                    amount,
                    view,
                    &mut resource_write_set,
                    &mut events,
                )?;

                if !exists || fail_on_account_existing {
                    self.check_or_create_account(
                        recipient,
                        fail_on_account_existing,
                        fail_on_account_missing,
                        false,
                        view,
                        &mut resource_write_set,
                    )?;
                }
            },
            NativeTransaction::BatchTransfer {
                sender,
                sequence_number,
                recipients,
                amounts,
                fail_on_recipient_account_existing,
                fail_on_recipient_account_missing,
            } => {
                self.check_and_set_sequence_number(
                    sender,
                    sequence_number,
                    view,
                    &mut resource_write_set,
                )?;

                let (deltas, amount_to_sender) =
                    compute_deltas_for_batch(recipients, amounts, sender);

                self.withdraw_apt(
                    sender,
                    amount_to_sender,
                    view,
                    gas,
                    &mut resource_write_set,
                    &mut events,
                )?;

                for (recipient_address, transfer_amount) in deltas.into_iter() {
                    let existed = self.deposit_apt(
                        recipient_address,
                        transfer_amount as u64,
                        view,
                        &mut resource_write_set,
                        &mut events,
                    )?;

                    if !existed || fail_on_recipient_account_existing {
                        self.check_or_create_account(
                            recipient_address,
                            fail_on_recipient_account_existing,
                            fail_on_recipient_account_missing,
                            false,
                            view,
                            &mut resource_write_set,
                        )?;
                    }
                }
            },
            NativeTransaction::BlockEpilogue
            | NativeTransaction::BlockMetadata
            | NativeTransaction::StateCheckpoint => unreachable!("handled above"),
        };

        events.push((
            FeeStatement::builder()
                .total_charge_gas_units(gas_units)
                .execution_gas_units(gas_units)
                .io_gas_units(0)
                .storage_fee_octas(0)
                .storage_fee_refund_octas(0)
                .build()
                .create_event_v2()
                .expect("Creating FeeStatement should always succeed"),
            None,
        ));

        Ok((
            VMChangeSet::new(resource_write_set, events, delayed_field_change_set),
            gas_units,
        ))
    }

    pub fn get_value<T: DeserializeOwned>(
        state_key: &StateKey,
        view: &(impl ExecutorView + ResourceGroupView),
    ) -> Result<Option<(T, StateValueMetadata)>, ()> {
        view.get_resource_state_value(state_key, None)
            .map_err(hide_error)?
            .map(|value| {
                bcs::from_bytes::<T>(value.bytes()).map(|bytes| (bytes, value.into_metadata()))
            })
            .transpose()
            .map_err(hide_error)
    }

    pub fn get_value_from_group<T: DeserializeOwned>(
        group_key: &StateKey,
        resource_tag: &StructTag,
        view: &(impl ExecutorView + ResourceGroupView),
    ) -> Result<Option<T>, ()> {
        Self::get_value_from_group_with_layout(group_key, resource_tag, view, None)
    }

    pub fn get_value_from_group_with_layout<T: DeserializeOwned>(
        group_key: &StateKey,
        resource_tag: &StructTag,
        view: &(impl ExecutorView + ResourceGroupView),
        maybe_layout: Option<&MoveTypeLayout>,
    ) -> Result<Option<T>, ()> {
        view.get_resource_from_group(group_key, resource_tag, maybe_layout)
            .map_err(hide_error)?
            .map(|value| bcs::from_bytes::<T>(&value))
            .transpose()
            .map_err(hide_error)
    }

    fn check_and_set_sequence_number(
        &self,
        sender_address: AccountAddress,
        sequence_number: u64,
        view: &(impl ExecutorView + ResourceGroupView),
        resource_write_set: &mut BTreeMap<StateKey, AbstractResourceWriteOp>,
    ) -> Result<(), ()> {
        let sender_account_key = self.db_util.new_state_key_account(&sender_address);

        let value = Self::get_value::<AccountResource>(&sender_account_key, view)?;

        match value {
            Some((mut account, metadata)) => {
                if sequence_number == account.sequence_number {
                    account.sequence_number += 1;
                    resource_write_set.insert(
                        sender_account_key,
                        AbstractResourceWriteOp::Write(
                            WriteOp::modification(
                                Bytes::from(bcs::to_bytes(&account).map_err(hide_error)?),
                                metadata,
                            ),
                            false,
                        ),
                    );
                    Ok(())
                } else {
                    Err(())
                }
            },
            None => {
                let mut account = DbAccessUtil::new_account_resource(sender_address);
                if sequence_number == 0 {
                    account.sequence_number = 1;
                    resource_write_set.insert(
                        sender_account_key,
                        AbstractResourceWriteOp::Write(
                            WriteOp::legacy_creation(Bytes::from(
                                bcs::to_bytes(&account).map_err(hide_error)?,
                            )),
                            false,
                        ),
                    );
                    Ok(())
                } else {
                    Err(())
                }
            },
        }
    }

    fn check_or_create_account(
        &self,
        address: AccountAddress,
        fail_on_account_existing: bool,
        fail_on_account_missing: bool,
        create_account_resource: bool,
        view: &(impl ExecutorView + ResourceGroupView),
        resource_write_set: &mut BTreeMap<StateKey, AbstractResourceWriteOp>,
    ) -> Result<(), ()> {
        let account_key = self.db_util.new_state_key_account(&address);

        let value = Self::get_value::<AccountResource>(&account_key, view)?;
        match value {
            Some(_) => {
                if fail_on_account_existing {
                    return Err(());
                }
            },
            None => {
                if fail_on_account_missing {
                    return Err(());
                } else if create_account_resource {
                    let account = DbAccessUtil::new_account_resource(address);

                    resource_write_set.insert(
                        account_key,
                        AbstractResourceWriteOp::Write(
                            WriteOp::legacy_creation(Bytes::from(
                                bcs::to_bytes(&account).map_err(hide_error)?,
                            )),
                            false,
                        ),
                    );
                }
            },
        }

        Ok(())
    }

    fn reduce_apt_supply(
        &self,
        gas: u64,
        view: &(impl ExecutorView + ResourceGroupView),
        resource_write_set: &mut BTreeMap<StateKey, AbstractResourceWriteOp>,
        delayed_field_change_set: &mut BTreeMap<DelayedFieldID, DelayedChange<DelayedFieldID>>,
    ) -> Result<(), ()> {
        self.reduce_fa_apt_supply(gas, view, resource_write_set, delayed_field_change_set)
    }

    fn reduce_fa_apt_supply(
        &self,
        gas: u64,
        view: &(impl ExecutorView + ResourceGroupView),
        resource_write_set: &mut BTreeMap<StateKey, AbstractResourceWriteOp>,
        delayed_field_change_set: &mut BTreeMap<DelayedFieldID, DelayedChange<DelayedFieldID>>,
    ) -> Result<(), ()> {
        let apt_metadata_object_state_key = self
            .db_util
            .new_state_key_object_resource_group(&AccountAddress::TEN);

        let concurrent_supply_rg_tag = &self.db_util.common.concurrent_supply;

        let concurrent_supply_layout = MoveTypeLayout::new_struct(MoveStructLayout::new(vec![
            MoveTypeLayout::Native(
                IdentifierMappingKind::Aggregator,
                Box::new(MoveTypeLayout::U128),
            ),
            MoveTypeLayout::U128,
        ]));

        let supply = Self::get_value_from_group_with_layout::<ConcurrentSupplyResource>(
            &apt_metadata_object_state_key,
            concurrent_supply_rg_tag,
            view,
            Some(&concurrent_supply_layout),
        )?
        .unwrap();

        let delayed_id = DelayedFieldID::from(*supply.current.get() as u64);
        view.validate_delayed_field_id(&delayed_id).unwrap();
        delayed_field_change_set.insert(
            delayed_id,
            DelayedChange::Apply(DelayedApplyChange::AggregatorDelta {
                delta: DeltaWithMax::new(SignedU128::Negative(gas as u128), u128::MAX),
            }),
        );
        let materialized_size = view
            .get_resource_state_value_size(&apt_metadata_object_state_key)
            .map_err(hide_error)?;
        let metadata = view
            .get_resource_state_value_metadata(&apt_metadata_object_state_key)
            .map_err(hide_error)?
            .unwrap();
        resource_write_set.insert(
            apt_metadata_object_state_key,
            AbstractResourceWriteOp::ResourceGroupInPlaceDelayedFieldChange(
                ResourceGroupInPlaceDelayedFieldChangeOp {
                    materialized_size,
                    metadata,
                },
            ),
        );
        Ok(())
    }

    fn withdraw_apt(
        &self,
        sender: AccountAddress,
        amount: u64,
        view: &(impl ExecutorView + ResourceGroupView),
        gas: u64,
        resource_write_set: &mut BTreeMap<StateKey, AbstractResourceWriteOp>,
        events: &mut Vec<(ContractEvent, Option<MoveTypeLayout>)>,
    ) -> Result<(), ()> {
        self.withdraw_fa_apt_from_signer(sender, amount, view, gas, resource_write_set, events)
    }

    fn withdraw_fa_apt_from_signer(
        &self,
        sender_address: AccountAddress,
        transfer_amount: u64,
        view: &(impl ExecutorView + ResourceGroupView),
        gas: u64,
        resource_write_set: &mut BTreeMap<StateKey, AbstractResourceWriteOp>,
        events: &mut Vec<(ContractEvent, Option<MoveTypeLayout>)>,
    ) -> Result<(), ()> {
        let sender_store_address = primary_apt_store(sender_address);
        let sender_fa_store_object_key = self
            .db_util
            .new_state_key_object_resource_group(&sender_store_address);
        let fungible_store_rg_tag = &self.db_util.common.fungible_store;

        match Self::get_value_from_group::<FungibleStoreResource>(
            &sender_fa_store_object_key,
            fungible_store_rg_tag,
            view,
        )? {
            Some(mut fa_store) => {
                if fa_store.balance >= transfer_amount + gas {
                    fa_store.balance -= transfer_amount + gas;
                    let fa_store_write = Self::create_single_resource_in_group_modification(
                        &fa_store,
                        &sender_fa_store_object_key,
                        fungible_store_rg_tag.clone(),
                        view,
                    )?;
                    resource_write_set.insert(sender_fa_store_object_key, fa_store_write);

                    if transfer_amount > 0 {
                        events.push((
                            WithdrawFAEvent {
                                store: sender_store_address,
                                amount: transfer_amount,
                            }
                            .create_event_v2()
                            .expect("Creating WithdrawFAEvent should always succeed"),
                            None,
                        ));
                    }
                    Ok(())
                } else {
                    Err(())
                }
            },
            None => Err(()),
        }
    }

    /// Returns bool whether FungibleStore existed.
    fn deposit_apt(
        &self,
        recipient_address: AccountAddress,
        transfer_amount: u64,
        view: &(impl ExecutorView + ResourceGroupView),
        resource_write_set: &mut BTreeMap<StateKey, AbstractResourceWriteOp>,
        events: &mut Vec<(ContractEvent, Option<MoveTypeLayout>)>,
    ) -> Result<bool, ()> {
        self.deposit_fa_apt(
            recipient_address,
            transfer_amount,
            view,
            resource_write_set,
            events,
        )
    }

    /// Returns bool whether FungibleStore existed.
    fn deposit_fa_apt(
        &self,
        recipient_address: AccountAddress,
        transfer_amount: u64,
        view: &(impl ExecutorView + ResourceGroupView),
        resource_write_set: &mut BTreeMap<StateKey, AbstractResourceWriteOp>,
        events: &mut Vec<(ContractEvent, Option<MoveTypeLayout>)>,
    ) -> Result<bool, ()> {
        let recipient_store_address = primary_apt_store(recipient_address);
        let recipient_fa_store_object_key = self
            .db_util
            .new_state_key_object_resource_group(&recipient_store_address);
        let fungible_store_rg_tag = &self.db_util.common.fungible_store;

        let (mut fa_store, rest_to_create, existed) =
            match Self::get_value_from_group::<FungibleStoreResource>(
                &recipient_fa_store_object_key,
                fungible_store_rg_tag,
                view,
            )? {
                Some(fa_store) => (fa_store, None, true),
                None => (
                    FungibleStoreResource::new_apt(0, false),
                    Some(BTreeMap::from([(
                        self.db_util.common.object_core.clone(),
                        bcs::to_bytes(&DbAccessUtil::new_object_core(
                            recipient_store_address,
                            recipient_address,
                        ))
                        .map_err(hide_error)?,
                    )])),
                    false,
                ),
            };

        fa_store.balance += transfer_amount;

        let fa_store_write = if existed {
            Self::create_single_resource_in_group_modification(
                &fa_store,
                &recipient_fa_store_object_key,
                fungible_store_rg_tag.clone(),
                view,
            )?
        } else {
            let mut rg = rest_to_create.unwrap();
            rg.insert(
                fungible_store_rg_tag.clone(),
                bcs::to_bytes(&fa_store).map_err(hide_error)?,
            );
            Self::create_resource_in_group_creation(&recipient_fa_store_object_key, rg, view)?
        };
        resource_write_set.insert(recipient_fa_store_object_key, fa_store_write);

        if transfer_amount > 0 {
            let event = DepositFAEvent {
                store: recipient_store_address,
                amount: transfer_amount,
            };
            events.push((
                event
                    .create_event_v2()
                    .expect("Creating DepositFAEvent should always succeed"),
                None,
            ));
        }
        Ok(existed)
    }

    fn create_single_resource_in_group_modification<T: Serialize>(
        value: &T,
        group_key: &StateKey,
        resource_tag: StructTag,
        view: &(impl ExecutorView + ResourceGroupView),
    ) -> Result<AbstractResourceWriteOp, ()> {
        let metadata = view
            .get_resource_state_value_metadata(group_key)
            .map_err(hide_error)?
            .unwrap();
        let size = view.resource_group_size(group_key).map_err(hide_error)?;
        let value_bytes = Bytes::from(bcs::to_bytes(value).map_err(hide_error)?);
        let group_write = AbstractResourceWriteOp::WriteResourceGroup(GroupWrite::new(
            WriteOp::modification(Bytes::new(), metadata),
            BTreeMap::from([(
                resource_tag,
                (WriteOp::legacy_modification(value_bytes), None),
            )]),
            size,
            size.get(),
        ));
        Ok(group_write)
    }

    fn create_resource_in_group_creation(
        group_key: &StateKey,
        resources: BTreeMap<StructTag, Vec<u8>>,
        view: &(impl ExecutorView + ResourceGroupView),
    ) -> Result<AbstractResourceWriteOp, ()> {
        let size = view.resource_group_size(group_key).map_err(hide_error)?;
        assert_eq!(size.get(), 0);
        let inner_ops = resources
            .into_iter()
            .map(|(resource_tag, value)| -> Result<_, ()> {
                Ok((
                    resource_tag,
                    (WriteOp::legacy_creation(Bytes::from(value)), None),
                ))
            })
            .collect::<Result<BTreeMap<_, _>, ()>>()?;

        let new_size = group_size_as_sum(
            inner_ops
                .iter()
                .map(|(resource_tag, (value, _layout))| (resource_tag, value.bytes_size())),
        )
        .map_err(hide_error)?;

        let group_write = AbstractResourceWriteOp::WriteResourceGroup(GroupWrite::new(
            WriteOp::legacy_creation(Bytes::new()),
            inner_ops,
            new_size,
            size.get(),
        ));
        Ok(group_write)
    }
}

/// Speculative reads fail routinely under Block-STM and are retried, so errors are dropped.
fn hide_error<E: Debug>(_e: E) {}
