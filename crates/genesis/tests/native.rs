//! The native backend against MoveVM: same genesis, same block of transfers. Fees differ by
//! design (flat native fee vs metered MoveVM gas), so balances must agree once each backend's
//! fees are added back.

use aptos_crypto::HashValue;
use aptos_types::{
    chain_id::ChainId,
    transaction::{ExecutionStatus, TransactionStatus},
};
use chain_execution::{
    ExecutionBackend, ExecutorConfig, InMemoryState, MoveVmBackend, NativeBackend, StateStore,
    native::NATIVE_GAS_UNITS, query, verify_signatures,
};
use chain_genesis::{GAS_UNIT_PRICE, Genesis, GenesisAccount, LocalAccount};

const APT: u64 = 100_000_000;

/// Runs `txns` on a copy of `state` and returns (balance + fees paid) per address.
fn balances_plus_fees(
    backend: &impl ExecutionBackend,
    state: &InMemoryState,
    txns: Vec<aptos_types::transaction::Transaction>,
    addresses: &[aptos_types::account_address::AccountAddress],
    native_fees: bool,
) -> Vec<u64> {
    let mut state = state.clone();
    let senders: Vec<_> = txns
        .iter()
        .map(|t| t.try_as_signed_user_txn().unwrap().sender())
        .collect();
    let block = state.next_block(HashValue::sha3_256_of(b"transfers"));
    let outputs = backend
        .execute_block(block, verify_signatures(txns), &state)
        .unwrap();
    let mut fees = vec![0; addresses.len()];
    for (sender, output) in senders.iter().zip(&outputs) {
        assert_eq!(
            *output.status(),
            TransactionStatus::Keep(ExecutionStatus::Success),
            "{}",
            backend.name()
        );
        let gas_units = if native_fees {
            NATIVE_GAS_UNITS
        } else {
            output.gas_used()
        };
        let i = addresses.iter().position(|a| a == sender).unwrap();
        fees[i] += gas_units * GAS_UNIT_PRICE;
    }
    state.apply_block(block, &outputs).unwrap();
    addresses
        .iter()
        .zip(fees)
        .map(|(a, fee)| query::apt_balance(&state, *a).unwrap() + fee)
        .collect()
}

#[test]
fn native_matches_move_vm_up_to_fees() {
    let chain_id = ChainId::new(42);
    let move_vm = MoveVmBackend::new(ExecutorConfig::new(4));
    let native = NativeBackend::new(4);
    let mut accounts = LocalAccount::generate([3; 32], 16);
    let fresh = LocalAccount::generate([4; 32], 1)[0].address();
    let genesis = Genesis {
        chain_id,
        accounts: accounts
            .iter()
            .map(|a| GenesisAccount {
                address: a.address(),
                balance: 1_000 * APT,
            })
            .collect(),
    };
    let state = genesis.build(&move_vm).unwrap();

    // Ring transfers, repeated senders in one block, a hot recipient and a new account.
    let mut addresses: Vec<_> = accounts.iter().map(|a| a.address()).collect();
    let n = accounts.len();
    let mut txns = Vec::new();
    for round in 0..3 {
        for i in 0..n {
            let to = if round == 2 { addresses[0] } else { addresses[(i + 1) % n] };
            txns.push(accounts[i].transfer(to, (i as u64 + 1) * APT, chain_id));
        }
    }
    txns.push(accounts[5].transfer(fresh, 7 * APT, chain_id));
    addresses.push(fresh);

    let expected = balances_plus_fees(&move_vm, &state, txns.clone(), &addresses, false);
    let actual = balances_plus_fees(&native, &state, txns, &addresses, true);
    assert_eq!(actual, expected);
}

#[test]
fn native_rejects_non_transfer_blocks() {
    let chain_id = ChainId::new(42);
    let native = NativeBackend::new(2);
    let state = InMemoryState::new();
    let mut minter = LocalAccount::core_resources();
    let mint = minter.sign(
        aptos_cached_packages::aptos_stdlib::aptos_coin_mint(minter.address(), 1),
        chain_id,
    );
    let block = state.next_block(HashValue::sha3_256_of(b"mint"));
    assert!(
        native
            .execute_block(block, verify_signatures(vec![mint]), &state)
            .is_err()
    );
}
