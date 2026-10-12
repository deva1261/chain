//! End to end through the execution layer: genesis, then a block of signed Aptos transfers
//! run by MoveVM on Block-STM, checked balance by balance.

use aptos_crypto::HashValue;
use aptos_types::{
    chain_id::ChainId,
    transaction::{ExecutionStatus, TransactionStatus},
};
use chain_execution::{
    ExecutionBackend, ExecutorConfig, MoveVmBackend, StateStore, query, verify_signatures,
};
use chain_genesis::{GAS_UNIT_PRICE, Genesis, GenesisAccount, LocalAccount};

const APT: u64 = 100_000_000;
const CHAIN_ID: u8 = 42;

#[test]
fn genesis_then_parallel_transfers() {
    let chain_id = ChainId::new(CHAIN_ID);
    let backend = MoveVmBackend::new(ExecutorConfig::new(4));
    let mut accounts = LocalAccount::generate([7; 32], 8);
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
    let mut state = genesis.build(&backend).unwrap();
    for a in &accounts {
        assert_eq!(query::apt_balance(&state, a.address()).unwrap(), 1_000 * APT);
    }

    // Ring of transfers: i -> i+1, plus one brand-new recipient and one overspend.
    let n = accounts.len();
    let addresses: Vec<_> = accounts.iter().map(|a| a.address()).collect();
    let fresh = LocalAccount::generate([9; 32], 1).remove(0).address();
    let mut txns: Vec<_> = (0..n)
        .map(|i| accounts[i].transfer(addresses[(i + 1) % n], 10 * APT, chain_id))
        .collect();
    txns.push(accounts[0].transfer(fresh, 5 * APT, chain_id));
    txns.push(accounts[1].transfer(addresses[2], 1_000_000 * APT, chain_id));

    let block = state.next_block(HashValue::sha3_256_of(b"block 1"));
    let outputs = backend
        .execute_block(block, verify_signatures(txns), &state)
        .unwrap();
    // One output per transaction plus Aptos' block epilogue.
    assert_eq!(outputs.len(), n + 3);
    for output in &outputs[..n + 1] {
        assert_eq!(
            *output.status(),
            TransactionStatus::Keep(ExecutionStatus::Success)
        );
    }
    // The overspend is ordered, fails during execution, and is still kept and charged (D3).
    let overspend = &outputs[n + 1];
    assert!(matches!(
        overspend.status(),
        TransactionStatus::Keep(ExecutionStatus::MoveAbort { .. })
    ));
    assert!(overspend.gas_used() > 0);
    assert_eq!(state.apply_block(block, &outputs).unwrap(), n + 3);

    // Expected balances: every account sent 10 and received 10; account 0 also sent 5;
    // every account paid gas for the transactions it signed.
    let mut gas = vec![0; n];
    for (i, output) in outputs[..n].iter().enumerate() {
        gas[i] += output.gas_used() * GAS_UNIT_PRICE;
    }
    gas[0] += outputs[n].gas_used() * GAS_UNIT_PRICE;
    gas[1] += overspend.gas_used() * GAS_UNIT_PRICE;
    for i in 0..n {
        let extra_sent = if i == 0 { 5 * APT } else { 0 };
        assert_eq!(
            query::apt_balance(&state, addresses[i]).unwrap(),
            1_000 * APT - extra_sent - gas[i],
            "balance of account {i}"
        );
        assert_eq!(
            query::sequence_number(&state, addresses[i]).unwrap(),
            accounts[i].sequence_number()
        );
    }
    assert_eq!(query::apt_balance(&state, fresh).unwrap(), 5 * APT);
}
