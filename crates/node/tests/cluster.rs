//! End to end in one process: 4 validators order, execute and commit signed transfers, and all
//! compute the same state roots.

use aptos_crypto::HashValue;
use aptos_types::chain_id::ChainId;
use chain_genesis::{GenesisAccount, LocalAccount};
use chain_node::{Cluster, ClusterConfig, NodeEvent};
use std::{
    collections::{BTreeMap, HashSet},
    time::{Duration, Instant},
};

const APT: u64 = 100_000_000;

#[test]
fn four_validators_agree_on_executed_state() {
    let mut accounts = LocalAccount::generate([21; 32], 40);
    let config = ClusterConfig::new(
        4,
        accounts
            .iter()
            .map(|a| GenesisAccount {
                address: a.address(),
                balance: 1_000 * APT,
            })
            .collect(),
    );
    let chain_id: ChainId = config.chain_id;
    let cluster = Cluster::start(config).unwrap();

    let mut roots: BTreeMap<u64, BTreeMap<u16, HashValue>> = BTreeMap::new();
    let mut succeeded = [0usize; 4];
    let mut committed = HashSet::new();
    let deadline = Instant::now() + Duration::from_secs(120);

    // Two waves: each account's second transfer is sent only after its first committed, so
    // sequence numbers always arrive in order.
    for wave in 0..2 {
        let mut wave_hashes = HashSet::new();
        let n = accounts.len();
        for (i, account) in accounts.iter_mut().enumerate() {
            let to = LocalAccount::generate([22; 32], n)[i].address();
            let txn = account.transfer(to, (wave + 1) * APT, chain_id);
            let txn = txn.try_as_signed_user_txn().unwrap().clone();
            wave_hashes.insert(txn.committed_hash());
            cluster.submit((i % 4) as u16, txn);
        }
        while !wave_hashes.is_subset(&committed) {
            assert!(Instant::now() < deadline, "wave {wave} did not commit in time");
            match cluster.events.recv_timeout(Duration::from_secs(1)) {
                Ok(NodeEvent::Committed { txn_hashes, .. }) => committed.extend(txn_hashes),
                Ok(NodeEvent::Executed { node, succeeded: s, .. }) => succeeded[node as usize] += s,
                Ok(NodeEvent::StateRoot { node, height, root }) => {
                    roots.entry(height).or_default().insert(node, root);
                },
                Ok(NodeEvent::Failed { node, error }) => panic!("validator {node} failed: {error}"),
                Ok(NodeEvent::Status { .. }) | Err(_) => {},
            }
        }
    }

    // Let every validator finish executing and hashing.
    while succeeded.iter().any(|&s| s < 80) {
        assert!(Instant::now() < deadline, "execution did not finish: {succeeded:?}");
        match cluster.events.recv_timeout(Duration::from_secs(1)) {
            Ok(NodeEvent::Executed { node, succeeded: s, .. }) => succeeded[node as usize] += s,
            Ok(NodeEvent::StateRoot { node, height, root }) => {
                roots.entry(height).or_default().insert(node, root);
            },
            Ok(NodeEvent::Failed { node, error }) => panic!("validator {node} failed: {error}"),
            _ => {},
        }
    }
    assert_eq!(succeeded, [80; 4], "every transfer succeeds on every validator");

    let mut compared = 0;
    for (height, by_node) in &roots {
        let distinct: HashSet<_> = by_node.values().collect();
        assert_eq!(distinct.len(), 1, "validators disagree on the root after block {height}");
        if by_node.len() == 4 {
            compared += 1;
        }
    }
    assert!(compared > 2, "compared roots at {compared} heights");
}
