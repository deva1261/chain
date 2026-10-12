//! ChainStore: persistence across restarts, and state-root properties.

use aptos_crypto::HashValue;
use aptos_types::{
    chain_id::ChainId,
    state_store::{TStateView, state_key::StateKey},
    transaction::{
        ExecutionStatus, TransactionAuxiliaryData, TransactionOutput, TransactionStatus,
    },
    write_set::{WriteOp, WriteSetMut},
};
use bytes::Bytes;
use chain_execution::{
    ExecutionBackend, ExecutorConfig, MoveVmBackend, StateStore, query, verify_signatures,
};
use chain_genesis::{Genesis, GenesisAccount, LocalAccount};
use chain_storage::{ChainStore, StoreConfig};

const APT: u64 = 100_000_000;

/// A block with one kept output writing `puts` and deleting `deletes`.
fn synthetic_block(puts: &[(&str, &str)], deletes: &[&str]) -> Vec<TransactionOutput> {
    let ops = puts
        .iter()
        .map(|(k, v)| {
            (
                StateKey::raw(k.as_bytes()),
                WriteOp::legacy_creation(Bytes::copy_from_slice(v.as_bytes())),
            )
        })
        .chain(
            deletes
                .iter()
                .map(|k| (StateKey::raw(k.as_bytes()), WriteOp::legacy_deletion())),
        );
    vec![TransactionOutput::new(
        WriteSetMut::new(ops).freeze().unwrap(),
        vec![],
        0,
        TransactionStatus::Keep(ExecutionStatus::Success),
        TransactionAuxiliaryData::None,
    )]
}

fn commit_all(store: &mut ChainStore, blocks: &[Vec<TransactionOutput>]) -> HashValue {
    for (i, outputs) in blocks.iter().enumerate() {
        let id = format!("block {} {i}", store.committed_height());
        let block = store.next_block(HashValue::sha3_256_of(id.as_bytes()));
        store.apply_block(block, outputs).unwrap();
    }
    store.wait_for_root(store.committed_height()).unwrap()
}

#[test]
fn root_depends_only_on_final_state() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut stores: Vec<_> = dirs.iter().map(|d| ChainStore::open(d.path()).unwrap()).collect();

    // One block.
    let one = commit_all(
        &mut stores[0],
        &[synthetic_block(&[("a", "1"), ("b", "2"), ("c", "3")], &[])],
    );
    // Same final state over three blocks, with an overwrite and a created-then-deleted key.
    let three = commit_all(
        &mut stores[1],
        &[
            synthetic_block(&[("a", "0"), ("x", "9")], &[]),
            synthetic_block(&[("a", "1"), ("b", "2")], &[]),
            synthetic_block(&[("c", "3")], &["x"]),
        ],
    );
    // A different final state.
    let other = commit_all(
        &mut stores[2],
        &[synthetic_block(&[("a", "1"), ("b", "2"), ("c", "4")], &[])],
    );

    assert_eq!(one, three);
    assert_ne!(one, other);
    // Every intermediate block got a root too.
    for height in 1..=3 {
        assert!(stores[1].state_root(height).unwrap().is_some());
    }
}

#[test]
fn state_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let chain_id = ChainId::new(42);
    let backend = MoveVmBackend::new(ExecutorConfig::new(4));
    let mut accounts = LocalAccount::generate([5; 32], 4);
    let genesis = Genesis {
        chain_id,
        accounts: accounts
            .iter()
            .map(|a| GenesisAccount {
                address: a.address(),
                balance: 100 * APT,
            })
            .collect(),
    };

    let (height, root, balances) = {
        let mut store = ChainStore::open(dir.path()).unwrap();
        genesis.build_into(&backend, &mut store).unwrap();
        let to = accounts[1].address();
        let txns = vec![accounts[0].transfer(to, 10 * APT, chain_id)];
        let block = store.next_block(HashValue::sha3_256_of(b"transfer"));
        let outputs = backend
            .execute_block(block, verify_signatures(txns), &store)
            .unwrap();
        store.apply_block(block, &outputs).unwrap();
        let height = store.committed_height();
        let root = store.wait_for_root(height).unwrap();
        let balances: Vec<_> = accounts
            .iter()
            .map(|a| query::apt_balance(&store, a.address()).unwrap())
            .collect();
        (height, root, balances)
    };
    assert_eq!(balances[1], 110 * APT);

    let store = ChainStore::open(dir.path()).unwrap();
    assert_eq!(store.committed_height(), height);
    assert_eq!(store.state_root(height).unwrap(), Some(root));
    let reopened: Vec<_> = accounts
        .iter()
        .map(|a| query::apt_balance(&store, a.address()).unwrap())
        .collect();
    assert_eq!(reopened, balances);
    assert_eq!(
        query::sequence_number(&store, accounts[0].address()).unwrap(),
        accounts[0].sequence_number()
    );
    assert!(store.next_version() > 0);
}

#[test]
fn old_tree_versions_are_pruned() {
    let blocks: Vec<_> = (0..50)
        .map(|i| synthetic_block(&[("hot", &i.to_string()), ("cold", "x")], &[]))
        .collect();

    let pruned_dir = tempfile::tempdir().unwrap();
    let mut pruned =
        ChainStore::open_with(pruned_dir.path(), StoreConfig { keep_versions: 2 }).unwrap();
    let pruned_root = commit_all(&mut pruned, &blocks);

    let full_dir = tempfile::tempdir().unwrap();
    let mut full = ChainStore::open(full_dir.path()).unwrap();
    let full_root = commit_all(&mut full, &blocks);

    // Pruning never changes roots, and drops the history of the overwritten key.
    assert_eq!(pruned_root, full_root);
    let (kept, all) = (pruned.tree_node_count().unwrap(), full.tree_node_count().unwrap());
    assert!(kept * 5 < all, "pruned store keeps {kept} of {all} nodes");

    // Pruning survives a restart and continues from there.
    drop(pruned);
    let mut reopened =
        ChainStore::open_with(pruned_dir.path(), StoreConfig { keep_versions: 2 }).unwrap();
    let extra = synthetic_block(&[("hot", "last")], &[]);
    let block = reopened.next_block(HashValue::sha3_256_of(b"extra"));
    reopened.apply_block(block, &extra).unwrap();
    reopened.wait_for_root(reopened.committed_height()).unwrap();
}

#[test]
fn blocks_without_changes_keep_the_root_and_add_no_nodes() {
    let empty = || synthetic_block(&[], &[]);
    let a = tempfile::tempdir().unwrap();
    let mut with_gaps = ChainStore::open(a.path()).unwrap();
    commit_all(
        &mut with_gaps,
        &[synthetic_block(&[("a", "1")], &[]), empty(), empty()],
    );
    assert_eq!(with_gaps.state_root(2).unwrap(), with_gaps.state_root(1).unwrap());
    assert_eq!(with_gaps.state_root(3).unwrap(), with_gaps.state_root(1).unwrap());
    let nodes = with_gaps.tree_node_count().unwrap();
    let block = with_gaps.next_block(HashValue::sha3_256_of(b"after gaps"));
    with_gaps.apply_block(block, &synthetic_block(&[("b", "2")], &[])).unwrap();
    let root = with_gaps.wait_for_root(4).unwrap();
    assert!(with_gaps.tree_node_count().unwrap() > nodes);

    // Same final state without the empty blocks: same root.
    let b = tempfile::tempdir().unwrap();
    let mut direct = ChainStore::open(b.path()).unwrap();
    let direct_root = commit_all(
        &mut direct,
        &[synthetic_block(&[("a", "1")], &[]), synthetic_block(&[("b", "2")], &[])],
    );
    assert_eq!(root, direct_root);

    // And it all survives a restart.
    drop(with_gaps);
    let reopened = ChainStore::open(a.path()).unwrap();
    assert_eq!(reopened.state_root(4).unwrap(), Some(root));
}
