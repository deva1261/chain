use aptos_crypto::HashValue;
use aptos_types::{
    chain_id::ChainId,
    transaction::{SignedTransaction, Transaction},
};
use chain_genesis::LocalAccount;
use chain_mempool::{Mempool, MempoolConfig};
use chain_types::{Batch, Block, BlockHeader, QuorumCert, ValidatorSigner, payload_digest};

const CHAIN: u8 = 42;

fn signed(t: Transaction) -> SignedTransaction {
    t.try_as_signed_user_txn().unwrap().clone()
}

fn txns(seed: u8, count: usize) -> Vec<SignedTransaction> {
    let mut accounts = LocalAccount::generate([seed; 32], count);
    let to = accounts[0].address();
    accounts
        .iter_mut()
        .map(|a| signed(a.transfer(to, 1, ChainId::new(CHAIN))))
        .collect()
}

fn pool(max_batch: usize, max_block: usize) -> Mempool {
    let mut config = MempoolConfig::new(ChainId::new(CHAIN));
    config.max_batch_txns = max_batch;
    config.max_block_txns = max_block;
    config.reproposal_rounds = 3;
    Mempool::new(0, config)
}

/// A block carrying `payload`; the mempool only looks at rounds and batches.
fn block(round: u64, payload: Vec<Batch>) -> Block {
    let header = BlockHeader {
        payload_digest: payload_digest(&payload),
        round,
        ..Block::genesis(ChainId::new(CHAIN), 1)
    };
    let signer = ValidatorSigner::new(0, aptos_keygen::KeyGen::from_seed([0; 32]).generate_ed25519_private_key());
    Block::new(header, QuorumCert::genesis(1, HashValue::zero()), None, payload, &signer).unwrap()
}

#[test]
fn intake_rejects_wrong_chain_and_duplicates() {
    let mut pool = pool(10, 100);
    let t = txns(1, 1).remove(0);
    pool.submit(t.clone()).unwrap();
    assert!(pool.submit(t).is_err());
    let mut other = LocalAccount::generate([2; 32], 1).remove(0);
    let wrong = signed(other.transfer(other.address(), 1, ChainId::new(7)));
    assert!(pool.submit(wrong).is_err());
    assert_eq!(pool.len(), 1);
}

#[test]
fn batches_drop_bad_signatures() {
    let mut pool = pool(10, 100);
    let good = txns(3, 3);
    // Valid authenticator from one transaction attached to another's payload.
    let forged = SignedTransaction::new_signed_transaction(
        good[1].raw_transaction_ref().clone(),
        good[0].authenticator(),
    );
    pool.submit(good[0].clone()).unwrap();
    pool.submit(forged).unwrap();
    let batch = pool.make_batch().unwrap();
    assert_eq!(batch.txns, vec![good[0].clone()]);
    assert_eq!(pool.stats().bad_signatures, 1);

    // A peer's batch with a forged transaction is dropped whole.
    let mut peer_batch = batch.clone();
    peer_batch.txns.push(SignedTransaction::new_signed_transaction(
        good[2].raw_transaction_ref().clone(),
        good[0].authenticator(),
    ));
    let mut leader = self::pool(10, 100);
    leader.add_batch(peer_batch, true);
    assert!(leader.pull(1).is_empty());
}

#[test]
fn pull_respects_block_limit_and_order() {
    let mut pool = pool(4, 8);
    for t in txns(4, 20) {
        pool.submit(t).unwrap();
    }
    while let Some(batch) = pool.make_batch() {
        pool.add_batch(batch, false);
    }
    let first = pool.pull(1);
    assert_eq!(first.iter().map(|b| b.txns.len()).sum::<usize>(), 8);
    assert_eq!(first[0].batch_id, 1);
    let second = pool.pull(2);
    assert_eq!(second[0].batch_id, 3);
}

#[test]
fn batches_in_someone_elses_block_are_dropped() {
    let mut a = pool(10, 100);
    let mut b = pool(10, 100);
    for t in txns(5, 5) {
        a.submit(t).unwrap();
    }
    let batch = a.make_batch().unwrap();
    a.add_batch(batch.clone(), false);
    b.add_batch(batch.clone(), true);
    // b proposes it first; a sees b's block and must not propose it again.
    let proposed = b.pull(1);
    a.on_block(&block(1, proposed));
    assert!(a.pull(2).is_empty());
}

#[test]
fn uncommitted_batches_are_proposed_again_committed_ones_never() {
    let mut pool = pool(10, 100);
    for t in txns(6, 4) {
        pool.submit(t).unwrap();
    }
    let batch = pool.make_batch().unwrap();
    pool.add_batch(batch, false);
    let proposed = pool.pull(1);
    pool.on_block(&block(1, proposed.clone()));
    assert!(pool.pull(2).is_empty(), "still in flight");
    // The block never committed; after the re-proposal window the batch comes back.
    assert_eq!(pool.pull(5), proposed);
    pool.on_commit(&[block(5, proposed)]);
    assert!(pool.pull(20).is_empty());
}

#[test]
fn capacity_counts_transactions_not_batches() {
    let mut config = MempoolConfig::new(ChainId::new(CHAIN));
    config.max_pool_txns = 10;
    let mut pool = Mempool::new(0, config);
    // Many tiny batches from peers must not make the pool look full.
    for (i, t) in txns(7, 8).into_iter().enumerate() {
        pool.add_batch(Batch { author: 1, batch_id: i as u64, txns: vec![t] }, false);
    }
    for t in txns(8, 2) {
        pool.submit(t).unwrap();
    }
    assert_eq!(pool.len(), 10);
    assert!(pool.submit(txns(9, 1).remove(0)).is_err(), "now it is full");
}
