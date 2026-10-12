//! Skipping signature checks at execution must never let an unverified transaction through:
//! only batches this validator verified itself (identical digest) skip the check.

use aptos_crypto::HashValue;
use aptos_types::{
    chain_id::ChainId,
    transaction::{SignedTransaction, Transaction},
};
use chain_execution::{ExecutorConfig, MoveVmBackend, StateStore};
use chain_genesis::{Genesis, GenesisAccount, LocalAccount};
use chain_mempool::{Mempool, MempoolConfig, VerifiedBatches};
use chain_node::pipeline::{ExecutionPipeline, PipelineEvent};
use chain_storage::ChainStore;
use chain_types::{Batch, Block, BlockHeader, QuorumCert, ValidatorSigner, payload_digest};
use std::{sync::{Arc, mpsc}, time::Duration};

const CHAIN: u8 = 42;

fn signed(t: Transaction) -> SignedTransaction {
    t.try_as_signed_user_txn().unwrap().clone()
}

#[test]
fn only_locally_verified_batches_skip_signature_checks() {
    let chain_id = ChainId::new(CHAIN);
    let mut accounts = LocalAccount::generate([5; 32], 6);
    let genesis = Genesis {
        chain_id,
        accounts: accounts
            .iter()
            .map(|a| GenesisAccount { address: a.address(), balance: 1_000_000_000 })
            .collect(),
    };
    let backend = MoveVmBackend::new(ExecutorConfig::new(2));
    let state = genesis.build(&backend).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut store = ChainStore::open(dir.path()).unwrap();
    let block = store.next_block(HashValue::sha3_256_of(b"genesis"));
    store.apply_block(block, &[state.snapshot_output().unwrap()]).unwrap();

    let to = accounts[0].address();
    let txns: Vec<SignedTransaction> = accounts.iter_mut().map(|a| signed(a.transfer(to, 1, chain_id))).collect();
    let forge = |payload: &SignedTransaction| {
        SignedTransaction::new_signed_transaction(payload.raw_transaction_ref().clone(), txns[0].authenticator())
    };

    // 1. Made by this validator's mempool: verified and recorded.
    let verified = VerifiedBatches::new(100);
    let mut pool = Mempool::new(0, MempoolConfig::new(chain_id)).with_verified(verified.clone());
    pool.submit(txns[0].clone()).unwrap();
    pool.submit(txns[1].clone()).unwrap();
    let own = pool.make_batch().unwrap();
    // 2. Never seen here, with a forged signature (as a Byzantine leader might include).
    let unseen = Batch { author: 3, batch_id: 1, txns: vec![txns[2].clone(), forge(&txns[3])] };
    // 3. A recorded batch altered afterwards: its digest no longer matches.
    let mut altered = Batch { author: 2, batch_id: 9, txns: vec![txns[4].clone()] };
    let mut peer_pool = Mempool::new(1, MempoolConfig::new(chain_id)).with_verified(verified.clone());
    peer_pool.add_batch(altered.clone(), true);
    altered.txns.push(forge(&txns[5]));

    let (events, rx) = mpsc::channel();
    let pipeline =
        ExecutionPipeline::start(store, Arc::new(backend), verified, events, |e| e).unwrap();
    let payload = vec![own, unseen, altered];
    let header = BlockHeader {
        height: 1,
        round: 1,
        payload_digest: payload_digest(&payload),
        ..Block::genesis(chain_id, 1)
    };
    let signer = ValidatorSigner::new(0, aptos_keygen::KeyGen::from_seed([0; 32]).generate_ed25519_private_key());
    let block = Block::new(header, QuorumCert::genesis(1, HashValue::zero()), None, payload, &signer).unwrap();
    pipeline.submit(&[block]).unwrap();

    loop {
        match rx.recv_timeout(Duration::from_secs(60)).unwrap() {
            PipelineEvent::Executed { txns, succeeded, signatures_checked, .. } => {
                assert_eq!(txns, 6);
                // Own batch (2) skipped; unseen (2) and altered (2) checked.
                assert_eq!(signatures_checked, 4);
                // Both forged transactions are discarded; the 4 genuine ones succeed.
                assert_eq!(succeeded, 4);
                break;
            },
            PipelineEvent::Failed(e) => panic!("{e}"),
            PipelineEvent::StateRoot { .. } => {},
        }
    }
}
