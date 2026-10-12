//! Verification rules for votes, certificates and blocks.

use aptos_crypto::{HashValue, PrivateKey, ed25519::Ed25519PrivateKey};
use aptos_keygen::KeyGen;
use aptos_types::chain_id::ChainId;
use chain_types::{
    Block, BlockHeader, Committee, QuorumCert, Timeout, TimeoutCert, ValidatorInfo,
    ValidatorSigner, Vote, VoteData, payload_digest,
};

struct Net {
    committee: Committee,
    signers: Vec<ValidatorSigner>,
    root: HashValue,
}

/// `stakes.len()` validators with the given stakes, epoch 1.
fn net(stakes: &[u64]) -> Net {
    let mut keygen = KeyGen::from_seed([1; 32]);
    let keys: Vec<Ed25519PrivateKey> = stakes
        .iter()
        .map(|_| keygen.generate_ed25519_private_key())
        .collect();
    let committee = Committee::new(
        1,
        keys.iter()
            .zip(stakes)
            .map(|(k, s)| ValidatorInfo {
                public_key: k.public_key(),
                network_address: "127.0.0.1:0".into(),
                stake: *s,
            })
            .collect(),
    )
    .unwrap();
    let mut signers: Vec<_> = keys
        .into_iter()
        .map(|k| ValidatorSigner::for_committee(k, &committee).unwrap())
        .collect();
    signers.sort_by_key(|s| s.index());
    let root = Block::genesis(ChainId::new(42), 1).hash_value();
    Net {
        committee,
        signers,
        root,
    }
}

trait HashOf {
    fn hash_value(&self) -> HashValue;
}
impl HashOf for BlockHeader {
    fn hash_value(&self) -> HashValue {
        aptos_crypto::hash::CryptoHash::hash(self)
    }
}

fn qc(net: &Net, round: u64, block_hash: HashValue, signers: &[usize]) -> QuorumCert {
    let data = VoteData {
        epoch: 1,
        round,
        block_hash,
        parent_round: round - 1,
        parent_hash: HashValue::zero(),
    };
    QuorumCert {
        signatures: signers
            .iter()
            .map(|&i| {
                let v = Vote::new(data.clone(), &net.signers[i]).unwrap();
                (v.voter, v.signature)
            })
            .collect(),
        data,
    }
}

fn header(net: &Net, round: u64, parent: HashValue, proposer: usize) -> BlockHeader {
    BlockHeader {
        chain_id: ChainId::new(42),
        epoch: 1,
        round,
        height: round,
        parent_hash: parent,
        proposer: net.signers[proposer].index(),
        timestamp_ms: 1,
        payload_digest: payload_digest(&[]),
        executed_height: 0,
        executed_state_root: HashValue::zero(),
    }
}

#[test]
fn quorum_is_more_than_two_thirds_of_stake() {
    assert_eq!(net(&[1, 1, 1, 1]).committee.quorum_stake(), 3);
    assert_eq!(net(&[1, 1, 1, 1, 1, 1]).committee.quorum_stake(), 5);
    assert_eq!(net(&[1, 1, 1]).committee.quorum_stake(), 3);
    // One heavy validator plus any other is a quorum; three light ones are not.
    let weighted = net(&[7, 1, 1, 1]);
    assert_eq!(weighted.committee.quorum_stake(), 7);
}

#[test]
fn votes_verify_only_for_their_signer() {
    let net = net(&[1, 1, 1, 1]);
    let data = VoteData {
        epoch: 1,
        round: 3,
        block_hash: HashValue::random(),
        parent_round: 2,
        parent_hash: HashValue::random(),
    };
    let mut vote = Vote::new(data, &net.signers[0]).unwrap();
    vote.verify(&net.committee).unwrap();
    vote.voter = net.signers[1].index();
    assert!(vote.verify(&net.committee).is_err());
}

#[test]
fn quorum_certificates() {
    let net = net(&[1, 1, 1, 1]);
    let block = HashValue::random();
    qc(&net, 5, block, &[0, 1, 2]).verify(&net.committee, net.root).unwrap();
    qc(&net, 5, block, &[0, 1, 2, 3]).verify(&net.committee, net.root).unwrap();
    // Too little stake.
    assert!(qc(&net, 5, block, &[0, 1]).verify(&net.committee, net.root).is_err());
    // A signer counted twice.
    assert!(qc(&net, 5, block, &[0, 0, 1]).verify(&net.committee, net.root).is_err());
    // Signatures over a different block.
    let mut forged = qc(&net, 5, block, &[0, 1, 2]);
    forged.data.block_hash = HashValue::random();
    assert!(forged.verify(&net.committee, net.root).is_err());
    // Genesis QC only for the root block.
    QuorumCert::genesis(1, net.root).verify(&net.committee, net.root).unwrap();
    assert!(
        QuorumCert::genesis(1, block)
            .verify(&net.committee, net.root)
            .is_err()
    );
}

#[test]
fn timeout_certificates() {
    let net = net(&[1, 1, 1, 1]);
    let high = qc(&net, 4, HashValue::random(), &[0, 1, 2]);
    let genesis = QuorumCert::genesis(1, net.root);
    let timeouts: Vec<_> = [(0, &high), (1, &genesis), (2, &high)]
        .into_iter()
        .map(|(i, q)| Timeout::new(1, 6, q.clone(), &net.signers[i]).unwrap())
        .collect();
    for t in &timeouts {
        t.verify(&net.committee, net.root).unwrap();
    }
    let tc = TimeoutCert {
        epoch: 1,
        round: 6,
        timeouts: timeouts
            .iter()
            .map(|t| (t.voter, t.data.high_qc_round, t.signature.clone()))
            .collect(),
    };
    tc.verify(&net.committee).unwrap();
    assert_eq!(tc.max_high_qc_round(), 4);

    // Claiming a higher QC round than was signed breaks the signature.
    let mut lying = tc.clone();
    lying.timeouts[1].1 = 5;
    assert!(lying.verify(&net.committee).is_err());
}

#[test]
fn blocks() {
    let net = net(&[1, 1, 1, 1]);
    let genesis_qc = QuorumCert::genesis(1, net.root);
    let b1 = Block::new(
        header(&net, 1, net.root, 0),
        genesis_qc.clone(),
        None,
        vec![],
        &net.signers[0],
    )
    .unwrap();
    b1.verify(&net.committee, net.root).unwrap();

    // Round 3 on a round-1 QC needs a TC for round 2.
    let qc1 = qc(&net, 1, b1.hash(), &[0, 1, 2]);
    let skip = Block::new(header(&net, 3, b1.hash(), 1), qc1.clone(), None, vec![], &net.signers[1])
        .unwrap();
    assert!(skip.verify(&net.committee, net.root).is_err());

    // Signed by someone other than the named proposer.
    let wrong_signer =
        Block::new(header(&net, 2, b1.hash(), 1), qc1.clone(), None, vec![], &net.signers[2])
            .unwrap();
    assert!(wrong_signer.verify(&net.committee, net.root).is_err());

    // Payload swapped after signing.
    let mut swapped =
        Block::new(header(&net, 2, b1.hash(), 1), qc1, None, vec![], &net.signers[1]).unwrap();
    swapped.payload.push(chain_types::Batch {
        author: 0,
        batch_id: 0,
        txns: vec![],
    });
    assert!(swapped.verify(&net.committee, net.root).is_err());
}
