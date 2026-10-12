//! Safety rules: equivocation, locking, round skipping, timeouts and crash-restart.
//! Certificates here carry no signatures: safety rules trust verified inputs, so only rounds
//! and parent links matter.

use aptos_crypto::HashValue;
use aptos_keygen::KeyGen;
use aptos_types::chain_id::ChainId;
use chain_consensus::{FileSafetyStorage, InMemorySafetyStorage, SafetyRules};
use chain_types::{
    Block, BlockHeader, QuorumCert, Round, TimeoutCert, ValidatorSigner, VoteData,
    payload_digest,
};

fn signer() -> ValidatorSigner {
    ValidatorSigner::new(0, KeyGen::from_seed([3; 32]).generate_ed25519_private_key())
}

/// QC for a block in `round` whose parent is in `parent_round`.
fn qc(round: Round, parent_round: Round) -> QuorumCert {
    QuorumCert {
        data: VoteData {
            epoch: 1,
            round,
            block_hash: HashValue::sha3_256_of(&round.to_be_bytes()),
            parent_round,
            parent_hash: HashValue::sha3_256_of(&parent_round.to_be_bytes()),
        },
        signatures: vec![],
    }
}

fn tc(round: Round, high_qc_rounds: &[Round]) -> TimeoutCert {
    let sig = signer().sign(&qc(1, 0).data).unwrap();
    TimeoutCert {
        epoch: 1,
        round,
        timeouts: high_qc_rounds
            .iter()
            .enumerate()
            .map(|(i, r)| (i as u16, *r, sig.clone()))
            .collect(),
    }
}

/// A block in `round` extending `qc`; `salt` makes conflicting blocks for the same round.
fn block(round: Round, qc: QuorumCert, tc: Option<TimeoutCert>, salt: u64) -> Block {
    let header = BlockHeader {
        chain_id: ChainId::new(42),
        epoch: 1,
        round,
        height: round,
        parent_hash: qc.block_hash(),
        proposer: 0,
        timestamp_ms: salt,
        payload_digest: payload_digest(&[]),
        executed_height: 0,
        executed_state_root: HashValue::zero(),
    };
    Block::new(header, qc, tc, vec![], &signer()).unwrap()
}

fn rules() -> SafetyRules<InMemorySafetyStorage> {
    SafetyRules::new(signer(), InMemorySafetyStorage::default(), 1).unwrap()
}

#[test]
fn one_vote_per_round_even_for_an_equivocating_leader() {
    let mut rules = rules();
    let a = block(1, QuorumCert::genesis(1, qc(0, 0).block_hash()), None, 1);
    let b = block(1, QuorumCert::genesis(1, qc(0, 0).block_hash()), None, 2);
    assert_ne!(a.hash(), b.hash());
    let vote_a = rules.vote(&a).unwrap();
    // Asked again for a conflicting block in the same round: the first vote comes back.
    let vote_b = rules.vote(&b).unwrap();
    assert_eq!(vote_a, vote_b);
    assert_eq!(vote_b.data.block_hash, a.hash());
}

#[test]
fn never_votes_backwards() {
    let mut rules = rules();
    rules.vote(&block(5, qc(4, 3), None, 0)).unwrap();
    assert!(rules.vote(&block(4, qc(3, 2), None, 0)).is_err());
}

#[test]
fn locked_on_the_two_chain_round() {
    let mut rules = rules();
    // Voting for round 6 on QC(5, parent 4) locks on round 4.
    rules.vote(&block(6, qc(5, 4), None, 0)).unwrap();
    assert_eq!(rules.data().preferred_round, 4);
    // Round 8 after TC(7) extending QC(3): below the lock, refused.
    assert!(rules.vote(&block(8, qc(3, 2), Some(tc(7, &[3])), 0)).is_err());
    // Extending QC(4) (= the lock) after TC(7) is fine.
    rules.vote(&block(8, qc(4, 3), Some(tc(7, &[4])), 0)).unwrap();
}

#[test]
fn skipping_rounds_needs_a_matching_timeout_certificate() {
    let mut rules = rules();
    // Round 3 on QC(1) with no TC.
    assert!(rules.vote(&block(3, qc(1, 0), None, 0)).is_err());
    // TC for the wrong round.
    assert!(rules.vote(&block(3, qc(1, 0), Some(tc(1, &[1])), 0)).is_err());
    // TC says someone had QC(2): extending QC(1) could fork away from it.
    assert!(rules.vote(&block(3, qc(1, 0), Some(tc(2, &[2, 1])), 0)).is_err());
    rules.vote(&block(3, qc(2, 1), Some(tc(2, &[2, 1])), 0)).unwrap();
}

#[test]
fn no_vote_after_timing_out_a_round() {
    let mut rules = rules();
    let high = qc(4, 3);
    rules.timeout(5, &high, None).unwrap();
    assert!(rules.vote(&block(5, high.clone(), None, 0)).is_err());
    // Voting and then timing out the same round is allowed.
    let mut fresh = self::rules();
    fresh.vote(&block(5, high.clone(), None, 0)).unwrap();
    fresh.timeout(5, &high, None).unwrap();
}

#[test]
fn timeouts_cannot_hide_a_known_qc() {
    let mut rules = rules();
    rules.vote(&block(6, qc(5, 4), None, 0)).unwrap();
    // Has seen QC(5); timing out round 7 claiming only QC(3) is refused.
    assert!(rules.timeout(7, &qc(3, 2), Some(&tc(6, &[3]))).is_err());
    rules.timeout(6, &qc(5, 4), None).unwrap();
    // Round must follow the high QC or the previous TC.
    assert!(rules.timeout(9, &qc(5, 4), None).is_err());
    rules.timeout(7, &qc(5, 4), Some(&tc(6, &[5]))).unwrap();
}

#[test]
fn state_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("safety.bcs");
    let a = block(2, qc(1, 0), None, 1);
    let b = block(2, qc(1, 0), None, 2);
    let first = {
        let mut rules = SafetyRules::new(signer(), FileSafetyStorage::new(&path), 1).unwrap();
        rules.vote(&a).unwrap()
    };
    let mut restarted = SafetyRules::new(signer(), FileSafetyStorage::new(&path), 1).unwrap();
    assert_eq!(restarted.data().last_voted_round, 2);
    // After a crash, a conflicting block in the same round still gets the original vote.
    assert_eq!(restarted.vote(&b).unwrap(), first);
    assert!(restarted.vote(&block(1, QuorumCert::genesis(1, HashValue::zero()), None, 0)).is_err());

    // A later epoch starts fresh; an earlier one is refused.
    assert_eq!(
        SafetyRules::new(signer(), FileSafetyStorage::new(&path), 2)
            .unwrap()
            .data()
            .last_voted_round,
        0
    );
    assert!(SafetyRules::new(signer(), FileSafetyStorage::new(&path), 1).is_err());
}
