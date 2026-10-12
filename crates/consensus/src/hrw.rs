//! Stake-weighted rendezvous (HRW) leader election (design doc 5.2).
//!
//! For round r: window = r / K; each validator v scores
//! `-stake_v / ln(u_v)` with `u_v = (H(seed ‖ window ‖ pubkey_v) + 1) / 2^n` in (0, 1].
//! The highest score leads; sorting all scores gives the full ranking, used to pick forwarding
//! targets (D6). Every node computes the same schedule without communication.
//!
//! The leader of a round depends only on the round (a timeout does not change who leads the
//! next round). With K = 1 a crashed leader costs one timeout each time it is drawn; with
//! larger K it costs up to K consecutive timeouts, so K = 1 is the default until leader
//! reputation is added.

use aptos_crypto::HashValue;
use chain_types::{Committee, Round, ValidatorIndex};

#[derive(Clone, Debug)]
pub struct LeaderSchedule {
    committee: Committee,
    /// Fixed per epoch and never derived from recent block hashes, so leaders cannot grind it.
    epoch_seed: HashValue,
    /// Consecutive rounds each leader holds (K).
    rounds_per_leader: u64,
}

impl LeaderSchedule {
    pub fn new(committee: Committee, epoch_seed: HashValue, rounds_per_leader: u64) -> Self {
        Self {
            committee,
            epoch_seed,
            rounds_per_leader: rounds_per_leader.max(1),
        }
    }

    /// Seed of the next epoch: H(prev_seed ‖ epoch) (design doc 5.2, POC rule).
    pub fn next_epoch_seed(prev_seed: HashValue, epoch: u64) -> HashValue {
        let mut bytes = prev_seed.to_vec();
        bytes.extend(epoch.to_be_bytes());
        HashValue::sha3_256_of(&bytes)
    }

    pub fn committee(&self) -> &Committee {
        &self.committee
    }

    pub fn leader(&self, round: Round) -> ValidatorIndex {
        self.ranking(round)[0]
    }

    /// All validators by descending score for `round`'s window. Ties (practically impossible)
    /// break by index.
    pub fn ranking(&self, round: Round) -> Vec<ValidatorIndex> {
        let window = round / self.rounds_per_leader;
        let mut scored: Vec<(f64, ValidatorIndex)> = self
            .committee
            .validators()
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let mut input = self.epoch_seed.to_vec();
                input.extend(window.to_be_bytes());
                input.extend(v.public_key.to_bytes());
                let hash = HashValue::sha3_256_of(&input);
                (score(v.stake, &hash), i as ValidatorIndex)
            })
            .collect();
        scored.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
        scored.into_iter().map(|(_, i)| i).collect()
    }

    /// Distinct leaders of the next rounds starting at `round`, in order, up to `count`
    /// (transaction forwarding targets, design doc D6).
    pub fn upcoming_leaders(&self, round: Round, count: usize) -> Vec<ValidatorIndex> {
        let count = count.min(self.committee.len());
        let mut leaders = Vec::with_capacity(count);
        let mut r = round;
        // Bounded: each window is checked once; give up after many windows of repeats.
        let mut windows = 0;
        while leaders.len() < count && windows < 64 * count as u64 {
            let leader = self.leader(r);
            if !leaders.contains(&leader) {
                leaders.push(leader);
            }
            r += self.rounds_per_leader;
            windows += 1;
        }
        leaders
    }
}

/// `-stake / ln(u)` with u from the hash's top 53 bits: u = (x + 1) / 2^53 in (0, 1].
fn score(stake: u64, hash: &HashValue) -> f64 {
    let top = u64::from_be_bytes(hash.as_ref()[..8].try_into().expect("8 bytes")) >> 11;
    let u = (top as f64 + 1.0) / (1u64 << 53) as f64;
    if u >= 1.0 {
        return f64::INFINITY;
    }
    -(stake as f64) / deterministic_ln(u)
}

/// Natural log of `x` in (0, 1) using only IEEE-754 basic operations, which are exactly rounded
/// on every platform, unlike `f64::ln` (libm). Leader election must agree bit for bit.
fn deterministic_ln(x: f64) -> f64 {
    const LN_2: f64 = std::f64::consts::LN_2;
    // x = m * 2^e with m in [1, 2), found by exact scaling by 2.
    let (mut m, mut e) = (x, 0i32);
    while m < 1.0 {
        m *= 2.0;
        e -= 1;
    }
    // ln(m) = 2 atanh(t), t = (m - 1) / (m + 1) in [0, 1/3); 40 terms reach full precision.
    let t = (m - 1.0) / (m + 1.0);
    let t2 = t * t;
    let mut term = t;
    let mut sum = 0.0;
    for k in 0..40 {
        sum += term / (2 * k + 1) as f64;
        term *= t2;
    }
    2.0 * sum + e as f64 * LN_2
}

#[cfg(test)]
mod tests {
    use super::*;
    use aptos_crypto::PrivateKey;
    use aptos_keygen::KeyGen;
    use chain_types::ValidatorInfo;

    fn committee(stakes: &[u64]) -> Committee {
        let mut keygen = KeyGen::from_seed([2; 32]);
        Committee::new(
            1,
            stakes
                .iter()
                .map(|s| ValidatorInfo {
                    public_key: keygen.generate_ed25519_private_key().public_key(),
                    network_address: String::new(),
                    stake: *s,
                })
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn ln_matches_std_closely() {
        for i in 1..1000 {
            let x = i as f64 / 1000.0;
            let (ours, std) = (deterministic_ln(x), x.ln());
            assert!((ours - std).abs() <= 4.0 * f64::EPSILON * std.abs().max(1.0), "{x}");
        }
        assert!((deterministic_ln(1e-15) - 1e-15f64.ln()).abs() < 1e-12);
    }

    #[test]
    fn same_inputs_same_schedule_and_rounds_per_leader() {
        let seed = HashValue::sha3_256_of(b"seed");
        let a = LeaderSchedule::new(committee(&[1, 1, 1, 1]), seed, 4);
        let b = LeaderSchedule::new(committee(&[1, 1, 1, 1]), seed, 4);
        for r in 0..100 {
            assert_eq!(a.ranking(r), b.ranking(r));
            // A leader holds all K rounds of its window.
            assert_eq!(a.leader(r), a.leader(r - r % 4));
        }
        let ranking = a.ranking(0);
        let mut sorted = ranking.clone();
        sorted.sort();
        assert_eq!(sorted, vec![0, 1, 2, 3], "ranking is a permutation");
    }

    #[test]
    fn leadership_is_proportional_to_stake() {
        let schedule = LeaderSchedule::new(
            committee(&[1, 2, 3, 4]),
            HashValue::sha3_256_of(b"stake"),
            1,
        );
        let rounds = 40_000;
        let mut counts = [0u32; 4];
        for r in 0..rounds {
            counts[schedule.leader(r) as usize] += 1;
        }
        let stakes = schedule.committee().validators().iter().map(|v| v.stake);
        for (count, stake) in counts.iter().zip(stakes) {
            let expected = rounds as f64 * stake as f64 / 10.0;
            assert!(
                (*count as f64 - expected).abs() < 0.05 * expected,
                "stake {stake}: {count} vs {expected}"
            );
        }
    }

    #[test]
    fn upcoming_leaders_are_distinct() {
        let schedule =
            LeaderSchedule::new(committee(&[1, 1, 1, 1, 1]), HashValue::sha3_256_of(b"x"), 1);
        let next = schedule.upcoming_leaders(10, 3);
        assert_eq!(next.len(), 3);
        assert_eq!(next[0], schedule.leader(10));
        assert!(next[0] != next[1] && next[1] != next[2] && next[0] != next[2]);
    }
}
