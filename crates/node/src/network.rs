//! Validator-to-validator messaging. The real transport is the QUIC mesh (design doc D5, section
//! 7); this in-process network stands in for it, delivering each message a fixed latency after
//! it was sent.

use chain_consensus::ConsensusMessage;
use chain_types::{Batch, ValidatorIndex};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    sync::mpsc::{self, RecvTimeoutError},
    thread::JoinHandle,
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NetMessage {
    Consensus(ConsensusMessage),
    /// A batch forwarded to an upcoming leader (design doc D6).
    Batch(Batch),
}

pub struct Delivery {
    pub from: ValidatorIndex,
    pub msg: NetMessage,
}

pub trait Network: Send {
    fn send(&self, to: ValidatorIndex, msg: NetMessage);
}

struct Envelope {
    due: Instant,
    from: ValidatorIndex,
    to: ValidatorIndex,
    msg: NetMessage,
}

/// Delivers messages between the validators of one process.
pub struct LocalNetwork {
    queue: Option<mpsc::Sender<Envelope>>,
    latency: Duration,
    thread: Option<JoinHandle<()>>,
}

impl LocalNetwork {
    /// What is sent to validator i arrives in `inboxes[i]`, converted by `wrap`.
    pub fn start<T: Send + 'static>(
        inboxes: Vec<mpsc::Sender<T>>,
        latency: Duration,
        wrap: fn(Delivery) -> T,
    ) -> Self {
        let (queue, envelopes) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("network".into())
            .spawn(move || deliver(envelopes, inboxes, wrap))
            .expect("spawning the network thread");
        Self {
            queue: Some(queue),
            latency,
            thread: Some(thread),
        }
    }

    /// The sending side for validator `me`.
    pub fn handle(&self, me: ValidatorIndex) -> LocalHandle {
        LocalHandle {
            me,
            latency: self.latency,
            queue: self.queue.clone().expect("network running"),
        }
    }
}

impl Drop for LocalNetwork {
    fn drop(&mut self) {
        drop(self.queue.take());
        if let Some(thread) = self.thread.take() {
            thread.join().ok();
        }
    }
}

pub struct LocalHandle {
    me: ValidatorIndex,
    latency: Duration,
    queue: mpsc::Sender<Envelope>,
}

impl Network for LocalHandle {
    fn send(&self, to: ValidatorIndex, msg: NetMessage) {
        let envelope = Envelope {
            due: Instant::now() + self.latency,
            from: self.me,
            to,
            msg,
        };
        // Fails only during shutdown.
        self.queue.send(envelope).ok();
    }
}

/// With one fixed latency, messages come due in the order they were sent, so a FIFO suffices.
fn deliver<T>(
    envelopes: mpsc::Receiver<Envelope>,
    inboxes: Vec<mpsc::Sender<T>>,
    wrap: fn(Delivery) -> T,
) {
    let mut in_transit: VecDeque<Envelope> = VecDeque::new();
    loop {
        let now = Instant::now();
        while in_transit.front().is_some_and(|e| e.due <= now) {
            let e = in_transit.pop_front().expect("checked");
            if let Some(inbox) = inboxes.get(e.to as usize) {
                inbox
                    .send(wrap(Delivery {
                        from: e.from,
                        msg: e.msg,
                    }))
                    .ok();
            }
        }
        let wait = in_transit
            .front()
            .map_or(Duration::from_secs(1), |e| e.due.saturating_duration_since(now));
        match envelopes.recv_timeout(wait) {
            Ok(e) => in_transit.push_back(e),
            Err(RecvTimeoutError::Timeout) => {},
            Err(RecvTimeoutError::Disconnected) if in_transit.is_empty() => return,
            Err(RecvTimeoutError::Disconnected) => {
                std::thread::sleep(wait);
            },
        }
    }
}

/// QUIC stream priority per traffic class (design doc 7.3): votes and timeouts first, then
/// proposals and block sync, then transaction batches.
pub fn priority(msg: &NetMessage) -> i32 {
    match msg {
        NetMessage::Consensus(
            ConsensusMessage::Vote(_)
            | ConsensusMessage::Timeout { .. }
            | ConsensusMessage::QuorumCert(_),
        ) => 3,
        NetMessage::Consensus(ConsensusMessage::Proposal(_)) => 2,
        NetMessage::Consensus(
            ConsensusMessage::BlockRequest(_) | ConsensusMessage::BlockResponse(_),
        ) => 1,
        NetMessage::Batch(_) => 0,
    }
}

impl Network for chain_network::QuicSender<NetMessage> {
    fn send(&self, to: ValidatorIndex, msg: NetMessage) {
        chain_network::QuicSender::send(self, to, &msg);
    }
}
