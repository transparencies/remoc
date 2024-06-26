//! A channel that exchanges binary data with a remote endpoint.
//!
//! Allows low-overhead exchange of binary data.
//!
//! At least one end of the channel must be remote.
//! Forwarding, i.e. both channel ends on remote endpoints, is supported.
//!
//! If the sole use is to transfer a large binary object into one direction,
//! consider using a [lazy blob](crate::robj::lazy_blob) instead.
//!
//! This is a wrapper around a [chmux](crate::chmux) channel that allows to
//! establish a connection by sending the sender or receiver to a remote endpoint.

use std::sync::{Arc, Mutex};

mod receiver;
mod sender;

pub use receiver::Receiver;
pub use sender::Sender;

use super::interlock::{Interlock, Location};

/// Creates a new binary channel that is established by sending either the sender or receiver
/// over a remote channel.
pub fn channel() -> (Sender, Receiver) {
    let (sender_tx, sender_rx) = tokio::sync::mpsc::unbounded_channel();
    let (receiver_tx, receiver_rx) = tokio::sync::mpsc::unbounded_channel();
    let interlock = Arc::new(Mutex::new(Interlock::new()));

    let sender = Sender {
        sender: None,
        sender_rx,
        receiver_tx: Some(receiver_tx),
        interlock: interlock.clone(),
        successor_tx: std::sync::Mutex::new(None),
    };
    let receiver = Receiver {
        receiver: None,
        sender_tx: Some(sender_tx),
        receiver_rx,
        interlock,
        successor_tx: std::sync::Mutex::new(None),
    };
    (sender, receiver)
}
