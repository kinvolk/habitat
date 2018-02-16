extern crate habitat_butterfly;

use std::cmp;
use std::net::SocketAddr;
use std::result::Result as StdResult;
use std::sync::{Mutex, MutexGuard};
use std::sync::mpsc::{Receiver, Sender, SendError};

use habitat_butterfly::error::{Error, Result};
use habitat_butterfly::network::{GossipReceiver, GossipSender, Network, SwimReceiver, SwimSender};

// TestMessage is a wrapper around the SWIM or gossip message sent by
// a butterfly server. Contains source and destination addresses used
// to determine a routing.
#[derive(Debug)]
struct TestMessage {
    source_addr: SocketAddr,
    target_addr: SocketAddr,
    bytes: Vec<u8>,
}

// LockedSender is a convenience struct to make mpsc::Sender fulfill
// the Send + Sync traits.
#[derive(Debug)]
struct LockedSender<T> {
    sender: Mutex<Sender<T>>,
}

impl<T> LockedSender<T> {
    pub fn new(sender: Sender<T>) -> Self {
        Self { sender: Mutex::new(sender) }
    }

    pub fn send(&self, t: T) -> StdResult<(), SendError<T>> {
        self.get_sender_guard().send(t)
    }

    pub fn cloned_sender(&self) -> Sender<T> {
        self.get_sender_guard().clone()
    }

    fn get_sender_guard(&self) -> MutexGuard<Sender<T>> {
        self.sender.lock().expect("Sender lock is poisoned")
    }
}

// TestSwimSender is an implementation of a SwimSender trait based on
// channels.
#[derive(Debug)]
struct TestSwimSender {
    addr: SocketAddr,
    sender: LockedSender<TestMessage>,
}

impl SwimSender for TestSwimSender {
    fn send(&self, buf: &[u8], addr: SocketAddr) -> Result<usize> {
        let msg = TestMessage {
            source_addr: self.addr,
            target_addr: addr,
            bytes: buf.to_owned(),
        };
        self.sender.send(msg).map_err(|_| {
            Error::SwimSendError("Receiver part of the channel is disconnected".to_owned())
        })?;
        Ok(buf.len())
    }
}

// TestSwimReceiver is an implementation of a SwimReceiver trait based
// on channels.
struct TestSwimReceiver(Receiver<TestMessage>);

impl SwimReceiver for TestSwimReceiver {
    fn receive(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        let msg = self.0.recv().map_err(|_| {
            Error::SwimReceiveError("Sender part of the channel is disconnected".to_owned())
        })?;
        let len = cmp::min(msg.bytes.len(), buf.len());
        buf[..len].copy_from_slice(&msg.bytes);
        Ok((len, msg.source_addr))
    }
}

// TestGossipSender is an implementation of a GossipSender trait based
// on channels.
struct TestGossipSender {
    source_addr: SocketAddr,
    target_addr: SocketAddr,
    sender: Sender<TestMessage>,
}

impl GossipSender for TestGossipSender {
    fn send(&self, buf: &[u8]) -> Result<()> {
        let msg = TestMessage {
            source_addr: self.source_addr,
            target_addr: self.target_addr,
            bytes: buf.to_vec(),
        };
        self.sender.send(msg).map_err(|_| {
            Error::GossipSendError("Receiver part of the channel is disconnected".to_owned())
        })?;
        Ok(())
    }
}

// TestGossipReceiver is an implementation of a GossipReceiver trait
// based on channels.
struct TestGossipReceiver(Receiver<TestMessage>);

impl GossipReceiver for TestGossipReceiver {
    fn receive(&self) -> Result<Vec<u8>> {
        let msg = self.0.recv().map_err(|_| {
            Error::GossipReceiveError("Sender part of the channel is disconnected".to_owned())
        })?;
        return Ok(msg.bytes);
    }
}

// TestNetwork is an implementation of a Network trait. It provides
// channel-based senders and receivers.
#[derive(Debug)]
struct TestNetwork {
    addr: SocketAddr,
    swim_in: LockedSender<TestMessage>,
    swim_out: Mutex<Option<Receiver<TestMessage>>>,
    gossip_in: LockedSender<TestMessage>,
    gossip_out: Mutex<Option<Receiver<TestMessage>>>,
}

impl TestNetwork {
    pub fn new(
        addr: SocketAddr,
        swim_in: Sender<TestMessage>,
        swim_out: Receiver<TestMessage>,
        gossip_in: Sender<TestMessage>,
        gossip_out: Receiver<TestMessage>,
    ) -> Self {
        Self {
            addr: addr,
            swim_in: LockedSender::new(swim_in),
            swim_out: Mutex::new(Some(swim_out)),
            gossip_in: LockedSender::new(gossip_in),
            gossip_out: Mutex::new(Some(gossip_out)),
        }
    }

    pub fn get_addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Network for TestNetwork {
    type SwimSender = TestSwimSender;
    type SwimReceiver = TestSwimReceiver;
    type GossipSender = TestGossipSender;
    type GossipReceiver = TestGossipReceiver;

    fn get_swim_addr(&self) -> SocketAddr {
        self.addr
    }

    fn create_swim_sender(&self) -> Result<Self::SwimSender> {
        Ok(Self::SwimSender {
            addr: self.addr,
            sender: LockedSender::new(self.swim_in.cloned_sender()),
        })
    }

    fn create_swim_receiver(&self) -> Result<Self::SwimReceiver> {
        match self.swim_out
            .lock()
            .expect("SWIM receiver lock is poisoned")
            .take() {
            Some(receiver) => Ok(TestSwimReceiver(receiver)),
            None => {
                Err(Error::SwimChannelSetupError(
                    format!("no test swim receiver, should not happen"),
                ))
            }
        }
    }

    fn get_gossip_addr(&self) -> SocketAddr {
        self.addr
    }

    fn create_gossip_sender(&self, addr: SocketAddr) -> Result<Self::GossipSender> {
        Ok(Self::GossipSender {
            source_addr: self.addr,
            target_addr: addr,
            sender: self.gossip_in.cloned_sender(),
        })
    }

    fn create_gossip_receiver(&self) -> Result<Self::GossipReceiver> {
        match self.gossip_out
            .lock()
            .expect("Gossip receiver lock is poisoned")
            .take() {
            Some(receiver) => Ok(TestGossipReceiver(receiver)),
            None => {
                Err(Error::SwimChannelSetupError(
                    format!("no test gossip receiver, should not happen"),
                ))
            }
        }
    }
}
