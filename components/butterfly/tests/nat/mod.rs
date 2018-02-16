extern crate habitat_butterfly;

use std::cmp;
use std::collections::{HashMap, HashSet, VecDeque};
use std::collections::hash_map::Entry;
use std::fmt::{Display, Result as FmtResult, Formatter};
use std::net::SocketAddr;
use std::result::Result as StdResult;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockWriteGuard};
use std::sync::mpsc::{Receiver, Sender, SendError};

use habitat_butterfly::error::{Error, Result};
use habitat_butterfly::network::{GossipReceiver, GossipSender, Network, SwimReceiver, SwimSender};

// ZoneID is a number that identifies a zone. Within a zone all the
// supervisors can talk to each other. For the interzone
// communication, a parent-child relationship needs to be established
// first, then supervisors in the child zone can talk to supervisors
// in the parent zone, but not the other way around.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct ZoneID(u8);

impl ZoneID {
    pub fn new(raw_id: u8) -> Self {
        assert!(raw_id > 0, "zone IDs must be greater than zero");
        ZoneID(raw_id)
    }

    pub fn raw(&self) -> u8 {
        self.0
    }
}

impl Display for ZoneID {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", self.raw())
    }
}

// ZoneInfo stores the relationship information of a zone.
#[derive(Debug, Default)]
struct ZoneInfo {
    parent: Option<ZoneID>,
    children: HashSet<ZoneID>,
}

struct ZoneMap(HashMap<ZoneID, Mutex<ZoneInfo>>);

impl ZoneMap {
    pub fn setup_zone_relationship(&mut self, parent_id: ZoneID, child_id: ZoneID) {
        assert_ne!(parent_id, child_id);
        self.ensure_zone(parent_id);
        self.ensure_zone(child_id);
        assert!(!self.is_zone_child_of_mut(parent_id, child_id));
        {
            let parent_zone = self.get_zone_mut(parent_id);
            parent_zone.children.insert(child_id);
        }
        {
            let child_zone = self.get_zone_mut(child_id);
            assert!(child_zone.parent.is_none());
            child_zone.parent = Some(parent_id);
        }
    }

    pub fn is_zone_child_of_mut(&mut self, child_id: ZoneID, parent_id: ZoneID) -> bool {
        let mut queue = VecDeque::new();
        queue.push_back(parent_id);
        while let Some(id) = queue.pop_front() {
            let zone = self.get_zone_mut(id);
            if zone.children.contains(&child_id) {
                return true;
            }
            queue.extend(zone.children.iter());
        }
        false
    }

    fn ensure_zone(&mut self, zone_id: ZoneID) {
        if let Entry::Vacant(v) = self.0.entry(zone_id) {
            v.insert(Mutex::new(ZoneInfo::default()));
        }
    }

    fn get_zone_mut(&mut self, zone_id: ZoneID) -> &mut ZoneInfo {
        self.0
            .get_mut(&zone_id)
            .expect(&format!("Zone {} not in zone map", zone_id))
            .get_mut()
            .expect(&format!("Zone {} lock is poisoned", zone_id))
    }
}

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

// TestNetworkSwitchBoard implements the multizone setup for testing
// the spanning ring.
#[derive(Clone)]
struct TestNetworkSwitchBoard {
    zones: Arc<RwLock<ZoneMap>>,
}

impl TestNetworkSwitchBoard {
    pub fn new() -> Self {
        Self { zones: Arc::new(RwLock::new(ZoneMap(HashMap::new()))) }
    }

    pub fn setup_zone_relationship(&self, parent_id: ZoneID, child_id: ZoneID) {
        let mut zones = self.write_zones();
        zones.setup_zone_relationship(parent_id, child_id);
    }

    fn write_zones(&self) -> RwLockWriteGuard<ZoneMap> {
        self.zones.write().expect("Zone map lock is poisoned")
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
