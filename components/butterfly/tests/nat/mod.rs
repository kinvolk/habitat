extern crate habitat_butterfly;

use std::cmp;
use std::collections::{HashMap, HashSet, VecDeque};
use std::collections::hash_map::Entry;
use std::fmt::{Display, Result as FmtResult, Formatter};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::result::Result as StdResult;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::mpsc::{self, Receiver, Sender, SendError};
use std::thread;

use habitat_core::service::ServiceGroup;
use habitat_butterfly::error::{Error, Result};
use habitat_butterfly::member::Member;
use habitat_butterfly::network::{GossipReceiver, GossipSender, Network, SwimReceiver, SwimSender};
use habitat_butterfly::server::{Server, Suitability};
use habitat_butterfly::server::timing::Timing;
use habitat_butterfly::trace::Trace;

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

    // This is basically a duplication of the is_zone_child_of
    // function, but without locking the mutex. Locking of the mutex
    // can be skipped, because here we own a mutable reference to
    // self, which means we are the only thread that owns it.
    //
    // FIXME(krnowak): Probably the only way to dedup is through a
    // macro, but readability would suffer I suppose. Maybe just drop
    // the mut variant of the function?
    pub fn is_zone_child_of(&self, child_id: ZoneID, parent_id: ZoneID) -> bool {
        let mut queue = VecDeque::new();
        queue.push_back(parent_id);
        while let Some(id) = queue.pop_front() {
            let zone_guard = self.get_zone_guard(id);
            if zone_guard.children.contains(&child_id) {
                return true;
            }
            queue.extend(zone_guard.children.iter());
        }
        false
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

    fn get_zone_guard(&self, zone_id: ZoneID) -> MutexGuard<ZoneInfo> {
        self.0
            .get(&zone_id)
            .expect(&format!("Zone {} not in zone map", zone_id))
            .lock()
            .expect(&format!("Zone {} lock is poisoned", zone_id))
    }

    fn get_zone_mut(&mut self, zone_id: ZoneID) -> &mut ZoneInfo {
        self.0
            .get_mut(&zone_id)
            .expect(&format!("Zone {} not in zone map", zone_id))
            .get_mut()
            .expect(&format!("Zone {} lock is poisoned", zone_id))
    }
}

fn create_member_from_addr(addr: SocketAddr) -> Member {
    let mut member = Member::default();
    let port = addr.port() as i32;
    member.set_address(format!("{}", addr.ip()));
    member.set_swim_port(port);
    member.set_gossip_port(port);
    member
}

// TalkTarget is a trait used for types that can be talked to. It is
// basically about establishing a ring with SWIM messages.
trait TalkTarget {
    fn create_member_info(&self) -> Member;
}

// TestServer is a (thin) wrapper around the butterfly server.
#[derive(Clone)]
struct TestServer {
    butterfly: Server<TestNetwork>,
}

impl TestServer {
    pub fn talk_to(&self, talk_targets: Vec<&TalkTarget>) {
        let mut members = Vec::with_capacity(talk_targets.len());
        for talk_target in talk_targets {
            members.push(talk_target.create_member_info());
        }
        self.butterfly.member_list.set_initial_members(members);
    }
}

impl TalkTarget for TestServer {
    fn create_member_info(&self) -> Member {
        let addr = self.butterfly.read_network().get_swim_addr();
        create_member_from_addr(addr)
    }
}

type ZoneToCountMap = HashMap<ZoneID, u8>;

// Addresses is used to generate addresses for TestServers. The
// generated IP4 addresses have a certain structure:
//
// <A>.<B>.0.0:42
// A - zone ID this IP is relevant for, >0
// B - server index in the zone
//
// So a fifth server in a first zone will have an IP 1.5.0.0:42.
struct Addresses {
    server_map: ZoneToCountMap,
}

impl Addresses {
    pub fn new() -> Self {
        Self { server_map: HashMap::new() }
    }

    pub fn generate_address_for_server(&mut self, zone_id: ZoneID) -> SocketAddr {
        let idx = Self::get_next_idx_for_zone(&mut self.server_map, zone_id);
        let port = 42 as u16;
        Self::generate_address(zone_id.raw(), idx, port)
    }

    pub fn get_zone_from_address(addr: &SocketAddr) -> ZoneID {
        if let IpAddr::V4(ipv4) = addr.ip() {
            ZoneID(ipv4.octets()[0])
        } else {
            unreachable!("test address ({:?}) is not V4", addr);
        }
    }

    fn get_next_idx_for_zone(map: &mut ZoneToCountMap, zone_id: ZoneID) -> u8 {
        match map.entry(zone_id) {
            Entry::Vacant(v) => {
                v.insert(1);
                1
            }
            Entry::Occupied(mut o) => {
                let value = o.get_mut();
                *value += 1;
                *value
            }
        }
    }

    fn generate_address(zone_id_raw: u8, server_idx: u8, port: u16) -> SocketAddr {
        let ip = IpAddr::V4(Ipv4Addr::new(zone_id_raw, server_idx, 0, 0));
        SocketAddr::new(ip, port)
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

// ChannelMap is a mapping from IP address to an mpsc::Sender.
type ChannelMap = HashMap<SocketAddr, LockedSender<TestMessage>>;

#[derive(Copy, Clone)]
enum ChannelType {
    SWIM,
    Gossip,
}

// TestNetworkSwitchBoard implements the multizone setup for testing
// the spanning ring.
#[derive(Clone)]
struct TestNetworkSwitchBoard {
    zones: Arc<RwLock<ZoneMap>>,
    servers: Arc<RwLock<Vec<TestServer>>>,
    addresses: Arc<Mutex<Addresses>>,
    swim_channel_map: Arc<RwLock<ChannelMap>>,
    gossip_channel_map: Arc<RwLock<ChannelMap>>,
}

#[derive(Debug)]
struct TestSuitability(u64);
impl Suitability for TestSuitability {
    fn get(&self, _service_group: &ServiceGroup) -> u64 {
        self.0
    }
}

impl TestNetworkSwitchBoard {
    pub fn new() -> Self {
        Self {
            zones: Arc::new(RwLock::new(ZoneMap(HashMap::new()))),
            servers: Arc::new(RwLock::new(Vec::new())),
            addresses: Arc::new(Mutex::new(Addresses::new())),
            swim_channel_map: Arc::new(RwLock::new(HashMap::new())),
            gossip_channel_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn setup_zone_relationship(&self, parent_id: ZoneID, child_id: ZoneID) {
        let mut zones = self.write_zones();
        zones.setup_zone_relationship(parent_id, child_id);
    }

    pub fn start_server_in_zone(&self, zone_id: ZoneID) -> TestServer {
        let addr = {
            let mut addresses = self.get_addresses_guard();
            addresses.generate_address_for_server(zone_id)
        };
        let network = self.create_test_network(addr);
        let mut servers = self.write_servers();
        let idx = servers.len();
        let server = self.create_test_server(network, idx as u64);
        servers.push(server.clone());
        server
    }

    fn create_test_network(&self, addr: SocketAddr) -> TestNetwork {
        let (swim_in, swim_out) = self.start_routing_thread(addr, ChannelType::SWIM);
        let (gossip_in, gossip_out) = self.start_routing_thread(addr, ChannelType::Gossip);
        TestNetwork::new(addr, swim_in, swim_out, gossip_in, gossip_out)
    }

    fn create_test_server(&self, network: TestNetwork, idx: u64) -> TestServer {
        let member = create_member_from_addr(network.get_addr());
        let trace = Trace::default();
        let ring_key = None;
        let name = None;
        let data_path = None::<PathBuf>;
        let suitability = Box::new(TestSuitability(idx));
        let mut butterfly = Server::new(
            network,
            member,
            trace,
            ring_key,
            name,
            data_path,
            suitability,
        );
        let timing = Timing::default();
        butterfly.start(timing).expect("failed to start server");
        TestServer { butterfly }
    }

    fn start_routing_thread(
        &self,
        addr: SocketAddr,
        channel_type: ChannelType,
    ) -> (Sender<TestMessage>, Receiver<TestMessage>) {
        let (msg_in, msg_mid_out) = mpsc::channel::<TestMessage>();
        let (msg_mid_in, msg_out) = mpsc::channel::<TestMessage>();
        {
            let mut channel_map = self.write_channel_map(channel_type);
            channel_map.insert(addr, LockedSender::new(msg_mid_in));
        }
        let self_for_thread = self.clone();
        thread::spawn(move || loop {
            match msg_mid_out.recv() {
                Ok(msg) => self_for_thread.process_msg(msg, channel_type),
                Err(_) => break,
            }
        });

        (msg_in, msg_out)
    }

    fn process_msg(&self, msg: TestMessage, channel_type: ChannelType) {
        let can_route = {
            let source_zone_id = Addresses::get_zone_from_address(&msg.source_addr);
            let target_zone_id = Addresses::get_zone_from_address(&msg.target_addr);
            if target_zone_id == source_zone_id {
                true
            } else {
                let zone_map = self.read_zones();
                // child zones can talk to parent zones, parent zones
                // can't talk to child zones
                zone_map.is_zone_child_of(source_zone_id, target_zone_id)
            }
        };
        if can_route {
            let target_addr = msg.target_addr;
            let maybe_out = {
                let map = self.read_channel_map(channel_type);
                map.get(&target_addr).map(|l| l.cloned_sender())
            };
            if let Some(out) = maybe_out {
                if out.send(msg).is_err() {
                    let mut map = self.write_channel_map(channel_type);
                    map.remove(&target_addr);
                }
            }
        }
    }

    fn read_zones(&self) -> RwLockReadGuard<ZoneMap> {
        self.zones.read().expect("Zone map lock is poisoned")
    }

    fn write_zones(&self) -> RwLockWriteGuard<ZoneMap> {
        self.zones.write().expect("Zone map lock is poisoned")
    }

    fn get_addresses_guard(&self) -> MutexGuard<Addresses> {
        self.addresses.lock().expect("Addresses lock is poisoned")
    }

    fn write_servers(&self) -> RwLockWriteGuard<Vec<TestServer>> {
        self.servers.write().expect("Servers lock is poisoned")
    }

    fn read_channel_map(&self, channel_type: ChannelType) -> RwLockReadGuard<ChannelMap> {
        self.get_channel_map_lock(channel_type).read().expect(
            "Channel map lock is poisoned",
        )
    }

    fn write_channel_map(&self, channel_type: ChannelType) -> RwLockWriteGuard<ChannelMap> {
        self.get_channel_map_lock(channel_type).write().expect(
            "Channel map lock is poisoned",
        )
    }

    fn get_channel_map_lock(&self, channel_type: ChannelType) -> &RwLock<ChannelMap> {
        match channel_type {
            ChannelType::SWIM => &self.swim_channel_map,
            ChannelType::Gossip => &self.gossip_channel_map,
        }
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
