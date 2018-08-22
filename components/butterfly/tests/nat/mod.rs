extern crate habitat_butterfly;

mod nat;

use std::cmp::{self, Ordering};
use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::error::Error as StdError;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::path::PathBuf;
use std::result::Result as StdResult;
use std::str::FromStr;
use std::sync::mpsc::{self, Receiver, SendError, Sender};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread;
use std::time::Duration;
use std::u8;

use habitat_butterfly::error::{Error, Result};
use habitat_butterfly::member::{Health, Member};
use habitat_butterfly::network::{Address, AddressAndPort, GossipReceiver, GossipSender, MyFromStr,
                                 Network, SwimReceiver, SwimSender};
use habitat_butterfly::server::timing::Timing;
use habitat_butterfly::server::{Server, Suitability};
use habitat_butterfly::trace::Trace;
use habitat_core::service::ServiceGroup;

// ZoneID is a number that identifies a zone. Within a zone all the
// supervisors can talk to each other. For the interzone
// communication, a parent-child relationship needs to be established
// first, then supervisors in the child zone can talk to supervisors
// in the parent zone, but not the other way around.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct ZoneID(u8);

impl ZoneID {
    pub fn new(raw_id: u8) -> Self {
        assert!(
            Self::is_raw_valid(raw_id),
            "zone IDs must be greater than zero"
        );
        ZoneID(raw_id)
    }

    pub fn raw(&self) -> u8 {
        self.0
    }

    pub fn is_raw_valid(raw: u8) -> bool {
        raw > 0
    }
}

impl Display for ZoneID {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", self.raw())
    }
}

impl FromStr for ZoneID {
    type Err = String;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        let raw_self = s.parse()
            .map_err(|e| format!("'{}' is not a u8: {}", s, e))?;
        if !Self::is_raw_valid(raw_self) {
            return Err(format!("{} is not a valid ZoneID", raw_self));
        }

        Ok(Self::new(raw_self))
    }
}

// ZoneInfo stores the relationship information of a zone.
#[derive(Debug, Default, Clone)]
struct ZoneInfo {
    parent: Option<ZoneID>,
    children: HashSet<ZoneID>,
}

struct ZoneMap(HashMap<ZoneID, Mutex<ZoneInfo>>);

#[derive(Copy, Clone, Eq, PartialEq)]
enum Direction {
    ParentToChild,
    ChildToParent,
}

struct DijkstraData {
    info: ZoneInfo,
    distance: usize,
}

impl DijkstraData {
    pub fn new_with_max_distance(info: &ZoneInfo) -> Self {
        Self::new(info, usize::max_value())
    }

    pub fn new_with_zero_distance(info: &ZoneInfo) -> Self {
        Self::new(info, 0)
    }

    fn new(info: &ZoneInfo, distance: usize) -> Self {
        let info = info.clone();
        Self { info, distance }
    }
}

#[derive(Clone, Eq, PartialEq)]
struct TraversalInfo {
    direction: Direction,
    from: ZoneID,
    to: ZoneID,
}

impl TraversalInfo {
    pub fn new(direction: Direction, from: ZoneID, to: ZoneID) -> Self {
        Self {
            direction,
            from,
            to,
        }
    }
}

#[derive(Eq, PartialEq)]
struct DijkstraState {
    cost: usize,
    id: ZoneID,
    route: Vec<TraversalInfo>,
}

impl DijkstraState {
    pub fn new_start(start_id: ZoneID) -> Self {
        Self::new(0, start_id, Vec::new())
    }

    pub fn new_incremental(old: &Self, new_id: ZoneID, direction: Direction) -> Self {
        let mut new_route = old.route.clone();

        new_route.push(TraversalInfo::new(direction, old.id, new_id));
        Self::new(old.cost + 1, new_id, new_route)
    }

    pub fn steal_route(self) -> Vec<TraversalInfo> {
        self.route
    }

    fn new(cost: usize, id: ZoneID, route: Vec<TraversalInfo>) -> Self {
        Self { cost, id, route }
    }
}

// This is to make BinaryHeap a min-heap instead of max-heap.
impl Ord for DijkstraState {
    fn cmp(&self, other: &DijkstraState) -> Ordering {
        other.cost.cmp(&self.cost)
    }
}

impl PartialOrd for DijkstraState {
    fn partial_cmp(&self, other: &DijkstraState) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

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

    /*
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
     */

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

    // Dijkstra, basically.
    pub fn get_route(&self, source_id: ZoneID, target_id: ZoneID) -> Option<Vec<TraversalInfo>> {
        if source_id == target_id {
            return Some(Vec::new());
        }

        let mut dd_map = HashMap::with_capacity(self.0.len());
        let mut heap = BinaryHeap::new();

        for (zone_id, info_lock) in &self.0 {
            let info = info_lock
                .lock()
                .expect(&format!("Zone {} lock is poisoned", zone_id));
            let dd = if *zone_id == source_id {
                DijkstraData::new_with_zero_distance(&info)
            } else {
                DijkstraData::new_with_max_distance(&info)
            };
            dd_map.insert(*zone_id, dd);
        }
        heap.push(DijkstraState::new_start(source_id));

        while let Some(ds) = heap.pop() {
            if ds.id == target_id {
                return Some(ds.steal_route());
            }

            let (parent, children) = {
                let dd = Self::get_dijkstra_data(&dd_map, ds.id);
                if ds.cost > dd.distance {
                    continue;
                }

                (dd.info.parent.clone(), dd.info.children.clone())
            };

            if let Some(parent_id) = parent {
                Self::dijkstra_step(
                    &mut dd_map,
                    parent_id,
                    &mut heap,
                    &ds,
                    Direction::ChildToParent,
                );
            }

            for child_id in children {
                Self::dijkstra_step(
                    &mut dd_map,
                    child_id,
                    &mut heap,
                    &ds,
                    Direction::ParentToChild,
                );
            }
        }

        None
    }

    fn dijkstra_step(
        dd_map: &mut HashMap<ZoneID, DijkstraData>,
        id: ZoneID,
        heap: &mut BinaryHeap<DijkstraState>,
        old_ds: &DijkstraState,
        direction: Direction,
    ) {
        let dd = Self::get_dijkstra_data_mut(dd_map, id);
        if old_ds.cost + 1 < dd.distance {
            let new_ds = DijkstraState::new_incremental(&old_ds, id, direction);

            dd.distance = new_ds.cost;
            heap.push(new_ds);
        }
    }

    fn get_dijkstra_data<'a>(
        map: &'a HashMap<ZoneID, DijkstraData>,
        id: ZoneID,
    ) -> &'a DijkstraData {
        map.get(&id)
            .expect(&format!("zone {} exists in dijkstra data map", id))
    }

    fn get_dijkstra_data_mut<'a>(
        map: &'a mut HashMap<ZoneID, DijkstraData>,
        id: ZoneID,
    ) -> &'a mut DijkstraData {
        map.get_mut(&id)
            .expect(&format!("zone {} exists in dijkstra data map", id))
    }

    fn ensure_zone(&mut self, zone_id: ZoneID) {
        if let Entry::Vacant(v) = self.0.entry(zone_id) {
            v.insert(Mutex::new(ZoneInfo::default()));
        }
    }

    /*
    fn get_zone_guard(&self, zone_id: ZoneID) -> MutexGuard<ZoneInfo> {
        self.0
            .get(&zone_id)
            .expect(&format!("Zone {} not in zone map", zone_id))
            .lock()
            .expect(&format!("Zone {} lock is poisoned", zone_id))
    }
     */

    fn get_zone_mut(&mut self, zone_id: ZoneID) -> &mut ZoneInfo {
        self.0
            .get_mut(&zone_id)
            .expect(&format!("Zone {} not in zone map", zone_id))
            .get_mut()
            .expect(&format!("Zone {} lock is poisoned", zone_id))
    }
}

#[derive(Debug)]
struct TestAddrParseError {
    failed_string: String,
    reason: String,
}

impl TestAddrParseError {
    fn new<T1, T2>(failed_string: T1, reason: T2) -> Self
    where
        T1: Into<String>,
        T2: Into<String>,
    {
        Self {
            failed_string: failed_string.into(),
            reason: reason.into(),
        }
    }
}

impl Display for TestAddrParseError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(
            f,
            "failed to parse TestAddr from {}: {}",
            self.failed_string, self.reason
        )
    }
}

impl StdError for TestAddrParseError {
    fn description(&self) -> &str {
        "failed to parse TestAddr from some string for some reason"
    }
}

#[derive(Debug)]
struct TestAddrParts {
    address_type: String,
    fields: Vec<String>,
    port: Option<u16>,
}

impl FromStr for TestAddrParts {
    type Err = TestAddrParseError;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        #[derive(PartialEq)]
        enum State {
            Start,
            OpenBracket,
            AddressType,
            CloseBracket,
            OpenBrace,
            CloseBrace,
            Colon,
            Port,
        };
        let mut state = State::Start;
        let final_states = vec![State::CloseBrace, State::Port];
        let mut address_type = String::new();
        let mut fields = Vec::new();
        let mut field = String::new();
        let mut maybe_port = None;

        for c in s.chars() {
            match state {
                State::Start => match c {
                    '[' => state = State::OpenBracket,
                    _ => return Err(Self::Err::new(s, "expected an opening bracket")),
                },
                State::OpenBracket => match c {
                    'a' ... 'z' | '-' => {
                        address_type.push(c);
                        state = State::AddressType;
                    }
                    _ => return Err(Self::Err::new(s, "expected an alphabetic ASCII char or a dash for the address type")),
                }
                State::AddressType => match c {
                    'a' ... 'z' | '-' => {
                        address_type.push(c);
                    }
                    ']' => state = State::CloseBracket,
                    _ => return Err(Self::Err::new(s, "expected an alphabetic ASCII char or a dash for the address type, or a closing bracket")),
                }
                State::CloseBracket => match c {
                    '{' => state = State::OpenBrace,
                    _ => return Err(Self::Err::new(s, "expected an opening brace for address contents")),
                }
                State::OpenBrace => match c {
                    'a' ... 'z' | '0' ... '9' => field.push(c),
                    ',' => {
                        fields.push(field);
                        field = String::new();
                    }
                    '}' => {
                        fields.push(field);
                        field = String::new();
                        state = State::CloseBrace;
                    }
                    _ => return Err(Self::Err::new(s, "expected either a closing brace or ASCII alphanumeric char for a field"))
                }
                State::CloseBrace => match c {
                    ':' => state = State::Colon,
                    _ => return Err(Self::Err::new(s, "expected a colon after the closing brace")),
                }
                State::Colon => match c {
                    '0' ... '9' => {
                        let mut port_str = String::new();

                        port_str.push(c);
                        maybe_port = Some(port_str);
                        state = State::Port;
                    }
                    _ => return Err(Self::Err::new(s, "expected a number after the colon")),
                }
                State::Port => match c {
                    '0' ... '9' => maybe_port.as_mut().unwrap().push(c),
                    _ => return Err(Self::Err::new(s, "expected a number for a port"))
                }
            }
        }

        if !final_states.contains(&state) {
            return Err(Self::Err::new(s, "premature end of address string"));
        }

        let port = match maybe_port {
            Some(port_str) => {
                let parsed_port = match port_str.parse::<u16>() {
                    Ok(port) => Ok(port),
                    Err(e) => Err(Self::Err::new(
                        s,
                        format!("still failed to parse port into a u16 number: {}", e),
                    )),
                }?;
                Some(parsed_port)
            }
            None => None,
        };

        Ok(Self {
            address_type,
            fields,
            port,
        })
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct TestPublicAddr {
    zone_id: ZoneID,
    idx: u8,
}

impl TestPublicAddr {
    fn new(zone_id: ZoneID, idx: u8) -> Self {
        Self { zone_id, idx }
    }

    fn from_parts(parts: TestAddrParts) -> StdResult<Self, String> {
        if parts.address_type != "public" {
            return Err(format!(
                "expected 'public' address type, got '{}'",
                parts.address_type
            ));
        }
        if let Some(port) = parts.port {
            return Err(format!("expected no port information, got '{}'", port));
        }

        if parts.fields.len() != 2 {
            return Err(format!(
                "expected exactly 2 fields, got {}",
                parts.fields.len()
            ));
        }

        let zone_id = parts.fields[0]
            .parse()
            .map_err(|e| format!("failed to get zone ID from first field: {}", e))?;
        let idx = parts.fields[1]
            .parse()
            .map_err(|e| format!("failed to get index from second field: {}", e))?;

        Ok(Self::new(zone_id, idx))
    }

    fn get_zone_id(&self) -> ZoneID {
        self.zone_id
    }

    fn get_valid_port() -> u16 {
        42
    }

    fn validate_port(port: u16) -> StdResult<u16, String> {
        let valid_port = Self::get_valid_port();
        if port == valid_port {
            Ok(valid_port)
        } else {
            Err(format!(
                "expected port for public address to be '{}', got '{}'",
                valid_port, port
            ))
        }
    }
}

impl Display for TestPublicAddr {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "[public]{{{},{}}}", self.zone_id, self.idx)
    }
}

impl FromStr for TestPublicAddr {
    type Err = TestAddrParseError;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        let parts = s.parse::<TestAddrParts>()?;

        Self::from_parts(parts)
            .map_err(|e| Self::Err::new(s, format!("badly formed public address: {}", e)))
    }
}

impl MyFromStr for TestPublicAddr {
    type MyErr = <Self as FromStr>::Err;
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct TestLocalAddr {
    zone_id: ZoneID,
    idx: u8,
}

impl TestLocalAddr {
    fn new(zone_id: ZoneID, idx: u8) -> Self {
        Self { zone_id, idx }
    }

    fn from_parts(parts: TestAddrParts) -> StdResult<Self, String> {
        if parts.address_type != "local" {
            return Err(format!(
                "expected 'local' address type, got '{}'",
                parts.address_type
            ));
        }
        if let Some(port) = parts.port {
            return Err(format!("expected no port information, got '{}'", port));
        }

        if parts.fields.len() != 2 {
            return Err(format!(
                "expected exactly 2 fields, got {}",
                parts.fields.len()
            ));
        }

        let zone_id = parts.fields[0]
            .parse()
            .map_err(|e| format!("failed to get zone ID from first field: {}", e))?;
        let idx = parts.fields[1]
            .parse()
            .map_err(|e| format!("failed to get index from second field: {}", e))?;

        Ok(Self::new(zone_id, idx))
    }

    fn get_zone_id(&self) -> ZoneID {
        self.zone_id
    }

    fn get_valid_port() -> u16 {
        85
    }

    fn validate_port(port: u16) -> StdResult<u16, String> {
        let valid_port = Self::get_valid_port();
        if port == valid_port {
            Ok(valid_port)
        } else {
            Err(format!(
                "expected port for local address to be '{}', got '{}'",
                valid_port, port
            ))
        }
    }
}

impl Display for TestLocalAddr {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "[local]{{{},{}}}", self.zone_id, self.idx)
    }
}

impl FromStr for TestLocalAddr {
    type Err = TestAddrParseError;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        let parts = s.parse::<TestAddrParts>()?;

        Self::from_parts(parts)
            .map_err(|e| Self::Err::new(s, format!("badly formed local address: {}", e)))
    }
}

impl MyFromStr for TestLocalAddr {
    type MyErr = <Self as FromStr>::Err;
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct TestPersistentMappingAddr {
    parent_zone_id: ZoneID,
    child_zone_id: ZoneID,
}

impl TestPersistentMappingAddr {
    fn new(parent_zone_id: ZoneID, child_zone_id: ZoneID) -> Self {
        Self {
            parent_zone_id,
            child_zone_id,
        }
    }

    fn from_parts(parts: TestAddrParts) -> StdResult<Self, String> {
        if parts.address_type != "perm-map" {
            return Err(format!(
                "expected 'perm-map' address type, got '{}'",
                parts.address_type
            ));
        }
        if let Some(port) = parts.port {
            return Err(format!("expected no port information, got '{}'", port));
        }

        if parts.fields.len() != 2 {
            return Err(format!(
                "expected exactly 2 fields, got {}",
                parts.fields.len()
            ));
        }

        let parent_zone_id = parts.fields[0]
            .parse()
            .map_err(|e| format!("failed to get parent zone ID from first field: {}", e))?;
        let child_zone_id = parts.fields[1]
            .parse()
            .map_err(|e| format!("failed to get child zone ID from second field: {}", e))?;

        Ok(Self::new(parent_zone_id, child_zone_id))
    }

    fn get_parent_zone_id(&self) -> ZoneID {
        self.parent_zone_id
    }

    fn get_child_zone_id(&self) -> ZoneID {
        self.child_zone_id
    }

    fn validate_port(port: u16) -> StdResult<u16, String> {
        if port <= u8::MAX.into() {
            Ok(port)
        } else {
            Err(format!("expected port to fit u8, got {}", port))
        }
    }
}

impl Display for TestPersistentMappingAddr {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(
            f,
            "[perm-map]{{{},{}}}",
            self.parent_zone_id, self.child_zone_id
        )
    }
}

impl FromStr for TestPersistentMappingAddr {
    type Err = TestAddrParseError;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        let parts = s.parse::<TestAddrParts>()?;

        Self::from_parts(parts).map_err(|e| {
            Self::Err::new(s, format!("badly formed persistent mapping address: {}", e))
        })
    }
}

impl MyFromStr for TestPersistentMappingAddr {
    type MyErr = <Self as FromStr>::Err;
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct TestTemporaryMappingAddr {
    parent_zone_id: ZoneID,
    parent_server_idx: u8,
    child_zone_id: ZoneID,
    child_server_idx: u8,
    random_value: u16,
}

impl TestTemporaryMappingAddr {
    fn new(
        parent_zone_id: ZoneID,
        parent_server_idx: u8,
        child_zone_id: ZoneID,
        child_server_idx: u8,
        random_value: u16,
    ) -> Self {
        Self {
            parent_zone_id,
            parent_server_idx,
            child_zone_id,
            child_server_idx,
            random_value,
        }
    }

    fn from_parts(parts: TestAddrParts) -> StdResult<Self, String> {
        if parts.address_type != "temp-map" {
            return Err(format!(
                "expected 'temp-map' address type, got '{}'",
                parts.address_type
            ));
        }
        if let Some(port) = parts.port {
            return Err(format!("expected no port information, got '{}'", port));
        }

        if parts.fields.len() != 5 {
            return Err(format!(
                "expected exactly 5 fields, got {}",
                parts.fields.len()
            ));
        }

        let parent_zone_id = parts.fields[0]
            .parse()
            .map_err(|e| format!("failed to get parent zone ID from first field: {}", e))?;
        let parent_server_idx = parts.fields[1]
            .parse()
            .map_err(|e| format!("failed to get parent server index from second field: {}", e))?;
        let child_zone_id = parts.fields[2]
            .parse()
            .map_err(|e| format!("failed to get child zone ID from third field: {}", e))?;
        let child_server_idx = parts.fields[3]
            .parse()
            .map_err(|e| format!("failed to get child server index from fourth field: {}", e))?;
        let random_value = parts.fields[4]
            .parse()
            .map_err(|e| format!("failed to get random u16 value from fifth field: {}", e))?;

        Ok(Self::new(
            parent_zone_id,
            parent_server_idx,
            child_zone_id,
            child_server_idx,
            random_value,
        ))
    }

    fn get_parent_zone_id(&self) -> ZoneID {
        self.parent_zone_id
    }

    fn validate_port(port: u16) -> StdResult<u16, String> {
        Ok(port)
    }
}

impl Display for TestTemporaryMappingAddr {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(
            f,
            "[temp-map]{{{},{},{},{},{}}}",
            self.parent_zone_id,
            self.parent_server_idx,
            self.child_zone_id,
            self.child_server_idx,
            self.random_value
        )
    }
}

impl FromStr for TestTemporaryMappingAddr {
    type Err = TestAddrParseError;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        let parts = s.parse::<TestAddrParts>()?;

        Self::from_parts(parts).map_err(|e| {
            Self::Err::new(s, format!("badly formed temporary mapping address: {}", e))
        })
    }
}

impl MyFromStr for TestTemporaryMappingAddr {
    type MyErr = <Self as FromStr>::Err;
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
enum TestAddr {
    Public(TestPublicAddr),
    Local(TestLocalAddr),
    PersistentMapping(TestPersistentMappingAddr),
    TemporaryMapping(TestTemporaryMappingAddr),
}

impl TestAddr {
    fn from_parts(parts: TestAddrParts) -> StdResult<Self, String> {
        match parts.address_type.as_str() {
            "public" => TestPublicAddr::from_parts(parts).map(|a| TestAddr::Public(a)),
            "local" => TestLocalAddr::from_parts(parts).map(|a| TestAddr::Local(a)),
            "perm-map" => {
                TestPersistentMappingAddr::from_parts(parts).map(|a| TestAddr::PersistentMapping(a))
            }
            "temp-map" => {
                TestTemporaryMappingAddr::from_parts(parts).map(|a| TestAddr::TemporaryMapping(a))
            }
            _ => Err(format!("unknown address type '{}'", parts.address_type)),
        }
    }

    fn get_zone_id(&self) -> ZoneID {
        match self {
            &TestAddr::Public(ref pip) => pip.get_zone_id(),
            &TestAddr::Local(ref lip) => lip.get_zone_id(),
            &TestAddr::PersistentMapping(ref pmip) => pmip.get_parent_zone_id(),
            &TestAddr::TemporaryMapping(ref tmip) => tmip.get_parent_zone_id(),
        }
    }

    fn validate_port(&self, port: u16) -> StdResult<u16, String> {
        match self {
            TestAddr::Public(_) => TestPublicAddr::validate_port(port),
            TestAddr::Local(_) => TestLocalAddr::validate_port(port),
            TestAddr::PersistentMapping(_) => TestPersistentMappingAddr::validate_port(port),
            TestAddr::TemporaryMapping(_) => TestTemporaryMappingAddr::validate_port(port),
        }
    }
}

impl Display for TestAddr {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            TestAddr::Public(addr) => addr.fmt(f),
            TestAddr::Local(addr) => addr.fmt(f),
            TestAddr::PersistentMapping(addr) => addr.fmt(f),
            TestAddr::TemporaryMapping(addr) => addr.fmt(f),
        }
    }
}

impl FromStr for TestAddr {
    type Err = TestAddrParseError;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        let parts = s.parse::<TestAddrParts>()?;
        let address_type = parts.address_type.clone();

        Ok(Self::from_parts(parts).map_err(|e| {
            Self::Err::new(
                s,
                format!("badly formed address of type '{}': {}", address_type, e),
            )
        })?)
    }
}

impl MyFromStr for TestAddr {
    type MyErr = <Self as FromStr>::Err;
}

impl Address for TestAddr {
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
struct TestAddrAndPort {
    addr: TestAddr,
    port: u16,
}

impl TestAddrAndPort {
    fn new(addr: TestAddr, port: u16) -> Self {
        assert!(addr.validate_port(port).is_ok());

        Self { addr, port }
    }

    pub fn get_zone_id(&self) -> ZoneID {
        self.addr.get_zone_id()
    }

    fn from_parts(mut parts: TestAddrParts) -> StdResult<Self, String> {
        let maybe_port = parts.port.take();
        let addr = TestAddr::from_parts(parts)?;
        let port = match maybe_port {
            Some(port) => addr.validate_port(port),
            None => Err(format!("missing port information")),
        }?;

        Ok(Self { addr, port })
    }
}

impl Display for TestAddrAndPort {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}:{}", self.addr, self.port)
    }
}

impl FromStr for TestAddrAndPort {
    type Err = TestAddrParseError;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        let parts = s.parse::<TestAddrParts>()?;
        let address_type = parts.address_type.clone();

        Ok(Self::from_parts(parts).map_err(|e| {
            Self::Err::new(
                s,
                format!(
                    "badly formed address and port of type '{}': {}",
                    address_type, e
                ),
            )
        })?)
    }
}

impl MyFromStr for TestAddrAndPort {
    type MyErr = <Self as FromStr>::Err;
}

impl AddressAndPort for TestAddrAndPort {
    type Address = TestAddr;

    fn new_from_address_and_port(addr: TestAddr, port: u16) -> Self {
        Self { addr, port }
    }

    fn get_address(&self) -> TestAddr {
        self.addr
    }

    fn get_port(&self) -> u16 {
        self.port
    }
}

fn create_member_from_addr(addr: TestAddrAndPort) -> Member {
    let mut member = Member::default();
    let port = addr.get_port() as i32;
    member.set_address(format!("{}", addr.get_address()));
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
    addr: TestAddrAndPort,
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

/*
// ExposedTestServer is a way for the supervisor in parent zone to be
// able to talk to the supervisor in child zone. The supervisor in
// child zone can be exposed to the parent zone, so the supervisor in
// parent zones can communicate with it.
struct ExposedTestServer {
    addr: SocketAddr,
}

impl ExposedTestServer {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}

impl TalkTarget for ExposedTestServer {
    fn create_member_info(&self) -> Member {
        create_member_from_addr(self.addr)
    }
}
 */

type ZoneToCountMap = HashMap<ZoneID, u8>;

struct Addresses {
    server_map: ZoneToCountMap,
    //exposed_map: ZoneToCountMap,
    mapping_map: ZoneToCountMap,
}

impl Addresses {
    pub fn new() -> Self {
        Self {
            server_map: HashMap::new(),
            //exposed_map: HashMap::new(),
            mapping_map: HashMap::new(),
        }
    }

    pub fn generate_public_address_for_server(&mut self, zone_id: ZoneID) -> TestAddrAndPort {
        let idx = self.get_server_idx(zone_id);
        let addr = TestAddr::Public(TestPublicAddr::new(zone_id, idx));

        TestAddrAndPort::new(addr, TestPublicAddr::get_valid_port())
    }

    pub fn generate_address_for_server(&mut self, zone_id: ZoneID) -> TestAddrAndPort {
        let idx = self.get_server_idx(zone_id);
        let addr = TestAddr::Local(TestLocalAddr::new(zone_id, idx));

        TestAddrAndPort::new(addr, TestLocalAddr::get_valid_port())
    }

    pub fn generate_persistent_mapping_address(
        &mut self,
        parent_zone_id: ZoneID,
        child_zone_id: ZoneID,
    ) -> TestAddrAndPort {
        let idx = self.get_mapping_idx(parent_zone_id);
        let addr = TestAddr::PersistentMapping(TestPersistentMappingAddr::new(
            parent_zone_id,
            child_zone_id,
        ));

        TestAddrAndPort::new(addr, idx.into())
    }

    /*
    pub fn generate_address_for_exposed_server(
        &mut self,
        server: &TestServer,
        exposed_zone_id: ZoneID,
    ) -> SocketAddr {
        let idx = Self::get_next_idx_for_zone(&mut self.exposed_map, exposed_zone_id) as u16;
        let server_zone_id_raw =
            Self::get_zone_from_address(&server.butterfly.read_network().get_swim_addr()).raw()
                as u16;
        let port = (server_zone_id_raw << 8) | idx;
        Self::generate_address(exposed_zone_id.raw(), 0, port)
    }
     */

    /*
    pub fn get_zone_from_address(addr: &SocketAddr) -> ZoneID {
        if let IpAddr::V4(ipv4) = addr.ip() {
            ZoneID(ipv4.octets()[0])
        } else {
            unreachable!("test address ({:?}) is not V4", addr);
        }
    }
     */

    fn get_server_idx(&mut self, zone_id: ZoneID) -> u8 {
        Self::get_next_idx_for_zone(&mut self.server_map, zone_id)
    }

    fn get_mapping_idx(&mut self, zone_id: ZoneID) -> u8 {
        Self::get_next_idx_for_zone(&mut self.mapping_map, zone_id)
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

    /*
    fn generate_address(zone_id_raw: u8, server_idx: u8, port: u16) -> SocketAddr {
        let ip = IpAddr::V4(Ipv4Addr::new(zone_id_raw, server_idx, 0, 0));
        SocketAddr::new(ip, port)
    }
     */
}

// TestMessage is a wrapper around the SWIM or gossip message sent by
// a butterfly server. Contains source and destination addresses used
// to determine a routing.
#[derive(Debug)]
struct TestMessage {
    source: TestAddrAndPort,
    target: TestAddrAndPort,
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
        Self {
            sender: Mutex::new(sender),
        }
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
type ChannelMap = HashMap<TestAddrAndPort, LockedSender<TestMessage>>;

#[derive(Copy, Clone, Debug)]
enum ChannelType {
    SWIM,
    Gossip,
}

/*
#[derive(Clone, Copy)]
struct TestNatOptions {
    asymmetric: bool,
    hairpinning: bool,
    ip_count: u8,
}
 */

type AddrToAddrMap = HashMap<TestAddrAndPort, TestAddrAndPort>;

struct Mappings {
    hole_to_internal: AddrToAddrMap,
    internal_to_hole: AddrToAddrMap,
}

impl Mappings {
    pub fn new() -> Self {
        Self {
            hole_to_internal: HashMap::new(),
            internal_to_hole: HashMap::new(),
        }
    }

    pub fn insert_both_ways(&mut self, hole: TestAddrAndPort, internal: TestAddrAndPort) {
        match self.hole_to_internal.entry(hole) {
            Entry::Vacant(v) => {
                v.insert(internal);
            }
            Entry::Occupied(_) => {
                unreachable!("mapping for hole {:?} already taken", hole);
            }
        };
        match self.internal_to_hole.entry(internal) {
            Entry::Vacant(v) => {
                v.insert(hole);
            }
            Entry::Occupied(_) => {
                unreachable!("mapping for internal {:?} already taken", internal);
            }
        };
    }

    pub fn hole_to_internal(&self, hole: TestAddrAndPort) -> Option<TestAddrAndPort> {
        Self::get_from_map(&self.hole_to_internal, hole)
    }

    pub fn internal_to_hole(&self, internal: TestAddrAndPort) -> Option<TestAddrAndPort> {
        Self::get_from_map(&self.internal_to_hole, internal)
    }

    fn get_from_map(map: &AddrToAddrMap, addr: TestAddrAndPort) -> Option<TestAddrAndPort> {
        map.get(&addr).cloned()
    }
}

#[derive(Copy, Clone)]
struct NatHole {
    addr: TestAddrAndPort,
}

impl NatHole {
    pub fn new(addr: TestAddrAndPort) -> Self {
        Self { addr }
    }
}

impl TalkTarget for NatHole {
    fn create_member_info(&self) -> Member {
        create_member_from_addr(self.addr)
    }
}

#[derive(Clone)]
struct TestNat {
    parent_id: ZoneID,
    child_id: ZoneID,
    addresses: Arc<Mutex<Addresses>>,
    //options: TestNatOptions,
    mappings: Arc<RwLock<Mappings>>,
    //TODO: temp_mappings: Arc<RwLock<MappingMap>>,
}

impl TestNat {
    pub fn new(
        parent_id: ZoneID,
        child_id: ZoneID,
        addresses: Arc<Mutex<Addresses>>,
        //options: TestNatOptions,
    ) -> Self {
        let mappings = Arc::new(RwLock::new(Mappings::new()));
        Self {
            parent_id,
            child_id,
            addresses,
            mappings,
        }
    }

    pub fn punch_hole(&mut self) -> NatHole {
        NatHole::new(
            self.get_addresses_guard()
                .generate_persistent_mapping_address(self.parent_id, self.child_id),
        )
    }

    pub fn make_route(&mut self, hole: NatHole, internal: TestAddrAndPort) {
        assert_eq!(hole.addr.get_zone_id(), self.parent_id);
        assert_eq!(internal.get_zone_id(), self.child_id);

        self.write_mappings().insert_both_ways(hole.addr, internal);
    }

    pub fn can_route(&self, msg: &mut TestMessage, ti: &TraversalInfo) -> bool {
        if ti.direction == Direction::ParentToChild {
            return false;
        }

        {
            let mappings = self.read_mappings();
            if let Some(hole) = mappings.internal_to_hole(msg.source) {
                msg.source = hole;
                return true;
            }
        }

        // TODO: handle temporary mappings

        return false;
    }

    pub fn route(&self, msg: &mut TestMessage) -> bool {
        let mappings = self.read_mappings();
        if let Some(internal) = mappings.hole_to_internal(msg.target) {
            msg.target = internal;
            return true;
        }

        // TODO: handle temporary mappings

        return false;
    }

    fn get_addresses_guard(&self) -> MutexGuard<Addresses> {
        self.addresses.lock().expect("Addresses lock is poisoned")
    }

    fn write_mappings(&self) -> RwLockWriteGuard<Mappings> {
        self.mappings.write().expect("Mappings lock is poisoned")
    }

    fn read_mappings(&self) -> RwLockReadGuard<Mappings> {
        self.mappings.read().expect("Mappings lock is poisoned")
    }
}

#[derive(Debug)]
struct TestSuitability(u64);
impl Suitability for TestSuitability {
    fn get(&self, _service_group: &ServiceGroup) -> u64 {
        self.0
    }
}

// ForwardsMap is a mapping from one IP address to another. Used in
// exposing the server from child zone in the parent zone.
//type ForwardsMap = HashMap<SocketAddr, SocketAddr>;
#[derive(PartialEq, Eq, Hash)]
struct NatsKey {
    first: ZoneID,
    second: ZoneID,
}

impl NatsKey {
    pub fn new(z1: ZoneID, z2: ZoneID) -> Self {
        let (first, second) = if z1.raw() < z2.raw() {
            (z1, z2)
        } else {
            (z2, z1)
        };
        Self { first, second }
    }
}

type NatsMap = HashMap<NatsKey, TestNat>;

// TestNetworkSwitchBoard implements the multizone setup for testing
// the spanning ring.
#[derive(Clone)]
struct TestNetworkSwitchBoard {
    zones: Arc<RwLock<ZoneMap>>,
    servers: Arc<RwLock<Vec<TestServer>>>,
    //forwards: Arc<RwLock<ForwardsMap>>,
    addresses: Arc<Mutex<Addresses>>,
    swim_channel_map: Arc<RwLock<ChannelMap>>,
    gossip_channel_map: Arc<RwLock<ChannelMap>>,
    nats: Arc<RwLock<NatsMap>>,
}

impl TestNetworkSwitchBoard {
    pub fn new() -> Self {
        Self {
            zones: Arc::new(RwLock::new(ZoneMap(HashMap::new()))),
            servers: Arc::new(RwLock::new(Vec::new())),
            //forwards: Arc::new(RwLock::new(HashMap::new())),
            addresses: Arc::new(Mutex::new(Addresses::new())),
            swim_channel_map: Arc::new(RwLock::new(HashMap::new())),
            gossip_channel_map: Arc::new(RwLock::new(HashMap::new())),
            nats: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn setup_nat(
        &self,
        parent_id: ZoneID,
        child_id: ZoneID,
        //options: Option<TestNatOptions>,
    ) -> TestNat {
        {
            let mut zones = self.write_zones();
            zones.setup_zone_relationship(parent_id, child_id);
        }
        let nat = TestNat::new(
            parent_id,
            child_id,
            Arc::clone(&self.addresses),
            //options: options,
        );
        let nats_key = NatsKey::new(child_id, parent_id);
        {
            let mut nats = self.write_nats();
            assert!(
                !nats.contains_key(&nats_key),
                "nat between zone {} and zone {} was already registered",
                child_id,
                parent_id,
            );
            nats.insert(nats_key, nat.clone());
        }
        nat
    }

    /*
    pub fn setup_zone_relationship(&self, parent_id: ZoneID, child_id: ZoneID) -> TestNat {
        let mut zones = self.write_zones();
        zones.setup_zone_relationship(parent_id, child_id);
    }
     */

    pub fn start_server_in_zone(&self, zone_id: ZoneID) -> TestServer {
        self.start_server_in_zone_with_holes(zone_id, Vec::new())
    }

    pub fn start_server_in_zone_with_holes(
        &self,
        zone_id: ZoneID,
        holes: Vec<NatHole>,
    ) -> TestServer {
        let addr = {
            let mut addresses = self.get_addresses_guard();
            addresses.generate_address_for_server(zone_id)
        };
        self.start_server(addr, holes)
    }

    pub fn start_public_server_in_zone(&self, zone_id: ZoneID) -> TestServer {
        self.start_public_server_in_zone_with_holes(zone_id, Vec::new())
    }

    pub fn start_public_server_in_zone_with_holes(
        &self,
        zone_id: ZoneID,
        holes: Vec<NatHole>,
    ) -> TestServer {
        let addr = {
            let mut addresses = self.get_addresses_guard();
            addresses.generate_public_address_for_server(zone_id)
        };
        self.start_server(addr, holes)
    }

    /*
    pub fn expose_server_in_zone(&self, server: &TestServer, zone_id: ZoneID) -> ExposedTestServer {
        let server_addr = server.butterfly.read_network().get_swim_addr();
        {
            let zone_map = self.read_zones();
            let server_zone_id = Addresses::get_zone_from_address(&server_addr);
            assert!(
                zone_map.is_zone_child_of(server_zone_id, zone_id),
                "only servers in child zones can be exposed in parent zones"
            );
        }
        let exposed_addr = {
            let mut addresses = self.get_addresses_guard();
            addresses.generate_address_for_exposed_server(&server, zone_id)
        };
        {
            let mut forwards = self.write_forwards();
            match forwards.entry(exposed_addr) {
                Entry::Vacant(v) => {
                    v.insert(server_addr);
                }
                Entry::Occupied(_) => {
                    unreachable!("should not happen, the generated address should be unique")
                }
            }
        }

        ExposedTestServer::new(exposed_addr)
    }
     */

    pub fn wait_for_health_all(&self, health: Health) -> bool {
        let server_count = self.read_servers().len();
        for l in 0..server_count {
            for r in 0..server_count {
                if l == r {
                    continue;
                }
                if !self.wait_for_health_of(l, r, health) {
                    return false;
                }
                if !self.wait_for_health_of(r, l, health) {
                    return false;
                }
            }
        }
        true
    }

    pub fn wait_for_same_settled_zone(&self, servers: Vec<&TestServer>) -> bool {
        self.wait_for_disjoint_settled_zones(vec![servers])
    }

    pub fn wait_for_disjoint_settled_zones(&self, disjoint_servers: Vec<Vec<&TestServer>>) -> bool {
        let rounds_in = self.gossip_rounds_in(4);
        loop {
            if Self::check_for_disjoint_settled_zones(&disjoint_servers) {
                return true;
            }
            if self.reached_max_rounds(&rounds_in) {
                return false;
            }
            thread::sleep(Duration::from_millis(500));
        }
    }

    fn check_for_disjoint_settled_zones(disjoint_servers: &Vec<Vec<&TestServer>>) -> bool {
        for servers in disjoint_servers.iter() {
            for pair in servers.windows(2) {
                let s0 = &pair[0];
                let s1 = &pair[1];

                if !s0.butterfly.is_zone_settled() {
                    return false;
                }
                if !s1.butterfly.is_zone_settled() {
                    return false;
                }
                if s0.butterfly.get_settled_zone_id() != s1.butterfly.get_settled_zone_id() {
                    return false;
                }
            }
        }

        let mut zone_uuids = disjoint_servers
            .iter()
            .filter_map(|v| v.first().map(|s| s.butterfly.get_settled_zone_id()))
            .collect::<Vec<_>>();
        zone_uuids.sort_unstable();

        let zones_count = zone_uuids.len();

        zone_uuids.dedup();
        zones_count == zone_uuids.len()
    }

    fn start_server(&self, addr: TestAddrAndPort, _holes: Vec<NatHole>) -> TestServer {
        let network = self.create_test_network(addr);
        let mut servers = self.write_servers();
        let idx = servers.len();
        let server = self.create_test_server(network, idx as u64);
        servers.push(server.clone());
        server
    }

    fn create_test_network(&self, addr: TestAddrAndPort) -> TestNetwork {
        let (swim_in, swim_out) = self.start_routing_thread(addr, ChannelType::SWIM);
        let (gossip_in, gossip_out) = self.start_routing_thread(addr, ChannelType::Gossip);
        TestNetwork::new(addr, swim_in, swim_out, gossip_in, gossip_out)
    }

    fn create_test_server(&self, network: TestNetwork, idx: u64) -> TestServer {
        let addr = network.get_addr();
        let member = create_member_from_addr(addr);
        let trace = Trace::default();
        let ring_key = None;
        let name = None;
        let data_path = None::<PathBuf>;
        let suitability = Box::new(TestSuitability(idx));
        let host_address = network.get_host_address().expect("failed to get host address");
        let mut butterfly = Server::new(
            network,
            host_address,
            member,
            trace,
            ring_key,
            name,
            data_path,
            suitability,
        );
        let timing = Timing::default();
        butterfly.start(timing).expect("failed to start server");
        TestServer { butterfly, addr }
    }

    fn wait_for_health_of(&self, from_idx: usize, to_idx: usize, health: Health) -> bool {
        let rounds_in = self.gossip_rounds_in(4);
        loop {
            if let Some(member_health) = self.health_of(from_idx, to_idx) {
                if member_health == health {
                    return true;
                }
            }
            if self.reached_max_rounds(&rounds_in) {
                return false;
            }
            thread::sleep(Duration::from_millis(500));
        }
    }

    pub fn reached_max_rounds(&self, rounds_in: &Vec<isize>) -> bool {
        let servers = self.read_servers();
        for (idx, round) in rounds_in.into_iter().enumerate() {
            let server = &servers[idx];
            if server.butterfly.paused() || server.butterfly.swim_rounds() > *round {
                continue;
            }
            return false;
        }
        true
    }

    fn health_of(&self, from_idx: usize, to_idx: usize) -> Option<Health> {
        let servers = self.read_servers();
        let to_member = servers[to_idx]
            .butterfly
            .member
            .read()
            .expect("Member lock is poisoned");
        servers[from_idx]
            .butterfly
            .member_list
            .health_of(&to_member)
    }

    fn gossip_rounds(&self) -> Vec<isize> {
        let servers = self.read_servers();
        servers
            .iter()
            .map(|s| s.butterfly.gossip_rounds())
            .collect()
    }

    fn gossip_rounds_in(&self, count: isize) -> Vec<isize> {
        self.gossip_rounds().iter().map(|r| r + count).collect()
    }

    fn start_routing_thread(
        &self,
        addr: TestAddrAndPort,
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

    fn process_msg(&self, mut msg: TestMessage, channel_type: ChannelType) {
        let src = msg.source;
        let tgt = msg.target;
        let source_zone_id = match &msg.source.addr {
            &TestAddr::Public(ref pip) => pip.get_zone_id(),
            &TestAddr::Local(ref lip) => lip.get_zone_id(),
            _ => {
                unreachable!(
                    "expected source address to be either local or public, but it is {:?}",
                    msg.source,
                );
            }
        };

        let can_route_across_zones = {
            if let &TestAddr::Public(_) = &msg.target.addr {
                true
            } else {
                let target_zone_id = msg.target.addr.get_zone_id();
                let zone_map = self.read_zones();
                let nats = self.read_nats();
                if let Some(route) = zone_map.get_route(source_zone_id, target_zone_id) {
                    let mut can_route = true;
                    for traversal_info in route {
                        let nats_key = NatsKey::new(traversal_info.from, traversal_info.to);
                        if let Some(nat) = nats.get(&nats_key) {
                            if !nat.can_route(&mut msg, &traversal_info) {
                                can_route = false;
                                break;
                            }
                        } else {
                            can_route = false;
                            break;
                        }
                    }
                    can_route
                } else {
                    false
                }
            }
        };
        let routed = if can_route_across_zones {
            let mut routed = true;
            loop {
                let nats = self.read_nats();
                match msg.target.addr {
                    TestAddr::PersistentMapping(m) => {
                        let parent_id = m.get_parent_zone_id();
                        let child_id = m.get_child_zone_id();
                        let nats_key = NatsKey::new(parent_id, child_id);
                        if let Some(nat) = nats.get(&nats_key) {
                            routed = nat.route(&mut msg);
                            if !routed {
                                break;
                            }
                        } else {
                            routed = false;
                        }
                    }
                    TestAddr::TemporaryMapping(_) => {
                        unreachable!("not implemented yet");
                    }
                    _ => break,
                }
            }
            routed
        } else {
            false
        };
        if routed {
            let maybe_out = {
                let map = self.read_channel_map(channel_type);
                map.get(&msg.target).map(|l| l.cloned_sender())
            };
            if let Some(out) = maybe_out {
                let target = msg.target;
                if out.send(msg).is_err() {
                    let mut map = self.write_channel_map(channel_type);
                    map.remove(&target);
                }
            }
        }
        println!("source: {}, target: {}, channel type: {:?}, can route across zones: {}, routed: {}", src, tgt, channel_type, can_route_across_zones, routed);
    }

    /*
    fn process_msg(&self, msg: TestMessage, channel_type: ChannelType) {
        let can_route = {
            let source_zone_id = Addresses::get_zone_from_address(&msg.source);
            let target_zone_id = Addresses::get_zone_from_address(&msg.target);
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
            let real_target = {
                let forwards = self.read_forwards();
                let addr_ref = forwards.get(&msg.target).unwrap_or(&msg.target);
                *addr_ref
            };
            let maybe_out = {
                let map = self.read_channel_map(channel_type);
                map.get(&real_target).map(|l| l.cloned_sender())
            };
            if let Some(out) = maybe_out {
                if out.send(msg).is_err() {
                    let mut map = self.write_channel_map(channel_type);
                    map.remove(&real_target);
                }
            }
        }
    }
    */

    fn read_zones(&self) -> RwLockReadGuard<ZoneMap> {
        self.zones.read().expect("Zone map lock is poisoned")
    }

    fn write_zones(&self) -> RwLockWriteGuard<ZoneMap> {
        self.zones.write().expect("Zone map lock is poisoned")
    }

    fn get_addresses_guard(&self) -> MutexGuard<Addresses> {
        self.addresses.lock().expect("Addresses lock is poisoned")
    }

    /*
    fn read_forwards(&self) -> RwLockReadGuard<ForwardsMap> {
        self.forwards.read().expect("Forwards lock is poisoned")
    }
     */

    /*
    fn write_forwards(&self) -> RwLockWriteGuard<ForwardsMap> {
        self.forwards.write().expect("Forwards lock is poisoned")
    }
     */

    fn read_servers(&self) -> RwLockReadGuard<Vec<TestServer>> {
        self.servers.read().expect("Servers lock is poisoned")
    }

    fn write_servers(&self) -> RwLockWriteGuard<Vec<TestServer>> {
        self.servers.write().expect("Servers lock is poisoned")
    }

    fn read_channel_map(&self, channel_type: ChannelType) -> RwLockReadGuard<ChannelMap> {
        self.get_channel_map_lock(channel_type)
            .read()
            .expect("Channel map lock is poisoned")
    }

    fn write_channel_map(&self, channel_type: ChannelType) -> RwLockWriteGuard<ChannelMap> {
        self.get_channel_map_lock(channel_type)
            .write()
            .expect("Channel map lock is poisoned")
    }

    fn get_channel_map_lock(&self, channel_type: ChannelType) -> &RwLock<ChannelMap> {
        match channel_type {
            ChannelType::SWIM => &self.swim_channel_map,
            ChannelType::Gossip => &self.gossip_channel_map,
        }
    }

    fn read_nats(&self) -> RwLockReadGuard<NatsMap> {
        self.nats.read().expect("Nats lock is poisoned")
    }

    fn write_nats(&self) -> RwLockWriteGuard<NatsMap> {
        self.nats.write().expect("Nats lock is poisoned")
    }
}

// TestSwimSender is an implementation of a SwimSender trait based on
// channels.
#[derive(Debug)]
struct TestSwimSender {
    addr: TestAddrAndPort,
    sender: LockedSender<TestMessage>,
}

impl SwimSender<TestAddrAndPort> for TestSwimSender {
    fn send(&self, buf: &[u8], addr: TestAddrAndPort) -> Result<usize> {
        let msg = TestMessage {
            source: self.addr,
            target: addr,
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

impl SwimReceiver<TestAddrAndPort> for TestSwimReceiver {
    fn receive(&self, buf: &mut [u8]) -> Result<(usize, TestAddrAndPort)> {
        let msg = self.0.recv().map_err(|_| {
            Error::SwimReceiveError("Sender part of the channel is disconnected".to_owned())
        })?;
        let len = cmp::min(msg.bytes.len(), buf.len());
        buf[..len].copy_from_slice(&msg.bytes);
        Ok((len, msg.source))
    }
}

// TestGossipSender is an implementation of a GossipSender trait based
// on channels.
struct TestGossipSender {
    source: TestAddrAndPort,
    target: TestAddrAndPort,
    sender: Sender<TestMessage>,
}

impl GossipSender for TestGossipSender {
    fn send(&self, buf: &[u8]) -> Result<()> {
        let msg = TestMessage {
            source: self.source,
            target: self.target,
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
    addr: TestAddrAndPort,
    swim_in: LockedSender<TestMessage>,
    swim_out: Mutex<Option<Receiver<TestMessage>>>,
    gossip_in: LockedSender<TestMessage>,
    gossip_out: Mutex<Option<Receiver<TestMessage>>>,
}

impl TestNetwork {
    pub fn new(
        addr: TestAddrAndPort,
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

    pub fn get_addr(&self) -> TestAddrAndPort {
        self.addr
    }
}

impl Network for TestNetwork {
    type AddressAndPort = TestAddrAndPort;
    type SwimSender = TestSwimSender;
    type SwimReceiver = TestSwimReceiver;
    type GossipSender = TestGossipSender;
    type GossipReceiver = TestGossipReceiver;

    fn get_host_address(&self) -> Result<TestAddr> {
        Ok(self.addr.get_address())
    }

    fn get_swim_addr(&self) -> TestAddrAndPort {
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
            .take()
        {
            Some(receiver) => Ok(TestSwimReceiver(receiver)),
            None => Err(Error::SwimChannelSetupError(format!(
                "no test swim receiver, should not happen"
            ))),
        }
    }

    fn get_gossip_addr(&self) -> TestAddrAndPort {
        self.addr
    }

    fn create_gossip_sender(&self, addr: TestAddrAndPort) -> Result<Self::GossipSender> {
        Ok(Self::GossipSender {
            source: self.addr,
            target: addr,
            sender: self.gossip_in.cloned_sender(),
        })
    }

    fn create_gossip_receiver(&self) -> Result<Self::GossipReceiver> {
        match self.gossip_out
            .lock()
            .expect("Gossip receiver lock is poisoned")
            .take()
        {
            Some(receiver) => Ok(TestGossipReceiver(receiver)),
            None => Err(Error::SwimChannelSetupError(format!(
                "no test gossip receiver, should not happen"
            ))),
        }
    }
}