// Copyright (c) 2018 Chef Software Inc. and/or applicable contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Tracks zones. Contains both the `Zone` struct and the `Zones`.

use std::cmp::Ordering;
use std::collections::{hash_map::Entry, HashMap, HashSet, VecDeque};
//use std::error::Error;
//use std::fmt::{Display, Formatter, Result as FmtResult};
use std::mem;
use std::ops::{Deref, DerefMut};
//use std::str::FromStr;

use protobuf::RepeatedField;

use message::{
    swim::{Rumor as ProtoRumor, Rumor_Type as ProtoRumorType, Zone as ProtoZone, ZoneAddress as ProtoZoneAddress},
    BfUuid, UuidSimple,
};
use network::{Address, AddressAndPort, Network};
use rumor::RumorKey;

/// A zone in the swim group. Passes most of its functionality along
/// to the internal protobuf representation.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Zone {
    pub proto: ProtoZone,
}

impl Zone {
    pub fn new(id: UuidSimple, maintainer_id: UuidSimple) -> Self {
        let mut proto_zone = ProtoZone::new();
        proto_zone.set_id(id);
        proto_zone.set_incarnation(0);
        proto_zone.set_maintainer_id(maintainer_id);
        Zone { proto: proto_zone }
    }

    pub fn get_uuid(&self) -> BfUuid {
        BfUuid::parse_or_nil(self.proto.get_id(), "zone ID")
    }
}

impl Deref for Zone {
    type Target = ProtoZone;

    fn deref(&self) -> &ProtoZone {
        &self.proto
    }
}

impl DerefMut for Zone {
    fn deref_mut(&mut self) -> &mut ProtoZone {
        &mut self.proto
    }
}

impl From<ProtoZone> for Zone {
    fn from(zone: ProtoZone) -> Zone {
        Zone { proto: zone }
    }
}

impl<'a> From<&'a ProtoZone> for Zone {
    fn from(zone: &'a ProtoZone) -> Zone {
        Zone {
            proto: zone.clone(),
        }
    }
}

impl From<Zone> for RumorKey {
    fn from(zone: Zone) -> RumorKey {
        RumorKey::new(ProtoRumorType::Zone, zone.get_id(), "")
    }
}

impl<'a> From<&'a Zone> for RumorKey {
    fn from(zone: &'a Zone) -> RumorKey {
        RumorKey::new(ProtoRumorType::Zone, zone.get_id(), "")
    }
}

impl<'a> From<&'a &'a Zone> for RumorKey {
    fn from(zone: &'a &'a Zone) -> RumorKey {
        RumorKey::new(ProtoRumorType::Zone, zone.get_id(), "")
    }
}

impl From<ProtoRumor> for Zone {
    fn from(mut pr: ProtoRumor) -> Zone {
        Zone {
            proto: pr.take_zone(),
        }
    }
}

struct ZoneAliasList {
    vecs: Vec<Vec<BfUuid>>,
    map: HashMap<BfUuid, usize>,
}

impl ZoneAliasList {
    fn new() -> Self {
        Self {
            vecs: Vec::new(),
            map: HashMap::new(),
        }
    }

    fn ensure_id(&mut self, uuid: BfUuid) -> usize {
        match self.map.entry(uuid) {
            Entry::Occupied(oe) => *(oe.get()),
            Entry::Vacant(ve) => {
                let idx = self.vecs.len();

                self.vecs.push(vec![uuid]);
                ve.insert(idx);
                idx
            }
        }
    }

    fn is_alias_of(&self, uuid1: BfUuid, uuid2: BfUuid) -> bool {
        match (self.map.get(&uuid1), self.map.get(&uuid2)) {
            (Some(idx1), Some(idx2)) => idx1 == idx2,
            (_, _) => false,
        }
    }

    fn take_aliases_from(&mut self, uuid1: BfUuid, uuid2: BfUuid) {
        let idx1 = self.ensure_id(uuid1);
        match self.map.entry(uuid2) {
            Entry::Occupied(mut oe) => {
                let idx2 = *oe.get();
                if idx1 == idx2 {
                    return;
                }
                *(oe.get_mut()) = idx1;
                let old_ids = mem::replace(&mut self.vecs[idx2], Vec::new());
                self.vecs[idx1].extend(old_ids);
            }
            Entry::Vacant(ve) => {
                self.vecs[idx1].push(uuid2);
                ve.insert(idx1);
            }
        }
    }

    fn into_max_set(self) -> HashSet<BfUuid> {
        let mut indices = self.map.values().collect::<Vec<_>>();

        indices.sort_unstable();
        indices.dedup();

        let mut set = HashSet::with_capacity(indices.len());

        for idx in indices {
            let max_uuid = self.vecs[*idx].iter().max().unwrap();

            set.insert(*max_uuid);
        }

        return set;
    }
}

pub enum Reachable {
    Yes,
    ThroughOtherZone(String),
    No,
}

#[derive(Debug)]
pub struct ZoneList {
    pub zones: HashMap<UuidSimple, Zone>,
    pub maintained_zone_id: Option<UuidSimple>,

    update_counter: usize,
}

impl ZoneList {
    pub fn new() -> Self {
        Self {
            zones: HashMap::new(),
            maintained_zone_id: None,
            update_counter: 0,
        }
    }

    pub fn available_zone_ids(&self) -> Vec<UuidSimple> {
        self.zones.keys().cloned().collect()
    }

    pub fn get_update_counter(&self) -> usize {
        self.update_counter
    }

    pub fn insert(&mut self, zone: Zone) -> Vec<RumorKey> {
        let keys = self.insert_internal(zone);

        if !keys.is_empty() {
            self.update_counter += 1;
        }

        keys
    }

    pub fn gather_all_aliases_of(&self, id: &str) -> HashSet<UuidSimple> {
        let mut aliases = HashSet::new();

        aliases.insert(id.to_string());
        if let Some(zone) = self.zones.get(id) {
            if zone.has_successor() {
                aliases.insert(zone.get_successor().to_string());
            }
            for zone_id in zone.get_predecessors().iter() {
                aliases.insert(zone_id.to_string());
            }
        }

        aliases
    }

    pub fn directly_reachable(
        &self,
        our_zone_id: &str,
        their_zone_id: &str,
        our_zone_addresses: &[ProtoZoneAddress],
        their_zone_addresses: &[ProtoZoneAddress],
    ) -> Reachable {
        if our_zone_id == their_zone_id {
            return Reachable::Yes;
        }

        let our_ids = self.gather_all_aliases_of(&our_zone_id);
        let their_ids = self.gather_all_aliases_of(their_zone_id);

        if !our_ids.is_disjoint(&their_ids) {
            return Reachable::Yes;
        }

        // TODO(krnowak): maybe instead of guessing which zone is
        // parent or child, take this information from the zone itself
        // (get_child_zone_ids(), get_parent_zone_id())

        // if this server is in child zone and is a gateway, and
        // member is in parent zone then this loop may catch that
        for zone_address in our_zone_addresses {
            let additional_zone_id = zone_address.get_zone_id();

            if their_ids.contains(additional_zone_id) {
                return Reachable::Yes;
            }
        }

        // if this server is in parent zone, and member is in child
        // zone and is a gateway then this loop may catch that
        for zone_address in their_zone_addresses {
            let additional_zone_id = zone_address.get_zone_id();

            if our_ids.contains(additional_zone_id) {
                return Reachable::ThroughOtherZone(additional_zone_id.to_string());
            }
        }

        Reachable::No
    }

    fn insert_internal(&mut self, mut zone: Zone) -> Vec<RumorKey> {
        let zone_uuid = zone.get_uuid();

        if zone_uuid.is_nil() {
            return Vec::new();
        }

        let current_zone = match self.zones.get(zone.get_id()).cloned() {
            Some(cz) => cz,
            None => {
                let rk = RumorKey::from(&zone);

                self.zones.insert(zone.get_id().to_string(), zone);

                return vec![rk];

                //return self.make_zones_consistent(zone);
            }
        };

        match current_zone.get_incarnation().cmp(&zone.get_incarnation()) {
            Ordering::Greater => Vec::new(),
            Ordering::Less => self.make_zones_consistent(zone),
            Ordering::Equal => {
                let mut predecessors = HashSet::new();
                // merge the info from current and new zone, but
                // do not increment the incarnation…
                match (zone.has_successor(), current_zone.has_successor()) {
                    (true, true) => {
                        let successor_uuid = BfUuid::must_parse(zone.get_successor());
                        let current_successor_uuid =
                            BfUuid::must_parse(current_zone.get_successor());

                        match successor_uuid.cmp(&current_successor_uuid) {
                            Ordering::Greater => {
                                predecessors.insert(current_successor_uuid.to_string());
                            }
                            Ordering::Equal => (),
                            Ordering::Less => {
                                predecessors.insert(successor_uuid.to_string());
                                zone.set_successor(current_successor_uuid.to_string());
                            }
                        }
                    }
                    (true, false) => {}
                    (false, true) => {
                        zone.set_successor(current_zone.get_successor().to_string());
                    }
                    (false, false) => {}
                }

                predecessors.extend(
                    current_zone
                        .get_predecessors()
                        .iter()
                        .map(|z| z.to_string()),
                );
                predecessors.extend(zone.get_predecessors().iter().map(|z| z.to_string()));

                zone.set_predecessors(predecessors.drain().collect());

                match (zone.has_parent_zone_id(), current_zone.has_parent_zone_id()) {
                    (true, true) => {
                        let current_parent = current_zone.get_parent_zone_id();

                        if self.is_alias_of(zone.get_parent_zone_id(), current_parent) {
                            let parent_uuid = BfUuid::must_parse(zone.get_parent_zone_id());
                            let current_parent_uuid = BfUuid::must_parse(&current_parent);

                            if current_parent_uuid > parent_uuid {
                                zone.set_parent_zone_id(current_parent.to_string());
                            }
                        } else {
                            println!(
                                "PARENTS: looks like a new parent ({}) for zone {} is not an alias of {}",
                                zone.get_parent_zone_id(),
                                zone.get_id(),
                                current_parent,
                            );
                            zone.set_parent_zone_id(current_parent.to_string());
                        }
                    }
                    (false, false) => (),
                    (true, false) => (),
                    (false, true) => {
                        zone.set_parent_zone_id(current_zone.get_parent_zone_id().to_string());
                    }
                }
                self.make_zones_consistent(zone)
            }
        }
    }

    fn is_alias_of(&self, id1: &str, id2: &str) -> bool {
        if id1 == id2 {
            return true;
        }

        if let Some(zone) = self.zones.get(id1) {
            if zone.has_successor() && zone.get_successor() == id2 {
                return true;
            }
            if zone.get_predecessors().iter().any(|id| id == id2) {
                return true;
            }
        }
        if let Some(zone) = self.zones.get(id2) {
            if zone.has_successor() && zone.get_successor() == id1 {
                return true;
            }
            if zone.get_predecessors().iter().any(|id| id == id1) {
                return true;
            }
        }

        false
    }

    fn make_zones_consistent(&mut self, zone: Zone) -> Vec<RumorKey> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut aliases = HashSet::new();
        let mut original_parent = None;
        let mut parents = HashSet::new();
        let mut children = HashSet::new();

        aliases.insert(zone.get_uuid());
        if zone.has_successor() {
            aliases.insert(BfUuid::must_parse(zone.get_successor()));
        }
        aliases.extend(
            zone.get_predecessors()
                .iter()
                .map(|id| BfUuid::must_parse(id)),
        );

        if zone.has_parent_zone_id() {
            let uuid = BfUuid::must_parse(zone.get_parent_zone_id());

            parents.insert(uuid);
            original_parent = Some(uuid);
        }
        children.extend(zone.get_child_zone_ids().iter().map(|id| BfUuid::must_parse(id)));
        queue.extend(aliases.iter().cloned());

        visited.insert(zone.get_uuid());
        while let Some(uuid) = queue.pop_front() {
            if visited.contains(&uuid) {
                continue;
            }
            visited.insert(uuid);
            if let Some(other_zone) = self.zones.get(&uuid.to_string()) {
                aliases.insert(uuid);
                if other_zone.has_successor() {
                    let successor = BfUuid::must_parse(other_zone.get_successor());

                    aliases.insert(successor);
                    queue.push_back(successor);
                }

                let predecessors = other_zone
                    .get_predecessors()
                    .iter()
                    .map(|i| BfUuid::must_parse(i))
                    .collect::<Vec<_>>();

                aliases.extend(predecessors.iter());
                queue.extend(predecessors);
                children.extend(other_zone.get_child_zone_ids().iter().map(|id| BfUuid::must_parse(id)));
            }
        }

        let successor = match aliases.iter().max() {
            Some(id) => *id,
            None => return vec![RumorKey::from(&zone)],
        };
        let predecessors = aliases
            .drain()
            .filter(|i| *i < successor)
            .collect::<Vec<BfUuid>>();
        let parent = {
            let mut zone_ids = self.filter_aliases(parents);

            match zone_ids.len() {
                0 => original_parent,
                1 => zone_ids.drain().next(),
                _ => {
                    println!("PARENTS: got some unrelated parents, {:#?}, using the original one {:#?}", zone_ids, original_parent);
                    original_parent
                }
            }
        };
        let final_children = self.filter_aliases(children);
        let mut rumor_keys = Vec::new();

        for zone_uuid in visited.drain() {
            let mut changed = false;
            let mut other_zone = match self.zones.get(&zone_uuid.to_string()).cloned() {
                Some(oz) => oz,
                None => continue,
            };
            let mut new_successor = false;
            let mut new_parent = None;

            if successor != zone_uuid {
                if other_zone.has_successor() {
                    if BfUuid::must_parse(other_zone.get_successor()) < successor {
                        new_successor = true;
                    }
                } else {
                    new_successor = true;
                }
            }
            if new_successor {
                other_zone.set_successor(successor.to_string());
                changed = true;
            }
            match (other_zone.has_parent_zone_id(), parent) {
                (true, Some(uuid)) => {
                    if BfUuid::must_parse(other_zone.get_parent_zone_id()) < uuid {
                        new_parent = Some(uuid);
                    }
                }
                (true, None) => {
                    println!("PARENTS: we had one parent in {:#?}, now we are supposed to have none?", other_zone);
                    // eh?
                }
                (false, Some(uuid)) => {
                    new_parent = Some(uuid);
                }
                (false, None) => {}
            }
            if let Some(uuid) = new_parent {
                other_zone.set_parent_zone_id(uuid.to_string());
                changed = true;
            }

            let mut filtered_predecessors = predecessors
                .iter()
                .filter(|uuid| **uuid != zone_uuid)
                .cloned()
                .collect::<HashSet<BfUuid>>();
            let old_predecessors = other_zone
                .get_predecessors()
                .iter()
                .map(|i| BfUuid::must_parse(i))
                .collect::<HashSet<BfUuid>>();

            // filtered predecessors is either a superset of old
            // predecessors or it's equal to it, so we can just use
            // difference instead of symmetric difference
            if filtered_predecessors
                .difference(&old_predecessors)
                .next()
                .is_some()
            {
                other_zone.set_predecessors(RepeatedField::from_vec(
                    filtered_predecessors
                        .drain()
                        .map(|uuid| uuid.to_string())
                        .collect(),
                ));
                changed = true;
            }

            let old_children = other_zone
                .get_child_zone_ids()
                .iter()
                .map(|id| BfUuid::must_parse(id))
                .collect::<HashSet<_>>();

            if final_children
                .symmetric_difference(&old_children)
                .next()
                .is_some() {
                    other_zone.set_child_zone_ids(RepeatedField::from_vec(
                        final_children
                            .iter()
                            .map(|uuid| uuid.to_string())
                            .collect(),
                    ));
                    changed = true;
                }

            if changed {
                if self.is_maintained_zone(other_zone.get_id()) {
                    let incarnation = other_zone.get_incarnation();

                    other_zone.set_incarnation(incarnation + 1);
                }

                rumor_keys.push(RumorKey::from(&other_zone));
                self.zones.insert(zone_uuid.to_string(), other_zone);
            }
        }

        rumor_keys
    }

    fn filter_aliases(&self, zone_uuids: HashSet<BfUuid>) -> HashSet<BfUuid> {
        match zone_uuids.len() {
            0 | 1 => zone_uuids,
            len => {
                let id_pairs = zone_uuids.iter().map(|uuid| (*uuid, uuid.to_string())).collect::<Vec<_>>();
                let mut zone_alias_list = ZoneAliasList::new();

                zone_alias_list.ensure_id(id_pairs[0].0);
                for first_idx in 0 .. (len - 1) {
                    let id_pair1 = &id_pairs[first_idx];

                    for second_idx in (first_idx + 1) .. len {
                        let id_pair2 = &id_pairs[second_idx];

                        if zone_alias_list.is_alias_of(id_pair1.0, id_pair2.0) {
                            continue;
                        }
                        if self.is_alias_of(&id_pair1.1, &id_pair2.1) {
                            zone_alias_list.take_aliases_from(id_pair1.0, id_pair2.0);
                        } else {
                            zone_alias_list.ensure_id(id_pair2.0);
                        }
                    }
                }
                zone_alias_list.into_max_set()
            }
        }
    }

    /*
    pub fn insert(&mut self, mut zone: Zone) -> bool {
        let zone_uuid = zone.get_uuid();
        if zone_uuid.is_nil() {
            return false;
        }

        let share_rumor = if let Some(current_zone) = self.zones.get(zone.get_id()) {
            match current_zone.get_incarnation().cmp(zone.get_incarnation()) {
                Ordering::Greater => false,
                Ordering::Less => true,
                Ordering::Equal => {
                    let mut changed = false;
                    let mut predecessors = HashSet::new();
                    // merge the info from current and new zone, but
                    // do not increment the incarnation…
                    match (zone.has_successor(), current_zone.has_successor()) {
                        (true, true) => {
                            let successor_uuid = BfUuid::must_parse(zone.get_successor());
                            let current_successor_uuid = BfUuid::must_parse(current_zone.get_successor());

                            match successor_uuid.cmp(&current_successor_uuid) {
                                Ordering::Greater => {
                                    predecessors.insert(current_successor_uuid.to_string());
                                    // changed because we have a new successor
                                    changed = true;
                                }
                                Ordering::Equal => (),
                                Ordering::Less => {
                                    predecessors.insert(successor_uuid.to_string());
                                    zone.set_successor(current_successor_uuid.to_string());
                                    // successor remains the same, so no change
                                }
                            }
                        }
                        (true, false) => {
                            // changed because we have a successor
                            changed = true;
                        }
                        (false, true) => {
                            // successor remains the same, so no change
                            zone.set_successor(current_zone.get_successor().to_string());
                        }
                        (false, false) => {
                            // nothing to do
                        }
                    }

                    predecessors.extend(current_zone.get_predecessors().iter().map(|z| z.to_string()));
                    predecessors.extend(zone.get_predecessors().iter().map(|z| z.to_string()));

                    if !changed {
                        let old_predecessors = current_zone.get_predecessors().iter().map(|z| z.to_string()).collect::<HashSet<_>>();

                        changed = predecessors.difference(&old_predecessors).next().is_some();
                    }
                    if changed {
                        zone.set_predecessors(predecessors.drain().collect());
                        if self.is_maintained_zone(zone.get_id()) {
                            let incarnation = zone.get_incarnation();

                            zone.set_incarnation(incarnation + 1);
                        }
                    }
                    changed
                }
            }
        } else {
            true
        };
        if share_rumor {
            self.zones.insert(zone.get_id().to_string(), zone);
        }
        share_rumor
    }
    */

    fn is_maintained_zone(&self, zone_id: &str) -> bool {
        if let Some(ref maintained_zone_id) = self.maintained_zone_id {
            zone_id == maintained_zone_id
        } else {
            false
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct AdditionalAddress<A: Address> {
    pub address: Option<A>,
    pub swim_port: u16,
    pub gossip_port: u16,
}

impl<A: Address> AdditionalAddress<A> {
    pub fn new(address: Option<A>, swim_port: u16, gossip_port: u16) -> Self {
        Self { address, swim_port, gossip_port }
    }
}

pub type TaggedAddressesFromAddress<A /*: Address*/> = HashMap<String, AdditionalAddress<A>>;
pub type TaggedAddressesFromNetwork<N/*: Network*/> = TaggedAddressesFromAddress<<<N as Network>::AddressAndPort as AddressAndPort>::Address>;

/*
#[derive(Debug)]
pub struct ExposeDataParseError {
    kind: ExposeDataParseErrorKind,
}

#[derive(Debug)]
enum ExposeDataParseErrorKind {
    InvalidFormat,
    InvalidAddress,
    InvalidSwimPort,
    InvalidGossipPort,
}

impl ExposeDataParseError {
    fn new_bad_format() -> Self {
        Self {
            kind: ExposeDataParseErrorKind::InvalidFormat,
        }
    }

    fn new_bad_address() -> Self {
        Self {
            kind: ExposeDataParseErrorKind::InvalidAddress,
        }
    }

    fn new_bad_swim_port() -> Self {
        Self {
            kind: ExposeDataParseErrorKind::InvalidSwimPort,
        }
    }

    fn new_bad_gossip_port() -> Self {
        Self {
            kind: ExposeDataParseErrorKind::InvalidGossipPort,
        }
    }

    fn describe(&self) -> &str {
        match self.kind {
            ExposeDataParseErrorKind::InvalidFormat => {
                "invalid format of the expose data string, should be either \
                 <address>:<swim port>:<gossip port> or <swim port>:<gossip_port>"
            }
            ExposeDataParseErrorKind::InvalidAddress => "invalid address in the expose data string",
            ExposeDataParseErrorKind::InvalidSwimPort => {
                "invalid SWIM port in the expose data string"
            }
            ExposeDataParseErrorKind::InvalidGossipPort => {
                "invalid gossip port in the expose data string"
            }
        }
    }
}

impl Display for ExposeDataParseError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        self.describe().fmt(f)
    }
}

impl Error for ExposeDataParseError {
    fn description(&self) -> &str {
        self.describe()
    }
}

// strings accepted:
//  address:swim_port:gossip_port
//  swim_port:gossip_port
impl<A: Address> FromStr for ExposeData<A> {
    type Err = ExposeDataParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.rsplit(':');
        let gossip_port = parts
            .next()
            .ok_or(ExposeDataParseError::new_bad_format())?
            .parse()
            .map_err(|_| ExposeDataParseError::new_bad_gossip_port())?;
        let swim_port = parts
            .next()
            .ok_or(ExposeDataParseError::new_bad_format())?
            .parse()
            .map_err(|_| ExposeDataParseError::new_bad_swim_port())?;
        // some -> parse -> Ok(addr) -> Ok(Some(addr))      -> ? -> Some(addr)
        //               -> Err(…)   -> Err(InvalidAddress) -> ? -> return Err(InvalidAddress)
        // none -> Ok(None)                                 -> ? -> None
        let address = parts
            .next()
            .map(|raw| {
                raw.parse()
                    .map_err(|_| ExposeDataParseError::new_bad_address())
                    .map(|addr| Some(addr))
            })
            .unwrap_or(Ok(None))?;

        if parts.next().is_some() {
            return Err(ExposeDataParseError::new_bad_format());
        }

        Ok(Self {
            address,
            swim_port,
            gossip_port,
        })
    }
}
*/
