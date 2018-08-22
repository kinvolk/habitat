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
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::{Deref, DerefMut};

use protobuf::RepeatedField;

use message::{
    swim::{Rumor as ProtoRumor, Rumor_Type as ProtoRumorType, Zone as ProtoZone}, BfUuid,
    UuidSimple,
};
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

#[derive(Debug)]
pub struct ZoneList {
    pub zones: HashMap<UuidSimple, Zone>,
    pub maintained_zone_id: Option<UuidSimple>,
}

impl ZoneList {
    pub fn new() -> Self {
        Self {
            zones: HashMap::new(),
            maintained_zone_id: None,
        }
    }

    pub fn available_zone_ids(&self) -> Vec<UuidSimple> {
        self.zones.keys().cloned().collect()
    }

    pub fn insert(&mut self, mut zone: Zone) -> Vec<RumorKey> {
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
                self.make_zones_consistent(zone)
            }
        }
    }

    fn make_zones_consistent(&mut self, zone: Zone) -> Vec<RumorKey> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut aliases = HashSet::new();

        aliases.insert(zone.get_uuid());
        if zone.has_successor() {
            aliases.insert(BfUuid::must_parse(zone.get_successor()));
        }
        aliases.extend(
            zone.get_predecessors()
                .iter()
                .map(|i| BfUuid::must_parse(i)),
        );

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
        let mut rumor_keys = Vec::new();

        for zone_uuid in visited.drain() {
            let mut changed = false;
            let mut other_zone = match self.zones.get(&zone_uuid.to_string()).cloned() {
                Some(oz) => oz,
                None => continue,
            };
            let mut new_successor = false;

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
