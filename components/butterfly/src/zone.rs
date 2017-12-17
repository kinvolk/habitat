// Copyright (c) 2016-2017 Chef Software Inc. and/or applicable contributors
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

use message::swim::{Zone as ProtoZone};

/// A zone in the swim group. Passes most of its functionality along
/// to the internal protobuf representation.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Zone {
    pub proto: ProtoMember,
}

impl Zone {
    pub fn new(maintainer_id: String) -> Self {
        let mut proto_zone = ProtoZone::new();
        let mut nil_uuid = Uuid::nil().simple().to_string();
        proto_zone.set_id(nil_uuid.clone());
        proto_zone.set_incarnation(0);
        proto_zone.set_parent_zone_id(nil_uuid.clone());
        proto_zone.set_maintainer_id(maintainer_id);
        Zone { proto: proto_zone }
    }

    pub fn get_uuid() -> Uuid {
        message::swim::parse_uuid(self.proto.get_id(), "zone ID")
    }

    pub fn get_parent_uuid -> Uuid {
        message::swim::parse_uuid(self.proto.get_parent_id(), "parent zone ID")
    }

    pub fn get_maintainer_uuid -> Uuid {
        message::swim::parse_uuid(self.proto.get_maintainer_id(), "zone maintainer ID")
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
        Zone { proto: zone.clone() }
    }
}

impl From<Zone> for RumorKey {
    fn from(zone: Zone) -> RumorKey {
        RumorKey::new(Rumor_Type::Zone, zone.get_id(), "")
    }
}

impl<'a> From<&'a Zone> for RumorKey {
    fn from(zone: &'a Zone) -> RumorKey {
        RumorKey::new(Rumor_Type::Zone, zone.get_id(), "")
    }
}

impl<'a> From<&'a &'a Zone> for RumorKey {
    fn from(zone: &'a &'a Zone) -> RumorKey {
        RumorKey::new(Rumor_Type::Zone, zone.get_id(), "")
    }
}

pub struct Zones {
    pub zones: Arc<RwLock<HashMap<UuidSimple, Zone>>>,
    update_counter: Arc<AtomicUsize>,
}

impl Serialize for Zones {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut strukt = serializer.serialize_struct("zones", 2)?;
        {
            let zones_struct = self.zones.read().expect("Zones lock is poisoned");
            strukt.serialize_field("zones", &*zones_struct)?;
        }
        {
            let update_number = self.update_counter.load(Ordering::SeqCst);
            strukt.serialize_field("update_counter", &update_number)?;
        }
        strukt.end()
    }
}

impl Zones {
    /// Creates a new, empty, Zones.
    pub fn new() -> Self {
        Self {
            zones: Arc::new(RwLock::new(HashMap::new())),
            update_counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn new_with_zone(zone: Zone) -> Self {
        let mut map = HashMap::new();
        map.insert(String::from(zone.get_id()), zone);
        Self {
            zones: Arc::new(RwLock::new(map)),
            update_counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn increment_update_counter(&self) {
        self.update_counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_update_counter(&self) -> usize {
        self.update_counter.load(Ordering::Relaxed)
    }

    pub fn insert(&self, zone: Zone) -> bool {
        if let Some(current_zone) =
            self.zones
                .read()
                .expect("Zones lock is poisoned")
                .get(zone.get_id())
        {
            let share_rumor = zone.get_incarnation() >= current_zone.get_incarnation();
            if share_rumor {
                self.increment_update_counter();
                self.members
                    .write()
                    .expect("Zones lock is poisoned")
                    .insert(String::from(zone.get_id()), zone);
            }
            share_rumor
        } else {
            false
        }
    }

    /// Returns a protobuf zone record for the given member id.
    pub fn zone_for(&self, member_id: &str) -> Option<ProtoZone> {
        self.read_zones()
            .get(member_id)
            .map(|zone| zone.proto.clone())
    }

    pub fn read_zones(&self) -> ReadGuard<Zone> {
        self.zones.read().expect("Zones lock is poisoned")
    }
}
