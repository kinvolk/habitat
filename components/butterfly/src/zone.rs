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

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use uuid::Uuid;

use message::{self,
              swim::{Rumor as ProtoRumor, Rumor_Type as ProtoRumorType, Zone as ProtoZone},
              UuidSimple};
use rumor::RumorKey;

/// A zone in the swim group. Passes most of its functionality along
/// to the internal protobuf representation.
#[derive(Clone, Debug, PartialEq)]
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

    pub fn get_uuid(&self) -> Uuid {
        message::parse_uuid(self.proto.get_id(), "zone ID")
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
}

impl ZoneList {
    pub fn new() -> Self {
        Self {
            zones: HashMap::new(),
        }
    }

    pub fn available_zone_ids(&self) -> Vec<UuidSimple> {
        self.zones
            .keys()
            .cloned()
            .collect()
    }

    pub fn insert(&mut self, zone: Zone) -> bool {
        if zone.get_uuid().is_nil() {
            return false;
        }

        let share_rumor = if let Some(current_zone) = self.zones.get(zone.get_id()) {
            if current_zone.get_incarnation() > zone.get_incarnation() {
                false
            } else if zone.get_incarnation() > current_zone.get_incarnation() {
                true
            } else {
                false
            }
        } else {
            true
        };
        if share_rumor {
            self.zones.insert(zone.get_id().to_string(), zone);
        }
        share_rumor
    }
}
