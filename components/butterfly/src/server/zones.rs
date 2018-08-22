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

//! This module handles updates to zones and members.
//!
//! Used from the inbound thread.

use std::cmp::Ordering as CmpOrdering;
use std::collections::{hash_map::Entry, HashMap, HashSet};

use protobuf::RepeatedField;

use member::Member;
use message::{
    swim::{Member as ProtoMember, Swim_Type, Zone as ProtoZone, ZoneAddress, ZoneChange},
    BfUuid,
};
use network::Network;
use zone::Zone;

#[derive(Debug, Default)]
pub struct ZoneChangeDbgData {
    pub zone_found: bool,
    pub is_a_maintainer: Option<bool>,
    pub real_maintainer_found: Option<bool>,
    pub borked_successor_state: Option<bool>,
    pub available_aliases: Option<Vec<String>>,
    pub our_old_successor: Option<String>,
    pub our_new_successor: Option<String>,
    pub our_old_member_zone_id: Option<String>,
    pub our_new_member_zone_id: Option<String>,
    pub added_predecessors: Option<Vec<String>>,
    pub sent_zone_change_with_alias_to: Option<Vec<(String, String)>>,
    pub forwarded_to: Option<(String, String)>,
}

#[derive(Clone, Debug, Default)]
pub struct ZoneChangeResults {
    pub original_maintained_zone: Zone,
    pub successor_for_maintained_zone: Option<String>,
    pub predecessors_to_add_to_maintained_zone: HashSet<String>,
    pub zones_to_insert: Vec<Zone>,
    pub zone_uuid_for_our_member: Option<BfUuid>,
    pub aliases_to_inform: HashSet<BfUuid>,
}

#[derive(Debug)]
pub enum ZoneChangeResultsMsgOrNothing {
    Nothing,
    Msg((ZoneChange, Member)),
    Results(ZoneChangeResults),
}

#[derive(Debug, Default)]
pub struct HandleZoneDbgData {
    pub to_address: String,
    pub to_port: u16,
    pub host_address: String,
    pub host_port: u16,
    pub from_zone_id: String,
    pub from_address: String,
    pub from_port: u16,
    pub real_from_address: String,
    pub real_from_port: u16,
    pub scenario: String,
    pub was_settled: bool,
    pub our_old_zone_id: String,
    pub our_new_zone_id: String,
    pub sender_zone_warning: Option<String>,
    pub handle_zone_results: HandleZoneResults,
    pub sender_in_the_same_zone_as_us: bool,
    pub from_kind: AddressKind,
    pub to_kind: AddressKind,
    pub parse_failures: Vec<String>,
    pub zone_change_dbg_data: Option<ZoneChangeDbgData>,
    pub additional_address_update: Option<(ZoneAddress, ZoneAddress)>,
    pub additional_address_msgs: Vec<String>,
    pub msg_and_target: Option<(ZoneChange, Member)>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AddressKind {
    Real,
    Additional,
    Unknown,
}

impl Default for AddressKind {
    fn default() -> Self {
        AddressKind::Unknown
    }
}

#[derive(Debug)]
pub struct HandleZoneData<'a, N: Network> {
    pub zones: &'a [ProtoZone],
    pub from_member: &'a ProtoMember,
    pub to_member: &'a ProtoMember,
    pub addr: <N as Network>::AddressAndPort,
    pub swim_type: Swim_Type,
    pub from_address_kind: AddressKind,
    pub to_address_kind: AddressKind,
    pub sender_in_the_same_zone_as_us: bool,
}

#[derive(Copy, Clone, Debug)]
pub enum ZoneRelative {
    Child,
    Parent,
}

#[derive(Clone, Debug, Default)]
pub struct HandleZoneResultsStuff {
    pub new_maintained_zone: Option<Zone>,
    pub zone_uuid_for_our_member: Option<BfUuid>,
    pub additional_address_for_our_member: Option<(ZoneAddress, ZoneAddress)>,
    pub call_ack: bool,
    pub sender_has_nil_zone: bool,
    pub msg_and_target: Option<(ZoneChange, Member)>,
    pub sender_relative: Option<(BfUuid, ZoneRelative)>,
}

#[derive(Clone, Debug)]
pub enum HandleZoneResults {
    Nothing,
    UnknownSenderAddress,
    SendAck,
    // naming is hard…
    Stuff(HandleZoneResultsStuff),
    ZoneProcessed(ZoneChangeResults),
}

impl Default for HandleZoneResults {
    fn default() -> Self {
        HandleZoneResults::Nothing
    }
}

pub fn process_zone_change_internal_state(
    mut maintained_zone_clone: Zone,
    mut maybe_successor_of_maintained_zone_clone: Option<ProtoZone>,
    mut our_zone_uuid: BfUuid,
    mut zone_change: ZoneChange,
    dbg_data: &mut ZoneChangeDbgData,
) -> ZoneChangeResults {
    let mut results = ZoneChangeResults::default();
    let maintained_zone_uuid = BfUuid::must_parse(maintained_zone_clone.get_id());
    let mut aliases_to_maybe_inform = HashMap::new();

    let mut dbg_available_aliases = Vec::new();
    let mut dbg_added_predecessors = Vec::new();

    results.original_maintained_zone = maintained_zone_clone.clone();
    match (
        maintained_zone_clone.has_successor(),
        maybe_successor_of_maintained_zone_clone.is_some(),
    ) {
        (true, true) | (false, false) => (),
        (true, false) => {
            dbg_data.borked_successor_state = Some(true);

            error!("passed maintained zone has a successor, but the successor was not passed");
            return results;
        }
        (false, true) => {
            dbg_data.borked_successor_state = Some(true);

            error!("passed maintained zone has no successor, but some successor was passed");
            return results;
        }
    }

    dbg_data.borked_successor_state = Some(false);
    dbg_data.our_old_successor = Some(maintained_zone_clone.get_successor().to_string());
    dbg_data.our_old_member_zone_id = Some(our_zone_uuid.to_string());

    results.zones_to_insert = zone_change
        .get_new_aliases()
        .iter()
        .cloned()
        .map(|pz| pz.into())
        .collect();

    let mut new_aliases = zone_change.take_new_aliases().into_vec();

    for alias_zone in new_aliases.drain(..) {
        dbg_available_aliases.push(alias_zone.get_id().to_string());

        let alias_uuid = match alias_zone.get_id().parse::<BfUuid>() {
            Ok(id) => id,
            Err(e) => {
                warn!(
                    "Failed to parse an alias id {} as UUID: {}",
                    alias_zone.get_id(),
                    e,
                );
                continue;
            }
        };
        let mut possible_predecessor = None;

        match alias_uuid.cmp(&maintained_zone_uuid) {
            CmpOrdering::Less => {
                possible_predecessor = Some(alias_zone);
            }
            CmpOrdering::Equal => (),
            CmpOrdering::Greater => {
                if maintained_zone_clone.has_successor() {
                    let successor_uuid = BfUuid::must_parse(maintained_zone_clone.get_successor());

                    match alias_uuid.cmp(&successor_uuid) {
                        CmpOrdering::Less => {
                            possible_predecessor = Some(alias_zone);
                        }
                        CmpOrdering::Equal => (),
                        CmpOrdering::Greater => {
                            possible_predecessor = maybe_successor_of_maintained_zone_clone;
                            maybe_successor_of_maintained_zone_clone = Some(alias_zone);
                        }
                    }
                } else {
                    maybe_successor_of_maintained_zone_clone = Some(alias_zone);
                }
            }
        }

        if let Some(ref new_successor) = &maybe_successor_of_maintained_zone_clone {
            if maintained_zone_clone.get_successor() != new_successor.get_id() {
                maintained_zone_clone.set_successor(new_successor.get_id().to_string());
                results.successor_for_maintained_zone = Some(new_successor.get_id().to_string());
                match aliases_to_maybe_inform.entry(alias_uuid) {
                    Entry::Occupied(_) => (),
                    Entry::Vacant(ve) => {
                        let mut abridged_successor = ProtoZone::default();

                        abridged_successor.set_id(alias_uuid.to_string());
                        abridged_successor.set_successor(new_successor.get_id().to_string());
                        abridged_successor.set_predecessors(RepeatedField::from_vec(
                            new_successor.get_predecessors().to_vec(),
                        ));

                        ve.insert(abridged_successor);
                    }
                }
            }

            let successor_uuid = BfUuid::must_parse(new_successor.get_id());

            if our_zone_uuid < successor_uuid {
                results.zone_uuid_for_our_member = Some(successor_uuid);
                our_zone_uuid = successor_uuid;
            }
        }

        if let Some(predecessor) = possible_predecessor {
            let mut found = false;

            for zone_id in maintained_zone_clone.get_predecessors().iter() {
                if zone_id == predecessor.get_id() {
                    found = true;
                    break;
                }
            }

            if !found {
                dbg_added_predecessors.push(predecessor.get_id().to_string());

                let predecessor_uuid = BfUuid::must_parse(predecessor.get_id());

                results
                    .predecessors_to_add_to_maintained_zone
                    .insert(predecessor.get_id().to_string());
                match aliases_to_maybe_inform.entry(predecessor_uuid) {
                    Entry::Occupied(_) => (),
                    Entry::Vacant(ve) => {
                        ve.insert(predecessor);
                    }
                };
            }
        }
    }

    dbg_data.our_new_successor = Some(maintained_zone_clone.get_successor().to_string());
    dbg_data.our_new_member_zone_id = Some(our_zone_uuid.to_string());
    dbg_data.available_aliases = Some(dbg_available_aliases);
    dbg_data.added_predecessors = Some(dbg_added_predecessors);

    for (zone_uuid, zone) in aliases_to_maybe_inform {
        if zone.get_successor() == maintained_zone_clone.get_id() {
            continue;
        }

        let mut found = false;

        for predecessor_id in zone.get_predecessors().iter() {
            if predecessor_id == maintained_zone_clone.get_id() {
                found = true;
                break;
            }
        }

        if found {
            continue;
        }

        results.aliases_to_inform.insert(zone_uuid);
    }

    results
}
