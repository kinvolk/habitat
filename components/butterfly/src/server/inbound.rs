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

//! The inbound thread.
//!
//! This module handles all the inbound SWIM messages.

use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use protobuf::{self, RepeatedField};

use error::Error;
use member::{Health, Member};
use message::{BfUuid,
              swim::{Member as ProtoMember, Swim, Swim_Type, Zone as ProtoZone, ZoneAddress, ZoneChange}};
use network::{AddressAndPort, MyFromStr, Network, SwimReceiver};
use server::{outbound, Server};
use trace::TraceKind;
use zone::Zone;

/// Takes the Server and a channel to send received Acks to the outbound thread.
pub struct Inbound<N: Network> {
    pub server: Server<N>,
    pub swim_receiver: N::SwimReceiver,
    pub swim_sender: N::SwimSender,
    pub tx_outbound: mpsc::Sender<(N::AddressAndPort, Swim)>,
}

struct HandleZoneData<'a, N: Network> {
    zones: &'a[ProtoZone],
    from_member: &'a ProtoMember,
    to_member: &'a ProtoMember,
    addr: <N as Network>::AddressAndPort,
    swim_type: Swim_Type,
    //from_address_kind: AddressKind,
    to_address_kind: AddressKind,
    sender_in_the_same_zone_as_us: bool,
}

#[derive(Default)]
struct DbgData {
    to_address: String,
    to_port: u16,
    host_address: String,
    host_port: u16,
    from_zone_id: String,
    from_address: String,
    from_port: u16,
    real_from_address: String,
    real_from_port: u16,
    scenario: String,
    was_settled: bool,
    our_old_zone_id: String,
    member_zone_uuid: String,
    handle_zone_result: HandleZoneResult,
    sender_in_the_same_zone_as_us: bool,
    from_kind: AddressKind,
    to_kind: AddressKind,
    parse_failures: Vec<String>,
    zone_change_dbg_data: Option<ZoneChangeDbgData>,
}

#[derive(Debug, Default)]
struct ZoneChangeDbgData {
    zone_found: bool,
    is_a_maintainer: Option<bool>,
    available_aliases: Option<Vec<String>>,
    our_old_successor: Option<String>,
    our_new_successor: Option<String>,
    our_old_member_zone_id: Option<String>,
    our_new_member_zone_id: Option<String>,
    added_predecessors: Option<Vec<String>>,
    sent_zone_change_with_alias_to: Option<Vec<(String, String)>>,
    forwarded_to: Option<(String, String)>,
}

enum ZoneChangeResultsMsgOrNothing {
    Results(ZoneChangeResults),
    Msg(ZoneChange),
    Nothing
}

#[derive(Default)]
struct ZoneChangeResults {
    original_maintained_zone: Zone,
    successor_for_maintained_zone: Option<String>,
    predecessors_to_add_to_maintained_zone: HashSet<String>,
    zones_to_insert: Vec<Zone>,
    zone_uuid_for_our_member: Option<BfUuid>,
    aliases_to_inform: HashSet<BfUuid>,
}

#[derive(Copy, Clone, PartialEq, Debug)]
enum AddressKind {
    Real,
    Additional,
    Unknown,
}

impl Default for AddressKind {
    fn default() -> Self {
        AddressKind::Unknown
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
enum HandleZoneResult {
    Ok,
    ConfirmZoneID,
    NilSenderZone,
    UnknownSenderAddress,
}

impl Default for HandleZoneResult {
    fn default() -> Self {
        HandleZoneResult::UnknownSenderAddress
    }
}

impl<N: Network> Inbound<N> {
    /// Create a new Inbound.
    pub fn new(
        server: Server<N>,
        swim_receiver: N::SwimReceiver,
        swim_sender: N::SwimSender,
        tx_outbound: mpsc::Sender<(N::AddressAndPort, Swim)>,
    ) -> Self {
        Self {
            server: server,
            swim_receiver: swim_receiver,
            swim_sender: swim_sender,
            tx_outbound: tx_outbound,
        }
    }

    /// Run the thread. Listens for messages up to 1k in size, and then processes them accordingly.
    pub fn run(&self) {
        let mut recv_buffer: Vec<u8> = vec![0; 1024];
        loop {
            if self.server.pause.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(100));
                continue;
            }
            match self.swim_receiver.receive(&mut recv_buffer[..]) {
                Ok((length, addr)) => {
                    let swim_payload = match self.server.unwrap_wire(&recv_buffer[0..length]) {
                        Ok(swim_payload) => swim_payload,
                        Err(e) => {
                            // NOTE: In the future, we might want to block people who send us
                            // garbage all the time.
                            error!("Error parsing protobuf: {:?}", e);
                            continue;
                        }
                    };

                    let msg: Swim = match protobuf::parse_from_bytes(&swim_payload) {
                        Ok(msg) => msg,
                        Err(e) => {
                            // NOTE: In the future, we might want to block people who send us
                            // garbage all the time.
                            error!("Error parsing protobuf: {:?}", e);
                            continue;
                        }
                    };
                    trace!("SWIM Message: {:?}", msg);
                    match msg.get_field_type() {
                        Swim_Type::PING => {
                            if self.server
                                .is_member_blocked(msg.get_ping().get_from().get_id())
                            {
                                debug!(
                                    "Not processing message from {} - it is blocked",
                                    msg.get_ping().get_from().get_id()
                                );
                                continue;
                            }
                            self.process_ping(addr, msg);
                        }
                        Swim_Type::ACK => {
                            if self.server
                                .is_member_blocked(msg.get_ack().get_from().get_id())
                                && !msg.get_ack().has_forward_to()
                            {
                                debug!(
                                    "Not processing message from {} - it is blocked",
                                    msg.get_ack().get_from().get_id()
                                );
                                continue;
                            }
                            self.process_ack(addr, msg);
                        }
                        Swim_Type::PINGREQ => {
                            if self.server
                                .is_member_blocked(msg.get_pingreq().get_from().get_id())
                            {
                                debug!(
                                    "Not processing message from {} - it is blocked",
                                    msg.get_pingreq().get_from().get_id()
                                );
                                continue;
                            }
                            self.process_pingreq(addr, msg);
                        }
                        Swim_Type::ZONE_CHANGE => {
                            if self.server
                                .is_member_blocked(msg.get_zone_change().get_from().get_id())
                            {
                                debug!(
                                    "Not processing message from {} - it is blocked",
                                    msg.get_zone_change().get_from().get_id()
                                );
                                continue;
                            }
                            self.process_zone_change(addr, msg);
                        }
                    }
                }
                Err(Error::SwimReceiveIOError(e)) => {
                    match e.raw_os_error() {
                        Some(35) | Some(11) | Some(10035) | Some(10060) => {
                            // This is the normal non-blocking result, or a timeout
                        }
                        Some(_) => {
                            error!("SWIM Receive error: {}", e);
                            debug!("SWIM Receive error debug: {:?}", e);
                        }
                        None => {
                            error!("SWIM Receive error: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("SWIM Receive error: {}", e);
                }
            }
        }
    }

    /// Process pingreq messages.
    fn process_pingreq(&self, addr: N::AddressAndPort, mut msg: Swim) {
        trace_it!(SWIM: &self.server,
                  TraceKind::RecvPingReq,
                  msg.get_pingreq().get_from().get_id(),
                  addr,
                  &msg);
        // We need to get msg to be owned by the closure, so we're going to have to
        // allocate here to get the id. Kind of a bummer, but life goes on.
        let mid = String::from(msg.get_pingreq().get_target().get_id());
        self.server.member_list.with_member(&mid, |m| {
            let target = match m {
                Some(target) => target,
                None => {
                    error!("PingReq request {:?} for invalid target", msg);
                    return;
                }
            };
            // Set the route-back address to the one we received the pingreq from
            let mut from = msg.mut_pingreq().take_from();
            from.set_address(format!("{}", addr.get_address()));
            outbound::ping(
                &self.server,
                &self.swim_sender,
                target,
                target.swim_socket_address(),
                Some(from.into()),
            );
        });
    }

    /// Process ack messages; forwards to the outbound thread.
    fn process_ack(&self, addr: N::AddressAndPort, mut msg: Swim) {
        match self.handle_zone_for_recipient(msg.get_zones(), msg.get_field_type(), msg.get_ack().get_from(), msg.get_ack().get_to(), addr) {
            HandleZoneResult::Ok => {}
            HandleZoneResult::ConfirmZoneID => {
                let target: Member = msg.get_ack().get_from().into();
                outbound::ack(
                    &self.server,
                    &self.swim_sender,
                    &target,
                    addr,
                    Some(msg.mut_ping().take_forward_to().into()),
                );
            },
            HandleZoneResult::NilSenderZone => {
                warn!(
                    "Supervisor {} sent an Ack with a nil zone ID",
                    msg.get_ack().get_from().get_id(),
                );
            }
            HandleZoneResult::UnknownSenderAddress => {
                warn!(
                    "Sender of the ACK message does not know its address {}. \
                     This shouldn't happen - this means that we sent a PING message to a server \
                     that is not directly reachable from us and it wasn't ignored by the receiver \
                     of the message",
                    addr,
                );
                return;
            }
        }
        trace_it!(SWIM: &self.server,
                  TraceKind::RecvAck,
                  msg.get_ack().get_from().get_id(),
                  addr,
                  &msg);
        trace!("Ack from {}@{}", msg.get_ack().get_from().get_id(), addr);
        if msg.get_ack().has_forward_to() {
            if self.server.member_id() != msg.get_ack().get_forward_to().get_id() {
                let forward_addr_str = format!(
                    "{}:{}",
                    msg.get_ack().get_forward_to().get_address(),
                    msg.get_ack().get_forward_to().get_swim_port()
                );
                let forward_to_addr = match N::AddressAndPort::create_from_str(&forward_addr_str) {
                    Ok(addr) => addr,
                    Err(e) => {
                        error!(
                            "Abandoning Ack forward: cannot parse member address: {}, {}",
                            msg.get_ack().get_forward_to().get_address(),
                            e
                        );
                        return;
                    }
                };
                trace!(
                    "Forwarding Ack from {}@{} to {}@{}",
                    msg.get_ack().get_from().get_id(),
                    addr,
                    msg.get_ack().get_forward_to().get_id(),
                    msg.get_ack().get_forward_to().get_address(),
                );
                msg.mut_ack()
                    .mut_from()
                    .set_address(format!("{}", addr.get_address()));
                outbound::forward_ack(&self.server, &self.swim_sender, forward_to_addr, msg);
                return;
            }
        }
        let membership = {
            let membership: Vec<(Member, Health)> = msg.take_membership()
                .iter()
                .map(|m| (Member::from(m.get_member()), Health::from(m.get_health())))
                .collect();
            membership
        };
        let zones = msg.take_zones()
            .iter()
            .map(|z| Zone::from(z))
            .collect();
        match self.tx_outbound.send((addr, msg)) {
            Ok(()) => {}
            Err(e) => panic!("Outbound thread has died - this shouldn't happen: #{:?}", e),
        }
        self.server.insert_member_from_rumors(membership);
        self.server.insert_zones_from_rumors(zones);
    }

    /// Process ping messages.
    fn process_ping(&self, addr: N::AddressAndPort, mut msg: Swim) {
        let target: Member = msg.get_ping().get_from().into();
        let insert_pinger = match self.handle_zone_for_recipient(msg.get_zones(), msg.get_field_type(), msg.get_ping().get_from(), msg.get_ping().get_to(), addr) {
            HandleZoneResult::Ok => true,
            HandleZoneResult::ConfirmZoneID => true,
            HandleZoneResult::NilSenderZone => false,
            HandleZoneResult::UnknownSenderAddress => {
                warn!(
                    "Sender of the PING message does not know its address {}. \
                     This shouldn't happen - this means that the sender sent a PING message to us \
                     and we are not directly reachable",
                    addr,
                );
                return;
            }
        };
        trace_it!(SWIM: &self.server,
                  TraceKind::RecvPing,
                  msg.get_ack().get_from().get_id(),
                  addr,
                  &msg);
        if msg.get_ping().has_forward_to() {
            outbound::ack(
                &self.server,
                &self.swim_sender,
                &target,
                addr,
                Some(msg.mut_ping().take_forward_to().into()),
            );
        } else {
            outbound::ack(&self.server, &self.swim_sender, &target, addr, None);
        }
        trace!("Ping from {}@{}", msg.get_ack().get_from().get_id(), addr);
        if insert_pinger {
            // Populate the member for this sender with its remote address
            let from = {
                let ping = msg.mut_ping();
                let mut from = ping.take_from();
                from.set_address(format!("{}", addr.get_address()));
                from
            };
            if from.get_departed() {
                self.server.insert_member(from.into(), Health::Departed);
            } else {
                self.server.insert_member(from.into(), Health::Alive);
            }
            let membership: Vec<(Member, Health)> = msg.take_membership()
                .iter()
                .map(|m| (Member::from(m.get_member()), Health::from(m.get_health())))
                .collect();
            self.server.insert_member_from_rumors(membership);
            let zones = msg.take_zones()
                .iter()
                .map(|z| Zone::from(z))
                .collect();
            self.server.insert_zones_from_rumors(zones);
        }
    }

    fn dbg<T: AsRef<str>>(&self, msg: T) {
        println!("{}: {}", self.server.member_id(), msg.as_ref());
    }

    fn process_zone_change(&self, addr: N::AddressAndPort, msg: Swim) {
        self.dbg("process_zone_change");
        trace_it!(SWIM: &self.server,
                  TraceKind::RecvZoneChange,
                  msg.get_zone_change().get_from().get_id(),
                  addr,
                  &msg);
        error!("Zone change from {}@{}", msg.get_zone_change().get_from().get_id(), addr);
        let mut dbg_data = ZoneChangeDbgData::default();
        let mut maybe_maintained_zone_clone = self.server.read_zone_list().get(zone_change.get_zone_id()).cloned();
        let mut results_msg_or_nothing = self.process_zone_change_internal(msg.take_zone_change(), &mut dbg_data);

        match results_msg_or_nothing {
            ZoneChangeResultsMsgOrNothing::Nothing => (),
            ZoneChangeResultsMsgOrNothing::Msg(zone_change) => {
                self.server.member_list.with_member(zone.get_maintainer_id(), |maybe_maintainer| {
                    if let Some(maintainer) = maybe_maintainer {
                        let addr: N::AddressAndPort = maintainer.swim_socket_address();

                        dbg_data.forwarded_to = Some((maintainer.get_id().to_string(), addr.to_string()));

                        outbound::zone_change(&self.server, &self.swim_sender, maintainer, zone_change);
                    }
                });
            }
            ZoneChangeResultsMsgOrNothing::ZoneChangeResults(mut results) => {
                let zone_changed = successor_uuid.is_some() || !results.predecessors_to_add_to_maintained_zone.is_empty();
                let mut maintained_zone = Zone::default();

                mem::swap(&mut maintained_zone, &mut results.original_maintained_zone);

                if let Some(successor_id) = results.successor_for_maintained_zone.take() {
                    maintained_zone.set_successor(successor_id);
                }
                for predecessor_id in results.predecessors_to_add_to_maintained_zone {
                    maintained_zone.mut_predecessors.push(predecessor_id);
                }
                if zone_changed {
                    let incarnation = maintained_zone.get_incarnation();

                    maintained_zone.set_incarnation(incarnation + 1);
                    self.server.insert_zone(maintained_zone);
                }
                for zone in results.zones_to_insert.drain() {
                    self.server.insert_zone(zone);
                }
                if let Some(zone_uuid) = results.zone_uuid_for_our_member {
                    let member = self.server.write_member();
                    let incarnation = member.get_incarnation();

                    member.set_zone_id(zone_uuid.to_string());
                    member.set_incarnation(incarnation + 1);

                    self.server.insert_member(member.clone(), Health::Alive);
                }

                let mut dbg_sent_zone_change_with_alias_to = Vec::new();

                if !results.aliases_to_inform.is_empty() {
                    self.server.member_list.with_member_list(|members_map| {
                        for uuid in results.aliases_to_inform {
                            let zone_id = uuid.to_string();
                            let maintainer_id = match self.server.read_zone_list().zones.get(&zone_id) {
                                Some(zone) = zone.get_maintainer_id().to_string(),
                                None => continue;
                            };

                            if let Some(maintainer) = members_map.get(&maintainer_id) {
                                let mut zone_change = ZoneChange::new();
                                let mut new_aliases = RepeatedField::new();
                                let alias = match self.server.read_zone_list().zones.get(&zone_id) {
                                    Some(zone) = zone.proto.clone(),
                                    None => continue;
                                };
                                let addr: N::AddressAndPort = maintainer.swim_socket_address();

                                dbg_sent_zone_change_with_alias_to.push((maintainer_id, addr.to_string()));
                                new_aliases.push(maintained_zone.proto.clone());
                                zone_change.set_zone_id(zone_id);
                                zone_change.set_new_aliases(new_aliases);
                                outbound::zone_change(&self.server, &self.swim_sender, maintainer.clone(), zone_change);
                            }
                        }
                    });
                }
                dbg_data.sent_zone_change_with_alias_to = dbg_sent_zone_change_with_alias_to;
            }
        }
        println!(
            "===========ZONE CHANGE=========\n\
             dbg:\n\
             \n\
             {:#?}\n\
             \n\
             ===============================",
            dbg_data,
        );
    }

    fn process_zone_change_internal(&self, zone_change: ZoneChange, dbg_data: &mut ZoneChangeDbgData) -> ZoneChangeResultsMsgOrNothing {
        let mut maintained_zone_clone = {
            let zone_list = self.server.read_zone_list();
            let maybe_maintained_zone = zone_list.zones.get(zone_change.get_zone_id());

            dbg_data.zone_found = maybe_maintained_zone.is_some();

            if let Some(maintained_zone) = maybe_maintained_zone {
                let im_a_maintainer = maintained_zone.get_maintainer_id() == self.server.member_id();

                dbg_data.is_a_maintainer = Some(im_a_maintainer);

                if im_a_maintainer {
                    maintained_zone.clone()
                } else {
                    return ZoneChangeResultsMsgOrNothing::Msg(zone_change);
                }
            } else {
                return ZoneChangeResultsMsgOrNothing::Nothing;
            }
        };

        ZoneChangeResultsMsgOrNothing::ZoneChangeResults(
            Self::process_zone_change_internal_state(
                maintained_zone_clone,
                self.server.read_zone_list().zones.get(&maintained_zone_clone.get_successor()).map(|z| pz.proto.clone()),
                BfUuid::must_parse(self.server.read_member().get_zone_id()),
                zone_change,
                dbg_data,
            )
        )
    }

    fn process_zone_change_internal_state(
        maintained_zone_clone: mut Zone,
        maybe_successor_of_maintained_zone_clone: mut Option<ProtoZone>,
        our_zone_uuid: mut BfUuid,
        zone_change: mut ZoneChange,
        dbg_data: &mut ZoneChangeDbgData
    ) -> ZoneChangeResults {
        let mut results = ZoneChangeResults::default();
        let maintained_zone_uuid = BfUuid::must_parse(maintained_zone_clone.get_id());
        let mut aliases_to_maybe_inform = HashMap::new();

        let dbg_available_aliases = Vec::new();
        let dbg_added_predecessors = Vec::new();

        match (maintained_zone_clone.has_successor(), maybe_successor_of_maintained_zone_clone.is_some()) {
            (true, true) | (false, false) => (),
            (true, false) => {
                error!("passed maintained zone has a successor, but the successor was not passed");
                return results;
            }
            (false, true) => {
                error!("passed maintained zone has no successor, but some successor was passed");
                return results;
            }
        }

        dbg_data.our_old_successor = Some(maintained_zone_clone.get_successor().to_string());
        dbg_data.our_old_member_zone_id = Some(our_zone_uuid.to_string());

        results.zones_to_insert = zone_change.get_new_aliases().iter().cloned().map(|pz| pz.into()).collect();

        for alias_zone in zone_change.take_new_aliases().into_vec().drain() {
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

            match alias_uuid.cmp(maintained_zone_uuid) {
                Ordering::Less => {
                    possible_predecessor = Some(alias_zone);
                }
                Ordering::Equal => (),
                Ordering::Greater => {
                    if maintained_zone_clone.has_successor() {
                        let successor_uuid = BfUuid::must_parse(maintained_zone_clone.get_successor());

                        match alias_uuid.cmp(successor_uuid) {
                            Ordering::Less => {
                                possible_predecessor = Some(alias_zone);
                            }
                            Ordering::Equal => (),
                            Ordering::Greater => {
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
                    // using alias_uuid is fine - the new successor
                    // can only come from one of the aliases
                    if our_zone_uuid < alias_uuid {
                        results.zone_uuid_for_our_member = Some(alias_uuid);
                        our_zone_uuid = alias_uuid;
                    }
                    match aliases_to_maybe_inform.entry(alias_uuid) {
                        Entry::Occupied(_) => (),
                        Entry::Vacant(ve) => {
                            let mut abridged_successor = ProtoZone::default();

                            abridged_successor.set_id(alias_uuid.to_string());
                            abridged_successor.set_successor(new_successor.get_id().to_string());
                            abridged_successor.set_predecessors(new_successor.get_predecessors.clone());
                        }
                    }
                }
            }

            if let Some(predecessor) = possible_predecessor {
                let mut found = false;

                for zone_id in maintained_zone.get_predecessors().iter() {
                    if zone_id == predecessor.get_id() {
                        found = true;
                        break;
                    }
                }

                if !found {
                    dbg_added_predecessors.push(predecessor.get_id().to_string());

                    let predecessor_uuid = BfUuid::must_parse(predecessor.get_id());

                    results.predecessors_to_add_to_maintained_zone.insert(predecessor.get_id().to_string());
                    match aliases_to_maybe_inform.entry(predecessor_uuid) {
                        Entry::Occupied(_) => (),
                        Entry::Vacant(ve) => ve.insert(predecessor),
                    }
                }
            }
        }

        dbg_data.our_new_successor = Some(maintained_zone_clone.get_successor().to_string());
        dbg_data.our_new_member_zone_id = Some(our_zone_uuid.to_string());
        dbg_data.available_aliases = Some(dbg_available_aliases);
        dbg_data.added_predecessors = Some(dbg_added_predecessors);

        results.zones_to_insert = zone_change.get_new_aliases().iter().map(|pz| pz.into()).collect();

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

            results.aliases_to_inform.push(zone_uuid);
        }

        results
    }

    fn parse_addr(addr_str: &str) -> Result<<<N as Network>::AddressAndPort as AddressAndPort>::Address, <<<N as Network>::AddressAndPort as AddressAndPort>::Address as MyFromStr>::MyErr> {
        <<N as Network>::AddressAndPort as AddressAndPort>::Address::create_from_str(addr_str)
    }

    fn address_kind(addr: <<N as Network>::AddressAndPort as AddressAndPort>::Address, member: &ProtoMember, dbg_data: &mut DbgData) -> AddressKind {
        let member_real_address = match Self::parse_addr(member.get_address()) {
            Ok(addr) => addr,
            Err(e) => {
                let msg = format!("Error parsing member {:?} address {}: {}", member, member.get_address(), e);
                error!("{}", msg);
                dbg_data.parse_failures.push(msg);
                return AddressKind::Unknown;
            }
        };

        if member_real_address == addr {
            return AddressKind::Real;
        }

        for zone_address in member.get_additional_addresses() {
            let member_additional_address = match Self::parse_addr(zone_address.get_address()) {
                Ok(addr) => addr,
                Err(e) => {
                    let msg = format!("Error parsing member {:?} additional address {}: {}", member, member.get_address(), e);
                    error!("{}", msg);
                    dbg_data.parse_failures.push(msg);
                    return AddressKind::Unknown;
                }
            };

            if member_additional_address == addr {
                return AddressKind::Additional;
            }
        }

        AddressKind::Unknown
    }

    fn address_kind_from_str(addr: &str, member: &ProtoMember, dbg_data: &mut DbgData) -> AddressKind {
        let real_address = match Self::parse_addr(addr) {
            Ok(addr) => addr,
            Err(e) => {
                error!("Error parsing address {}: {}", addr, e);
                return AddressKind::Unknown;
            }
        };

        Self::address_kind(real_address, member, dbg_data)
    }

    fn handle_zone_for_recipient(
        &self,
        zones: &[ProtoZone],
        swim_type: Swim_Type,
        from: &ProtoMember,
        to: &ProtoMember,
        addr: N::AddressAndPort
    ) -> HandleZoneResult {
        self.dbg("handle_zone_for_recipient");
        let mut dbg_data = DbgData::default();
        let from_address_kind = Self::address_kind(addr.get_address(), from, &mut dbg_data);
        let to_address_kind = Self::address_kind_from_str(to.get_address(), &self.server.read_member(), &mut dbg_data);

        dbg_data.from_kind = from_address_kind;
        dbg_data.to_kind = to_address_kind;
        // we are dealing with several addresses here:
        //
        // real from address - can be an address of a mapping on a NAT
        // or a real one
        //
        // member from - contains a real address and additional addresses
        //
        // member to - contains an address that can be either real or
        // a mapping on a NAT
        //
        // member us - contains a real address and additional addresses
        //
        // address kinds:
        // 1. real - an address is the same as member's address
        // 2. additional - an address is the same as one of member's
        // additional addresses
        // 3. unknown - if none of the above applies
        //
        // scenarios:
        // 1. from real to real - message sent between two servers in
        // the same zone
        //
        // 2. from real to additional - message sent from parent zone
        // to child zone
        //
        // 3. from real to unknown - message likely sent from parent
        // zone to child zone for the first time
        //
        // 4. from additional to real - message sent from child zone
        // to parent zone
        //
        // 5. from additional to additional - probably message sent
        // from a zone to a sibling zone
        //
        // 6. from additional to unknown - probably message sent from
        // a zone to a sibling zone for the first time
        //
        // 7. from unknown to real - probably message sent from child
        // zone to parent zone, but the sender either does not know
        // that it can be reached with the address the message came
        // from. This likely should not happen - if the server in
        // child zone is not exposed in the parent zone, the message
        // should be routed through the gateway
        //
        // 8. from unknown to additional - probably message sent from
        // zone to a sibling zone, but the sender either does not know
        // that it can be reached with the address the message came
        // from. This likely should not happen - if the server in
        // child zone is not exposed in the parent zone, the message
        // should be routed through the gateway
        //
        // 9. from unknown to unknown - probably a message sent from
        // zone to a sibling zone for the first time, but the sender
        // either does not know that it can be reached with the
        // address the message came from. This likely should not
        // happen - if the server in child zone is not exposed in the
        // parent zone, the message should be routed through the
        // gateway
        let sender_in_the_same_zone_as_us;
        let mut maybe_result = None;
        match (from_address_kind, to_address_kind) {
            (AddressKind::Real, AddressKind::Real) => {
                sender_in_the_same_zone_as_us = true;
            }
            (AddressKind::Real, AddressKind::Additional) => {
                sender_in_the_same_zone_as_us = false;
            }
            (AddressKind::Additional, AddressKind::Real) => {
                sender_in_the_same_zone_as_us = false;
            }
            (AddressKind::Additional, AddressKind::Additional) => {
                sender_in_the_same_zone_as_us = false;
            }
            (AddressKind::Unknown, _) => {
                sender_in_the_same_zone_as_us = false;
                maybe_result = Some(HandleZoneResult::UnknownSenderAddress);
            }
            (_, _) => {
                sender_in_the_same_zone_as_us = false;
            }
        };

        dbg_data.to_address = to.get_address().to_string();
        dbg_data.to_port = to.get_swim_port() as u16;
        dbg_data.host_address = self.server.host_address.to_string();
        dbg_data.host_port = self.server.swim_port();
        dbg_data.sender_in_the_same_zone_as_us = sender_in_the_same_zone_as_us;
        dbg_data.from_address = from.get_address().to_string();
        dbg_data.from_port = from.get_swim_port() as u16;
        dbg_data.real_from_address = addr.get_address().to_string();
        dbg_data.real_from_port = addr.get_port();

        let handle_zone_result = if let Some(result) = maybe_result {
            result
        } else {
            let handle_zone_data = HandleZoneData {
                zones: zones,
                from_member: from,
                to_member: to,
                addr: addr,
                swim_type: swim_type,
                //from_address_kind: from_address_kind,
                to_address_kind: to_address_kind,
                sender_in_the_same_zone_as_us: sender_in_the_same_zone_as_us,
            };
            self.handle_zone(handle_zone_data, &mut dbg_data)
        };
        dbg_data.handle_zone_result = handle_zone_result;
        /*println!("=========={:?}==========\n\
                  from address:      {}\n\
                  from port:         {}\n\
                  real from address: {}\n\
                  real from port:    {}\n\
                  from address kind: {:?}\n\
                  to address:        {}\n\
                  to port:           {}\n\
                  host address:      {}\n\
                  host port:         {}\n\
                  to address kind:   {:?}\n\
                  from zone id: {}\n\
                  scenario: {}\n\
                  was settled: {}\n\
                  our old zone id: {}\n\
                  our new zone id: {}\n\
                  handle zone result: {:#?}\n\
                  sender in the same zone as us: {}\n\
                  parse_failures: {:#?}\n\
                  \n\
                  member us: {:#?}\n\
                  member from: {:#?}\n\
                  \n\
                  zone change debug info:\n\
                  {:#?}\n\
                  =====================",
                 swim_type,
                 dbg_data.from_address,
                 dbg_data.from_port,
                 dbg_data.real_from_address,
                 dbg_data.real_from_port,
                 dbg_data.from_kind,
                 dbg_data.to_address,
                 dbg_data.to_port,
                 dbg_data.host_address,
                 dbg_data.host_port,
                 dbg_data.to_kind,
                 dbg_data.from_zone_id,
                 dbg_data.scenario,
                 dbg_data.was_settled,
                 dbg_data.our_old_zone_id,
                 dbg_data.member_zone_uuid,
                 dbg_data.handle_zone_result,
                 dbg_data.sender_in_the_same_zone_as_us,
                 dbg_data.parse_failures,
                 self.server.read_member().proto,
                 from,
                 dbg_data.zone_change_dbg_data,
        );*/
        self.dbg("end handle_zone_for_recipient");
        handle_zone_result
    }

    fn handle_zone(
        &self,
        hz_data: HandleZoneData<N>,
        dbg_data: &mut DbgData
    ) -> HandleZoneResult {
        self.dbg("handle_zone");
        // scenarios:
        // - 0 sender has nil zone id
        //   - 0a. i'm not settled
        //     - 0aa. sender in the same private network as me
        //       - generate my own zone
        //     - 0ab. sender in a different private network than me
        //       - generate my own zone
        //       - store the recipient address if not stored (ports
        //         should already be available)
        //   - 0b. i'm settled
        //     - 0ba. sender in the same private network as me
        //       - do nothing
        //     - 0bb. sender in a different private network than me
        //       - store the recipient address if not stored (ports
        //         should already be available)
        // - 1 sender has non-nil zone id
        //   - 1a. i'm not settled
        //     - 1aa. sender in the same private network as me
        //       - assume sender's zone
        //     - 1ab. sender in a different private network than me
        //       - generate my own zone
        //       - store the recipient address if not stored (ports
        //         should already be available)
        //       - store sender zone id? what did i mean by that?
        //   - 1b. i'm settled
        //     - 1ba. sender in the same private network as me
        //       - 1ba<. senders zone id is less than mine
        //         - if message was ack then send another ack back to
        //           enlighten the sender about newer and better zone
        //       - 1ba=. senders zone id is equal to mine
        //         - do nothing
        //       - 1ba>. senders zone id is greater than mine
        //         - use Self::process_zone_change_internal_state
        //     - 1bb. sender in a different private network than me
        //       - store the recipient address if not stored (ports
        //         should already be available)
        //       - store sender zone id? what did i mean by that?
        let mut member = self.server.read_member().clone();
        let mut member_zone_uuid = BfUuid::must_parse(member.get_zone_id());
        let mut zone_settled = *(self.server.read_zone_settled());

        let mut member_changed = false;
        let mut old_zone_is_dead = false;
        let mut insert_member_zone = false;
        let mut sender_has_nonnil_zone_id = true;
        let mut maybe_sender_zone = Self::get_zone_from_protozones(hz_data.zones, hz_data.from_member.get_zone_id());
        let mut store_additional_address = false;
        let mut send_ack = false;

        let dbg_scenario;
        let dbg_was_settled = zone_settled;
        let dbg_our_old_zone_id = member_zone_uuid.clone();

        if let Some(sender_zone) = maybe_sender_zone.take() {
            let sender_zone_uuid = sender_zone.get_uuid();

            if sender_zone_uuid.is_nil() {
                // this shouldn't happen
                if !zone_settled {
                    if hz_data.sender_in_the_same_zone_as_us {
                        // 0a.
                        dbg_scenario = "0a. should not happen";
                        member_zone_uuid = BfUuid::generate();
                        member.set_zone_id(member_zone_uuid.to_string());
                        member_changed = true;
                        insert_member_zone = true;
                        zone_settled = true;
                    } else {
                        // 0c.
                        dbg_scenario = "0c. should not happen";
                        member_zone_uuid = BfUuid::generate();
                        member.set_zone_id(member_zone_uuid.to_string());
                        member_changed = true;
                        insert_member_zone = true;
                        zone_settled = true;
                        store_additional_address = true;
                    }
                } else {
                    if hz_data.sender_in_the_same_zone_as_us {
                        // 0b.
                        dbg_scenario = "0b. should not happen";
                    } else {
                        // 0d.
                        store_additional_address = true;
                        dbg_scenario = "0d. should not happen";
                    }
                }
                sender_has_nonnil_zone_id = false;
            } else if !zone_settled {
                if hz_data.sender_in_the_same_zone_as_us {
                    // 1.
                    dbg_scenario = "1.";
                    member_zone_uuid = sender_zone_uuid.clone();
                    member.set_zone_id(member_zone_uuid.to_string());
                    member_changed = true;
                } else {
                    // 2.
                    dbg_scenario = "2.";
                    member_zone_uuid = BfUuid::generate();
                    member.set_zone_id(member_zone_uuid.to_string());
                    member_changed = true;
                    insert_member_zone = true;
                    store_additional_address = true;
                }
                zone_settled = true;
            } else {
                if hz_data.sender_in_the_same_zone_as_us {
                    if sender_zone_uuid < member_zone_uuid {
                        // 3.
                        dbg_scenario = "3.";
                        if hz_data.swim_type == Swim_Type::ACK {
                            send_ack = true;
                        }
                    } else if sender_zone_uuid == member_zone_uuid {
                        // 4.
                        dbg_scenario = "4.";
                    } else {
                        // 5.
                        dbg_scenario = "5.";
                        member_zone_uuid = sender_zone_uuid.clone();
                        member.set_zone_id(member_zone_uuid.to_string());
                        member_changed = true;
                        old_zone_is_dead = true;
                    }
                } else {
                    // 6.
                    dbg_scenario = "6.";
                    store_additional_address = true;
                }
            }
            if sender_has_nonnil_zone_id {
                maybe_sender_zone = Some(sender_zone)
            }
        } else {
            if !zone_settled {
                if hz_data.sender_in_the_same_zone_as_us {
                    // 0a.
                    dbg_scenario = "0a.";
                    member_zone_uuid = BfUuid::generate();
                    member.set_zone_id(member_zone_uuid.to_string());
                    member_changed = true;
                    insert_member_zone = true;
                    zone_settled = true;
                } else {
                    // 0c.
                    dbg_scenario = "0c.";
                    member_zone_uuid = BfUuid::generate();
                    member.set_zone_id(member_zone_uuid.to_string());
                    member_changed = true;
                    insert_member_zone = true;
                    zone_settled = true;
                    store_additional_address = true;
                }
            } else {
                if hz_data.sender_in_the_same_zone_as_us {
                    // 0b.
                    dbg_scenario = "0b.";
                } else {
                    // 0d.
                    store_additional_address = true;
                    dbg_scenario = "0d.";
                }
            }
            sender_has_nonnil_zone_id = false;
        }

        *(self.server.write_zone_settled()) = zone_settled;
        if store_additional_address && hz_data.to_address_kind != AddressKind::Real {
            let mut found = false;

            for zone_address in member.mut_additional_addresses().iter_mut() {
                if zone_address.get_address() == hz_data.to_member.get_address() {
                    if sender_has_nonnil_zone_id {
                        if zone_address.get_zone_id() == hz_data.from_member.get_zone_id() {
                            found = true;
                            break;
                        } else {
                            if BfUuid::parse_or_nil(zone_address.get_zone_id(), "additional zone id").is_nil() {
                                found = true;
                                member_changed = true;
                                zone_address.set_zone_id(hz_data.from_member.get_zone_id().to_string());
                                break;
                            }
                        }
                    } else {
                        found = true;
                        break;
                    }
                }
            }
            if !found {
                let additional_addresses = member.mut_additional_addresses();
                let mut zone_address = ZoneAddress::default();

                zone_address.set_zone_id(hz_data.from_member.get_zone_id().to_string());
                zone_address.set_address(hz_data.to_member.get_address().to_string());
                if hz_data.to_member.has_swim_port() {
                    zone_address.set_swim_port(hz_data.to_member.get_swim_port());
                }
                if hz_data.to_member.has_gossip_port() {
                    zone_address.set_gossip_port(hz_data.to_member.get_gossip_port());
                }

                additional_addresses.push(zone_address);
                member_changed = true;
            }
        }
        // zone has changed, additional addresses too
        if member_changed {
            let incarnation = member.get_incarnation();

            member.set_incarnation(incarnation + 1);
            *(self.server.write_member()) = member.clone();
            self.server.insert_member(member.clone(), Health::Alive);
        }
        if insert_member_zone {
            self.server
                .insert_zone(Zone::new(member_zone_uuid.to_string(), self.server.member_id().to_string()));
        }
        if let Some(sender_zone) = maybe_sender_zone {
            self.server.insert_zone(sender_zone.clone());

            if old_zone_is_dead {
                let mut zone_change = ZoneChange::new();
                let mut zone_change_dbg_data = ZoneChangeDbgData::default();

                // not setting the from field - it is used
                // only for blocking the message
                zone_change.set_zone_id(dbg_our_old_zone_id.to_string());
                zone_change.set_new_successor(sender_zone.proto);
                // TODO: use Self::process_zone_change_internal_state
                let results = Self::process_zone_change_internal_state(
                    maintained_zone_clone: mut Zone,
                    maybe_successor_of_maintained_zone_clone: mut Option<ProtoZone>,
                    our_zone_uuid: mut BfUuid,
                    zone_change: mut ZoneChange,
                    dbg_data: &mut ZoneChangeDbgData
                ) -> ZoneChangeResults {

                self.process_zone_change_internal(&zone_change, &mut zone_change_dbg_data);
                dbg_data.zone_change_dbg_data = Some(zone_change_dbg_data);
            }
        }

        dbg_data.from_zone_id = hz_data.from_member.get_zone_id().to_string();
        dbg_data.scenario = dbg_scenario.to_string();
        dbg_data.was_settled = dbg_was_settled;
        dbg_data.our_old_zone_id = dbg_our_old_zone_id.to_string();
        dbg_data.member_zone_uuid = member_zone_uuid.to_string();

        if hz_data.swim_type == Swim_Type::PING {
            self.dbg("end handle_zone");
            if sender_has_nonnil_zone_id {
                HandleZoneResult::Ok
            } else {
                HandleZoneResult::NilSenderZone
            }
        } else {
            let mut need_confirmation = false;

            if !hz_data.sender_in_the_same_zone_as_us {
                let test_addr = hz_data.addr.get_address();
                dbg_data.parse_failures.push(format!("test addr: {}", test_addr));
                for zone_address in hz_data.from_member.get_additional_addresses().iter() {
                    dbg_data.parse_failures.push(format!("addr: {}, id: {}", zone_address.get_address(), zone_address.get_zone_id()));
                    let parsed_zone_address = match Self::parse_addr(zone_address.get_address()) {
                        Ok(parsed_zone_address) => parsed_zone_address,
                        Err(e) => {
                            dbg_data.parse_failures.push(format!("Failed to parse additional address {}: {}", zone_address.get_address(), e));
                            continue;
                        }
                    };
                    if parsed_zone_address != test_addr {
                        continue;
                    }
                    dbg_data.parse_failures.push(format!("zone addr {} and addr {} are the same", parsed_zone_address, test_addr));
                    dbg_data.parse_failures.push(format!("checking zone id {} for nil", zone_address.get_zone_id()));
                    if BfUuid::parse_or_nil(zone_address.get_zone_id(), "zone id in from's additional addresses").is_nil() {
                        dbg_data.parse_failures.push("need confirmation".to_string());
                        need_confirmation = true;
                        break;
                    }
                }
            } else if send_ack {
                need_confirmation = true;
            }

            self.dbg("end handle_zone");
            if need_confirmation {
                // TODO: rename it to "send ack with zone id info"
                HandleZoneResult::ConfirmZoneID
            } else if sender_has_nonnil_zone_id {
                HandleZoneResult::Ok
            } else {
                HandleZoneResult::NilSenderZone
            }
        }
    }

    fn get_zone_from_protozones(zones: &[ProtoZone], zone_id: &str) -> Option<Zone> {
        for zone in zones {
            if zone.get_id() == zone_id {
                return Some(zone.into());
            }
        }
        None
    }
}
