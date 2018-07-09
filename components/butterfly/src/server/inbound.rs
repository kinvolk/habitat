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

use protobuf;

use error::Error;
use member::{Health, Member};
use message::{self,
              swim::{Member as ProtoMember, Swim, Swim_Type, Zone as ProtoZone, ZoneAddress}};
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
    from_address_kind: AddressKind,
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
                                    msg.get_zone_change().get_from().get_iid()
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
                error!(
                    "Supervisor {} sent an Ack with a nil zone ID",
                    msg.get_ack().get_from().get_id(),
                );
            }
            HandleZoneResult::UnknownSenderAddress => {
                error!(
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
                error!(
                    "Sender of the ACK message does not know its address {}. \
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

    fn process_zone_change(&self, addr: N::AddressAndPort, mut msg: Swim) {
        trace_it!(SWIM: &self.server,
                  TraceKind::RecvZoneChange,
                  msg.get_zone_change().get_from().get_id(),
                  addr,
                  &msg);
        trace!("Zone change from {}@{}", msg.get_zone_change().get_from().get_id(), addr);
        self.process_zone_change_internal(msg.get_zone_change());
    }

    fn process_zone_change_internal(&self, zone_change: &ZoneChange) {
        if let Some(zone) = self.server.read_zone_list().zones.get(zone_change.get_zone_id()) {
            if zone.get_maintainer_id() == self.server.member_id() {
                let mut possible_predecessor = None;
                let mut zone_changed = false;
                let mut member_changed = false;
                let mut our_zone = self.server.read_zone_list().zones.get(self.server.read_member().get_zone_id()).cloned().unwrap();
                let mut our_successor_to_inform = None;
                let mut our_predecessors_to_inform = Vec::new();
                let mut our_member = *(self.server.read_member()).clone();

                if zone_change.has_new_successor() && zone_change.get_new_successor() != our_zone.get_id() {
                    let successor = zone_change.get_new_successor();
                    let mut successor_zone_uuid = message::parse_uuid(successor.get_id(), "successor zone id");
                    if our_zone.has_successor() {
                        let mut our_successor_zone_uuid = message::parse_uuid(our_zone.get_successor(), "successor zone id");

                        if successor_zone_uuid < our_successor_zone_uuid {
                            possible_predecessor = Some(successor);
                        } else if successor_zone_uuid > our_successor_zone_uuid {
                            possible_predessor = Some(self.server.read_zone_list().zones.get(our_zone.get_successor()).unwrap());
                            our_zone.set_successor(successor.get_id());
                            zone_changed = true;
                            our_successor_to_inform = Some(successor.get_id());
                        }

                        if our_member.get_uuid() < successor_zone_uuid {
                            our_member.set_zone_id(successor.get_id());
                            member_changed = true;
                        }
                    }
                    if let Some(predecessor) = possible_predecessor {
                        let mut found = false;

                        for zone id in our_zone.get_predecessors().iter() {
                            if zone_id == predecessor.get_id() {
                                found = true;
                                break;
                            }
                        }

                        if !found {
                            our_zone.mut_predecessors().push(predecessor.get_id().to_string());
                            zone_changed = true;
                            our_predecessor_to_inform.push(predecessor.get_maintainer_id());
                        }
                    }
                }
                if zone_change.has_new_predecessors() {
                    for predecessor in zone_change.get_new_predecessors().iter() {
                        if predecessor.get_id() == our_zone.get_id() {
                            // haha, v.funny
                            continue;
                        }
                        let mut found = false;

                        for zone_id in our_zone.get_predecessors().iter() {
                            found = true;
                            break;
                        }

                        if !found {
                            our_zone.mut_predecessors().push(predecessor.get_id().to_string());
                            zone_changed = true;
                            our_predecessor_to_inform.push(predecessor.get_maintainer_id());;
                        }
                    }
                }

                if msg.get_zone_changed().has_new_successor() {
                    self.server.insert_zone(msg.get_zone_changed().get_new_successor().into());
                }
                for predecessor in msg.get_zone_changed().get_new_predecessors() {
                    self.server.insert_zone(predecessor.into());
                }
                if zone_changed {
                    let incarnation = our_zone.get_incarnation();

                    our_zone.set_incarnation(incarnation + 1);
                    self.server.insert_zone(our_zone);
                }

                if member_changed {
                    let incarnation = our_member.get_incarnation();

                    our_member.set_incarnation(incarnation + 1);
                    self.server.insert_member(our_member);
                }

                if let Some(successor_id) = our_successor_to_inform {
                    let maintainer_id = self.server.read_zone_list().zones.get(successor_id).unwrap().get_maintainer_id();
                    self.server.member_list.with_member(maintainer_id, |maybe_maintainer| {
                        if let Some(maintainer) = maybe_maintainer {
                            let mut zone_change = ZoneChange::new();
                            let mut predecessors = RepeatedField::new();

                            predecessors.push(self.server.read_zone_list().zones.get(self.server.read_member().get_zone_id()).unwrap());
                            zone_change.set_zone_id(successor_id);
                            zone_change.set_new_predecessors(predecessors);
                            outbound::zone_change(&self.server, &self.swim_sender, maintainer, zone_change);
                        }
                    });
                }
                for predecessor_id = our_predecessors_to_inform {
                    let maintainer_id = self.server.read_zone_list().zones.get(predecessor_id).unwrap().get_maintainer_id();
                    self.server.member_list.with_member(maintainer_id, |maybe_maintainer| {
                        if let Some(maintainer) = maybe_maintainer {
                            let mut zone_change = ZoneChange::new();

                            zone_change.set_zone_id(predecessor_id);
                            zone_change.set_new_successor(self.server.read_zone_list().zones.get(self.server.read_member().get_zone_id()).unwrap());
                            outbound::zone_change(&self.server, &self.swim_sender, maintainer, zone_change);
                        }
                    });
                }
            } else {
                self.server.member_list.with_member(zone.get_maintainer_id(), |maybe_maintainer| {
                    if let Some(maintainer) = maybe_maintainer {
                        outbound::zone_change(&self.server, &self.swim_sender, maintainer, zone_change);
                    }
                });
            }
        }
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
        // 2. additional - an address is the same as one of member's additional addresses
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
                from_address_kind: from_address_kind,
                to_address_kind: to_address_kind,
                sender_in_the_same_zone_as_us: sender_in_the_same_zone_as_us,
            };
            self.handle_zone(handle_zone_data, &mut dbg_data)
        };
        dbg_data.handle_zone_result = handle_zone_result;
        println!("=========={:?}==========\n\
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
        );
        handle_zone_result
    }

    fn handle_zone(
        &self,
        hz_data: HandleZoneData<N>,
        dbg_data: &mut DbgData
    ) -> HandleZoneResult {
        // scenarios:
        // 0a. sender has nil zone id, i'm not settled, sender in the same zone as me
        //   - generate my own zone id
        // 0b. sender has nil zone id, i'm settled, sender in the same zone as me
        //   - do nothing
        // 0c. sender has nil zone id, i'm not settled, sender in different same zone than me
        //   - generate my own zone id
        //   - store the recipient address
        // 0d. sender has nil zone id, i'm settled, sender in different same zone than me
        //   - store the recipient address
        // 1. i'm not settled, sender in the same zone as me
        //   - assume sender's zone id
        // 2. i'm not settled, sender in different zone than me
        //   - generate my own zone id
        //   - store sender zone id and the recipient address
        // 3. i'm settled and sender in the same zone as me, senders zone id is lesser or equal than ours
        //   - do nothing
        // 4. i'm settled and sender in the same zone as me, senders zone id is greater than ours
        //   - assume sender's zone id
        // 5. i'm settled and sender in the different zone than me
        //   - store sender zone id and the recipient address
        let mut member = *(self.server.read_member()).clone();
        let mut member_zone_uuid = message::parse_uuid(member.get_zone_id(), "our own zone id");
        let mut zone_settled = *(self.server.read_zone_settled());

        let mut member_zone_changed = false;
        let mut old_zone_is_dead = false;
        let mut insert_member_zone = false;
        let mut sender_has_nonnil_zone_id = true;
        let mut maybe_sender_zone = Self::get_zone_from_protozones(hz_data.zones, hz_data.from_member.get_zone_id());
        let mut store_additional_address = false;

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
                        member_zone_uuid = message::parse_uuid(&message::generate_uuid(), "our new own zone id");
                        member.set_zone_id(member_zone_uuid.to_string());
                        member_zone_changed = true;
                        insert_member_zone = true;
                        zone_settled = true;
                    } else {
                        // 0c.
                        dbg_scenario = "0c. should not happen";
                        member_zone_uuid = message::parse_uuid(&message::generate_uuid(), "our new own zone id");
                        member.set_zone_id(member_zone_uuid.to_string());
                        member_zone_changed = true;
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
                    member_zone_changed = true;
                } else {
                    // 2.
                    dbg_scenario = "2.";
                    member_zone_uuid = message::parse_uuid(&message::generate_uuid(), "our new own zone id");
                    member.set_zone_id(member_zone_uuid.to_string());
                    member_zone_changed = true;
                    insert_member_zone = true;
                    store_additional_address = true;
                }
                zone_settled = true;
            } else {
                if hz_data.sender_in_the_same_zone_as_us {
                    if sender_zone_uuid <= member_zone_uuid {
                        // 3.
                        dbg_scenario = "3.";
                    } else {
                        // 4.
                        dbg_scenario = "4.";
                        member_zone_uuid = sender_zone_uuid.clone();
                        member.set_zone_id(member_zone_uuid.to_string());
                        member_zone_changed = true;
                        old_zone_is_dead = true;
                    }
                } else {
                    // 5.
                    dbg_scenario = "5.";
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
                    member_zone_uuid = message::parse_uuid(&message::generate_uuid(), "our new own zone id");
                    member.set_zone_id(member_zone_uuid.to_string());
                    member_zone_changed = true;
                    insert_member_zone = true;
                    zone_settled = true;
                } else {
                    // 0c.
                    dbg_scenario = "0c.";
                    member_zone_uuid = message::parse_uuid(&message::generate_uuid(), "our new own zone id");
                    member.set_zone_id(member_zone_uuid.to_string());
                    member_zone_changed = true;
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
                            if message::parse_uuid(zone_address.get_zone_id(), "additional zone id").is_nil() {
                                found = true;
                                member_zone_changed = true;
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
                member_zone_changed = true;
            }
        }
        if member_zone_changed {
            let mut member_clone = member.clone();
            let incarnation = member_clone.get_incarnation();

            member_clone.set_incarnation(incarnation + 1);
            self.server.insert_member(member_clone, Health::Alive);
        }
        if insert_member_zone {
            self.server
                .insert_zone(Zone::new(member_zone_uuid.to_string(), self.server.member_id()));
        }
        if let Some(sender_zone) = maybe_sender_zone {
            self.server.insert_zone(sender_zone);
        }
        if old_zone_is_dead {
            let mut zone_change = ZoneChange::new();

            // not setting the from field - it is used
            // only for blocking the message
            zone_change.set_zone_id(dbg_our_old_zone_id.to_string());
            zone_change.set_new_successor(member_zone_uuid.to_string());
            self.process_zone_change_internal(zone_change);
        }

        dbg_data.from_zone_id = hz_data.from_member.get_zone_id().to_string();
        dbg_data.scenario = dbg_scenario.to_string();
        dbg_data.was_settled = dbg_was_settled;
        dbg_data.our_old_zone_id = dbg_our_old_zone_id.to_string();
        dbg_data.member_zone_uuid = member_zone_uuid.to_string();

        if hz_data.swim_type == Swim_Type::PING {
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
                    if message::parse_uuid(zone_address.get_zone_id(), "zone id in from's additional addresses").is_nil() {
                        dbg_data.parse_failures.push("need confirmation".to_string());
                        need_confirmation = true;
                        break;
                    }
                }
            }

            if need_confirmation {
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
