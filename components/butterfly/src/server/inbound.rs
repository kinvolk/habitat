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

use std::cmp::Ordering as CmpOrdering;
use std::mem;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use protobuf::{self, RepeatedField};

use error::Error;
use member::{Health, Member};
use message::{
    swim::{Member as ProtoMember, Swim, Swim_Type, Zone as ProtoZone, ZoneChange}, BfUuid,
};
use network::{AddressAndPort, MyFromStr, Network, SwimReceiver};
use server::{
    outbound,
    zones::{
        self, AddressKind, HandleZoneData, HandleZoneDbgData, HandleZoneResults,
        HandleZoneResultsStuff, ZoneChangeDbgData, ZoneChangeResultsMsgOrNothing,
    },
    Server,
};
use trace::TraceKind;
use zone::Zone;

/// Takes the Server and a channel to send received Acks to the outbound thread.
pub struct Inbound<N: Network> {
    pub server: Server<N>,
    pub swim_receiver: N::SwimReceiver,
    pub swim_sender: N::SwimSender,
    pub tx_outbound: mpsc::Sender<(N::AddressAndPort, Swim)>,
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

    fn dbg<T: AsRef<str>>(&self, msg: T) {
        println!("{}: {}", self.server.member_id(), msg.as_ref());
    }

    /// Run the thread. Listens for messages up to 1k in size, and then processes them accordingly.
    pub fn run(&self) {
        let mut recv_buffer: Vec<u8> = vec![0; 4096];
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
                                self.dbg(format!(
                                    "Dropped ping from {}@{}",
                                    msg.get_ping().get_from().get_id(),
                                    addr
                                ));
                                continue;
                            }
                            self.dbg("ping start");
                            self.process_ping(addr, msg);
                            self.dbg("ping end");
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
                                self.dbg(format!(
                                    "Dropped ack from {}@{}",
                                    msg.get_ack().get_from().get_id(),
                                    addr
                                ));
                                continue;
                            }
                            self.dbg("ack start");
                            self.process_ack(addr, msg);
                            self.dbg("ack end");
                        }
                        Swim_Type::PINGREQ => {
                            if self.server
                                .is_member_blocked(msg.get_pingreq().get_from().get_id())
                            {
                                debug!(
                                    "Not processing message from {} - it is blocked",
                                    msg.get_pingreq().get_from().get_id()
                                );
                                self.dbg(format!(
                                    "Dropped pingreq from {}@{}",
                                    msg.get_pingreq().get_from().get_id(),
                                    addr
                                ));
                                continue;
                            }
                            self.dbg("pingreq start");
                            self.process_pingreq(addr, msg);
                            self.dbg("pingreq end");
                        }
                        Swim_Type::ZONE_CHANGE => {
                            if self.server
                                .is_member_blocked(msg.get_zone_change().get_from().get_id())
                            {
                                debug!(
                                    "Not processing message from {} - it is blocked",
                                    msg.get_zone_change().get_from().get_id()
                                );
                                self.dbg(format!(
                                    "Dropped zone change from {}@{}",
                                    msg.get_zone_change().get_from().get_id(),
                                    addr
                                ));
                                continue;
                            }
                            self.dbg("zone change start");
                            self.process_zone_change(addr, msg);
                            self.dbg("zone change end");
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

    fn parse_addr(
        addr_str: &str,
    ) -> Result<
        <<N as Network>::AddressAndPort as AddressAndPort>::Address,
        <<<N as Network>::AddressAndPort as AddressAndPort>::Address as MyFromStr>::MyErr,
    > {
        <<N as Network>::AddressAndPort as AddressAndPort>::Address::create_from_str(addr_str)
    }

    /// Process ack messages; forwards to the outbound thread.
    fn process_ack(&self, addr: N::AddressAndPort, mut msg: Swim) {
        let mut send_ack = false;
        match self.handle_zone_for_recipient(
            msg.get_zones(),
            msg.get_field_type(),
            msg.get_ack().get_from(),
            msg.get_ack().get_to(),
            addr,
        ) {
            HandleZoneResults::Nothing => (),
            HandleZoneResults::UnknownSenderAddress => {
                warn!(
                    "Sender of the ACK message does not know its address {}. \
                     This shouldn't happen - this means that we sent a PING message to a server \
                     that is not directly reachable from us and it wasn't ignored by the receiver \
                     of the message",
                    addr,
                );
                return;
            }
            HandleZoneResults::SendAck => {
                send_ack = true;
            }
            HandleZoneResults::Stuff(stuff) => {
                if stuff.sender_has_nil_zone {
                    warn!(
                        "Supervisor {} sent an Ack with a nil zone ID",
                        msg.get_ack().get_from().get_id(),
                    );
                }
                send_ack = stuff.call_ack;
                if let Some(zone) = stuff.new_maintained_zone {
                    let zone_id = zone.get_id().to_string();
                    self.server.insert_zone(zone);
                    let mut zone_list = self.server.write_zone_list();
                    zone_list.maintained_zone_id = Some(zone_id);
                }
                let member_changed = stuff.zone_uuid_for_our_member.is_some()
                    || stuff.additional_address_for_our_member.is_some();
                if member_changed {
                    let our_member_clone = {
                        let mut our_member = self.server.write_member();
                        let incarnation = our_member.get_incarnation();

                        our_member.set_incarnation(incarnation + 1);
                        if let Some(zone_uuid) = stuff.zone_uuid_for_our_member {
                            our_member.set_zone_id(zone_uuid.to_string());
                        }
                        if let Some((old, new)) = stuff.additional_address_for_our_member {
                            for zone_address in our_member.mut_additional_addresses().iter_mut() {
                                if zone_address.get_address() != old.get_address() {
                                    continue;
                                }
                                if zone_address.get_swim_port() != old.get_swim_port() {
                                    continue;
                                }
                                if zone_address.get_zone_id() != old.get_zone_id() {
                                    continue;
                                }
                                zone_address.set_address(new.get_address().to_string());
                                zone_address.set_zone_id(new.get_zone_id().to_string());
                                break;
                            }
                        }

                        our_member.clone()
                    };
                    *self.server.write_zone_settled() = true;
                    self.server.insert_member(our_member_clone, Health::Alive);
                }
                if let Some((msg, target)) = stuff.msg_and_target {
                    outbound::zone_change(&self.server, &self.swim_sender, &target, msg);
                }
            }
            HandleZoneResults::ZoneProcessed(mut results) => {
                let zone_changed = results.successor_for_maintained_zone.is_some()
                    || !results.predecessors_to_add_to_maintained_zone.is_empty();
                let mut maintained_zone = Zone::default();

                mem::swap(&mut maintained_zone, &mut results.original_maintained_zone);

                if let Some(successor_id) = results.successor_for_maintained_zone.take() {
                    maintained_zone.set_successor(successor_id);
                }
                for predecessor_id in results.predecessors_to_add_to_maintained_zone {
                    maintained_zone.mut_predecessors().push(predecessor_id);
                }
                if zone_changed {
                    let incarnation = maintained_zone.get_incarnation();

                    maintained_zone.set_incarnation(incarnation + 1);
                    self.server.insert_zone(maintained_zone.clone());
                    send_ack = true;
                }
                for zone in results.zones_to_insert.drain(..) {
                    self.server.insert_zone(zone);
                }
                if let Some(zone_uuid) = results.zone_uuid_for_our_member {
                    let our_member_clone = {
                        let mut our_member = self.server.write_member();
                        let incarnation = our_member.get_incarnation();

                        our_member.set_zone_id(zone_uuid.to_string());
                        our_member.set_incarnation(incarnation + 1);

                        our_member.clone()
                    };
                    *self.server.write_zone_settled() = true;
                    self.server.insert_member(our_member_clone, Health::Alive);
                    send_ack = true;
                }

                //let mut dbg_sent_zone_change_with_alias_to = Vec::new();

                if !results.aliases_to_inform.is_empty() {
                    send_ack = true;
                    let mut zone_ids_and_maintainer_ids = {
                        let zone_list = self.server.read_zone_list();

                        results
                            .aliases_to_inform
                            .iter()
                            .filter_map(|uuid| {
                                let zone_id = uuid.to_string();

                                zone_list
                                    .zones
                                    .get(&zone_id)
                                    .map(|zone| (zone_id, zone.get_maintainer_id().to_string()))
                            })
                            .collect::<Vec<_>>()
                    };

                    let mut msgs_and_targets = Vec::new();

                    {
                        let mut msgs_and_targets = &mut msgs_and_targets;
                        let mut zone_ids_and_maintainer_ids = &mut zone_ids_and_maintainer_ids;

                        self.server
                            .member_list
                            .with_member_list(move |members_map| {
                                for (zone_id, maintainer_id) in
                                    zone_ids_and_maintainer_ids.drain(..)
                                {
                                    if let Some(maintainer) = members_map.get(&maintainer_id) {
                                        let mut zone_change = ZoneChange::new();
                                        //let addr: N::AddressAndPort = maintainer.swim_socket_address();

                                        //dbg_sent_zone_change_with_alias_to.push((maintainer_id, addr.to_string()));
                                        zone_change.set_zone_id(zone_id);
                                        zone_change.set_new_aliases(RepeatedField::from_vec(vec![
                                            maintained_zone.proto.clone(),
                                        ]));
                                        msgs_and_targets.push((zone_change, maintainer.clone()));
                                    }
                                }
                            });
                    }

                    for (msg, target) in msgs_and_targets {
                        outbound::zone_change(&self.server, &self.swim_sender, &target, msg);
                    }
                }
                //dbg_data.sent_zone_change_with_alias_to = dbg_sent_zone_change_with_alias_to;
            }
        }
        if send_ack {
            outbound::ack(
                &self.server,
                &self.swim_sender,
                &msg.get_ack().get_from().clone().into(),
                addr,
                None,
            );
        }
        trace_it!(SWIM: &self.server,
                  TraceKind::RecvAck,
                  msg.get_ack().get_from().get_id(),
                  addr,
                  &msg);
        trace!("Ack from {}@{}", msg.get_ack().get_from().get_id(), addr);
        if msg.get_ack().has_forward_to() {
            if self.server.member_id() != msg.get_ack().get_forward_to().get_id() {
                let forward_addr =
                    match Self::parse_addr(msg.get_ack().get_forward_to().get_address()) {
                        Ok(addr) => addr,
                        Err(e) => {
                            error!(
                                "Abandoning Ack forward: cannot parse member address {}: {}",
                                msg.get_ack().get_forward_to().get_address(),
                                e
                            );
                            return;
                        }
                    };
                let forward_to_addr = N::AddressAndPort::new_from_address_and_port(
                    forward_addr,
                    msg.get_ack().get_forward_to().get_swim_port() as u16,
                );
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
        let zones = msg.take_zones().iter().map(|z| Zone::from(z)).collect();
        match self.tx_outbound.send((addr, msg)) {
            Ok(()) => {}
            Err(e) => panic!("Outbound thread has died - this shouldn't happen: #{:?}", e),
        }
        self.server.insert_member_from_rumors(membership);
        self.server.insert_zones_from_rumors(zones);
    }

    /// Process ping messages.
    fn process_ping(&self, addr: N::AddressAndPort, mut msg: Swim) {
        let mut insert_pinger = true;
        match self.handle_zone_for_recipient(
            msg.get_zones(),
            msg.get_field_type(),
            msg.get_ping().get_from(),
            msg.get_ping().get_to(),
            addr,
        ) {
            HandleZoneResults::Nothing => (),
            HandleZoneResults::UnknownSenderAddress => {
                warn!(
                    "Sender of the PING message does not know its address {}. \
                     This shouldn't happen - this means that the sender sent a PING message to us \
                     and we are not directly reachable",
                    addr,
                );
                return;
            }
            HandleZoneResults::SendAck => {
                // we are going to send it anyway
            }
            HandleZoneResults::Stuff(stuff) => {
                if stuff.sender_has_nil_zone {
                    insert_pinger = false;
                }
                if let Some(zone) = stuff.new_maintained_zone {
                    let zone_id = zone.get_id().to_string();
                    self.server.insert_zone(zone);
                    let mut zone_list = self.server.write_zone_list();
                    zone_list.maintained_zone_id = Some(zone_id);
                }
                let member_changed = stuff.zone_uuid_for_our_member.is_some()
                    || stuff.additional_address_for_our_member.is_some();
                if member_changed {
                    let our_member_clone = {
                        let mut our_member = self.server.write_member();
                        let incarnation = our_member.get_incarnation();

                        our_member.set_incarnation(incarnation + 1);
                        if let Some(zone_uuid) = stuff.zone_uuid_for_our_member {
                            our_member.set_zone_id(zone_uuid.to_string());
                        }
                        if let Some((old, new)) = stuff.additional_address_for_our_member {
                            for zone_address in our_member.mut_additional_addresses().iter_mut() {
                                if zone_address.get_address() != old.get_address() {
                                    continue;
                                }
                                if zone_address.get_swim_port() != old.get_swim_port() {
                                    continue;
                                }
                                if zone_address.get_zone_id() != old.get_zone_id() {
                                    continue;
                                }
                                zone_address.set_address(new.get_address().to_string());
                                zone_address.set_zone_id(new.get_zone_id().to_string());
                                break;
                            }
                        }

                        our_member.clone()
                    };
                    *self.server.write_zone_settled() = true;
                    self.server.insert_member(our_member_clone, Health::Alive);
                }
                if let Some((msg, target)) = stuff.msg_and_target {
                    outbound::zone_change(&self.server, &self.swim_sender, &target, msg);
                }
            }
            HandleZoneResults::ZoneProcessed(mut results) => {
                let zone_changed = results.successor_for_maintained_zone.is_some()
                    || !results.predecessors_to_add_to_maintained_zone.is_empty();
                let mut maintained_zone = Zone::default();

                mem::swap(&mut maintained_zone, &mut results.original_maintained_zone);

                if let Some(successor_id) = results.successor_for_maintained_zone.take() {
                    maintained_zone.set_successor(successor_id);
                }
                for predecessor_id in results.predecessors_to_add_to_maintained_zone {
                    maintained_zone.mut_predecessors().push(predecessor_id);
                }
                if zone_changed {
                    let incarnation = maintained_zone.get_incarnation();

                    maintained_zone.set_incarnation(incarnation + 1);
                    self.server.insert_zone(maintained_zone.clone());
                }
                for zone in results.zones_to_insert.drain(..) {
                    self.server.insert_zone(zone);
                }
                if let Some(zone_uuid) = results.zone_uuid_for_our_member {
                    let our_member_clone = {
                        let mut our_member = self.server.write_member();
                        let incarnation = our_member.get_incarnation();

                        our_member.set_zone_id(zone_uuid.to_string());
                        our_member.set_incarnation(incarnation + 1);

                        our_member.clone()
                    };

                    *self.server.write_zone_settled() = true;
                    self.server.insert_member(our_member_clone, Health::Alive);
                }

                //let mut dbg_sent_zone_change_with_alias_to = Vec::new();

                if !results.aliases_to_inform.is_empty() {
                    let mut zone_ids_and_maintainer_ids = {
                        let zone_list = self.server.read_zone_list();

                        results
                            .aliases_to_inform
                            .iter()
                            .filter_map(|uuid| {
                                let zone_id = uuid.to_string();

                                zone_list
                                    .zones
                                    .get(&zone_id)
                                    .map(|zone| (zone_id, zone.get_maintainer_id().to_string()))
                            })
                            .collect::<Vec<_>>()
                    };

                    let mut msgs_and_targets = Vec::new();

                    {
                        let mut msgs_and_targets = &mut msgs_and_targets;
                        let mut zone_ids_and_maintainer_ids = &mut zone_ids_and_maintainer_ids;

                        self.server.member_list.with_member_list(|members_map| {
                            for (zone_id, maintainer_id) in zone_ids_and_maintainer_ids.drain(..) {
                                if let Some(maintainer) = members_map.get(&maintainer_id) {
                                    let mut zone_change = ZoneChange::new();
                                    //let addr: N::AddressAndPort = maintainer.swim_socket_address();

                                    //dbg_sent_zone_change_with_alias_to.push((maintainer_id, addr.to_string()));
                                    zone_change.set_zone_id(zone_id);
                                    zone_change.set_new_aliases(RepeatedField::from_vec(vec![
                                        maintained_zone.proto.clone(),
                                    ]));
                                    msgs_and_targets.push((zone_change, maintainer.clone()));
                                }
                            }
                        });
                    }

                    for (msg, target) in msgs_and_targets {
                        outbound::zone_change(&self.server, &self.swim_sender, &target, msg);
                    }
                }
                //dbg_data.sent_zone_change_with_alias_to = dbg_sent_zone_change_with_alias_to;
            }
        }
        let target: Member = msg.get_ping().get_from().into();
        trace_it!(SWIM: &self.server,
                  TraceKind::RecvPing,
                  msg.get_ping().get_from().get_id(),
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
        trace!("Ping from {}@{}", msg.get_ping().get_from().get_id(), addr);
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
            let zones = msg.take_zones().iter().map(|z| Zone::from(z)).collect();
            self.server.insert_zones_from_rumors(zones);
        }
    }

    fn process_zone_change(&self, addr: N::AddressAndPort, mut msg: Swim) {
        //self.dbg("process_zone_change");
        trace_it!(SWIM: &self.server,
                  TraceKind::RecvZoneChange,
                  msg.get_zone_change().get_from().get_id(),
                  addr,
                  &msg);
        trace!(
            "Zone change from {}@{}",
            msg.get_zone_change().get_from().get_id(),
            addr
        );

        let mut dbg_data = ZoneChangeDbgData::default();
        let results_msg_or_nothing =
            self.process_zone_change_internal(msg.take_zone_change(), &mut dbg_data);

        match results_msg_or_nothing {
            ZoneChangeResultsMsgOrNothing::Nothing => (),
            ZoneChangeResultsMsgOrNothing::Msg((zone_change, target)) => {
                outbound::zone_change(&self.server, &self.swim_sender, &target, zone_change);
            }
            ZoneChangeResultsMsgOrNothing::Results(mut results) => {
                let zone_changed = results.successor_for_maintained_zone.is_some()
                    || !results.predecessors_to_add_to_maintained_zone.is_empty();
                let mut maintained_zone = Zone::default();

                mem::swap(&mut maintained_zone, &mut results.original_maintained_zone);

                if let Some(successor_id) = results.successor_for_maintained_zone.take() {
                    maintained_zone.set_successor(successor_id);
                }
                for predecessor_id in results.predecessors_to_add_to_maintained_zone {
                    maintained_zone.mut_predecessors().push(predecessor_id);
                }
                if zone_changed {
                    let incarnation = maintained_zone.get_incarnation();

                    maintained_zone.set_incarnation(incarnation + 1);
                    self.server.insert_zone(maintained_zone.clone());
                }
                for zone in results.zones_to_insert.drain(..) {
                    self.server.insert_zone(zone);
                }
                if let Some(zone_uuid) = results.zone_uuid_for_our_member {
                    let our_member_clone = {
                        let mut our_member = self.server.write_member();
                        let incarnation = our_member.get_incarnation();

                        our_member.set_zone_id(zone_uuid.to_string());
                        our_member.set_incarnation(incarnation + 1);

                        our_member.clone()
                    };

                    *self.server.write_zone_settled() = true;
                    self.server.insert_member(our_member_clone, Health::Alive);
                }

                let mut dbg_sent_zone_change_with_alias_to = Vec::new();

                if !results.aliases_to_inform.is_empty() {
                    let mut zone_ids_and_maintainer_ids = {
                        let zone_list = self.server.read_zone_list();

                        results
                            .aliases_to_inform
                            .iter()
                            .filter_map(|uuid| {
                                let zone_id = uuid.to_string();

                                zone_list
                                    .zones
                                    .get(&zone_id)
                                    .map(|zone| (zone_id, zone.get_maintainer_id().to_string()))
                            })
                            .collect::<Vec<_>>()
                    };

                    let mut msgs_and_targets = Vec::new();

                    {
                        let mut msgs_and_targets = &mut msgs_and_targets;
                        let mut zone_ids_and_maintainer_ids = &mut zone_ids_and_maintainer_ids;

                        self.server.member_list.with_member_list(|members_map| {
                            for (zone_id, maintainer_id) in zone_ids_and_maintainer_ids.drain(..) {
                                if let Some(maintainer) = members_map.get(&maintainer_id) {
                                    let mut zone_change = ZoneChange::new();
                                    let addr: N::AddressAndPort = maintainer.swim_socket_address();

                                    dbg_sent_zone_change_with_alias_to
                                        .push((maintainer_id, addr.to_string()));
                                    zone_change.set_zone_id(zone_id);
                                    zone_change.set_new_aliases(RepeatedField::from_vec(vec![
                                        maintained_zone.proto.clone(),
                                    ]));
                                    msgs_and_targets.push((zone_change, maintainer.clone()));
                                }
                            }
                        });
                    }

                    for (msg, target) in msgs_and_targets {
                        outbound::zone_change(&self.server, &self.swim_sender, &target, msg);
                    }
                }
                dbg_data.sent_zone_change_with_alias_to = Some(dbg_sent_zone_change_with_alias_to);
            }
        }
        outbound::ack(
            &self.server,
            &self.swim_sender,
            &msg.get_zone_change().get_from().clone().into(),
            addr,
            None,
        );
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

    fn process_zone_change_internal(
        &self,
        zone_change: ZoneChange,
        dbg_data: &mut ZoneChangeDbgData,
    ) -> ZoneChangeResultsMsgOrNothing {
        // meh…
        enum YaddaYadda {
            MaintainedZone(Zone),
            MaintainerID(String),
        }

        let yadda_yadda = {
            let zone_list = self.server.read_zone_list();
            let maybe_maintained_zone = zone_list.zones.get(zone_change.get_zone_id());

            dbg_data.zone_found = maybe_maintained_zone.is_some();

            if let Some(maintained_zone) = maybe_maintained_zone {
                let im_a_maintainer =
                    maintained_zone.get_maintainer_id() == self.server.member_id();

                dbg_data.is_a_maintainer = Some(im_a_maintainer);

                if im_a_maintainer {
                    YaddaYadda::MaintainedZone(maintained_zone.clone())
                } else {
                    YaddaYadda::MaintainerID(maintained_zone.get_maintainer_id().to_string())
                }
            } else {
                return ZoneChangeResultsMsgOrNothing::Nothing;
            }
        };
        let maintained_zone_clone = {
            match yadda_yadda {
                YaddaYadda::MaintainedZone(zone) => zone,
                YaddaYadda::MaintainerID(id) => {
                    let mut maybe_maintainer_clone = None;

                    self.server
                        .member_list
                        .with_member(&id, |maybe_maintainer| {
                            maybe_maintainer_clone = maybe_maintainer.cloned()
                        });

                    dbg_data.real_maintainer_found = Some(maybe_maintainer_clone.is_some());

                    if let Some(maintainer_clone) = maybe_maintainer_clone {
                        let addr: N::AddressAndPort = maintainer_clone.swim_socket_address();

                        dbg_data.forwarded_to =
                            Some((maintainer_clone.get_id().to_string(), addr.to_string()));

                        return ZoneChangeResultsMsgOrNothing::Msg((zone_change, maintainer_clone));
                    }

                    return ZoneChangeResultsMsgOrNothing::Nothing;
                }
            }
        };

        let maybe_successor_clone = self.server
            .read_zone_list()
            .zones
            .get(maintained_zone_clone.get_successor())
            .map(|z| z.proto.clone());
        let our_member_uuid = BfUuid::must_parse(self.server.read_member().get_zone_id());

        ZoneChangeResultsMsgOrNothing::Results(zones::process_zone_change_internal_state(
            maintained_zone_clone,
            maybe_successor_clone,
            our_member_uuid,
            zone_change,
            dbg_data,
        ))
    }

    fn address_kind(
        addr: <<N as Network>::AddressAndPort as AddressAndPort>::Address,
        member: &ProtoMember,
        dbg_data: &mut HandleZoneDbgData,
    ) -> AddressKind {
        let member_real_address = match Self::parse_addr(member.get_address()) {
            Ok(addr) => addr,
            Err(e) => {
                let msg = format!(
                    "Error parsing member {:?} address {}: {}",
                    member,
                    member.get_address(),
                    e
                );
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
                    let msg = format!(
                        "Error parsing member {:?} additional address {}: {}",
                        member,
                        member.get_address(),
                        e
                    );
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

    fn address_kind_from_str(
        addr: &str,
        member: &ProtoMember,
        dbg_data: &mut HandleZoneDbgData,
    ) -> AddressKind {
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
        addr: N::AddressAndPort,
    ) -> HandleZoneResults {
        //self.dbg("handle_zone_for_recipient");
        let mut dbg_data = HandleZoneDbgData::default();
        let from_address_kind = Self::address_kind(addr.get_address(), from, &mut dbg_data);
        let to_address_kind = Self::address_kind_from_str(
            to.get_address(),
            &self.server.read_member(),
            &mut dbg_data,
        );

        dbg_data.from_kind = from_address_kind;
        dbg_data.to_kind = to_address_kind;
        // we are dealing with several addresses here:
        //
        // real from address - can be an address of a mapping on a NAT
        // or a local one
        //
        // member from - contains a real address and additional addresses
        //
        // member to - contains an address that can be either local or
        // a mapping on a NAT
        //
        // member us - contains a local address and additional addresses
        //
        // address kinds:
        // 1. real - an address is the same as member's local address
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
        // from or it knows it can be reached, but does not know the
        // exact address (only ports). This likely should not happen -
        // if the server in child zone is not exposed in the parent
        // zone, the message should be routed through the gateway
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
                maybe_result = Some(HandleZoneResults::UnknownSenderAddress);
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

        let handle_zone_results = if let Some(result) = maybe_result {
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
        dbg_data.handle_zone_results = handle_zone_results.clone();
        println!(
            "=========={:?}==========\n\
             dbg:\n\
             \n\
             {:#?}\n\
             \n\
             member us: {:#?}\n\
             member from: {:#?}\n\
             \n\
             =====================",
            swim_type,
            dbg_data,
            self.server.read_member().proto,
            from,
        );
        //self.dbg("end handle_zone_for_recipient");
        handle_zone_results
    }

    fn handle_zone(
        &self,
        hz_data: HandleZoneData<N>,
        dbg_data: &mut HandleZoneDbgData,
    ) -> HandleZoneResults {
        //self.dbg("handle_zone");
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
        //
        // actions:
        // - settle zone
        // - generate my own zone
        //   - new zone uuid for our member
        //   - new maintained zone
        //   - send an ack
        // - store the additional address if not stored (ports should
        //   already be available)
        //   - if this is an ack and to zone id is nil and from is additional
        //     - send ack
        //   - this should always be an update of an existing address
        //     entry, never an addition
        //   - scenarios:
        //     - 0. nil sender zone id
        //       - search for fitting port number with no address and no zone
        //       - if there is only one then update the address, zone is still nil
        //       - otherwise ignore it
        //     - 1. non nil sender zone id
        //       - search for fitting port number with a specific zone
        //         - if found and address is the same, nothing to add
        //         - if found and address is different, continue with the other approach
        //         - if not found, continue with the other approach
        //       - search for fitting port number with a specific address
        //         - if found and zone id is the same, nothing to add (should be caught earlier, though)
        //         - if found and zone id is nil, update the zone id
        //         - if found and zone id is a child of sender - update the zone
        //         - if found and zone id is a parent of sender - no
        //           clue, do nothing, add another entry as a copy of
        //           this one? or rather warn?
        //         - if found and zone id is something else - ignore? should not happen?
        // - assume sender's zone (means that we were not settled yet)
        //   - new uuid for our member
        // - store sender zone id? what did i mean by that?
        // - if message was ack then send another ack back to
        //   enlighten the sender about newer and better zone
        // - use Self::process_zone_change_internal_statei
        let maybe_not_nil_sender_zone_and_uuid = {
            if let Some(zone) =
                Self::get_zone_from_protozones(hz_data.zones, hz_data.from_member.get_zone_id())
            {
                let zone_uuid = match zone.get_id().parse::<BfUuid>() {
                    Ok(uuid) => {
                        if uuid.is_nil() {
                            dbg_data.sender_zone_warning =
                                Some("Got a zone with a nil UUID, ignoring it".to_string());
                            warn!("Got a zone with a nil UUID, ignoring it");
                        }
                        uuid
                    }
                    Err(e) => {
                        dbg_data.sender_zone_warning = Some(format!(
                            "Got a zone with an invalid UUID {}, falling back to nil: {}",
                            zone.get_id(),
                            e
                        ));
                        warn!(
                            "Got a zone with an invalid UUID {}, falling back to nil: {}",
                            zone.get_id(),
                            e
                        );
                        BfUuid::nil()
                    }
                };
                if zone_uuid.is_nil() {
                    None
                } else {
                    Some((zone, zone_uuid))
                }
            } else {
                match hz_data.from_member.get_zone_id().parse::<BfUuid>() {
                    Ok(uuid) => {
                        if !uuid.is_nil() {
                            dbg_data.sender_zone_warning =
                                Some(format!("Got no zone info for {}", uuid));
                            warn!("Got no zone info for {}", uuid,);
                        }
                    }
                    Err(e) => {
                        dbg_data.sender_zone_warning = Some(format!(
                            "Got no zone info for invalid uuid {}: {}",
                            hz_data.from_member.get_zone_id(),
                            e
                        ));
                        warn!(
                            "Got no zone info for invalid uuid {}: {}",
                            hz_data.from_member.get_zone_id(),
                            e
                        );
                    }
                }
                None
            }
        };
        let zone_settled = *(self.server.read_zone_settled());
        let same_private_network = hz_data.sender_in_the_same_zone_as_us;
        let our_member_clone = self.server.read_member().clone();
        let (
            maybe_maintained_zone_clone,
            maybe_successor_of_maintained_zone_clone,
            maybe_our_zone_clone,
        ) = {
            let zone_list = self.server.read_zone_list();
            let maybe_our_zone_clone = zone_list.zones.get(our_member_clone.get_zone_id()).cloned();
            let zone_pair = if let &Some(ref maintained_zone_id) = &zone_list.maintained_zone_id {
                if let Some(maintained_zone) = zone_list.zones.get(maintained_zone_id) {
                    if maintained_zone.has_successor() {
                        if let Some(successor) =
                            zone_list.zones.get(maintained_zone.get_successor())
                        {
                            (Some(maintained_zone.clone()), Some(successor.proto.clone()))
                        } else {
                            warn!(
                                "Maintained zone {} has successor {}, \
                                 but we don't have it in our zone list",
                                maintained_zone_id,
                                maintained_zone.get_successor(),
                            );
                            (None, None)
                        }
                    } else {
                        (Some(maintained_zone.clone()), None)
                    }
                } else {
                    warn!(
                        "Maintained zone ID is {}, but we don't have it in our zone list",
                        maintained_zone_id
                    );
                    (None, None)
                }
            } else {
                (None, None)
            };

            (zone_pair.0, zone_pair.1, maybe_our_zone_clone)
        };
        let maybe_our_zone_maintainer_clone = if let Some(ref our_zone_clone) = maybe_our_zone_clone
        {
            let mut maybe_member = None;

            self.server.member_list.with_member(
                our_zone_clone.get_maintainer_id(),
                |maybe_maintainer| {
                    maybe_member = maybe_maintainer.cloned();
                },
            );

            maybe_member
        } else {
            None
        };

        dbg_data.was_settled = zone_settled;
        dbg_data.our_old_zone_id = our_member_clone.get_zone_id().to_string();

        let results = match (
            maybe_not_nil_sender_zone_and_uuid,
            zone_settled,
            same_private_network,
        ) {
            // 0aa.
            (None, false, true) => {
                dbg_data.scenario = "0aa".to_string();

                let mut stuff = HandleZoneResultsStuff::default();

                stuff.sender_has_nil_zone = true;
                // generate my own zone
                {
                    let new_zone_uuid = BfUuid::generate();

                    stuff.new_maintained_zone = Some(Zone::new(
                        new_zone_uuid.to_string(),
                        our_member_clone.get_id().to_string(),
                    ));
                    stuff.zone_uuid_for_our_member = Some(new_zone_uuid);
                    stuff.call_ack = true;

                    dbg_data.our_new_zone_id = new_zone_uuid.to_string();
                }

                HandleZoneResults::Stuff(stuff)
            }
            // 0ab.
            (None, false, false) => {
                dbg_data.scenario = "0ab".to_string();

                let mut stuff = HandleZoneResultsStuff::default();

                stuff.sender_has_nil_zone = true;
                // generate my own zone
                {
                    let new_zone_uuid = BfUuid::generate();

                    stuff.new_maintained_zone = Some(Zone::new(
                        new_zone_uuid.to_string(),
                        our_member_clone.get_id().to_string(),
                    ));
                    stuff.zone_uuid_for_our_member = Some(new_zone_uuid);
                    stuff.call_ack = true;

                    dbg_data.our_new_zone_id = new_zone_uuid.to_string();
                }
                // store the recipient address if not stored (ports
                // should already be available)
                {
                    if hz_data.from_address_kind == AddressKind::Additional
                        && BfUuid::parse_or_nil(
                            hz_data.to_member.get_zone_id(),
                            "to member zone id",
                        ).is_nil()
                    {
                        stuff.call_ack = true;
                        dbg_data
                            .additional_address_msgs
                            .push("will send an ack".to_string());
                    }
                    dbg_data.additional_address_msgs.push(format!(
                        "got message on {:?} address",
                        hz_data.to_address_kind
                    ));
                    if hz_data.to_address_kind != AddressKind::Real {
                        for zone_address in our_member_clone.get_additional_addresses().iter() {
                            if zone_address.get_swim_port() != hz_data.to_member.get_swim_port() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has swim port different than {}, skipping",
                                    zone_address,
                                    hz_data.to_member.get_swim_port()
                                ));
                                continue;
                            }

                            let zone_address_uuid = BfUuid::must_parse(zone_address.get_zone_id());

                            if !zone_address_uuid.is_nil() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has non-nil zone id, skipping",
                                    zone_address
                                ));
                                continue;
                            }
                            if zone_address.has_address() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} already has an address, skipping",
                                    zone_address
                                ));
                                continue;
                            }

                            let mut new_zone_address = zone_address.clone();

                            new_zone_address
                                .set_address(hz_data.to_member.get_address().to_string());
                            stuff.additional_address_for_our_member =
                                Some((zone_address.clone(), new_zone_address));

                            dbg_data.additional_address_update =
                                stuff.additional_address_for_our_member.clone();

                            break;
                        }
                    }
                }

                HandleZoneResults::Stuff(stuff)
            }
            // 0ba.
            (None, true, true) => {
                dbg_data.scenario = "0ba".to_string();

                let mut stuff = HandleZoneResultsStuff::default();

                stuff.sender_has_nil_zone = true;
                stuff.call_ack = true;

                HandleZoneResults::Stuff(stuff)
            }
            // 0bb.
            (None, true, false) => {
                dbg_data.scenario = "0bb".to_string();

                let mut stuff = HandleZoneResultsStuff::default();

                stuff.sender_has_nil_zone = true;
                // store the recipient address if not stored (ports
                // should already be available)
                {
                    if hz_data.from_address_kind == AddressKind::Additional
                        && BfUuid::parse_or_nil(
                            hz_data.to_member.get_zone_id(),
                            "to member zone id",
                        ).is_nil()
                    {
                        stuff.call_ack = true;
                        dbg_data
                            .additional_address_msgs
                            .push("will send an ack".to_string());
                    }
                    dbg_data.additional_address_msgs.push(format!(
                        "got message on {:?} address",
                        hz_data.to_address_kind
                    ));
                    if hz_data.to_address_kind != AddressKind::Real {
                        for zone_address in our_member_clone.get_additional_addresses().iter() {
                            if zone_address.get_swim_port() != hz_data.to_member.get_swim_port() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has swim port different than {}, skipping",
                                    zone_address,
                                    hz_data.to_member.get_swim_port()
                                ));
                                continue;
                            }

                            let zone_address_uuid = BfUuid::must_parse(zone_address.get_zone_id());

                            if !zone_address_uuid.is_nil() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has non-nil zone id, skipping",
                                    zone_address
                                ));
                                continue;
                            }
                            if zone_address.has_address() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} already has an address, skipping",
                                    zone_address
                                ));
                                continue;
                            }

                            let mut new_zone_address = zone_address.clone();

                            new_zone_address
                                .set_address(hz_data.to_member.get_address().to_string());
                            stuff.additional_address_for_our_member =
                                Some((zone_address.clone(), new_zone_address));

                            dbg_data.additional_address_update =
                                stuff.additional_address_for_our_member.clone();

                            break;
                        }
                    }
                }

                HandleZoneResults::Stuff(stuff)
            }
            // 1aa.
            (Some((_sender_zone, sender_zone_uuid)), false, true) => {
                dbg_data.scenario = "1aa".to_string();

                let mut stuff = HandleZoneResultsStuff::default();

                // assume sender's zone
                {
                    stuff.zone_uuid_for_our_member = Some(sender_zone_uuid);
                    stuff.call_ack = true;

                    dbg_data.our_new_zone_id = sender_zone_uuid.to_string();
                }

                HandleZoneResults::Stuff(stuff)
            }
            // 1ab.
            (Some((sender_zone, sender_zone_uuid)), false, false) => {
                dbg_data.scenario = "1ab".to_string();

                let mut stuff = HandleZoneResultsStuff::default();

                // generate my own zone
                {
                    let new_zone_uuid = BfUuid::generate();
                    stuff.new_maintained_zone = Some(Zone::new(
                        new_zone_uuid.to_string(),
                        our_member_clone.get_id().to_string(),
                    ));
                    stuff.zone_uuid_for_our_member = Some(new_zone_uuid);
                    stuff.call_ack = true;

                    dbg_data.our_new_zone_id = new_zone_uuid.to_string();
                }
                // store the recipient address if not stored (ports
                // should already be available)
                //
                // - 1. non nil sender zone id
                //   - search for a zone address instance with a
                //     variant-fitting zone
                //     - variant-fitting zone means a variant of a
                //       sender zone (successor/predecessor/itself)
                //     - found and both address and port are the
                //       same
                //       - zone in the instance is the same as sender
                //         zone or sender zone successor
                //         - nothing to do
                //       - zone in the instance is the same as one of
                //         the sender zone's predecessors
                //         - update the zone to sender's successor or
                //           to sender zone itself
                //     - not found
                //       - continue with the other approach
                //   - search for a zone address instance with a
                //     relation-fitting zone
                //     - relation-fitting zone means a relative of a
                //       sender zone (child/parent/itself)
                //     - found and both address and port are the
                //       same
                //       - zone in the instance is the same as sender
                //         zone or parent of the sender zone
                //         - do nothing (not sure about doing nothing
                //           for the parent case)
                //       - zone in the instance is the same as one of
                //         the children of the sender zone
                //         - update the zone in some way?
                //     - not found
                //       - continue with the other approach
                //   - search for a zone address instance with a nil zone
                //     - found and both address and port are the
                //       same
                //       - update the zone to sender zone itself
                //     - not found
                //       - continue with the other approach
                //   - search for a zone address instance with a nil
                //     zone and an unset address
                //     - found and ports are the same
                //       - update the zone to sender zone itself
                //       - update the address
                //     - not found
                //       - warn
                {
                    if hz_data.from_address_kind == AddressKind::Additional
                        && BfUuid::parse_or_nil(
                            hz_data.to_member.get_zone_id(),
                            "to member zone id",
                        ).is_nil()
                    {
                        stuff.call_ack = true;
                        dbg_data
                            .additional_address_msgs
                            .push("will send an ack".to_string());
                    }
                    // this is to ignore messages that arrived to our
                    // real address, not the additional one
                    let mut done = hz_data.to_address_kind == AddressKind::Real;
                    dbg_data.additional_address_msgs.push(format!(
                        "got message on {:?} address",
                        hz_data.to_address_kind
                    ));

                    if !done {
                        dbg_data
                            .additional_address_msgs
                            .push("going with the variant-fitting scenario".to_string());
                        for zone_address in our_member_clone.get_additional_addresses() {
                            if !zone_address.has_address() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has no address, skipping",
                                    zone_address
                                ));
                                continue;
                            }
                            if zone_address.get_address() != hz_data.to_member.get_address() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has different address than {}, skipping",
                                    zone_address,
                                    hz_data.to_member.get_address()
                                ));
                                continue;
                            }

                            if zone_address.get_swim_port() != hz_data.to_member.get_swim_port() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has different swim port than {}, skipping",
                                    zone_address,
                                    hz_data.to_member.get_swim_port()
                                ));
                                continue;
                            }

                            let zone_address_uuid = BfUuid::must_parse(zone_address.get_zone_id());

                            if zone_address_uuid == sender_zone_uuid {
                                done = true;
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has the same zone id as sender, done",
                                    zone_address
                                ));
                                break;
                            }

                            if sender_zone.has_successor() {
                                let sender_successor_uuid =
                                    BfUuid::must_parse(sender_zone.get_successor());

                                if sender_successor_uuid == zone_address_uuid {
                                    dbg_data.additional_address_msgs.push(format!("zone address {:#?} has the same zone id as sender's successor, done", zone_address));
                                    done = true;
                                    break;
                                }
                            }

                            let mut maybe_new_zone_id = None;

                            for predecessor_id in sender_zone.get_predecessors() {
                                let predecessor_uuid = BfUuid::must_parse(predecessor_id);

                                if predecessor_uuid == zone_address_uuid {
                                    dbg_data.additional_address_msgs.push(format!("zone address {:#?} has the same zone id as sender's predecessor, done", zone_address));
                                    if sender_zone.has_successor() {
                                        maybe_new_zone_id =
                                            Some(sender_zone.get_successor().to_string());
                                    } else {
                                        maybe_new_zone_id = Some(sender_zone_uuid.to_string());
                                    }
                                }
                            }
                            done = match maybe_new_zone_id {
                                Some(zone_id) => {
                                    let mut new_zone_address = zone_address.clone();

                                    new_zone_address.set_zone_id(zone_id);
                                    new_zone_address
                                        .set_address(hz_data.to_member.get_address().to_string());
                                    stuff.additional_address_for_our_member =
                                        Some((zone_address.clone(), new_zone_address));

                                    dbg_data.additional_address_update =
                                        stuff.additional_address_for_our_member.clone();

                                    true
                                }
                                None => {
                                    dbg_data.additional_address_msgs.push(format!(
                                        "zone address {:#?} does not match the sender, skipping",
                                        zone_address
                                    ));
                                    false
                                }
                            };
                            if done {
                                break;
                            }
                        }
                    }
                    if !done {
                        dbg_data
                            .additional_address_msgs
                            .push("going with the relative-fitting scenario".to_string());
                        // TODO: handle parent/child relationships
                        // following the steps written above
                        dbg_data
                            .additional_address_msgs
                            .push("haha not really, not implemented".to_string());
                    }
                    if !done {
                        dbg_data
                            .additional_address_msgs
                            .push("going with the nil-zoned scenario".to_string());
                        for zone_address in our_member_clone.get_additional_addresses() {
                            if !zone_address.has_address() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has no address, skipping",
                                    zone_address
                                ));
                                continue;
                            }
                            if zone_address.get_address() != hz_data.to_member.get_address() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has different address than {}, skipping",
                                    zone_address,
                                    hz_data.to_member.get_address()
                                ));
                                continue;
                            }

                            if zone_address.get_swim_port() != hz_data.to_member.get_swim_port() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has different swim port than {}, skipping",
                                    zone_address,
                                    hz_data.to_member.get_swim_port()
                                ));
                                continue;
                            }

                            let zone_address_uuid = BfUuid::must_parse(zone_address.get_zone_id());

                            if !zone_address_uuid.is_nil() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has non-nil zone, skipping",
                                    zone_address
                                ));
                                continue;
                            }

                            let mut new_zone_address = zone_address.clone();

                            new_zone_address.set_zone_id(sender_zone_uuid.to_string());
                            stuff.additional_address_for_our_member =
                                Some((zone_address.clone(), new_zone_address));

                            dbg_data.additional_address_update =
                                stuff.additional_address_for_our_member.clone();

                            done = true;
                            break;
                        }
                    }
                    if !done {
                        dbg_data.additional_address_msgs.push(
                            "going with the nil-zoned, address-guessing scenario".to_string(),
                        );
                        for zone_address in our_member_clone.get_additional_addresses() {
                            if zone_address.has_address() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} already has an address, skipping",
                                    zone_address
                                ));
                                continue;
                            }

                            if zone_address.get_swim_port() != hz_data.to_member.get_swim_port() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has different swim port than {}, skipping",
                                    zone_address,
                                    hz_data.to_member.get_swim_port()
                                ));
                                continue;
                            }

                            let zone_address_uuid = BfUuid::must_parse(zone_address.get_zone_id());

                            if !zone_address_uuid.is_nil() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has non-nil zone, skipping",
                                    zone_address
                                ));
                                continue;
                            }

                            let mut new_zone_address = zone_address.clone();

                            new_zone_address.set_zone_id(sender_zone_uuid.to_string());
                            new_zone_address
                                .set_address(hz_data.to_member.get_address().to_string());
                            stuff.additional_address_for_our_member =
                                Some((zone_address.clone(), new_zone_address));

                            dbg_data.additional_address_update =
                                stuff.additional_address_for_our_member.clone();

                            done = true;
                            break;
                        }
                    }
                    if !done {
                        dbg_data
                            .additional_address_msgs
                            .push("unhandled zone address…".to_string());
                        warn!("Arf")
                    }
                }

                HandleZoneResults::Stuff(stuff)
            }
            // 1ba.
            (Some((sender_zone, sender_zone_uuid)), true, true) => {
                dbg_data.scenario = "1ba".to_string();

                // - 0. we maintain a zone
                //   - use process_zone_change_internal_state
                // - 1. we do not maintain a zone
                //   - 1a. sender's zone id is less than ours
                //     - send ack back if this message was ack
                //   - 1b. sender's zone id is equal to ours
                //     - do nothing
                //   - 1c. sender's zone id is greater than ours
                //     - update our member's zone id
                //     - send zone change to the maintainer of the old
                //       zone id if the old zone has no info about the
                //       new successor
                let our_member_zone_uuid = BfUuid::must_parse(our_member_clone.get_zone_id());

                if let Some(maintained_zone_clone) = maybe_maintained_zone_clone {
                    let mut zone_change_dbg_data = ZoneChangeDbgData::default();
                    let mut zone_change = ZoneChange::default();

                    // zone_change.set_from() is not necessary, it is
                    // used only for checking in the block list
                    zone_change.set_zone_id(maintained_zone_clone.get_id().to_string());
                    zone_change.set_new_aliases(RepeatedField::from_vec(vec![sender_zone.proto]));

                    let zone_change_results = zones::process_zone_change_internal_state(
                        maintained_zone_clone,
                        maybe_successor_of_maintained_zone_clone,
                        our_member_zone_uuid,
                        zone_change,
                        &mut zone_change_dbg_data,
                    );

                    dbg_data.zone_change_dbg_data = Some(zone_change_dbg_data);

                    HandleZoneResults::ZoneProcessed(zone_change_results)
                } else {
                    match sender_zone_uuid.cmp(&our_member_zone_uuid) {
                        CmpOrdering::Less => HandleZoneResults::SendAck,
                        CmpOrdering::Equal => HandleZoneResults::Nothing,
                        CmpOrdering::Greater => {
                            let mut stuff = HandleZoneResultsStuff::default();
                            let maybe_msg_and_target = if let Some(our_zone_clone) =
                                maybe_our_zone_clone
                            {
                                let maybe_target = {
                                    dbg_data
                                        .parse_failures
                                        .push(format!("our zone clone: {:#?}", our_zone_clone));
                                    if our_zone_clone.has_successor() {
                                        dbg_data
                                            .parse_failures
                                            .push("our zone clone has successor".to_string());

                                        let successor_uuid =
                                            BfUuid::must_parse(our_zone_clone.get_successor());

                                        if successor_uuid < sender_zone_uuid {
                                            dbg_data.parse_failures.push(format!("our zone clone successor {} is less than sender zone {}, targetting {:#?}", successor_uuid, sender_zone_uuid, maybe_our_zone_maintainer_clone));
                                            maybe_our_zone_maintainer_clone
                                        } else {
                                            dbg_data.parse_failures.push(format!("our zone clone successor {} is NOT less than sender zone {}", successor_uuid, sender_zone_uuid));
                                            None
                                        }
                                    } else {
                                        dbg_data.parse_failures.push(format!(
                                            "our zone clone has no successor, targetting {:#?}",
                                            maybe_our_zone_maintainer_clone
                                        ));
                                        maybe_our_zone_maintainer_clone
                                    }
                                };

                                if let Some(target) = maybe_target {
                                    let mut zone_change = ZoneChange::default();
                                    let aliases = vec![sender_zone.proto.clone()];

                                    zone_change.set_from(our_member_clone.proto.clone());
                                    zone_change.set_zone_id(our_member_zone_uuid.to_string());
                                    zone_change.set_new_aliases(RepeatedField::from_vec(aliases));

                                    Some((zone_change, target))
                                } else {
                                    None
                                }
                            } else {
                                error!(
                                    "We have no information about our current zone {}",
                                    our_member_clone.get_zone_id()
                                );
                                None
                            };

                            stuff.zone_uuid_for_our_member = Some(sender_zone_uuid);
                            stuff.msg_and_target = maybe_msg_and_target;

                            dbg_data.our_new_zone_id = sender_zone_uuid.to_string();
                            dbg_data.msg_and_target = stuff.msg_and_target.clone();

                            HandleZoneResults::Stuff(stuff)
                        }
                    }
                }
            }
            // 1bb.
            (Some((sender_zone, sender_zone_uuid)), true, false) => {
                dbg_data.scenario = "1bb".to_string();

                let mut stuff = HandleZoneResultsStuff::default();

                // store the recipient address if not stored (ports
                // should already be available)
                //
                // - 1. non nil sender zone id
                //   - search for a zone address instance with a
                //     variant-fitting zone
                //     - variant-fitting zone means a variant of a
                //       sender zone (successor/predecessor/itself)
                //     - found and both address and port are the
                //       same
                //       - zone in the instance is the same as sender
                //         zone or sender zone successor
                //         - nothing to do
                //       - zone in the instance is the same as one of
                //         the sender zone's predecessors
                //         - update the zone to sender's successor or
                //           to sender zone itself
                //     - not found
                //       - continue with the other approach
                //   - search for a zone address instance with a
                //     relation-fitting zone
                //     - relation-fitting zone means a (direct?)
                //       relative of a sender zone (child/parent/itself)
                //     - found and both address and port are the
                //       same
                //       - zone in the instance is the same as sender
                //         zone or parent of the sender zone
                //         - do nothing (not sure about doing nothing
                //           for the parent case)
                //       - zone in the instance is the same as one of
                //         the children of the sender zone
                //         - update the zone in some way?
                //     - not found
                //       - continue with the other approach
                //   - search for a zone address instance with a nil zone
                //     - found and both address and port are the
                //       same
                //       - update the zone to sender zone itself
                //     - not found
                //       - continue with the other approach
                //   - search for a zone address instance with a nil
                //     zone and an unset address
                //     - found and both address and port are the
                //       same
                //       - update the zone to sender zone itself
                //     - not found
                //       - warn
                {
                    if hz_data.from_address_kind == AddressKind::Additional
                        && BfUuid::parse_or_nil(
                            hz_data.to_member.get_zone_id(),
                            "to member zone id",
                        ).is_nil()
                    {
                        stuff.call_ack = true;
                        dbg_data
                            .additional_address_msgs
                            .push("will send an ack".to_string());
                    }
                    // this is to ignore messages that arrived to our
                    // real address, not the additional one
                    let mut done = hz_data.to_address_kind == AddressKind::Real;
                    dbg_data.additional_address_msgs.push(format!(
                        "got message on {:?} address",
                        hz_data.to_address_kind
                    ));

                    if !done {
                        dbg_data
                            .additional_address_msgs
                            .push("going with the variant-fitting scenario".to_string());
                        for zone_address in our_member_clone.get_additional_addresses() {
                            if !zone_address.has_address() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has no address, skipping",
                                    zone_address
                                ));
                                continue;
                            }
                            if zone_address.get_address() != hz_data.to_member.get_address() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has different address than {}, skipping",
                                    zone_address,
                                    hz_data.to_member.get_address()
                                ));
                                continue;
                            }

                            if zone_address.get_swim_port() != hz_data.to_member.get_swim_port() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has different swim port than {}, skipping",
                                    zone_address,
                                    hz_data.to_member.get_swim_port()
                                ));
                                continue;
                            }

                            let zone_address_uuid = BfUuid::must_parse(zone_address.get_zone_id());

                            if zone_address_uuid == sender_zone_uuid {
                                done = true;
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has the same zone id as sender, done",
                                    zone_address
                                ));
                                break;
                            }

                            if sender_zone.has_successor() {
                                let sender_successor_uuid =
                                    BfUuid::must_parse(sender_zone.get_successor());

                                if sender_successor_uuid == zone_address_uuid {
                                    dbg_data.additional_address_msgs.push(format!("zone address {:#?} has the same zone id as sender's successor, done", zone_address));
                                    done = true;
                                    break;
                                }
                            }

                            let mut maybe_new_zone_id = None;

                            for predecessor_id in sender_zone.get_predecessors() {
                                let predecessor_uuid = BfUuid::must_parse(predecessor_id);

                                if predecessor_uuid == zone_address_uuid {
                                    dbg_data.additional_address_msgs.push(format!("zone address {:#?} has the same zone id as sender's predecessor, done", zone_address));
                                    if sender_zone.has_successor() {
                                        maybe_new_zone_id =
                                            Some(sender_zone.get_successor().to_string());
                                    } else {
                                        maybe_new_zone_id = Some(sender_zone_uuid.to_string());
                                    }
                                }
                            }
                            done = match maybe_new_zone_id {
                                Some(zone_id) => {
                                    let mut new_zone_address = zone_address.clone();

                                    new_zone_address.set_zone_id(zone_id);
                                    new_zone_address
                                        .set_address(hz_data.to_member.get_address().to_string());
                                    stuff.additional_address_for_our_member =
                                        Some((zone_address.clone(), new_zone_address));

                                    dbg_data.additional_address_update =
                                        stuff.additional_address_for_our_member.clone();

                                    true
                                }
                                None => {
                                    dbg_data.additional_address_msgs.push(format!(
                                        "zone address {:#?} does not match the sender, skipping",
                                        zone_address
                                    ));
                                    false
                                }
                            };
                            if done {
                                break;
                            }
                        }
                    }
                    if !done {
                        dbg_data
                            .additional_address_msgs
                            .push("going with the relative-fitting scenario".to_string());
                        // TODO: handle parent/child relationships
                        // following the steps written above
                        dbg_data
                            .additional_address_msgs
                            .push("haha not really, not implemented".to_string());
                    }
                    if !done {
                        dbg_data
                            .additional_address_msgs
                            .push("going with the nil-zoned scenario".to_string());
                        for zone_address in our_member_clone.get_additional_addresses() {
                            if !zone_address.has_address() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has no address, skipping",
                                    zone_address
                                ));
                                continue;
                            }
                            if zone_address.get_address() != hz_data.to_member.get_address() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has different address than {}, skipping",
                                    zone_address,
                                    hz_data.to_member.get_address()
                                ));
                                continue;
                            }

                            if zone_address.get_swim_port() != hz_data.to_member.get_swim_port() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has different swim port than {}, skipping",
                                    zone_address,
                                    hz_data.to_member.get_swim_port()
                                ));
                                continue;
                            }

                            let zone_address_uuid = BfUuid::must_parse(zone_address.get_zone_id());

                            if !zone_address_uuid.is_nil() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has non-nil zone, skipping",
                                    zone_address
                                ));
                                continue;
                            }

                            let mut new_zone_address = zone_address.clone();

                            new_zone_address.set_zone_id(sender_zone_uuid.to_string());
                            stuff.additional_address_for_our_member =
                                Some((zone_address.clone(), new_zone_address));

                            dbg_data.additional_address_update =
                                stuff.additional_address_for_our_member.clone();

                            done = true;
                            break;
                        }
                    }
                    if !done {
                        dbg_data.additional_address_msgs.push(
                            "going with the nil-zoned, address-guessing scenario".to_string(),
                        );
                        for zone_address in our_member_clone.get_additional_addresses() {
                            if zone_address.has_address() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} already has an address, skipping",
                                    zone_address
                                ));
                                continue;
                            }

                            if zone_address.get_swim_port() != hz_data.to_member.get_swim_port() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has different swim port than {}, skipping",
                                    zone_address,
                                    hz_data.to_member.get_swim_port()
                                ));
                                continue;
                            }

                            let zone_address_uuid = BfUuid::must_parse(zone_address.get_zone_id());

                            if !zone_address_uuid.is_nil() {
                                dbg_data.additional_address_msgs.push(format!(
                                    "zone address {:#?} has non-nil zone, skipping",
                                    zone_address
                                ));
                                continue;
                            }

                            let mut new_zone_address = zone_address.clone();

                            new_zone_address.set_zone_id(sender_zone_uuid.to_string());
                            new_zone_address
                                .set_address(hz_data.to_member.get_address().to_string());
                            stuff.additional_address_for_our_member =
                                Some((zone_address.clone(), new_zone_address));

                            dbg_data.additional_address_update =
                                stuff.additional_address_for_our_member.clone();

                            done = true;
                            break;
                        }
                    }
                    if !done {
                        dbg_data
                            .additional_address_msgs
                            .push("unhandled zone address…".to_string());
                        warn!("Arf")
                    }
                }

                HandleZoneResults::Stuff(stuff)
            }
        };

        results
        /*
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
                // TODO: rename it to "send ack with zone info"
                HandleZoneResult::ConfirmZoneID
            } else if sender_has_nonnil_zone_id {
                HandleZoneResult::Ok
            } else {
                HandleZoneResult::NilSenderZone
            }
        }
         */
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
