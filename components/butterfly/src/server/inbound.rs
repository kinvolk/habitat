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
              swim::{Swim, Swim_Type, Zone as ProtoZone}};
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
        if !self.handle_zone_for_recipient(msg.get_zones(), msg.get_ack().get_from().get_zone_id(), msg.get_ack().get_to().get_address()) {
            error!(
                "Supervisor {} sent an Ack with a nil zone ID",
                msg.get_ack().get_from().get_id(),
            )
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
        let insert_pinger = self.handle_zone_for_recipient(msg.get_zones(), msg.get_ping().get_from().get_zone_id(), msg.get_ping().get_to().get_address());
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

    fn handle_zone_for_recipient(&self, zones: &[ProtoZone], sender_zone_id: &str, our_recipient_address: &str) -> bool {
        let sender_in_the_same_zone_as_us = {
            match <<N as Network>::AddressAndPort as AddressAndPort>::Address::create_from_str(our_recipient_address) {
                Ok(addr) => addr == self.server.host_address,
                Err(e) => {
                    error!("Error parsing recipient address {}: {}", our_recipient_address, e);
                    false
                }
            }
        };

        self.handle_zone(zones, sender_zone_id, sender_in_the_same_zone_as_us)
    }

    fn handle_zone(&self, zones: &[ProtoZone], sender_zone_id: &str, sender_in_the_same_zone_as_us: bool) -> bool {
        // scenarios:
        // 0a. sender has nil id, i'm not settled
        //   - generate my own zone id
        // 0b. sender has nil id, i'm settled
        //   - do nothing
        // 1. i'm not settled, sender in the same zone as me
        //   - assume sender's zone id
        // 2. i'm not settled, sender in different zone than me
        //   - generate my own zone id
        // 3. i'm settled and sender in the same zone as me, senders zone id is lesser or equal than ours
        //   - do nothing
        // 4. i'm settled and sender in the same zone as me, senders zone id is greater than ours
        //   - assume sender's zone id
        // 5. i'm settled and sender in the different zone than me
        //   - do nothing
        let mut member = self.server.write_member();
        let mut member_zone_uuid = message::parse_uuid(member.get_zone_id(), "our own zone id");
        let mut zone_settled_guard = self.server.write_zone_settled();
        let mut zone_settled = *zone_settled_guard;

        let mut member_zone_changed = false;
        let mut old_zone_is_dead = false;
        let mut insert_member_zone = false;
        let mut insert_sender_member = true;
        let mut maybe_sender_zone = Self::get_zone_from_protozones(zones, sender_zone_id);

        let dbg_scenario;
        let dbg_was_settled = zone_settled;
        let dbg_our_old_zone_id = member_zone_uuid.clone();

        if let Some(sender_zone) = maybe_sender_zone.take() {
            let sender_zone_uuid = sender_zone.get_uuid();

            if sender_zone_uuid.is_nil() {
                // this shouldn't happen
                if !zone_settled {
                    // 0a.
                    dbg_scenario = "0a. should not happen";
                    member_zone_uuid = message::parse_uuid(&message::generate_uuid(), "our new own zone id");
                    member.set_zone_id(member_zone_uuid.to_string());
                    member_zone_changed = true;
                    insert_member_zone = true;
                    zone_settled = true;
                } else {
                    // 0b.
                    dbg_scenario = "0b. should not happen";
                }
                insert_sender_member = false;
            } else if !zone_settled {
                if sender_in_the_same_zone_as_us {
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
                }
                zone_settled = true;
            } else {
                if sender_in_the_same_zone_as_us {
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
                }
            }
            if insert_sender_member {
                maybe_sender_zone = Some(sender_zone)
            }
        } else {
            if !zone_settled {
                // 0a.
                dbg_scenario = "0a.";
                member_zone_uuid = message::parse_uuid(&message::generate_uuid(), "our new own zone id");
                member.set_zone_id(member_zone_uuid.to_string());
                member_zone_changed = true;
                insert_member_zone = true;
                zone_settled = true;
            } else {
                dbg_scenario = "0b.";
                // 0b.
            }
            insert_sender_member = false;
        }

        *zone_settled_guard = zone_settled;
        if member_zone_changed {
            let mut member_clone = (*member).clone();
            let incarnation = member_clone.get_incarnation();

            member_clone.set_incarnation(incarnation + 1);
            self.server.insert_member(member_clone, Health::Alive);
        }
        if insert_member_zone {
            self.server
                .insert_zone(Zone::new(member_zone_uuid.to_string()));
        }
        if let Some(sender_zone) = maybe_sender_zone {
            self.server.insert_zone(sender_zone);
        }
        if old_zone_is_dead {
            // TODO: if I'm the maintainer of the old zone, update the
            // zone info to mark it as dead and send acks to
            // maintainers of parent/children zones of the old zone
        }

        println!(
            "sender zone id: {}, \
             scenario: {}, \
             was settled: {}, \
             our old zone id: {}, \
             our new zone id: {}, \
             insert pinger: {}",
            sender_zone_id,
            dbg_scenario,
            dbg_was_settled,
            dbg_our_old_zone_id,
            member_zone_uuid,
            insert_sender_member);

        insert_sender_member
    }

    fn get_zone_from_protozones(zones: &[ProtoZone], zone_id: &str) -> Option<Zone> {
        let zone_ids = zones.iter()
            .map(|z| z.get_id())
            .collect::<Vec<_>>();
        for zone in zones {
            if zone.get_id() == zone_id {
                println!("get_zone_from_protozones: found zone id {} in {:?}", zone_id, zone_ids);
                return Some(zone.into());
            }
        }
        println!("get_zone_from_protozones: did not find zone id {} in {:?}", zone_id, zone_ids);
        return None;
    }
}
