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

//! The outbound thread.
//!
//! This module handles the implementation of the swim probe protocol.

use std::collections::HashSet;
use std::fmt;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use protobuf::{Message, RepeatedField};
use time::SteadyTime;

use member::{Health, Member};
use message::{
    swim::{Ack, Member as ProtoMember, Ping, PingReq, Rumor_Type, Swim, Swim_Type, ZoneChange},
    BfUuid,
};
use network::{AddressAndPort, Network, SwimSender};
use rumor::RumorKey;
use server::timing::Timing;
use server::Server;
use trace::TraceKind;
use zone::Zone;

#[derive(Debug, Default)]
struct ReachableDbg {
    our_member: Member,
    their_member: Member,
    our_zone: Option<Zone>,
    their_zone: Option<Zone>,
    our_ids: Option<HashSet<String>>,
    their_ids: Option<HashSet<String>>,
}

/// How long to sleep between calls to `recv`.
const PING_RECV_QUEUE_EMPTY_SLEEP_MS: u64 = 10;

/// Where an Ack came from; either Ping or PingReq.
#[derive(Debug)]
enum AckFrom {
    Ping,
    PingReq,
}

impl fmt::Display for AckFrom {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &AckFrom::Ping => write!(f, "Ping"),
            &AckFrom::PingReq => write!(f, "PingReq"),
        }
    }
}

/// The outbound thread
pub struct Outbound<N: Network> {
    pub server: Server<N>,
    pub swim_sender: N::SwimSender,
    pub rx_inbound: mpsc::Receiver<(N::AddressAndPort, Swim)>,
    pub timing: Timing,
}

impl<N: Network> Outbound<N> {
    /// Creates a new Outbound struct.
    pub fn new(
        server: Server<N>,
        swim_sender: N::SwimSender,
        rx_inbound: mpsc::Receiver<(N::AddressAndPort, Swim)>,
        timing: Timing,
    ) -> Self {
        Self {
            server: server,
            swim_sender: swim_sender,
            rx_inbound: rx_inbound,
            timing: timing,
        }
    }

    /// Run the outbound thread. Gets a list of members to ping, then walks the list, probing each
    /// member.
    ///
    /// If the probe completes before the next protocol period is scheduled, waits for the protocol
    /// period to finish before starting the next probe.
    pub fn run(&mut self) {
        let mut have_members = false;
        loop {
            if !have_members {
                let num_initial = self.server.member_list.len_initial_members();
                if num_initial != 0 {
                    // The minimum that's strictly more than half
                    let min_to_start = num_initial / 2 + 1;

                    if self.server.member_list.len() >= min_to_start {
                        have_members = true;
                    } else {
                        self.server.member_list.with_initial_members(|member| {
                            ping(
                                &self.server,
                                &self.swim_sender,
                                &member,
                                member.swim_socket_address(),
                                None,
                            );
                        });
                    }
                }
            }

            if self.server.pause.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(100));
                continue;
            }

            self.server.update_swim_round();

            let long_wait = self.timing.next_protocol_period();

            let check_list = self.server.member_list.check_list(self.server.member_id());

            for member in check_list {
                if self.server.member_list.pingable(&member) {
                    if !Self::directly_reachable(&self.server, &member) {
                        continue;
                    }
                    // This is the timeout for the next protocol period - if we
                    // complete faster than this, we want to wait in the end
                    // until this timer expires.
                    let next_protocol_period = self.timing.next_protocol_period();

                    self.probe(member);

                    if SteadyTime::now() <= next_protocol_period {
                        let wait_time =
                            (next_protocol_period - SteadyTime::now()).num_milliseconds();
                        if wait_time > 0 {
                            debug!("Waiting {} until the next protocol period", wait_time);
                            thread::sleep(Duration::from_millis(wait_time as u64));
                        }
                    }
                }
            }

            if SteadyTime::now() <= long_wait {
                let wait_time = (long_wait - SteadyTime::now()).num_milliseconds();
                if wait_time > 0 {
                    thread::sleep(Duration::from_millis(wait_time as u64));
                }
            }
        }
    }

    pub fn directly_reachable(server: &Server<N>, member: &Member) -> bool {
        let mut dbg = ReachableDbg::default();
        let reachable = Self::directly_reachable_internal(server, member, &mut dbg);

        println!(
            "====REACHABLE====\n\
             reachable: {}\n\
             {:#?}\n\
             =================",
            reachable, dbg,
        );

        reachable
    }

    fn directly_reachable_internal(
        server: &Server<N>,
        member: &Member,
        dbg: &mut ReachableDbg,
    ) -> bool {
        dbg.our_member = server.read_member().clone();
        dbg.their_member = member.clone();
        dbg.our_zone = server
            .read_zone_list()
            .zones
            .get(dbg.our_member.get_zone_id())
            .cloned();
        dbg.their_zone = server
            .read_zone_list()
            .zones
            .get(dbg.their_member.get_zone_id())
            .cloned();

        let our_zone_id = server.read_member().get_zone_id().to_string();
        let their_zone_id = member.get_zone_id();

        if our_zone_id == their_zone_id {
            return true;
        }

        let mut our_ids = HashSet::new();
        let mut their_ids = HashSet::new();

        our_ids.insert(our_zone_id.to_string());
        their_ids.insert(their_zone_id.to_string());

        if let Some(zone) = server.read_zone_list().zones.get(&our_zone_id) {
            if zone.has_successor() {
                our_ids.insert(zone.get_successor().to_string());
            }
            for zone_id in zone.get_predecessors().iter() {
                our_ids.insert(zone_id.to_string());
            }
        }

        if let Some(zone) = server.read_zone_list().zones.get(their_zone_id) {
            if zone.has_successor() {
                their_ids.insert(zone.get_successor().to_string());
            }
            for zone_id in zone.get_predecessors().iter() {
                their_ids.insert(zone_id.to_string());
            }
        }

        let have_common_ids = !our_ids.is_disjoint(&their_ids);

        dbg.our_ids = Some(our_ids);
        dbg.their_ids = Some(their_ids);

        if have_common_ids {
            return true;
        }

        for zone_address in member.get_additional_addresses() {
            let additional_zone_id = zone_address.get_zone_id();

            if additional_zone_id == our_zone_id {
                return true;
            }

            if let Some(zone) = server.read_zone_list().zones.get(additional_zone_id) {
                if zone.get_successor() == our_zone_id {
                    return true;
                }
                for zone_id in zone.get_predecessors().iter() {
                    if *zone_id == our_zone_id {
                        return true;
                    }
                }
            }
        }

        for zone_address in server.read_member().get_additional_addresses() {
            let additional_zone_id = zone_address.get_zone_id();

            if zone_address.get_zone_id() == their_zone_id {
                return true;
            }

            if let Some(zone) = server.read_zone_list().zones.get(additional_zone_id) {
                if zone.get_successor() == their_zone_id {
                    return true;
                }
                for zone_id in zone.get_predecessors().iter() {
                    if zone_id == their_zone_id {
                        return true;
                    }
                }
            }
        }

        false
    }

    ///
    /// Probe Loop
    ///
    /// First, we send the ping to the remote address. This operation never blocks - we just
    /// pass the data straight on to the kernel for UDP goodness. Then we grab a timer for how
    /// long we're willing to run this phase, and start listening for Ack packets from the
    /// Inbound thread. If we receive an Ack that is for any Member other than the one we are
    /// currently pinging, we discard it. Otherwise, we set the address for the Member whose Ack
    /// we received to the one we saw on the wire, and insert it into the MemberList.
    ///
    /// If we don't receive anything on the channel, we check if the current time has exceeded
    /// our timeout. If it has, we break out of the Ping loop, and proceed to the PingReq loop.
    /// If the timer has not been exceeded, we park this thread for
    /// PING_RECV_QUEUE_EMPTY_SLEEP_MS, and try again.
    ///
    /// If we don't receive anything at all in the Ping/PingReq loop, we mark the member as Suspect.
    fn probe(&mut self, member: Member) {
        let addr = if let Some(addr) =
            member.swim_socket_address_for_zone(self.server.read_member().get_zone_id())
        {
            addr
        } else {
            member.swim_socket_address()
        };

        trace_it!(PROBE: &self.server, TraceKind::ProbeBegin, member.get_id(), addr);

        // Ping the member, and wait for the ack.
        ping(&self.server, &self.swim_sender, &member, addr, None);
        if self.recv_ack(&member, addr, AckFrom::Ping) {
            trace_it!(PROBE: &self.server, TraceKind::ProbeAckReceived, member.get_id(), addr);
            trace_it!(PROBE: &self.server, TraceKind::ProbeComplete, member.get_id(), addr);
            return;
        }

        self.server.member_list.with_pingreq_targets(
            self.server.member_id(),
            member.get_id(),
            |pingreq_target| {
                trace_it!(PROBE: &self.server,
                          TraceKind::ProbePingReq,
                          pingreq_target.get_id(),
                          pingreq_target.get_address());
                pingreq(&self.server, &self.swim_sender, &pingreq_target, &member);
            },
        );
        if !self.recv_ack(&member, addr, AckFrom::PingReq) {
            // We mark as suspect when we fail to get a response from the PingReq. That moves us
            // into the suspicion phase, where anyone marked as suspect has a certain number of
            // protocol periods to recover.
            warn!("Marking {} as Suspect", member.get_id());
            trace_it!(PROBE: &self.server, TraceKind::ProbeSuspect, member.get_id(), addr);
            trace_it!(PROBE: &self.server, TraceKind::ProbeComplete, member.get_id(), addr);
            self.server.insert_member(member, Health::Suspect);
        } else {
            trace_it!(PROBE: &self.server, TraceKind::ProbeComplete, member.get_id(), addr);
        }
    }

    /// Listen for an ack from the `Inbound` thread.
    fn recv_ack(&mut self, member: &Member, addr: N::AddressAndPort, ack_from: AckFrom) -> bool {
        let timeout = match ack_from {
            AckFrom::Ping => self.timing.ping_timeout(),
            AckFrom::PingReq => self.timing.pingreq_timeout(),
        };
        loop {
            match self.rx_inbound.try_recv() {
                Ok((real_addr, mut swim)) => {
                    let mut ack_from = swim.mut_ack().take_from();

                    // If this was forwarded to us, we want to retain the address of the member who
                    // sent the ack, not the one we received on the socket.
                    if !swim.get_ack().has_forward_to() {
                        ack_from.set_address(format!("{}", real_addr.get_address()));
                    }
                    let is_departed = ack_from.get_departed();
                    let ack_from_member: Member = ack_from.into();
                    if member.get_id() != ack_from_member.get_id() {
                        if is_departed {
                            self.server.insert_member(ack_from_member, Health::Departed);
                        } else {
                            self.server.insert_member(ack_from_member, Health::Alive);
                        }
                        // Keep listening, we want the ack we expected
                        continue;
                    } else {
                        // We got the ack we are looking for; return.
                        if is_departed {
                            self.server.insert_member(ack_from_member, Health::Departed);
                        } else {
                            self.server.insert_member(ack_from_member, Health::Alive);
                        }
                        return true;
                    }
                }
                Err(mpsc::TryRecvError::Empty) => {
                    if SteadyTime::now() > timeout {
                        warn!(
                            "Timed out waiting for Ack from {}@{}",
                            member.get_id(),
                            addr
                        );
                        return false;
                    }
                    thread::sleep(Duration::from_millis(PING_RECV_QUEUE_EMPTY_SLEEP_MS));
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    panic!("Outbound thread has disconnected! This is fatal.");
                }
            }
        }
    }
}

pub fn create_to_member<AP: AddressAndPort>(addr: AP, target: &Member) -> ProtoMember {
    let mut proto_member = ProtoMember::new();
    let address_str = addr.get_address().to_string();
    let port = addr.get_port() as i32;
    let zone_id = {
        if target.get_address() == address_str && target.get_swim_port() == port {
            target.get_zone_id().to_string()
        } else {
            let mut zone_id = String::new();

            for zone_address in target.get_additional_addresses() {
                if zone_address.get_address() == address_str && zone_address.get_swim_port() == port
                {
                    zone_id = zone_address.get_zone_id().to_string();
                    break;
                }
            }

            if zone_id.is_empty() {
                zone_id = BfUuid::nil().to_string()
            }
            zone_id
        }
    };

    proto_member.set_address(address_str);
    proto_member.set_swim_port(port);
    proto_member.set_zone_id(zone_id);
    proto_member
}

/// Populate a SWIM message with rumors.
pub fn populate_membership_rumors<N: Network>(
    server: &Server<N>,
    target: &Member,
    swim: &mut Swim,
) {
    let mut membership_entries = RepeatedField::new();
    // TODO (CM): magic number!
    let magic_number = 5;
    // If this isn't the first time we are communicating with this target, we want to include this
    // targets current status. This ensures that members always get a "Confirmed" rumor, before we
    // have the chance to flip it to "Alive", which helps make sure we heal from a partition.
    if server.member_list.contains_member(target.get_id()) {
        if let Some(always_target) = server.member_list.membership_for(target.get_id()) {
            membership_entries.push(always_target);
        }
    }

    // NOTE: the way this is currently implemented, this is grabbing
    // the 5 coolest (but still warm!) Member rumors.
    let rumors: Vec<RumorKey> = server
        .rumor_heat
        .currently_hot_rumors(target.get_id())
        .into_iter()
        .filter(|ref r| r.kind == Rumor_Type::Member)
        .take(magic_number)
        .collect();

    for ref rkey in rumors.iter() {
        if let Some(member) = server.member_list.membership_for(&rkey.key()) {
            membership_entries.push(member);
        }
    }
    swim.set_membership(membership_entries);

    let mut zone_entries = RepeatedField::new();
    let our_own_zone_id = server.read_member().get_zone_id().to_string();
    let mut our_own_zone_gossiped = false;
    let zone_rumors = server
        .rumor_heat
        .currently_hot_rumors(target.get_id())
        .into_iter()
        .filter(|ref r| r.kind == Rumor_Type::Zone)
        .take(magic_number)
        .collect::<Vec<_>>();

    {
        let zone_list = server.read_zone_list();

        for ref rkey in zone_rumors.iter() {
            if let Some(zone) = zone_list.zones.get(&rkey.id) {
                zone_entries.push(zone.proto.clone());
                if rkey.id == our_own_zone_id {
                    our_own_zone_gossiped = true;
                }
            }
        }
    }
    // Always include zone information of the sender
    let zone_settled = *(server.read_zone_settled());
    if zone_settled && !our_own_zone_gossiped {
        if let Some(zone) = server
            .read_zone_list()
            .zones
            .get(&server.get_settled_zone_id())
        {
            zone_entries.push(zone.proto.clone());
        }
    }
    // We don't want to update the heat for rumors that we know we are sending to a target that is
    // confirmed dead; the odds are, they won't receive them. Lets spam them a little harder with
    // rumors.
    if !server.member_list.persistent_and_confirmed(target) {
        server.rumor_heat.cool_rumors(target.get_id(), &rumors);
        server.rumor_heat.cool_rumors(target.get_id(), &zone_rumors);
    }
    swim.set_zones(zone_entries);
}

/// Send a PingReq.
pub fn pingreq<N: Network>(
    server: &Server<N>,
    swim_sender: &N::SwimSender,
    pingreq_target: &Member,
    target: &Member,
) {
    let addr = pingreq_target.swim_socket_address();
    let mut swim = Swim::new();
    swim.set_field_type(Swim_Type::PINGREQ);
    let mut pingreq = PingReq::new();
    {
        let member = server.read_member();
        pingreq.set_from(member.proto.clone());
    }
    pingreq.set_target(target.proto.clone());
    swim.set_pingreq(pingreq);
    populate_membership_rumors(server, target, &mut swim);
    let bytes = match swim.write_to_bytes() {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("Generating protobuf failed: {}", e);
            return;
        }
    };
    let payload = match server.generate_wire(bytes) {
        Ok(payload) => payload,
        Err(e) => {
            error!("Generating protobuf failed: {}", e);
            return;
        }
    };
    match swim_sender.send(&payload, addr) {
        Ok(_s) => trace!(
            "Sent PingReq to {}@{} for {}@{}",
            pingreq_target.get_id(),
            addr,
            target.get_id(),
            target.swim_socket_address::<N::AddressAndPort>()
        ),
        Err(e) => error!(
            "Failed PingReq to {}@{} for {}@{}: {}",
            pingreq_target.get_id(),
            addr,
            target.get_id(),
            target.swim_socket_address::<N::AddressAndPort>(),
            e
        ),
    }
    trace_it!(
        SWIM: server,
        TraceKind::SendPingReq,
        pingreq_target.get_id(),
        addr,
        &swim
    );
}

/// Send a Ping.
pub fn ping<N: Network>(
    server: &Server<N>,
    swim_sender: &N::SwimSender,
    target: &Member,
    addr: N::AddressAndPort,
    mut forward_to: Option<Member>,
) {
    let mut swim = Swim::new();
    swim.set_field_type(Swim_Type::PING);
    let mut ping = Ping::new();
    {
        let member = server.read_member();
        ping.set_from(member.proto.clone());
    }
    if forward_to.is_some() {
        let member = forward_to.take().unwrap();
        ping.set_forward_to(member.proto);
    }
    ping.set_to(create_to_member(addr, &target));
    swim.set_ping(ping);
    populate_membership_rumors(server, target, &mut swim);

    let bytes = match swim.write_to_bytes() {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("Generating protobuf failed: {}", e);
            return;
        }
    };
    let payload = match server.generate_wire(bytes) {
        Ok(payload) => payload,
        Err(e) => {
            error!("Generating protobuf failed: {}", e);
            return;
        }
    };

    match swim_sender.send(&payload, addr) {
        Ok(_s) => {
            if forward_to.is_some() {
                trace!(
                    "Sent Ping to {} on behalf of {}@{}",
                    addr,
                    swim.get_ping().get_forward_to().get_id(),
                    swim.get_ping().get_forward_to().get_address()
                );
            } else {
                trace!("Sent Ping to {}", addr);
            }
        }
        Err(e) => error!("Failed Ping to {}: {}", addr, e),
    }
    trace_it!(
        SWIM: server,
        TraceKind::SendPing,
        target.get_id(),
        addr,
        &swim
    );
}

/// Forward an ack on.
pub fn forward_ack<N: Network>(
    server: &Server<N>,
    swim_sender: &N::SwimSender,
    addr: N::AddressAndPort,
    swim: Swim,
) {
    trace_it!(
        SWIM: server,
        TraceKind::SendForwardAck,
        swim.get_ack().get_from().get_id(),
        addr,
        &swim
    );

    let bytes = match swim.write_to_bytes() {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("Generating protobuf failed: {}", e);
            return;
        }
    };
    let payload = match server.generate_wire(bytes) {
        Ok(payload) => payload,
        Err(e) => {
            error!("Generating protobuf failed: {}", e);
            return;
        }
    };

    match swim_sender.send(&payload, addr) {
        Ok(_s) => trace!(
            "Forwarded ack to {}@{}",
            swim.get_ack().get_from().get_id(),
            addr
        ),
        Err(e) => error!(
            "Failed ack to {}@{}: {}",
            swim.get_ack().get_from().get_id(),
            addr,
            e
        ),
    }
}

/// Send an Ack.
pub fn ack<N: Network>(
    server: &Server<N>,
    swim_sender: &N::SwimSender,
    target: &Member,
    addr: N::AddressAndPort,
    mut forward_to: Option<Member>,
) {
    let mut swim = Swim::new();
    swim.set_field_type(Swim_Type::ACK);
    let mut ack = Ack::new();
    {
        let member = server.read_member();
        ack.set_from(member.proto.clone());
    }
    if forward_to.is_some() {
        let member = forward_to.take().unwrap();
        ack.set_forward_to(member.proto);
    }
    ack.set_to(create_to_member(addr, &target));
    swim.set_ack(ack);
    populate_membership_rumors(server, target, &mut swim);

    let bytes = match swim.write_to_bytes() {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("Generating protobuf failed: {}", e);
            return;
        }
    };
    let payload = match server.generate_wire(bytes) {
        Ok(payload) => payload,
        Err(e) => {
            error!("Generating protobuf failed: {}", e);
            return;
        }
    };

    match swim_sender.send(&payload, addr) {
        Ok(_s) => trace!(
            "Sent ack to {}@{}",
            swim.get_ack().get_from().get_id(),
            addr
        ),
        Err(e) => error!(
            "Failed ack to {}@{}: {}",
            swim.get_ack().get_from().get_id(),
            addr,
            e
        ),
    }
    trace_it!(
        SWIM: server,
        TraceKind::SendAck,
        target.get_id(),
        addr,
        &swim
    );
}

/// Send a ZoneChange.
pub fn zone_change<N: Network>(
    server: &Server<N>,
    swim_sender: &N::SwimSender,
    target: &Member,
    zone_change: ZoneChange,
) {
    let mut swim = Swim::new();
    swim.set_field_type(Swim_Type::ZONE_CHANGE);
    swim.set_zone_change(zone_change);

    let bytes = match swim.write_to_bytes() {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("Generating protobuf failed: {}", e);
            return;
        }
    };
    let payload = match server.generate_wire(bytes) {
        Ok(payload) => payload,
        Err(e) => {
            error!("Generating protobuf failed: {}", e);
            return;
        }
    };
    let addr = target.swim_socket_address();

    match swim_sender.send(&payload, addr) {
        Ok(_s) => trace!("Sent zone change to {}@{}", target.get_id(), addr),
        Err(e) => error!("Failed zone change to {}@{}: {}", target.get_id(), addr, e),
    }
    trace_it!(
        SWIM: server,
        TraceKind::SendZoneChange,
        target.get_id(),
        addr,
        &swim
    );
}
