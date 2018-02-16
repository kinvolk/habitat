use nat::{TestNetworkSwitchBoard, ZoneID};

use habitat_butterfly::member::Health;

#[test]
fn six_servers_in_the_same_zone() {
    let switch_board = TestNetworkSwitchBoard::new();
    let zone = ZoneID::new(1);
    let server0 = switch_board.start_server_in_zone(zone);
    let server1 = switch_board.start_server_in_zone(zone);
    let server2 = switch_board.start_server_in_zone(zone);
    let server3 = switch_board.start_server_in_zone(zone);
    let server4 = switch_board.start_server_in_zone(zone);
    let server5 = switch_board.start_server_in_zone(zone);
    server0.talk_to(vec![&server1, &server2, &server5]);
    server2.talk_to(vec![&server3, &server4]);
    server4.talk_to(vec![&server5]);
    assert!(switch_board.wait_for_health_all(Health::Alive));
}

#[allow(dead_code)]
fn three_servers_in_parent_zone_and_three_servers_in_child_zone_new_style() {
    let switch_board = TestNetworkSwitchBoard::new();
    let parent_zone = ZoneID::new(1);
    let child_zone = ZoneID::new(2);
    let mut nat = switch_board.setup_nat(
        parent_zone,
        child_zone,
        //None,
    );
    let hole0 = nat.punch_hole();
    let hole1 = nat.punch_hole();
    let hole2 = nat.punch_hole();
    let parent_server0 = switch_board.start_server_in_zone(parent_zone);
    let parent_server1 = switch_board.start_server_in_zone(parent_zone);
    let parent_server2 = switch_board.start_server_in_zone(parent_zone);
    let child_server0 = switch_board.start_server_in_zone_with_holes(child_zone, vec![hole0]);
    let child_server1 = switch_board.start_server_in_zone_with_holes(child_zone, vec![hole1]);
    let child_server2 = switch_board.start_server_in_zone_with_holes(child_zone, vec![hole2]);

    nat.make_route(hole0, child_server0.addr);
    nat.make_route(hole1, child_server1.addr);
    nat.make_route(hole2, child_server2.addr);
    parent_server0.talk_to(vec![&parent_server1, &parent_server2, &child_server2]);
    parent_server2.talk_to(vec![&child_server0, &child_server1]);
    child_server1.talk_to(vec![&child_server2]);
    assert!(switch_board.wait_for_health_all(Health::Alive));
}

#[allow(dead_code)]
fn new_style() {
    let switch_board = TestNetworkSwitchBoard::new();
    let parent_zone = ZoneID::new(1);
    let child_zone = ZoneID::new(2);
    let mut nat = switch_board.setup_nat(
        parent_zone,
        child_zone,
        //None,
    );
    let hole = nat.punch_hole();
    let parent_server0 = switch_board.start_server_in_zone(parent_zone);
    let parent_server1 = switch_board.start_server_in_zone(parent_zone);
    let parent_server2 = switch_board.start_server_in_zone(parent_zone);
    let child_server0 = switch_board.start_server_in_zone_with_holes(child_zone, vec![hole]);
    let child_server1 = switch_board.start_public_server_in_zone(child_zone);
    let child_server2 = switch_board.start_server_in_zone(child_zone);

    nat.make_route(hole, child_server0.addr);
    parent_server0.talk_to(vec![&parent_server1, &parent_server2]);
    parent_server2.talk_to(vec![&child_server0, &child_server1]);
    child_server1.talk_to(vec![&child_server2]);
    assert!(switch_board.wait_for_health_all(Health::Alive));
}
