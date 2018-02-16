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
fn three_servers_in_parent_zone_and_three_servers_in_child_zone() {
    let switch_board = TestNetworkSwitchBoard::new();
    let parent_zone = ZoneID::new(1);
    let child_zone = ZoneID::new(2);
    switch_board.setup_zone_relationship(parent_zone, child_zone);
    let parent_server0 = switch_board.start_server_in_zone(parent_zone);
    let parent_server1 = switch_board.start_server_in_zone(parent_zone);
    let parent_server2 = switch_board.start_server_in_zone(parent_zone);
    let child_server0 = switch_board.start_server_in_zone(child_zone);
    let child_server1 = switch_board.start_server_in_zone(child_zone);
    let child_server2 = switch_board.start_server_in_zone(child_zone);
    let child_exposed0 = switch_board.expose_server_in_zone(&child_server0, parent_zone);
    let child_exposed1 = switch_board.expose_server_in_zone(&child_server1, parent_zone);
    let child_exposed2 = switch_board.expose_server_in_zone(&child_server2, parent_zone);
    parent_server0.talk_to(vec![&parent_server1, &parent_server2, &child_exposed2]);
    parent_server2.talk_to(vec![&child_exposed0, &child_exposed1]);
    child_server1.talk_to(vec![&child_exposed2]);
    assert!(switch_board.wait_for_health_all(Health::Alive));
}
