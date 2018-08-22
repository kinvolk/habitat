use env_logger;

use nat::{TestNetworkSwitchBoard, ZoneID};

use habitat_butterfly::member::Health;

#[test]
fn servers_establish_the_same_zone_few() {
    let switch_board = TestNetworkSwitchBoard::new();
    let zone = ZoneID::new(1);
    let server0 = switch_board.start_server_in_zone(zone);
    let server1 = switch_board.start_server_in_zone(zone);

    server0.talk_to(vec![&server1]);
    assert!(switch_board.wait_for_health_all(Health::Alive));
    assert!(switch_board.wait_for_same_settled_zone(vec![&server0, &server1]));
}

#[test]
fn servers_establish_the_same_zone_many() {
    env_logger::init();

    let switch_board = TestNetworkSwitchBoard::new();
    let zone = ZoneID::new(1);
    let server0 = switch_board.start_server_in_zone(zone);
    let server1 = switch_board.start_server_in_zone(zone);
    let server2 = switch_board.start_server_in_zone(zone);
    let server3 = switch_board.start_server_in_zone(zone);
    let server4 = switch_board.start_server_in_zone(zone);
    let server5 = switch_board.start_server_in_zone(zone);

    server0.talk_to(vec![&server1, &server2, &server3]);
    server2.talk_to(vec![&server3, &server4, &server5]);
    assert!(switch_board.wait_for_health_all(Health::Alive));
    assert!(switch_board.wait_for_same_settled_zone(vec![
        &server0, &server1, &server2, &server3, &server4, &server5,
    ]));
}

#[test]
fn sibling_zones_get_merged() {
    let switch_board = TestNetworkSwitchBoard::new();
    let zone = ZoneID::new(1);
    let server0a = switch_board.start_server_in_zone(zone);
    let server0b = switch_board.start_server_in_zone(zone);
    let server1a = switch_board.start_server_in_zone(zone);
    let server1b = switch_board.start_server_in_zone(zone);

    server0a.talk_to(vec![&server0b]);
    server1a.talk_to(vec![&server1b]);
    // TODO(krnowak): This won't work, it tries to check health status
    // of a server in one ring as seen by a server in another ring
    //
    //assert!(switch_board.wait_for_health_all(Health::Alive));
    assert!(switch_board.wait_for_disjoint_settled_zones(vec![
        vec![&server0a, &server0b],
        vec![&server1a, &server1b],
    ]));

    let server2 = switch_board.start_server_in_zone(zone);

    server2.talk_to(vec![&server0a, &server1a]);
    assert!(switch_board.wait_for_health_all(Health::Alive));
    assert!(
        switch_board
            .wait_for_same_settled_zone(vec![&server0a, &server0b, &server1a, &server1b, &server2])
    );
}

#[test]
fn different_zones_get_different_ids_few() {
    let switch_board = TestNetworkSwitchBoard::new();
    let parent_zone = ZoneID::new(1);
    let child_zone = ZoneID::new(2);
    let mut nat = switch_board.setup_nat(
        parent_zone,
        child_zone,
        //None,
    );
    let hole0 = nat.punch_hole();
    let parent_server0 = switch_board.start_server_in_zone(parent_zone);
    let child_server0 = switch_board.start_server_in_zone_with_holes(child_zone, vec![hole0]);

    nat.make_route(hole0, child_server0.addr);
    parent_server0.talk_to(vec![&hole0]);
    assert!(switch_board.wait_for_health_all(Health::Alive));
    assert!(
        switch_board
            .wait_for_disjoint_settled_zones(vec![vec![&parent_server0], vec![&child_server0]])
    );
}

#[test]
fn different_zones_get_different_ids_many() {
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
    parent_server0.talk_to(vec![&parent_server1, &parent_server2]);
    child_server0.talk_to(vec![&child_server1, &child_server2]);
    parent_server1.talk_to(vec![&hole0, &hole1]);
    parent_server2.talk_to(vec![&hole2]);
    assert!(switch_board.wait_for_health_all(Health::Alive));
    assert!(switch_board.wait_for_disjoint_settled_zones(vec![
        vec![&parent_server0, &parent_server1, &parent_server2],
        vec![&child_server0, &child_server1, &child_server2],
    ]));
}
