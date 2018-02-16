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
