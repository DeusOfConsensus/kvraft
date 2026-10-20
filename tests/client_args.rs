use kvraft::client::parse_addr_map;

#[test]
fn parses_a_cluster_list() {
    let m = parse_addr_map("1=127.0.0.1:7001, 2=127.0.0.1:7002,3=[::1]:7003").unwrap();
    assert_eq!(m.len(), 3);
    assert_eq!(m[&1].port(), 7001);
    assert_eq!(m[&3].port(), 7003);
}

#[test]
fn rejects_malformed_lists() {
    for bad in [
        "",
        ",",
        "127.0.0.1:7001",
        "x=127.0.0.1:7001",
        "1=not-an-address",
        "1=127.0.0.1:7001,1=127.0.0.1:7002",
        "1=127.0.0.1:99999",
    ] {
        assert!(parse_addr_map(bad).is_err(), "{bad:?} should be rejected");
    }
}
