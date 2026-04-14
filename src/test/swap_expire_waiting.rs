use super::*;

const TEST_DIR_BASE: &str = "tmp/swap_expire_waiting/";
const SHORT_TIMEOUT_SEC: u32 = 1;

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[traced_test]
async fn swap_expire_waiting() {
    initialize();

    let test_dir_node1 = format!("{TEST_DIR_BASE}node1");
    let test_dir_node2 = format!("{TEST_DIR_BASE}node2");
    let (node1_addr, _) = start_node(&test_dir_node1, NODE1_PEER_PORT, false).await;
    let (node2_addr, _) = start_node(&test_dir_node2, NODE2_PEER_PORT, false).await;

    fund_and_create_utxos(node1_addr, None).await;

    let asset_id = issue_asset_nia(node1_addr).await.asset_id;
    let node2_pubkey = node_info(node2_addr).await.pubkey;

    open_channel(
        node1_addr,
        &node2_pubkey,
        Some(NODE2_PEER_PORT),
        None,
        None,
        Some(600),
        Some(&asset_id),
    )
    .await;

    // create 3 swaps with a very short timeout that will NOT be executed or joined
    let resp1 =
        maker_init(node1_addr, 50000, None, 10, Some(&asset_id), SHORT_TIMEOUT_SEC).await;
    let resp2 =
        maker_init(node1_addr, 40000, None, 20, Some(&asset_id), SHORT_TIMEOUT_SEC).await;
    let resp3 =
        maker_init(node1_addr, 30000, None, 30, Some(&asset_id), SHORT_TIMEOUT_SEC).await;

    // verify all 3 start as Waiting
    let swaps_before = list_swaps(node1_addr).await;
    let waiting_before: Vec<_> = swaps_before
        .maker
        .iter()
        .filter(|s| {
            s.status == SwapStatus::Waiting
                && [
                    resp1.payment_hash.as_str(),
                    resp2.payment_hash.as_str(),
                    resp3.payment_hash.as_str(),
                ]
                .contains(&s.payment_hash.as_str())
        })
        .collect();
    assert_eq!(
        waiting_before.len(),
        3,
        "expected all 3 swaps to start as Waiting"
    );

    // wait for the swaps to expire
    tokio::time::sleep(Duration::from_secs(SHORT_TIMEOUT_SEC as u64 + 1)).await;

    // get_swap should trigger expiration for swap 1
    let swap1 = get_swap(node1_addr, &resp1.payment_hash, false).await;
    assert_eq!(
        swap1.status,
        SwapStatus::Expired,
        "expected swap {} to be Expired via get_swap, got {:?}",
        resp1.payment_hash,
        swap1.status
    );

    // list_swaps should trigger expiration for swaps 2 and 3
    let swaps_after = list_swaps(node1_addr).await;
    for hash in [resp2.payment_hash.as_str(), resp3.payment_hash.as_str()] {
        let swap = swaps_after
            .maker
            .iter()
            .find(|s| s.payment_hash == hash)
            .unwrap_or_else(|| panic!("swap {hash} not found"));
        assert_eq!(
            swap.status,
            SwapStatus::Expired,
            "expected swap {hash} to be Expired via list_swaps, got {:?}",
            swap.status
        );
    }

    // sanity: none of the three should still be Waiting
    let still_waiting: Vec<_> = swaps_after
        .maker
        .iter()
        .filter(|s| {
            s.status == SwapStatus::Waiting
                && [
                    resp1.payment_hash.as_str(),
                    resp2.payment_hash.as_str(),
                    resp3.payment_hash.as_str(),
                ]
                .contains(&s.payment_hash.as_str())
        })
        .collect();
    assert!(
        still_waiting.is_empty(),
        "found expired swaps still Waiting: {still_waiting:?}"
    );
}
