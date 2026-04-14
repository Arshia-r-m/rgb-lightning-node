use super::*;

const TEST_DIR_BASE: &str = "tmp/swap_expire_pending/";
const SWAP_TIMEOUT_SEC: u32 = 3;

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[traced_test]
async fn swap_pending_expires() {
    initialize();

    let test_dir_node1 = format!("{TEST_DIR_BASE}node1");
    let test_dir_node2 = format!("{TEST_DIR_BASE}node2");
    let (node1_addr, _) = start_node(&test_dir_node1, NODE1_PEER_PORT, false).await;
    let (node2_addr, _) = start_node(&test_dir_node2, NODE2_PEER_PORT, false).await;

    fund_and_create_utxos(node1_addr, None).await;
    fund_and_create_utxos(node2_addr, None).await;

    let asset_id = issue_asset_nia(node1_addr).await.asset_id;
    let node1_pubkey = node_info(node1_addr).await.pubkey;
    let node2_pubkey = node_info(node2_addr).await.pubkey;

    // node1→node2 RGB channel (maker's outbound for the swap payment)
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
    // node2→node1 BTC channel (gives node1 the inbound BTC liquidity needed to route)
    open_channel(
        node2_addr,
        &node1_pubkey,
        Some(NODE1_PEER_PORT),
        Some(5_000_000),
        Some(546_000),
        None,
        None,
    )
    .await;

    let maker_addr = node1_addr;
    let qty_from = 36_000u64;
    let qty_to = 10u64;

    // Install the gate: the PaymentFailed handler will block before writing SwapStatus::Failed,
    // keeping the swap in Pending long enough to observe the expiry bug.
    let gate = Arc::new(tokio::sync::Notify::new());
    *crate::ldk::payment_failed_gate().lock().unwrap() = Some(Arc::clone(&gate));

    // Create swap — taker intentionally does NOT call /taker, so the HTLC will be rejected
    let resp =
        maker_init(maker_addr, qty_from, None, qty_to, Some(&asset_id), SWAP_TIMEOUT_SEC).await;

    // Execute — maker's swap transitions to Pending and the payment goes in flight
    maker_execute(
        maker_addr,
        resp.swapstring,
        resp.payment_secret,
        node2_pubkey,
    )
    .await;

    // Wait for the HTLC rejection to propagate and PaymentFailed to hit the gate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Confirm the swap is stuck in Pending (gate is holding the handler)
    let swap = get_swap(maker_addr, &resp.payment_hash, false).await;
    assert_eq!(
        swap.status,
        SwapStatus::Pending,
        "expected swap to be Pending while gate is held, got {:?}",
        swap.status
    );

    // Wait past swap_info.expiry
    tokio::time::sleep(Duration::from_secs(SWAP_TIMEOUT_SEC as u64 + 1)).await;

    // A Pending swap past its expiry should show Expired — not Pending
    let swap = get_swap(maker_addr, &resp.payment_hash, false).await;
    assert_eq!(
        swap.status,
        SwapStatus::Expired,
        "expected Pending swap past expiry to show Expired via get_swap, got {:?}",
        swap.status
    );

    let swaps = list_swaps(maker_addr).await;
    let swap = swaps
        .maker
        .iter()
        .find(|s| s.payment_hash == resp.payment_hash)
        .unwrap();
    assert_eq!(
        swap.status,
        SwapStatus::Expired,
        "expected Pending swap past expiry to show Expired via list_swaps, got {:?}",
        swap.status
    );

    // Release the gate — PaymentFailed handler completes and writes Failed to storage
    gate.notify_one();
    *crate::ldk::payment_failed_gate().lock().unwrap() = None;

    // The swap should eventually settle to Failed (payment was explicitly rejected)
    wait_for_swap_status(maker_addr, &resp.payment_hash, SwapStatus::Failed).await;
}
