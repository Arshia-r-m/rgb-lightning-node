use super::*;

const TEST_DIR_BASE: &str = "tmp/swap_concurrent_same_asset_channels/";

fn total_local_balance_sat(channels: &[Channel], channel_ids: &[&str]) -> u64 {
    channels
        .iter()
        .filter(|c| channel_ids.contains(&c.channel_id.as_str()))
        .map(|c| c.local_balance_sat)
        .sum()
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[traced_test]
async fn swap_concurrent_same_asset_channels() {
    initialize();

    let test_dir_node1 = format!("{TEST_DIR_BASE}node1");
    let test_dir_node2 = format!("{TEST_DIR_BASE}node2");
    let test_dir_node3 = format!("{TEST_DIR_BASE}node3");
    let (node1_addr, _) = start_node(&test_dir_node1, NODE1_PEER_PORT, false).await;
    let (node2_addr, _) = start_node(&test_dir_node2, NODE2_PEER_PORT, false).await;
    let (node3_addr, _) = start_node(&test_dir_node3, NODE3_PEER_PORT, false).await;

    fund_and_create_utxos(node1_addr, None).await;
    fund_and_create_utxos(node2_addr, None).await;
    fund_and_create_utxos(node3_addr, None).await;

    let asset_id = issue_asset_nia(node1_addr).await.asset_id;

    let node2_pubkey = node_info(node2_addr).await.pubkey;

    let channel_12_a = open_channel(
        node1_addr,
        &node2_pubkey,
        Some(NODE2_PEER_PORT),
        Some(100000),
        Some(50000000),
        Some(500),
        Some(&asset_id),
    )
    .await;
    let channel_12_b = open_channel(
        node1_addr,
        &node2_pubkey,
        Some(NODE2_PEER_PORT),
        Some(100000),
        Some(50000000),
        Some(500),
        Some(&asset_id),
    )
    .await;
    let channel_ids = [
        channel_12_a.channel_id.as_str(),
        channel_12_b.channel_id.as_str(),
    ];

    let channels_1_before = list_channels(node1_addr).await;
    let channels_2_before = list_channels(node2_addr).await;
    assert_eq!(
        channels_1_before
            .iter()
            .filter(|c| channel_ids.contains(&c.channel_id.as_str()))
            .count(),
        2
    );
    let node1_local_balance_sat_before = total_local_balance_sat(&channels_1_before, &channel_ids);
    let node2_local_balance_sat_before = total_local_balance_sat(&channels_2_before, &channel_ids);

    println!("\nsetup swaps");
    let maker_addr = node1_addr;
    let taker_addr = node2_addr;
    let qty_from = 25000;
    let qty_to = 10;
    let maker_init_response_1 =
        maker_init(maker_addr, qty_from, None, qty_to, Some(&asset_id), 3600).await;
    let maker_init_response_2 =
        maker_init(maker_addr, qty_from, None, qty_to, Some(&asset_id), 3600).await;
    taker(taker_addr, maker_init_response_1.swapstring.clone()).await;
    taker(taker_addr, maker_init_response_2.swapstring.clone()).await;

    let swaps_maker = list_swaps(maker_addr).await;
    assert!(swaps_maker.taker.is_empty());
    assert_eq!(swaps_maker.maker.len(), 2);
    for maker_init_response in [&maker_init_response_1, &maker_init_response_2] {
        let swap_maker = swaps_maker
            .maker
            .iter()
            .find(|s| s.payment_hash == maker_init_response.payment_hash)
            .unwrap();
        assert_eq!(swap_maker.qty_from, qty_from);
        assert_eq!(swap_maker.qty_to, qty_to);
        assert_eq!(swap_maker.from_asset, None);
        assert_eq!(swap_maker.to_asset, Some(asset_id.clone()));
        assert_eq!(swap_maker.status, SwapStatus::Waiting);
    }
    let swaps_taker = list_swaps(taker_addr).await;
    assert!(swaps_taker.maker.is_empty());
    assert_eq!(swaps_taker.taker.len(), 2);
    for maker_init_response in [&maker_init_response_1, &maker_init_response_2] {
        let swap_taker = swaps_taker
            .taker
            .iter()
            .find(|s| s.payment_hash == maker_init_response.payment_hash)
            .unwrap();
        assert_eq!(swap_taker.qty_from, qty_from);
        assert_eq!(swap_taker.qty_to, qty_to);
        assert_eq!(swap_taker.from_asset, None);
        assert_eq!(swap_taker.to_asset, Some(asset_id.clone()));
        assert_eq!(swap_taker.status, SwapStatus::Waiting);
    }

    let payment_hash_1 = maker_init_response_1.payment_hash.clone();
    let payment_hash_2 = maker_init_response_2.payment_hash.clone();

    println!("\nexecute swaps concurrently");
    let (_swap_1, _swap_2) = tokio::join!(
        maker_execute(
            maker_addr,
            maker_init_response_1.swapstring,
            maker_init_response_1.payment_secret,
            node2_pubkey.clone(),
        ),
        maker_execute(
            maker_addr,
            maker_init_response_2.swapstring,
            maker_init_response_2.payment_secret,
            node2_pubkey.clone(),
        )
    );

    wait_for_ln_balance(maker_addr, &asset_id, 980).await;
    wait_for_ln_balance(taker_addr, &asset_id, 20).await;
    wait_for_swap_status(maker_addr, &payment_hash_1, SwapStatus::Succeeded).await;
    wait_for_swap_status(maker_addr, &payment_hash_2, SwapStatus::Succeeded).await;
    wait_for_swap_status(taker_addr, &payment_hash_1, SwapStatus::Succeeded).await;
    wait_for_swap_status(taker_addr, &payment_hash_2, SwapStatus::Succeeded).await;

    println!("\nrestart nodes");
    shutdown(&[node1_addr, node2_addr]).await;
    let (node1_addr, _) = start_node(&test_dir_node1, NODE1_PEER_PORT, true).await;
    let (node2_addr, _) = start_node(&test_dir_node2, NODE2_PEER_PORT, true).await;
    let maker_addr = node1_addr;
    let taker_addr = node2_addr;
    wait_for_usable_channels(node1_addr, 2).await;
    wait_for_usable_channels(node2_addr, 2).await;

    println!("\ncheck balances and channels after nodes have restarted");
    let balance_1 = asset_balance(node1_addr, &asset_id).await;
    let balance_2 = asset_balance(node2_addr, &asset_id).await;
    assert_eq!(balance_1.offchain_outbound, 980);
    assert_eq!(balance_1.offchain_inbound, 20);
    assert_eq!(balance_2.offchain_outbound, 20);
    assert_eq!(balance_2.offchain_inbound, 980);

    let swaps_maker = list_swaps(maker_addr).await;
    assert_eq!(swaps_maker.maker.len(), 2);
    for payment_hash in [&payment_hash_1, &payment_hash_2] {
        let swap_maker = swaps_maker
            .maker
            .iter()
            .find(|s| &s.payment_hash == payment_hash)
            .unwrap();
        assert_eq!(swap_maker.status, SwapStatus::Succeeded);
    }
    let swaps_taker = list_swaps(taker_addr).await;
    assert_eq!(swaps_taker.taker.len(), 2);
    for payment_hash in [&payment_hash_1, &payment_hash_2] {
        let swap_taker = swaps_taker
            .taker
            .iter()
            .find(|s| &s.payment_hash == payment_hash)
            .unwrap();
        assert_eq!(swap_taker.status, SwapStatus::Succeeded);
    }

    let payments_maker = list_payments(maker_addr).await;
    assert!(payments_maker.is_empty());
    let payments_taker = list_payments(taker_addr).await;
    assert!(payments_taker.is_empty());

    let channels_1 = list_channels(node1_addr).await;
    let channels_2 = list_channels(node2_addr).await;
    assert_eq!(
        total_local_balance_sat(&channels_1, &channel_ids),
        node1_local_balance_sat_before + (2 * qty_from / 1000)
    );
    assert_eq!(
        total_local_balance_sat(&channels_2, &channel_ids),
        node2_local_balance_sat_before - (2 * qty_from / 1000)
    );

    println!("\nclose channels");
    close_channel(node1_addr, &channel_12_a.channel_id, &node2_pubkey, false).await;
    close_channel(node1_addr, &channel_12_b.channel_id, &node2_pubkey, false).await;
    wait_for_balance(node1_addr, &asset_id, 980).await;
    wait_for_balance(node2_addr, &asset_id, 20).await;

    println!("\nspend assets");
    let recipient_id = rgb_invoice(node3_addr, None, false).await.recipient_id;
    send_asset(
        node1_addr,
        &asset_id,
        Assignment::Fungible(200),
        recipient_id,
        None,
    )
    .await;
    mine(false);
    refresh_transfers(node3_addr).await;
    refresh_transfers(node3_addr).await;
    refresh_transfers(node1_addr).await;

    let recipient_id = rgb_invoice(node3_addr, None, false).await.recipient_id;
    send_asset(
        node2_addr,
        &asset_id,
        Assignment::Fungible(5),
        recipient_id,
        None,
    )
    .await;
    mine(false);
    refresh_transfers(node3_addr).await;
    refresh_transfers(node3_addr).await;
    refresh_transfers(node2_addr).await;

    assert_eq!(asset_balance_spendable(node1_addr, &asset_id).await, 780);
    assert_eq!(asset_balance_spendable(node2_addr, &asset_id).await, 15);
    assert_eq!(asset_balance_spendable(node3_addr, &asset_id).await, 205);
}
