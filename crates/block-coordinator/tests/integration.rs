//! Integration tests for BlockMachineCoordinator.
//!
//! These tests feed the coordinator through its channels with realistic
//! BlockSM event sequences and verify the output.

use solana_commitment_config::CommitmentLevel;
use solana_hash::Hash;
use tokio::sync::mpsc;
use yellowstone_block_machine::state_machine::{
    BlockReplayEvent, BlockSummary, ConsensusUpdate, EntryInfo, SlotCommitmentStatusUpdate,
    SlotLifecycle, SlotLifecycleUpdate,
};
use yellowstone_vixen_block_coordinator::{
    BlockMachineCoordinator, ConfirmedSlot, CoordinatorInput, CoordinatorMessage,
};

/// Send the full lifecycle of a slot through the input channel:
/// FirstShredReceived → CreatedBank → Entry → Completed → BlockSummary
///
/// This is the replay side. Consensus (Processed/Confirmed) must be sent separately.
async fn send_slot_lifecycle(
    input_tx: &mpsc::Sender<CoordinatorInput>,
    slot: u64,
    parent_slot: u64,
    tx_count: u64,
) {
    let parent = Some(parent_slot);
    let blockhash = Hash::new_unique();

    // 1. FirstShredReceived
    input_tx
        .send(CoordinatorInput::Replay(
            BlockReplayEvent::SlotLifecycleStatus(SlotLifecycleUpdate {
                slot,
                parent_slot: parent,
                stage: SlotLifecycle::FirstShredReceived,
            }),
        ))
        .await
        .unwrap();

    // 2. CreatedBank
    input_tx
        .send(CoordinatorInput::Replay(
            BlockReplayEvent::SlotLifecycleStatus(SlotLifecycleUpdate {
                slot,
                parent_slot: parent,
                stage: SlotLifecycle::CreatedBank,
            }),
        ))
        .await
        .unwrap();

    // 3. Entry (single entry containing all transactions)
    input_tx
        .send(CoordinatorInput::Replay(BlockReplayEvent::Entry(
            EntryInfo {
                slot,
                entry_index: 0,
                starting_txn_index: 0,
                entry_hash: Hash::new_unique(),
                executed_txn_count: tx_count,
            },
        )))
        .await
        .unwrap();

    // 4. Completed
    input_tx
        .send(CoordinatorInput::Replay(
            BlockReplayEvent::SlotLifecycleStatus(SlotLifecycleUpdate {
                slot,
                parent_slot: parent,
                stage: SlotLifecycle::Completed,
            }),
        ))
        .await
        .unwrap();

    // 5. BlockSummary
    input_tx
        .send(CoordinatorInput::Replay(BlockReplayEvent::BlockSummary(
            BlockSummary {
                slot,
                entry_count: 1,
                parent_slot,
                executed_transaction_count: tx_count,
                blockhash,
            },
        )))
        .await
        .unwrap();

    // 6. BlockExtra (block_time + block_height)
    input_tx
        .send(CoordinatorInput::BlockExtra {
            slot,
            block_time: Some(1700000000),
            block_height: Some(slot - 1), // height roughly tracks slot
        })
        .await
        .unwrap();
}

/// Send a consensus commitment event.
async fn send_commitment(
    input_tx: &mpsc::Sender<CoordinatorInput>,
    slot: u64,
    parent_slot: u64,
    commitment: CommitmentLevel,
) {
    input_tx
        .send(CoordinatorInput::Consensus(
            ConsensusUpdate::SlotCommitmentStatus(SlotCommitmentStatusUpdate {
                slot,
                parent_slot: Some(parent_slot),
                commitment,
            }),
        ))
        .await
        .unwrap();
}

/// Send parsed records and TransactionParsed signals for a slot.
async fn send_parsed_transactions(
    parsed_tx: &mpsc::Sender<CoordinatorMessage<String>>,
    slot: u64,
    records: Vec<(u64, Vec<usize>, String)>, // (tx_index, ix_path, record)
    tx_count: u64,
) {
    for (tx_index, ix_path, record) in records {
        parsed_tx
            .send(CoordinatorMessage::Parsed {
                slot,
                tx_index,
                ix_path,
                record,
            })
            .await
            .unwrap();
    }
    for _ in 0..tx_count {
        parsed_tx
            .send(CoordinatorMessage::TransactionParsed { slot })
            .await
            .unwrap();
    }
}

/// Send a dead slot lifecycle event.
async fn send_dead_slot(
    input_tx: &mpsc::Sender<CoordinatorInput>,
    slot: u64,
    parent_slot: u64,
) {
    input_tx
        .send(CoordinatorInput::Replay(
            BlockReplayEvent::SlotLifecycleStatus(SlotLifecycleUpdate {
                slot,
                parent_slot: Some(parent_slot),
                stage: SlotLifecycle::Dead,
            }),
        ))
        .await
        .unwrap();
}

/// Spawn the coordinator and return the channel handles.
fn spawn_coordinator() -> (
    mpsc::Sender<CoordinatorInput>,
    mpsc::Sender<CoordinatorMessage<String>>,
    mpsc::Receiver<ConfirmedSlot<String>>,
) {
    let (input_tx, input_rx) = mpsc::channel(256);
    let (parsed_tx, parsed_rx) = mpsc::channel(256);
    let (output_tx, output_rx) = mpsc::channel(64);

    let coordinator = BlockMachineCoordinator::new(input_rx, parsed_rx, output_tx);
    tokio::spawn(coordinator.run());

    (input_tx, parsed_tx, output_rx)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Full end-to-end: lifecycle + parsed records + confirmed → flush with records in order.
#[tokio::test]
async fn two_gate_flush_end_to_end() {
    let (input_tx, parsed_tx, mut output_rx) = spawn_coordinator();

    let slot = 100;
    let parent = 99;
    let tx_count = 2;

    // Send full lifecycle (BlockSM will emit FrozenBlock).
    send_slot_lifecycle(&input_tx, slot, parent, tx_count).await;

    // Send parsed records out of order.
    send_parsed_transactions(
        &parsed_tx,
        slot,
        vec![
            (1, vec![0], "tx1-ix0".into()),
            (0, vec![0, 1], "tx0-cpi".into()),
            (0, vec![0], "tx0-ix0".into()),
        ],
        tx_count,
    )
    .await;

    // Processed commitment — should NOT flush (only confirmed triggers flush).
    send_commitment(&input_tx, slot, parent, CommitmentLevel::Processed).await;

    // Give coordinator a moment to process.
    tokio::task::yield_now().await;
    assert!(
        output_rx.try_recv().is_err(),
        "Should not flush at processed commitment"
    );

    // Confirmed commitment — now both gates satisfied, should flush.
    send_commitment(&input_tx, slot, parent, CommitmentLevel::Confirmed).await;

    let confirmed = tokio::time::timeout(std::time::Duration::from_secs(2), output_rx.recv())
        .await
        .expect("Timed out waiting for confirmed slot")
        .expect("Channel closed");

    assert_eq!(confirmed.slot, slot);
    assert_eq!(confirmed.parent_slot, parent);
    assert_eq!(confirmed.executed_transaction_count, tx_count);
    assert_eq!(confirmed.block_time, Some(1700000000));
    assert_eq!(confirmed.block_height, Some(slot - 1));

    // Records should be in sorted order: tx0-ix0, tx0-cpi, tx1-ix0.
    assert_eq!(
        confirmed.records,
        vec![
            "tx0-ix0".to_string(),
            "tx0-cpi".to_string(),
            "tx1-ix0".to_string(),
        ]
    );
}

/// Slot 102 ready before slot 101 → should wait. Slot 101 ready → both flush in order.
#[tokio::test]
async fn sequential_flush_order() {
    let (input_tx, parsed_tx, mut output_rx) = spawn_coordinator();

    // Set up slot 101 and 102.
    send_slot_lifecycle(&input_tx, 101, 100, 1).await;
    send_slot_lifecycle(&input_tx, 102, 101, 1).await;

    // Parse and confirm slot 102 first.
    send_parsed_transactions(&parsed_tx, 102, vec![(0, vec![0], "rec-102".into())], 1).await;
    send_commitment(&input_tx, 102, 101, CommitmentLevel::Processed).await;
    send_commitment(&input_tx, 102, 101, CommitmentLevel::Confirmed).await;

    // Yield and check — 102 should NOT have flushed (101 is blocking).
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert!(
        output_rx.try_recv().is_err(),
        "Slot 102 should not flush before 101"
    );

    // Now finish slot 101.
    send_parsed_transactions(&parsed_tx, 101, vec![(0, vec![0], "rec-101".into())], 1).await;
    send_commitment(&input_tx, 101, 100, CommitmentLevel::Processed).await;
    send_commitment(&input_tx, 101, 100, CommitmentLevel::Confirmed).await;

    // Both should flush in order: 101 then 102.
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), output_rx.recv())
        .await
        .expect("Timed out")
        .expect("Channel closed");
    assert_eq!(first.slot, 101);
    assert_eq!(first.records, vec!["rec-101".to_string()]);

    let second = tokio::time::timeout(std::time::Duration::from_secs(2), output_rx.recv())
        .await
        .expect("Timed out")
        .expect("Channel closed");
    assert_eq!(second.slot, 102);
    assert_eq!(second.records, vec!["rec-102".to_string()]);
}

/// Dead slot discards buffered records.
#[tokio::test]
async fn dead_slot_discarded() {
    let (input_tx, parsed_tx, mut output_rx) = spawn_coordinator();

    // Start slot lifecycle.
    send_slot_lifecycle(&input_tx, 100, 99, 1).await;

    // Send some parsed records.
    send_parsed_transactions(&parsed_tx, 100, vec![(0, vec![0], "will-die".into())], 1).await;

    // Mark slot as dead.
    send_dead_slot(&input_tx, 100, 99).await;

    // Yield to let coordinator process.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // Should NOT produce output — the slot was discarded.
    assert!(
        output_rx.try_recv().is_err(),
        "Dead slot should not flush"
    );
}

/// Dead slot at head of buffer unblocks the next ready slot.
#[tokio::test]
async fn dead_slot_unblocks_next() {
    let (input_tx, parsed_tx, mut output_rx) = spawn_coordinator();

    // Set up slot 100 (will die) and 101 (will be ready).
    send_slot_lifecycle(&input_tx, 100, 99, 1).await;
    send_slot_lifecycle(&input_tx, 101, 100, 1).await;

    // Fully prepare slot 101.
    send_parsed_transactions(&parsed_tx, 101, vec![(0, vec![0], "survives".into())], 1).await;
    send_commitment(&input_tx, 101, 100, CommitmentLevel::Processed).await;
    send_commitment(&input_tx, 101, 100, CommitmentLevel::Confirmed).await;

    // Yield — slot 101 should be blocked by 100.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert!(
        output_rx.try_recv().is_err(),
        "Slot 101 should be blocked by 100"
    );

    // Kill slot 100 → should unblock 101.
    send_dead_slot(&input_tx, 100, 99).await;

    let flushed = tokio::time::timeout(std::time::Duration::from_secs(2), output_rx.recv())
        .await
        .expect("Timed out")
        .expect("Channel closed");

    assert_eq!(flushed.slot, 101);
    assert_eq!(flushed.records, vec!["survives".to_string()]);
}

/// Empty slot (0 transactions) flushes when confirmed.
#[tokio::test]
async fn empty_slot_flushes() {
    let (input_tx, _parsed_tx, mut output_rx) = spawn_coordinator();

    // Slot with 0 transactions.
    send_slot_lifecycle(&input_tx, 100, 99, 0).await;
    send_commitment(&input_tx, 100, 99, CommitmentLevel::Processed).await;
    send_commitment(&input_tx, 100, 99, CommitmentLevel::Confirmed).await;

    let confirmed = tokio::time::timeout(std::time::Duration::from_secs(2), output_rx.recv())
        .await
        .expect("Timed out")
        .expect("Channel closed");

    assert_eq!(confirmed.slot, 100);
    assert!(confirmed.records.is_empty());
    assert_eq!(confirmed.executed_transaction_count, 0);
}
