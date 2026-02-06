//! Slot buffer that collects transactions and block metadata, emitting on confirmation.
//!
//! Receives TransactionUpdates, BlockMetaUpdates, and SlotUpdates from Vixen handlers.
//! Buffers data per slot and only emits when a slot is confirmed.

use std::collections::BTreeMap;

use tokio::sync::mpsc;
use yellowstone_grpc_proto::geyser::{SlotStatus, SubscribeUpdateTransactionInfo};
use yellowstone_vixen_core::{BlockMetaUpdate, SlotUpdate, TransactionUpdate};

// TODO: later: potentially register it as an handler

/// Assembled slot ready for processing.
/// Contains all transactions and block metadata for a confirmed slot.
#[derive(Debug, Clone)]
pub struct AssembledSlot {
    pub slot: u64,
    pub blockhash: String,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub parent_slot: u64,
    pub executed_transaction_count: u64,
    pub transactions: Vec<SubscribeUpdateTransactionInfo>,
}

/// Block metadata stored in pending slot.
#[derive(Debug, Clone)]
struct BlockMeta {
    blockhash: String,
    block_time: Option<i64>,
    block_height: Option<u64>,
    parent_slot: u64,
    executed_transaction_count: u64,
}

impl From<&BlockMetaUpdate> for BlockMeta {
    fn from(meta: &BlockMetaUpdate) -> Self {
        Self {
            blockhash: meta.blockhash.clone(),
            block_time: meta.block_time.as_ref().map(|bt| bt.timestamp),
            block_height: meta.block_height.as_ref().map(|bh| bh.block_height),
            parent_slot: meta.parent_slot,
            executed_transaction_count: meta.executed_transaction_count,
        }
    }
}

/// Pending slot being buffered.
#[derive(Debug, Default)]
struct PendingSlot {
    /// Buffered transactions, keyed by transaction index for deduplication.
    transactions: BTreeMap<u64, SubscribeUpdateTransactionInfo>,
    block_meta: Option<BlockMeta>,
}

impl PendingSlot {
    fn is_complete(&self) -> bool {
        let Some(ref meta) = self.block_meta else {
            return false;
        };
        self.transactions.len() as u64 == meta.executed_transaction_count
    }

    fn into_assembled(self, slot: u64) -> Option<AssembledSlot> {
        let meta = self.block_meta?;

        // Transactions are already sorted by index (BTreeMap)
        let transactions: Vec<_> = self.transactions.into_values().collect();

        Some(AssembledSlot {
            slot,
            blockhash: meta.blockhash,
            block_time: meta.block_time,
            block_height: meta.block_height,
            parent_slot: meta.parent_slot,
            executed_transaction_count: meta.executed_transaction_count,
            transactions,
        })
    }
}

/// Slot buffer that collects data and emits on confirmation.
pub struct SlotBuffer {
    /// Pending slots being buffered, keyed by slot number.
    pending_slots: BTreeMap<u64, PendingSlot>,
    /// Channel to send confirmed slots to the processor.
    tx: mpsc::Sender<AssembledSlot>,
    /// Last committed block_height - skip slots at or before this.
    last_committed_block_height: Option<u64>,
    /// Maximum number of pending slots to buffer.
    max_pending_slots: usize,
}

impl SlotBuffer {
    pub fn new(
        tx: mpsc::Sender<AssembledSlot>,
        last_committed_block_height: Option<u64>,
        max_pending_slots: usize,
    ) -> Self {
        Self {
            pending_slots: BTreeMap::new(),
            tx,
            last_committed_block_height,
            max_pending_slots,
        }
    }

    /// Add a transaction to the buffer.
    pub fn add_transaction(&mut self, update: TransactionUpdate) {
        let slot = update.slot;

        let Some(tx_info) = update.transaction else {
            tracing::warn!(slot, "TransactionUpdate missing transaction info");
            return;
        };

        tracing::trace!(slot, tx_index = tx_info.index, "Buffering transaction");

        let pending = self.pending_slots.entry(slot).or_default();
        pending.transactions.insert(tx_info.index, tx_info);

        self.prune_if_needed();
    }

    fn prune_if_needed(&mut self) {
        while self.pending_slots.len() > self.max_pending_slots {
            if let Some((old_slot, pending)) = self.pending_slots.pop_first() {
                tracing::warn!(
                    slot = old_slot,
                    tx_count = pending.transactions.len(),
                    has_meta = pending.block_meta.is_some(),
                    max_pending = self.max_pending_slots,
                    "Pruning old slot (buffer limit reached)"
                );
            }
        }
    }

    pub fn add_block_meta(&mut self, meta: BlockMetaUpdate) {
        let slot = meta.slot;

        if let Some(last_height) = self.last_committed_block_height {
            if let Some(height) = meta.block_height.as_ref().map(|h| h.block_height) {
                if height <= last_height {
                    tracing::debug!(
                        slot,
                        block_height = height,
                        last_committed = last_height,
                        "Skipping already-committed block meta"
                    );
                    return;
                }
            }
        }

        let pending = self.pending_slots.entry(slot).or_default();

        if pending.block_meta.is_some() {
            tracing::trace!(slot, "Duplicate block meta received, ignoring");
            return;
        }

        tracing::debug!(
            slot,
            block_height = ?meta.block_height.as_ref().map(|h| h.block_height),
            executed_tx_count = meta.executed_transaction_count,
            buffered_tx_count = pending.transactions.len(),
            "Received block meta"
        );

        pending.block_meta = Some(BlockMeta::from(&meta));
    }

    /// Process a slot status update.
    /// On SLOT_CONFIRMED/FINALIZED: validate, emit, and clean up old slots.
    /// On SLOT_DEAD: discard the slot.
    pub async fn process_slot_status(&mut self, update: &SlotUpdate) {
        let slot = update.slot;
        let status = SlotStatus::try_from(update.status).unwrap_or(SlotStatus::SlotProcessed);

        match status {
            SlotStatus::SlotConfirmed | SlotStatus::SlotFinalized => {
                self.on_slot_confirmed(slot, status).await;
            },
            SlotStatus::SlotDead => {
                self.on_slot_dead(slot, update.dead_error.as_deref());
            },
            _ => {
                // Ignore other statuses (Processed, FirstShredReceived, Completed, CreatedBank)
                tracing::trace!(slot, status = ?status, "Ignoring intermediate slot status");
            },
        }
    }

    async fn on_slot_confirmed(&mut self, confirmed_slot: u64, status: SlotStatus) {
        if let Some(pending) = self.pending_slots.remove(&confirmed_slot) {
            if pending.is_complete() {
                if let Some(assembled) = pending.into_assembled(confirmed_slot) {
                    if let Some(height) = assembled.block_height {
                        if self
                            .last_committed_block_height
                            .map_or(true, |h| height > h)
                        {
                            self.last_committed_block_height = Some(height);
                        }
                    }

                    tracing::info!(
                        slot = confirmed_slot,
                        block_height = ?assembled.block_height,
                        tx_count = assembled.transactions.len(),
                        status = ?status,
                        "Slot confirmed, sending to processor"
                    );

                    if let Err(e) = self.tx.send(assembled).await {
                        tracing::error!(?e, slot = confirmed_slot, "Failed to send confirmed slot");
                    }
                } else {
                    tracing::error!(slot = confirmed_slot, "Failed to assemble confirmed slot");
                }
            } else {
                // TODO: should not happen, check why it happens
                // Slot confirmed but incomplete
                let tx_count = pending.transactions.len();
                let expected = pending
                    .block_meta
                    .as_ref()
                    .map(|m| m.executed_transaction_count);
                tracing::warn!(
                    slot = confirmed_slot,
                    tx_count,
                    expected_tx_count = ?expected,
                    has_meta = pending.block_meta.is_some(),
                    "Slot confirmed but incomplete - data may still be in flight"
                );
                self.pending_slots.insert(confirmed_slot, pending);
            }
        } else {
            tracing::trace!(
                slot = confirmed_slot,
                "Confirmation for unknown slot (may have been pruned or already processed)"
            );
        }

        let slots_to_remove: Vec<u64> = self
            .pending_slots
            .keys()
            .take_while(|&&s| s < confirmed_slot)
            .copied()
            .collect();

        for old_slot in slots_to_remove {
            if let Some(pending) = self.pending_slots.remove(&old_slot) {
                tracing::debug!(
                    slot = old_slot,
                    confirmed_slot,
                    tx_count = pending.transactions.len(),
                    has_meta = pending.block_meta.is_some(),
                    "Removing old slot (newer slot confirmed)"
                );
            }
        }
    }

    fn on_slot_dead(&mut self, slot: u64, dead_error: Option<&str>) {
        if let Some(pending) = self.pending_slots.remove(&slot) {
            tracing::warn!(
                slot,
                tx_count = pending.transactions.len(),
                has_meta = pending.block_meta.is_some(),
                dead_error = ?dead_error,
                "Slot forked (dead), discarding"
            );
        }
    }

    pub fn stats(&self) -> (usize, Option<u64>, Option<u64>) {
        let count = self.pending_slots.len();
        let min_slot = self.pending_slots.keys().next().copied();
        let max_slot = self.pending_slots.keys().next_back().copied();
        (count, min_slot, max_slot)
    }
}

// Keep the old name as alias for backwards compatibility
pub type SlotAssembler = SlotBuffer;
