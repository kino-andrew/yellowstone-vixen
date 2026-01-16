//! Block processor with slot ordering guarantees.

use std::{
    collections::{BTreeMap, HashSet},
    future::Future,
    sync::Arc,
    time::Duration,
};

use futures::future::join_all;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::sync::mpsc;
use yellowstone_vixen_core::BlockUpdate;

use crate::{
    config::KafkaSinkConfig,
    events::{PreparedRecord, SlotCommitEvent},
};

/// Trait for parsing blocks into Kafka records.
/// Implement this to customize how blocks are processed.
pub trait BlockRecordPreparer: Send + Sync {
    /// Prepare Kafka records from a block.
    /// Returns a list of prepared records and the count of decoded instructions.
    fn prepare_records(
        &self,
        block: &BlockUpdate,
    ) -> impl Future<Output = (Vec<PreparedRecord>, u64)> + Send;
}

/// Block processor that maintains slot ordering and handles Kafka publishing.
pub struct BlockProcessor<P: BlockRecordPreparer> {
    config: KafkaSinkConfig,
    producer: Arc<FutureProducer>,
    preparer: P,
    /// Pending blocks waiting to be processed in order.
    pending_blocks: BTreeMap<u64, BlockUpdate>,
    /// Next slot we expect to process (for ordering).
    next_slot: Option<u64>,
    /// Last committed slot from Kafka - skip this on first receive.
    last_committed_slot: Option<u64>,
    /// Slots we've already processed (for deduplication).
    processed_slots: HashSet<u64>,
}

impl<P: BlockRecordPreparer> BlockProcessor<P> {
    /// Create a new block processor.
    pub fn new(
        config: KafkaSinkConfig,
        producer: Arc<FutureProducer>,
        preparer: P,
        last_committed_slot: Option<u64>,
    ) -> Self {
        Self {
            config,
            producer,
            preparer,
            pending_blocks: BTreeMap::new(),
            next_slot: None,
            last_committed_slot,
            processed_slots: HashSet::new(),
        }
    }

    /// Run the block processor, consuming blocks from the channel.
    pub async fn run(mut self, mut rx: mpsc::Receiver<BlockUpdate>) {
        tracing::info!(
            last_committed = ?self.last_committed_slot,
            "Block processor started, waiting for blocks..."
        );

        while let Some(block) = rx.recv().await {
            let slot = block.slot;
            let tx_count = block.transactions.len();

            // Discard blocks that were already committed (resume case)
            if let Some(last_committed) = self.last_committed_slot {
                if slot <= last_committed {
                    tracing::debug!(slot, last_committed, "Discarding already-committed block");
                    continue;
                }
                // slot > last_committed: clear the check and process normally
                self.last_committed_slot = None;
            }

            // Deduplication: skip if already processed or pending
            if self.processed_slots.contains(&slot) || self.pending_blocks.contains_key(&slot) {
                tracing::debug!(slot, "Skipping duplicate block");
                continue;
            }

            tracing::info!(slot, tx_count, "Received block, queued for processing");

            // Add block to pending queue
            self.pending_blocks.insert(slot, block);

            // Initialize next_slot if this is the first block
            if self.next_slot.is_none() {
                self.next_slot = Some(slot);
                tracing::info!(slot, "Initialized slot ordering starting from slot");
            }

            // Process all consecutive blocks we have
            self.process_ready_blocks().await;
        }

        tracing::warn!("Block processor channel closed, shutting down");
    }

    /// Process blocks in strict slot order for Kafka ordering guarantee.
    async fn process_ready_blocks(&mut self) {
        loop {
            let Some(expected) = self.next_slot else {
                // First block - just process whatever we have
                if let Some(slot) = self.pending_blocks.keys().next().copied() {
                    self.process_and_advance(slot).await;
                    continue;
                }
                break;
            };

            // Case 1: Expected slot is available - process it
            if self.pending_blocks.contains_key(&expected) {
                self.process_and_advance(expected).await;
                continue;
            }

            // Case 2: Expected slot not available - check if we should skip it
            let buffered_slots: Vec<u64> = self.pending_blocks.keys().copied().collect();

            if buffered_slots.is_empty() {
                break;
            }

            let first_buffered = buffered_slots[0];

            // If we have enough later slots, the expected slot is probably skipped by Solana
            if first_buffered > expected && buffered_slots.len() >= self.config.skip_threshold {
                let skipped = first_buffered - expected;
                tracing::debug!(
                    expected,
                    first_available = first_buffered,
                    buffered_count = buffered_slots.len(),
                    skipped,
                    "Skipping missed slots (Solana leader skip)"
                );
                self.next_slot = Some(first_buffered);
                continue;
            }

            // Otherwise, wait for the expected slot to arrive
            break;
        }
    }

    /// Process a single slot and advance next_slot.
    async fn process_and_advance(&mut self, slot: u64) {
        let block = self.pending_blocks.remove(&slot).unwrap();

        if let Err(e) = self.process_block(&block).await {
            tracing::error!(?e, slot, "Error processing block");
        }

        // Mark as processed for deduplication
        self.processed_slots.insert(slot);

        // Prune old slots to keep memory bounded
        if self.processed_slots.len() > self.config.buffer_size {
            if let Some(min_slot) = self.processed_slots.iter().min().copied() {
                self.processed_slots.remove(&min_slot);
            }
        }

        self.next_slot = Some(slot + 1);
    }

    /// Process a single block: prepare records, publish to Kafka, commit slot.
    async fn process_block(
        &self,
        block: &BlockUpdate,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let slot = block.slot;
        let tx_count = block.transactions.len();
        tracing::info!(slot, tx_count, ">>> Processing block START");

        // Phase 1: Prepare Kafka records
        let (records, decoded_instruction_count) = self.preparer.prepare_records(block).await;

        // Phase 2: Batch send all records to Kafka
        let record_count = records.len();
        tracing::info!(slot, record_count, "Sending batch to Kafka");

        let futures: Vec<_> = records
            .iter()
            .map(|r| {
                self.producer.send(
                    FutureRecord::to(&r.topic).payload(&r.payload).key(&r.key),
                    Duration::from_secs(5),
                )
            })
            .collect();

        let results = join_all(futures).await;

        // Phase 3: Log failures
        let mut success_count = 0;
        let mut failure_count = 0;
        for (result, record) in results.into_iter().zip(records.iter()) {
            match result {
                Ok(_) => {
                    success_count += 1;
                    if record.is_decoded {
                        tracing::debug!(slot, label = %record.label, topic = %record.topic, "Published");
                    }
                }
                Err((e, _)) => {
                    failure_count += 1;
                    tracing::error!(
                        ?e,
                        slot,
                        label = %record.label,
                        topic = %record.topic,
                        "Kafka publish failed"
                    );
                }
            }
        }

        if failure_count > 0 {
            tracing::warn!(
                slot,
                success_count,
                failure_count,
                "Block had partial publish failures"
            );
        }

        // Phase 4: Commit the slot
        self.commit_slot(block, decoded_instruction_count).await?;

        tracing::info!(
            slot,
            decoded_instruction_count,
            record_count,
            "<<< Processing block DONE"
        );
        Ok(())
    }

    /// Commit the slot to the slots topic after block is fully processed.
    async fn commit_slot(
        &self,
        block: &BlockUpdate,
        decoded_count: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let event = SlotCommitEvent {
            slot: block.slot,
            blockhash: block.blockhash.clone(),
            block_time: block.block_time.as_ref().map(|bt| bt.timestamp),
            block_height: block.block_height.as_ref().map(|bh| bh.block_height),
            transaction_count: block.executed_transaction_count,
            decoded_instruction_count: decoded_count,
        };

        let payload = serde_json::to_string(&event)?;
        let slot_key = block.slot.to_string();

        let record = FutureRecord::to(&self.config.slots_topic)
            .payload(&payload)
            .key(&slot_key);

        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok(_) => tracing::info!(
                slot = block.slot,
                decoded_count,
                topic = %self.config.slots_topic,
                "Kafka: committed slot"
            ),
            Err((e, _)) => {
                tracing::error!(
                    ?e,
                    slot = block.slot,
                    topic = %self.config.slots_topic,
                    "Kafka: failed to commit slot"
                )
            }
        }

        Ok(())
    }
}
