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

/// Block processor that maintains block_height ordering and handles Kafka publishing.
/// Uses block_height (not slot) because block heights are strictly sequential,
/// while slots can have gaps due to Solana leader skips.
pub struct BlockProcessor<P: BlockRecordPreparer> {
    config: KafkaSinkConfig,
    producer: Arc<FutureProducer>,
    preparer: P,
    /// Pending blocks waiting to be processed in order, keyed by block_height.
    pending_blocks: BTreeMap<u64, BlockUpdate>,
    /// Next block_height we expect to process (for ordering).
    next_block_height: Option<u64>,
    /// Last committed block_height from Kafka - skip this on first receive.
    last_committed_block_height: Option<u64>,
    /// Block heights we've already processed (for deduplication).
    processed_block_heights: HashSet<u64>,
}

impl<P: BlockRecordPreparer> BlockProcessor<P> {
    pub fn new(
        config: KafkaSinkConfig,
        producer: Arc<FutureProducer>,
        preparer: P,
        last_committed_block_height: Option<u64>,
    ) -> Self {
        Self {
            config,
            producer,
            preparer,
            pending_blocks: BTreeMap::new(),
            next_block_height: None,
            last_committed_block_height,
            processed_block_heights: HashSet::new(),
        }
    }

    /// Run the block processor, consuming blocks from the mpsc channel
    pub async fn run(mut self, mut rx: mpsc::Receiver<BlockUpdate>) {
        tracing::info!(
            last_committed_block_height = ?self.last_committed_block_height,
            "Block processor started, waiting for blocks..."
        );

        while let Some(block) = rx.recv().await {
            let slot = block.slot;
            let tx_count = block.transactions.len();

            // Extract block_height - skip blocks without it
            let block_height = match block.block_height.as_ref() {
                Some(bh) => bh.block_height,
                None => {
                    tracing::warn!(slot, "Block missing block_height, skipping");
                    continue;
                },
            };

            // Discard blocks that were already committed (resume case)
            if let Some(last_committed) = self.last_committed_block_height {
                if block_height <= last_committed {
                    tracing::debug!(slot, block_height, last_committed, "Discarding already-committed block");
                    continue;
                }
                // block_height > last_committed: clear the check and process normally
                self.last_committed_block_height = None;
            }

            // Deduplication: skip if already processed or pending
            if self.processed_block_heights.contains(&block_height)
                || self.pending_blocks.contains_key(&block_height)
            {
                tracing::debug!(slot, block_height, "Skipping duplicate block");
                continue;
            }

            tracing::info!(slot, block_height, tx_count, "Received block, queued for processing");

            // Add block to pending queue, keyed by block_height
            self.pending_blocks.insert(block_height, block);

            // Initialize next_block_height if this is the first block
            if self.next_block_height.is_none() {
                self.next_block_height = Some(block_height);
                tracing::info!(block_height, "Initialized ordering starting from block_height");
            }

            // Process all consecutive blocks we have
            self.process_ready_blocks().await;
        }

        tracing::warn!("Block processor channel closed, shutting down");
    }

    /// Process blocks in strict block_height order for Kafka ordering guarantee.
    /// Block heights are strictly sequential (no gaps), so we simply process
    /// the next expected height when available.
    async fn process_ready_blocks(&mut self) {
        loop {
            let Some(expected) = self.next_block_height else {
                // First block - just process whatever we have
                if let Some(height) = self.pending_blocks.keys().next().copied() {
                    self.process_and_advance(height).await;
                    continue;
                }
                break;
            };

            // Process if expected block_height is available
            if self.pending_blocks.contains_key(&expected) {
                self.process_and_advance(expected).await;
                continue;
            }

            // Block heights are sequential - wait for the expected height to arrive
            break;
        }
    }

    /// Process a single block and advance next_block_height.
    async fn process_and_advance(&mut self, block_height: u64) {
        let block = self.pending_blocks.remove(&block_height).unwrap();

        if let Err(e) = self.process_block(&block).await {
            tracing::error!(?e, slot = block.slot, block_height, "Error processing block");
        }

        // Mark as processed for deduplication
        self.processed_block_heights.insert(block_height);

        // Prune old heights to keep memory bounded
        if self.processed_block_heights.len() > self.config.buffer_size {
            if let Some(min_height) = self.processed_block_heights.iter().min().copied() {
                self.processed_block_heights.remove(&min_height);
            }
        }

        self.next_block_height = Some(block_height + 1);
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
                },
                Err((e, _)) => {
                    failure_count += 1;
                    tracing::error!(
                        ?e,
                        slot,
                        label = %record.label,
                        topic = %record.topic,
                        "Kafka publish failed"
                    );
                },
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
        let block_height = block
            .block_height
            .as_ref()
            .map(|bh| bh.block_height)
            .unwrap_or(0);
        let height_key = block_height.to_string();

        let record = FutureRecord::to(&self.config.slots_topic)
            .payload(&payload)
            .key(&height_key);

        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok(_) => tracing::info!(
                slot = block.slot,
                block_height,
                decoded_count,
                topic = %self.config.slots_topic,
                "Kafka: committed block"
            ),
            Err((e, _)) => {
                tracing::error!(
                    ?e,
                    slot = block.slot,
                    topic = %self.config.slots_topic,
                    "Kafka: failed to commit slot"
                )
            },
        }

        Ok(())
    }
}
