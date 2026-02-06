use tokio::sync::mpsc;
use yellowstone_vixen::{self as vixen, HandlerResult};
use yellowstone_vixen_core::{BlockMetaUpdate, BlockUpdate, SlotUpdate, TransactionUpdate};

use crate::{assembler::AssembledSlot, config::KafkaSinkConfig};

#[derive(Clone)]
pub struct BlockBufferHandler {
    tx: mpsc::Sender<BlockUpdate>,
    buffer_size: usize,
}

impl std::fmt::Debug for BlockBufferHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockBufferHandler")
            .field("buffer_capacity", &self.buffer_size)
            .finish()
    }
}

impl BlockBufferHandler {
    pub fn new(tx: mpsc::Sender<BlockUpdate>, config: &KafkaSinkConfig) -> Self {
        Self {
            tx,
            buffer_size: config.buffer_size,
        }
    }

    pub fn create(config: &KafkaSinkConfig) -> (Self, mpsc::Receiver<BlockUpdate>) {
        let (tx, rx) = mpsc::channel(config.buffer_size);
        (Self::new(tx, config), rx)
    }
}

impl vixen::Handler<BlockUpdate, BlockUpdate> for BlockBufferHandler {
    async fn handle(&self, block: &BlockUpdate, _raw: &BlockUpdate) -> HandlerResult<()> {
        let slot = block.slot;
        let tx_count = block.transactions.len();

        match self.tx.send(block.clone()).await {
            Ok(_) => {
                tracing::debug!(slot, tx_count, "Block received, buffered for processing");
            },
            Err(e) => {
                tracing::error!(
                    ?e,
                    slot,
                    "Failed to buffer block - processor may be overwhelmed"
                );
            },
        }

        Ok(())
    }
}

// ============================================================================
// Transaction + BlockMeta handlers
// ============================================================================

#[derive(Clone)]
pub struct TransactionHandler {
    tx: mpsc::Sender<TransactionUpdate>,
}

impl std::fmt::Debug for TransactionHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionHandler").finish()
    }
}

impl TransactionHandler {
    pub fn new(tx: mpsc::Sender<TransactionUpdate>) -> Self {
        Self { tx }
    }
}

impl vixen::Handler<TransactionUpdate, TransactionUpdate> for TransactionHandler {
    async fn handle(
        &self,
        update: &TransactionUpdate,
        _raw: &TransactionUpdate,
    ) -> HandlerResult<()> {
        let slot = update.slot;
        let has_tx_info = update.transaction.is_some();

        tracing::debug!(slot, has_tx_info, "TransactionHandler received transaction");

        if let Err(e) = self.tx.send(update.clone()).await {
            tracing::error!(?e, slot, "Failed to send transaction to assembler");
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct BlockMetaHandler {
    tx: mpsc::Sender<BlockMetaUpdate>,
}

impl std::fmt::Debug for BlockMetaHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockMetaHandler").finish()
    }
}

impl BlockMetaHandler {
    pub fn new(tx: mpsc::Sender<BlockMetaUpdate>) -> Self {
        Self { tx }
    }
}

impl vixen::Handler<BlockMetaUpdate, BlockMetaUpdate> for BlockMetaHandler {
    async fn handle(&self, meta: &BlockMetaUpdate, _raw: &BlockMetaUpdate) -> HandlerResult<()> {
        let slot = meta.slot;

        tracing::debug!(
            slot,
            block_height = ?meta.block_height.as_ref().map(|h| h.block_height),
            executed_tx_count = meta.executed_transaction_count,
            "BlockMeta received"
        );

        if let Err(e) = self.tx.send(meta.clone()).await {
            tracing::error!(?e, slot, "Failed to send block meta to assembler");
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct SlotHandler {
    tx: mpsc::Sender<SlotUpdate>,
}

impl std::fmt::Debug for SlotHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotHandler").finish()
    }
}

impl SlotHandler {
    pub fn new(tx: mpsc::Sender<SlotUpdate>) -> Self {
        Self { tx }
    }
}

impl vixen::Handler<SlotUpdate, SlotUpdate> for SlotHandler {
    async fn handle(&self, update: &SlotUpdate, _raw: &SlotUpdate) -> HandlerResult<()> {
        let slot = update.slot;
        let status = update.status;

        tracing::debug!(slot, status, "Slot status update received");

        if let Err(e) = self.tx.send(update.clone()).await {
            tracing::error!(
                ?e,
                slot,
                "Failed to send slot update to confirmation buffer"
            );
        }

        Ok(())
    }
}

/// Channels for the slot assembler and confirmation buffer.
pub struct AssemblerChannels {
    pub tx_sender: mpsc::Sender<TransactionUpdate>,
    pub tx_receiver: mpsc::Receiver<TransactionUpdate>,
    pub meta_sender: mpsc::Sender<BlockMetaUpdate>,
    pub meta_receiver: mpsc::Receiver<BlockMetaUpdate>,
    pub slot_sender: mpsc::Sender<SlotUpdate>,
    pub slot_receiver: mpsc::Receiver<SlotUpdate>,
    pub assembled_sender: mpsc::Sender<AssembledSlot>,
    pub assembled_receiver: mpsc::Receiver<AssembledSlot>,
}

impl AssemblerChannels {
    pub fn new(buffer_size: usize) -> Self {
        let (tx_sender, tx_receiver) = mpsc::channel(buffer_size * 100); // More txs than blocks
        let (meta_sender, meta_receiver) = mpsc::channel(buffer_size);
        let (slot_sender, slot_receiver) = mpsc::channel(buffer_size * 10); // Slot updates come frequently
        let (assembled_sender, assembled_receiver) = mpsc::channel(buffer_size);

        Self {
            tx_sender,
            tx_receiver,
            meta_sender,
            meta_receiver,
            slot_sender,
            slot_receiver,
            assembled_sender,
            assembled_receiver,
        }
    }

    pub fn handlers(&self) -> (TransactionHandler, BlockMetaHandler, SlotHandler) {
        (
            TransactionHandler::new(self.tx_sender.clone()),
            BlockMetaHandler::new(self.meta_sender.clone()),
            SlotHandler::new(self.slot_sender.clone()),
        )
    }
}
