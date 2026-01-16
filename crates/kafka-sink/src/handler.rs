use tokio::sync::mpsc;
use yellowstone_vixen::{self as vixen, HandlerResult};
use yellowstone_vixen_core::BlockUpdate;

use crate::config::KafkaSinkConfig;

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
    /// Create a new handler with the given channel sender.
    pub fn new(tx: mpsc::Sender<BlockUpdate>, config: &KafkaSinkConfig) -> Self {
        Self {
            tx,
            buffer_size: config.buffer_size,
        }
    }

    /// Create a channel and handler pair.
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
