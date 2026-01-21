//! Pass-through parsers for TransactionUpdate, BlockMetaUpdate, and SlotUpdate.
//!
//! These parsers simply forward the updates without transformation,
//! allowing Vixen to route them to the appropriate handlers.

use std::borrow::Cow;

use yellowstone_vixen_core::{
    BlockMetaPrefilter, BlockMetaUpdate, ParseResult, Parser, Prefilter, SlotPrefilter,
    SlotUpdate, TransactionPrefilter, TransactionUpdate,
};

/// Pass-through parser for transaction updates.
/// Subscribes to all transactions and forwards them as-is.
#[derive(Debug, Clone, Copy)]
pub struct TransactionParser;

impl Parser for TransactionParser {
    type Input = TransactionUpdate;
    type Output = TransactionUpdate;

    fn id(&self) -> Cow<'static, str> {
        "kafka-sink::TransactionParser".into()
    }

    fn prefilter(&self) -> Prefilter {
        let prefilter = Prefilter {
            transaction: Some(TransactionPrefilter {
                // Include ALL transactions (successful and failed) for block assembly
                failed: None,
                ..Default::default()
            }),
            ..Default::default()
        };
        tracing::info!(
            parser_id = %self.id(),
            has_transaction_filter = prefilter.transaction.is_some(),
            "TransactionParser prefilter created - receiving all transactions"
        );
        prefilter
    }

    async fn parse(&self, value: &Self::Input) -> ParseResult<Self::Output> {
        Ok(value.clone())
    }
}

/// Pass-through parser for block metadata updates.
/// Subscribes to all block metas and forwards them as-is.
#[derive(Debug, Clone, Copy)]
pub struct BlockMetaParser;

impl Parser for BlockMetaParser {
    type Input = BlockMetaUpdate;
    type Output = BlockMetaUpdate;

    fn id(&self) -> Cow<'static, str> {
        "kafka-sink::BlockMetaParser".into()
    }

    fn prefilter(&self) -> Prefilter {
        let prefilter = Prefilter {
            block_meta: Some(BlockMetaPrefilter {}),
            ..Default::default()
        };
        tracing::info!(
            parser_id = %self.id(),
            has_block_meta_filter = prefilter.block_meta.is_some(),
            "BlockMetaParser prefilter created"
        );
        prefilter
    }

    async fn parse(&self, value: &Self::Input) -> ParseResult<Self::Output> {
        Ok(value.clone())
    }
}

/// Pass-through parser for slot status updates.
/// Subscribes to ALL slot status transitions (processed, confirmed, finalized, dead)
/// by setting filter_by_commitment to false.
#[derive(Debug, Clone, Copy)]
pub struct SlotParser;

impl Parser for SlotParser {
    type Input = SlotUpdate;
    type Output = SlotUpdate;

    fn id(&self) -> Cow<'static, str> {
        "kafka-sink::SlotParser".into()
    }

    fn prefilter(&self) -> Prefilter {
        let prefilter = Prefilter {
            slot: Some(SlotPrefilter {
                // Receive ALL slot status updates (processed, confirmed, finalized, dead)
                filter_by_commitment: false,
            }),
            ..Default::default()
        };
        tracing::info!(
            parser_id = %self.id(),
            filter_by_commitment = false,
            "SlotParser prefilter created - receiving all slot status updates"
        );
        prefilter
    }

    async fn parse(&self, value: &Self::Input) -> ParseResult<Self::Output> {
        Ok(value.clone())
    }
}
