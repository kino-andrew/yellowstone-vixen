//! Kafka sink builder that accepts Vixen parsers as configuration.
//!
//! This module provides a clean API for configuring kafka-sink with Vixen parsers.
//! Users pass their Vixen parser implementations, and kafka-sink handles the rest.

use std::{future::Future, pin::Pin, sync::Arc};

use yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction;
use yellowstone_vixen_core::{
    bs58, instruction::InstructionUpdate, BlockUpdate, ParseError, Parser,
};

use crate::{
    events::{DecodedInstructionEvent, PreparedRecord, RawInstructionEvent},
    processor::BlockRecordPreparer,
    utils::{format_path, get_all_ix_with_index, make_record_key},
};

/// Parsed instruction data (type-erased).
#[derive(Debug, Clone)]
pub struct ParsedInstruction {
    /// Human-readable instruction name (e.g., "TransferChecked").
    pub instruction_name: String,
    /// Discriminant/variant identifier.
    pub instruction_type: String,
    /// Full instruction data (debug format).
    pub data: String,
}

impl ParsedInstruction {
    pub fn from_debug<T: std::fmt::Debug>(output: &T) -> Self {
        let debug_str = format!("{:?}", output);
        let instruction_name = debug_str
            .split_once('{')
            .map(|(name, _)| name.trim())
            .or_else(|| debug_str.split_once('(').map(|(name, _)| name.trim()))
            .unwrap_or(&debug_str)
            .to_string();
        let instruction_type = format!("{:?}", std::mem::discriminant(output));

        Self {
            instruction_name,
            instruction_type,
            data: debug_str,
        }
    }
}

/// Type-erased instruction parser trait.
/// This allows storing different parser types in a collection.
pub trait DynInstructionParser: Send + Sync {
    /// Try to parse an instruction. Returns None if this parser doesn't handle it.
    fn try_parse<'a>(
        &'a self,
        ix: &'a InstructionUpdate,
    ) -> Pin<Box<dyn Future<Output = Option<ParsedInstruction>> + Send + 'a>>;

    fn topic(&self) -> &str;

    fn program_name(&self) -> &str;
}

/// Wrapper that implements DynInstructionParser for any Vixen Parser.
struct ParserWrapper<P> {
    parser: P,
    topic: String,
    program_name: String,
}

impl<P, O> DynInstructionParser for ParserWrapper<P>
where
    P: Parser<Input = InstructionUpdate, Output = O> + Send + Sync,
    O: std::fmt::Debug + Send + Sync,
{
    fn try_parse<'a>(
        &'a self,
        ix: &'a InstructionUpdate,
    ) -> Pin<Box<dyn Future<Output = Option<ParsedInstruction>> + Send + 'a>> {
        Box::pin(async move {
            match self.parser.parse(ix).await {
                Ok(output) => Some(ParsedInstruction::from_debug(&output)),
                Err(ParseError::Filtered) => None, // Not handled by this parser
                Err(e) => {
                    tracing::warn!(?e, program = %self.program_name, "Error parsing instruction");
                    None
                },
            }
        })
    }

    fn topic(&self) -> &str {
        &self.topic
    }

    fn program_name(&self) -> &str {
        &self.program_name
    }
}

pub struct KafkaSinkBuilder {
    parsers: Vec<Arc<dyn DynInstructionParser>>,
    fallback_topic: String,
}

impl Default for KafkaSinkBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl KafkaSinkBuilder {
    pub fn new() -> Self {
        Self {
            parsers: Vec::new(),
            fallback_topic: "unknown.instructions".to_string(),
        }
    }

    /// Add a Vixen parser with its program name and Kafka topic.
    ///
    /// # Arguments
    /// * `parser` - A Vixen `Parser<Input=InstructionUpdate>` implementation
    /// * `program_name` - Name of the program (e.g., "spl-token")
    /// * `topic` - Kafka topic for this parser's output
    pub fn parser<P, O>(mut self, parser: P, program_name: &str, topic: &str) -> Self
    where
        P: Parser<Input = InstructionUpdate, Output = O> + Send + Sync + 'static,
        O: std::fmt::Debug + Send + Sync + 'static,
    {
        self.parsers.push(Arc::new(ParserWrapper {
            parser,
            topic: topic.to_string(),
            program_name: program_name.to_string(),
        }));
        self
    }

    /// Set the fallback topic for instructions that no parser handles.
    pub fn fallback_topic(mut self, topic: &str) -> Self {
        self.fallback_topic = topic.to_string();
        self
    }

    pub fn build(self) -> ConfiguredParsers {
        ConfiguredParsers {
            parsers: self.parsers,
            fallback_topic: self.fallback_topic,
        }
    }

    pub fn topics(&self) -> Vec<&str> {
        let mut topics: Vec<&str> = self.parsers.iter().map(|p| p.topic()).collect();
        topics.push(&self.fallback_topic);
        topics.dedup();
        topics
    }
}

#[derive(Clone)]
pub struct ConfiguredParsers {
    parsers: Vec<Arc<dyn DynInstructionParser>>,
    fallback_topic: String,
}

impl Default for ConfiguredParsers {
    fn default() -> Self {
        Self {
            parsers: Vec::new(),
            fallback_topic: "unknown.instructions".to_string(),
        }
    }
}

impl ConfiguredParsers {
    pub fn topics(&self) -> Vec<&str> {
        let mut topics: Vec<&str> = self.parsers.iter().map(|p| p.topic()).collect();
        topics.push(&self.fallback_topic);
        topics.dedup();
        topics
    }

    pub fn fallback_topic(&self) -> &str {
        &self.fallback_topic
    }

    pub async fn try_parse(
        &self,
        ix: &InstructionUpdate,
    ) -> Option<(ParsedInstruction, &str, &str)> {
        for parser in &self.parsers {
            if let Some(parsed) = parser.try_parse(ix).await {
                return Some((parsed, parser.program_name(), parser.topic()));
            }
        }
        None
    }

    fn prepare_decoded_record(
        &self,
        slot: u64,
        signature: &[u8],
        path: &[usize],
        parsed: ParsedInstruction,
        program_name: &str,
        topic: &str,
    ) -> PreparedRecord {
        let sig_str = bs58::encode(signature).into_string();
        let path_str = format_path(path);

        let event = DecodedInstructionEvent {
            slot,
            signature: sig_str.clone(),
            ix_index: path_str.clone(),
            program: program_name.to_string(),
            instruction_type: parsed.instruction_type,
            instruction_name: parsed.instruction_name.clone(),
            data: parsed.data,
        };

        PreparedRecord {
            topic: topic.to_string(),
            payload: serde_json::to_string(&event).unwrap_or_default(),
            key: make_record_key(&sig_str, &path_str),
            label: parsed.instruction_name,
            is_decoded: true,
        }
    }

    fn prepare_fallback_record(
        &self,
        slot: u64,
        signature: &[u8],
        path: &[usize],
        ix: &InstructionUpdate,
    ) -> PreparedRecord {
        let sig_str = bs58::encode(signature).into_string();
        let path_str = format_path(path);
        let program_id = bs58::encode(ix.program).into_string();

        let event = RawInstructionEvent {
            slot,
            signature: sig_str.clone(),
            ix_index: path_str.clone(),
            program_id: program_id.clone(),
            data: bs58::encode(&ix.data).into_string(),
        };

        PreparedRecord {
            topic: self.fallback_topic.clone(),
            payload: serde_json::to_string(&event).unwrap_or_default(),
            key: make_record_key(&sig_str, &path_str),
            label: program_id,
            is_decoded: false,
        }
    }
}

impl BlockRecordPreparer for ConfiguredParsers {
    async fn prepare_records(&self, block: &BlockUpdate) -> (Vec<PreparedRecord>, u64) {
        let slot = block.slot;
        let mut records = Vec::new();
        let mut decoded_count = 0u64;

        for tx_info in &block.transactions {
            let tx_update = SubscribeUpdateTransaction {
                slot,
                transaction: Some(tx_info.clone()),
            };

            let instructions = match InstructionUpdate::parse_from_txn(&tx_update) {
                Ok(ixs) => ixs,
                Err(e) => {
                    tracing::warn!(?e, slot, "Failed to parse transaction instructions");
                    continue;
                },
            };

            for (ix_index, ix_update) in instructions.iter().enumerate() {
                for (path, ix) in get_all_ix_with_index(ix_update, ix_index) {
                    let record = match self.try_parse(ix).await {
                        Some((parsed, program_name, topic)) => {
                            decoded_count += 1;
                            self.prepare_decoded_record(
                                slot,
                                &ix.shared.signature,
                                &path,
                                parsed,
                                program_name,
                                topic,
                            )
                        },
                        None => self.prepare_fallback_record(slot, &ix.shared.signature, &path, ix),
                    };

                    records.push(record);
                }
            }
        }

        (records, decoded_count)
    }
}
