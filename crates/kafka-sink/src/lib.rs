//! Kafka sink for Yellowstone Vixen.
//!
//! This crate provides a Kafka sink for writing Solana blockchain data from Vixen
//! to Kafka with the following features:
//!
//! - **Slot ordering**: Blocks are processed in strict slot order for Kafka ordering guarantees
//! - **Deduplication**: Unique keys (`signature:ix_index`) enable log compaction
//! - **Batch publishing**: Instructions are batched for efficient Kafka writes
//! - **Resume support**: Reads last committed slot from Kafka and attempts replay
//! - **Missed slot handling**: Detects and skips Solana leader-skipped slots
//!
//! # Example
//!
//! ```ignore
//! use std::sync::Arc;
//! use yellowstone_vixen_kafka_sink::{
//!     KafkaSinkBuilder, KafkaSinkConfig, BlockProcessor, BlockBufferHandler,
//!     create_producer, ensure_topics_exist_with_log_compaction, read_latest_committed_slot,
//! };
//! use yellowstone_vixen_spl_token_parser::InstructionParser;
//!
//! // Configure parsers using the builder
//! let sink = KafkaSinkBuilder::new()
//!     .parser(InstructionParser, "spl-token", "spl-token.instructions")
//!     .fallback_topic("unknown.instructions")
//!     .build();
//!
//! // Configure Kafka
//! let config = KafkaSinkConfig::new("localhost:9092");
//!
//! ensure_topics_exist_with_log_compaction(&config, &sink.topics());
//!
//! // Read last committed slot for resume
//! let last_slot = read_latest_committed_slot(&config);
//!
//! // Create producer and handler
//! let producer = Arc::new(create_producer(&config));
//! let (handler, rx) = BlockBufferHandler::create(&config);
//!
//! // Create processor with configured parsers
//! let processor = BlockProcessor::new(config, producer, sink, last_slot);
//!
//! // Run processor in background
//! tokio::spawn(processor.run(rx));
//! ```

pub mod config;
pub mod events;
pub mod handler;
pub mod processor;
pub mod producer;
pub mod sink;
pub mod topics;
pub mod utils;

// Re-export main types
pub use config::KafkaSinkConfig;
pub use events::{DecodedInstructionEvent, PreparedRecord, RawInstructionEvent, SlotCommitEvent};
pub use handler::BlockBufferHandler;
pub use processor::{BlockProcessor, BlockRecordPreparer};
pub use producer::create_producer;
pub use sink::{ConfiguredParsers, KafkaSinkBuilder, ParsedInstruction};
pub use topics::{ensure_topics_exist_with_log_compaction, read_latest_committed_slot};
pub use utils::{format_path, make_record_key, visit_instructions_with_path};

// Re-export rdkafka types for convenience
pub use rdkafka::producer::FutureProducer;
