pub mod assembler;
pub mod config;
pub mod events;
pub mod handler;
pub mod parsers;
pub mod processor;
pub mod producer;
pub mod sink;
pub mod topics;
pub mod utils;

// Re-export main types
pub use assembler::{AssembledSlot, SlotAssembler, SlotBuffer};
pub use config::KafkaSinkConfig;
pub use events::{DecodedInstructionEvent, PreparedRecord, RawInstructionEvent, SlotCommitEvent};
pub use handler::{
    AssemblerChannels, BlockBufferHandler, BlockMetaHandler, SlotHandler, TransactionHandler,
};
pub use parsers::{BlockMetaParser, SlotParser, TransactionParser};
pub use processor::{BlockProcessor, BlockRecordPreparer, ProcessableBlock};
pub use producer::create_producer;
pub use sink::{ConfiguredParsers, KafkaSinkBuilder, ParsedInstruction};
pub use topics::{ensure_topics_exist_with_log_compaction, read_last_committed_block, LastCommitted};
pub use utils::{format_path, get_all_ix_with_index, make_record_key};

// Re-export rdkafka types for convenience
pub use rdkafka::producer::FutureProducer;
