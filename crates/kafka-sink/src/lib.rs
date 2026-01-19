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
pub use utils::{format_path, get_all_ix_with_index, make_record_key};

// Re-export rdkafka types for convenience
pub use rdkafka::producer::FutureProducer;
