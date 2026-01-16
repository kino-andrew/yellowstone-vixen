use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSinkConfig {
    /// Kafka bootstrap servers (e.g., "localhost:9092").
    pub brokers: String,

    #[serde(default = "default_slots_topic")]
    pub slots_topic: String,

    /// Block buffer size for the processing channel.
    /// Blocks are buffered here to handle bursts.
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,

    /// Threshold for skipping missed slots.
    /// Ensure the slot(s) was/were skipped by Solana and not missed by the GRPC sources
    /// If we have this many later slots buffered, assume earlier slots are skipped.
    #[serde(default = "default_skip_threshold")]
    pub skip_threshold: usize,

    #[serde(default = "default_message_timeout_ms")]
    pub message_timeout_ms: u32,

    #[serde(default = "default_queue_buffering_max_messages")]
    pub queue_buffering_max_messages: u32,

    #[serde(default = "default_batch_num_messages")]
    pub batch_num_messages: u32,
}

fn default_slots_topic() -> String {
    "solana.slots".to_string()
}

fn default_buffer_size() -> usize {
    100
}

fn default_skip_threshold() -> usize {
    3
}

fn default_message_timeout_ms() -> u32 {
    5000
}

fn default_queue_buffering_max_messages() -> u32 {
    100000
}

fn default_batch_num_messages() -> u32 {
    1000
}

impl Default for KafkaSinkConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            slots_topic: default_slots_topic(),
            buffer_size: default_buffer_size(),
            skip_threshold: default_skip_threshold(),
            message_timeout_ms: default_message_timeout_ms(),
            queue_buffering_max_messages: default_queue_buffering_max_messages(),
            batch_num_messages: default_batch_num_messages(),
        }
    }
}

impl KafkaSinkConfig {
    pub fn new(brokers: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            ..Default::default()
        }
    }
}
