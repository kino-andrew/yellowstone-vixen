//! Janus - Vixen block processor that writes to Kafka/Redpanda.
//!
//! This example demonstrates using the kafka-sink crate to process Solana blocks
//! and publish decoded instructions to Kafka with ordering guarantees.

// TODO: not sure about kafka-sink for the name of the lib

use std::{borrow::Cow, path::PathBuf, sync::Arc};

use clap::Parser as _;
use tracing_subscriber::EnvFilter;
use yellowstone_vixen::{self as vixen, config::VixenConfig, Pipeline};
use yellowstone_vixen_core::{BlockUpdate, CommitmentLevel, ParseResult, Parser, Prefilter};
use yellowstone_vixen_kafka_sink::{
    create_producer, ensure_topics_exist_with_log_compaction, read_last_committed_block,
    BlockBufferHandler, BlockProcessor, FutureProducer, KafkaSinkBuilder, KafkaSinkConfig,
};
use yellowstone_vixen_spl_token_parser::InstructionParser;
use yellowstone_vixen_yellowstone_grpc_source::{YellowstoneGrpcConfig, YellowstoneGrpcSource};

#[derive(clap::Parser)]
#[command(
    version,
    author,
    about = "Janus - Vixen Block Processor with Kafka Sink"
)]
pub struct Opts {
    #[arg(long, short)]
    config: PathBuf,

    #[arg(long, env = "KAFKA_BROKERS", default_value = "localhost:9092")]
    kafka_brokers: String,
}

#[derive(Debug, Clone, Copy)]
pub struct BlockParser;

impl Parser for BlockParser {
    type Input = BlockUpdate;
    type Output = BlockUpdate;

    fn id(&self) -> Cow<'static, str> {
        "janus::BlockParser".into()
    }

    fn prefilter(&self) -> Prefilter {
        Prefilter::builder()
            .block_include_transactions()
            .build()
            .unwrap()
    }

    async fn parse(&self, block: &BlockUpdate) -> ParseResult<Self::Output> {
        Ok(block.clone())
    }
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("yellowstone_vixen_janus=info".parse().unwrap())
                .add_directive("yellowstone_vixen=info".parse().unwrap())
                .add_directive("yellowstone_vixen_kafka_sink=info".parse().unwrap())
                .add_directive("rdkafka=warn".parse().unwrap()),
        )
        .init();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let Opts {
        config: config_path,
        kafka_brokers,
    } = Opts::parse();

    let config_str = std::fs::read_to_string(&config_path).expect("Error reading config file");
    let mut vixen_config: VixenConfig<YellowstoneGrpcConfig> =
        toml::from_str(&config_str).expect("Error parsing config");

    // TODO: make it configurable for the processed cluster later
    vixen_config.source.commitment_level = Some(CommitmentLevel::Confirmed);

    let sink = KafkaSinkBuilder::new()
        .parser(InstructionParser, "spl-token", "spl-token.instructions")
        .fallback_topic("unknown.instructions")
        .build();

    let kafka_config = KafkaSinkConfig::new(&kafka_brokers);

    ensure_topics_exist_with_log_compaction(&kafka_config, &sink.topics());

    let last_committed = read_last_committed_block(&kafka_config);
    if let Some(ref committed) = last_committed {
        tracing::info!(
            slot = committed.slot,
            block_height = committed.block_height,
            "Last committed block from Kafka"
        );
    } else {
        tracing::info!("No committed blocks found - starting fresh");
    }

    tracing::info!(kafka_brokers, "Starting Janus - Vixen Kafka Sink");
    tracing::info!(
        "Architecture: Richat gRPC (confirmed) -> Buffer -> Ordered Processing -> Kafka"
    );

    let producer = Arc::new(create_producer(&kafka_config));

    // Try to resume from last committed slot
    if let Some(committed) = last_committed {
        let mut config_with_slot = vixen_config;
        config_with_slot.source.from_slot = Some(committed.slot);
        tracing::info!(slot = committed.slot, "Attempting to resume from slot");

        match run_pipeline(
            config_with_slot,
            kafka_config.clone(),
            &producer,
            &sink,
            Some(committed.block_height),
        ) {
            Ok(()) => return,
            Err(e) => {
                tracing::warn!(
                    slot = committed.slot,
                    error = %e,
                    "Resume from slot failed - retrying without from_slot"
                );
                let config_str =
                    std::fs::read_to_string(&config_path).expect("Error reading config file");
                vixen_config = toml::from_str(&config_str).expect("Error parsing config");
                vixen_config.source.commitment_level = Some(CommitmentLevel::Confirmed);
            },
        }
    }

    tracing::info!("Starting from live stream");
    if let Err(e) = run_pipeline(vixen_config, kafka_config, &producer, &sink, None) {
        panic!("Fatal error: {}", e);
    }
}

fn run_pipeline(
    vixen_config: VixenConfig<YellowstoneGrpcConfig>,
    kafka_config: KafkaSinkConfig,
    producer: &Arc<FutureProducer>,
    sink: &yellowstone_vixen_kafka_sink::ConfiguredParsers,
    last_committed_block_height: Option<u64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (block_handler, rx) = BlockBufferHandler::create(&kafka_config);

    let processor = BlockProcessor::new(
        kafka_config,
        Arc::clone(producer),
        sink.clone(), // ConfiguredParsers contains Arc<dyn ...> so cloning is cheap
        last_committed_block_height,
    );

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create processor runtime");
        rt.block_on(processor.run(rx));
    });

    vixen::Runtime::<YellowstoneGrpcSource>::builder()
        .block(Pipeline::new(BlockParser, [block_handler]))
        .build(vixen_config)
        .try_run()
        .map_err(|e| e.into())
}
