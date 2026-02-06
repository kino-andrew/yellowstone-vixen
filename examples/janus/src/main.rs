//! Janus - Vixen block processor that writes to Kafka/Redpanda.
//!
//! This example demonstrates using the kafka-sink crate to process Solana blocks
//! and publish decoded instructions to Kafka with ordering guarantees.
//! Will be moved to Janus repository later.

use std::{path::PathBuf, sync::Arc};

use clap::Parser as _;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;
use yellowstone_vixen::{self as vixen, Pipeline};
use yellowstone_vixen_core::{BlockMetaUpdate, CommitmentLevel, SlotUpdate, TransactionUpdate};
use yellowstone_vixen_kafka_sink::{
    create_producer, ensure_topics_exist_with_log_compaction, read_last_committed_block,
    AssembledSlot, AssemblerChannels, BlockMetaParser, BlockProcessor, KafkaSinkBuilder,
    KafkaSinkConfig, SlotBuffer, SlotParser, TransactionParser,
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
    let mut vixen_config: vixen::config::VixenConfig<YellowstoneGrpcConfig> =
        toml::from_str(&config_str).expect("Error parsing config");

    vixen_config.source.commitment_level = Some(CommitmentLevel::Processed);

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
        "Architecture: Richat gRPC (processed) -> SlotBuffer (confirm) -> Processor -> Kafka"
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
                vixen_config.source.commitment_level = Some(CommitmentLevel::Processed);
            },
        }
    }

    tracing::info!("Starting from live stream");
    if let Err(e) = run_pipeline(vixen_config, kafka_config, &producer, &sink, None) {
        panic!("Fatal error: {}", e);
    }
}

fn run_pipeline(
    vixen_config: vixen::config::VixenConfig<YellowstoneGrpcConfig>,
    kafka_config: KafkaSinkConfig,
    producer: &Arc<yellowstone_vixen_kafka_sink::FutureProducer>,
    sink: &yellowstone_vixen_kafka_sink::ConfiguredParsers,
    last_committed_block_height: Option<u64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channels = AssemblerChannels::new(kafka_config.buffer_size);
    let (tx_handler, meta_handler, slot_handler) = channels.handlers();

    let (confirmed_tx, confirmed_rx) = mpsc::channel::<AssembledSlot>(kafka_config.buffer_size);

    let buffer_handle = spawn_slot_buffer(
        channels.tx_receiver,
        channels.meta_receiver,
        channels.slot_receiver,
        confirmed_tx,
        last_committed_block_height,
        kafka_config.buffer_size,
    );

    let processor = BlockProcessor::new(
        kafka_config,
        Arc::clone(producer),
        sink.clone(),
        last_committed_block_height,
    );

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create processor runtime");
        rt.block_on(processor.run(confirmed_rx));
    });

    vixen::Runtime::<YellowstoneGrpcSource>::builder()
        .transaction(Pipeline::new(TransactionParser, [tx_handler]))
        .block_meta(Pipeline::new(BlockMetaParser, [meta_handler]))
        .slot(Pipeline::new(SlotParser, [slot_handler]))
        .build(vixen_config)
        .try_run()
        .map_err(|e| {
            drop(buffer_handle);
            e.into()
        })
}

fn spawn_slot_buffer(
    mut tx_rx: mpsc::Receiver<TransactionUpdate>,
    mut meta_rx: mpsc::Receiver<BlockMetaUpdate>,
    mut slot_rx: mpsc::Receiver<SlotUpdate>,
    confirmed_tx: mpsc::Sender<AssembledSlot>,
    last_committed_block_height: Option<u64>,
    max_pending_slots: usize,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create slot buffer runtime");

        rt.block_on(async move {
            let mut buffer =
                SlotBuffer::new(confirmed_tx, last_committed_block_height, max_pending_slots);

            loop {
                tokio::select! {
                    Some(tx) = tx_rx.recv() => {
                        buffer.add_transaction(tx);
                    }
                    Some(meta) = meta_rx.recv() => {
                        buffer.add_block_meta(meta);
                    }
                    Some(slot_update) = slot_rx.recv() => {
                        buffer.process_slot_status(&slot_update).await;
                    }
                    else => {
                        tracing::warn!("SlotBuffer channels closed, shutting down");
                        break;
                    }
                }
            }
        });
    })
}
