use std::borrow::Cow;

use yellowstone_grpc_proto::geyser::SubscribeUpdateBlockMeta;
use yellowstone_vixen_core::{ParseResult, Parser, Prefilter, ProgramParser, Pubkey};

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RewardProto {
    #[prost(string, tag = "1")]
    pub pubkey: ::prost::alloc::string::String,
    #[prost(int64, tag = "2")]
    pub lamports: i64,
    #[prost(uint64, tag = "3")]
    pub post_balance: u64,
    #[prost(int32, tag = "4")]
    pub reward_type: i32,
    #[prost(string, tag = "5")]
    pub commission: ::prost::alloc::string::String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RewardsProto {
    #[prost(message, repeated, tag = "1")]
    pub rewards: ::prost::alloc::vec::Vec<RewardProto>,
    #[prost(uint64, optional, tag = "2")]
    pub num_partitions: ::core::option::Option<u64>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockMetaUpdateProto {
    #[prost(uint64, tag = "1")]
    pub slot: u64,
    #[prost(string, tag = "2")]
    pub blockhash: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub rewards: ::core::option::Option<RewardsProto>,
    #[prost(int64, optional, tag = "4")]
    pub block_time: ::core::option::Option<i64>,
    #[prost(uint64, optional, tag = "5")]
    pub block_height: ::core::option::Option<u64>,
    #[prost(uint64, tag = "6")]
    pub parent_slot: u64,
    #[prost(string, tag = "7")]
    pub parent_blockhash: ::prost::alloc::string::String,
    #[prost(uint64, tag = "8")]
    pub executed_transaction_count: u64,
    #[prost(uint64, tag = "9")]
    pub entries_count: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct BlockMetaParser;

impl Parser for BlockMetaParser {
    type Input = SubscribeUpdateBlockMeta;
    type Output = BlockMetaUpdateProto;

    fn id(&self) -> Cow<'static, str> { "yellowstone::BlockMetaParser".into() }

    fn prefilter(&self) -> Prefilter { Prefilter::builder().block_metas().build().unwrap() }

    async fn parse(&self, block_meta: &SubscribeUpdateBlockMeta) -> ParseResult<Self::Output> {
        let rewards = block_meta.rewards.as_ref().map(|r| RewardsProto {
            rewards: r
                .rewards
                .iter()
                .map(|x| RewardProto {
                    pubkey: x.pubkey.clone(),
                    lamports: x.lamports,
                    post_balance: x.post_balance,
                    reward_type: x.reward_type,
                    commission: x.commission.clone(),
                })
                .collect(),
            num_partitions: r.num_partitions.as_ref().map(|n| n.num_partitions),
        });

        Ok(BlockMetaUpdateProto {
            slot: block_meta.slot,
            blockhash: block_meta.blockhash.clone(),
            rewards,
            block_time: block_meta.block_time.as_ref().map(|t| t.timestamp),
            block_height: block_meta.block_height.as_ref().map(|h| h.block_height),
            parent_slot: block_meta.parent_slot,
            parent_blockhash: block_meta.parent_blockhash.clone(),
            executed_transaction_count: block_meta.executed_transaction_count,
            entries_count: block_meta.entries_count,
        })
    }
}

impl ProgramParser for BlockMetaParser {
    /// "B111111111111111111111111111111111111111112"
    #[inline]
    fn program_id(&self) -> Pubkey {
        Pubkey::new([
            2, 143, 206, 223, 9, 17, 53, 163, 33, 32, 251, 255, 120, 243, 177, 49, 160, 203, 100,
            118, 223, 255, 122, 65, 91, 88, 104, 0, 0, 0, 0, 1,
        ])
    }
}
