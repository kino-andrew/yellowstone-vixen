use borsh::BorshDeserialize;
use spl_stake_pool::instruction::StakePoolInstruction;
use yellowstone_vixen_core::{
    instruction::InstructionUpdate, ParseError, ParseResult, Parser, Prefilter, ProgramParser,
};
use yellowstone_vixen_parser::check_min_accounts_req;

use crate::instructions::*;

#[derive(Copy, Clone)]
pub struct InstructionParser;

impl Parser for InstructionParser {
    type Input = InstructionUpdate;
    type Output = StakePoolProgram;

    fn id(&self) -> std::borrow::Cow<'static, str> { "StakePool::InstructionParser".into() }

    fn prefilter(&self) -> Prefilter {
        Prefilter::builder()
            .transaction_accounts([spl_stake_pool::id()])
            .build()
            .unwrap()
    }

    async fn parse(&self, ix_update: &InstructionUpdate) -> ParseResult<Self::Output> {
        if ix_update.program.equals_ref(spl_stake_pool::id()) {
            InstructionParser::parse_impl(ix_update)
        } else {
            Err(ParseError::Filtered)
        }
    }
}

impl ProgramParser for InstructionParser {
    #[inline]
    fn program_id(&self) -> yellowstone_vixen_core::Pubkey {
        spl_stake_pool::id().to_bytes().into()
    }
}

impl InstructionParser {
    pub(crate) fn parse_impl(ix: &InstructionUpdate) -> ParseResult<StakePoolProgram> {
        let ix_type = StakePoolInstruction::try_from_slice(ix.data.as_slice())?;
        let accounts_len = ix.accounts.len();

        use stake_pool_program::Instruction as Out;

        let instruction = match ix_type {
            StakePoolInstruction::Initialize {
                fee,
                withdrawal_fee,
                deposit_fee,
                referral_fee,
                max_validators,
            } => {
                check_min_accounts_req(accounts_len, 9)?;

                Out::Initialize(InitializeInstruction {
                    accounts: Some(InitializeAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        manager: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        staker: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        stake_pool_withdraw_authority: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        validator_list: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        reserve_stake: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        pool_mint: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                        manager_pool_account: crate::PublicKey { value: ix.accounts[7].into_bytes().to_vec() },
                        token_program: crate::PublicKey { value: ix.accounts[8].into_bytes().to_vec() },
                        deposit_authority: ix.accounts.get(9).map(|a| crate::PublicKey { value: a.into_bytes().to_vec() }),
                    }),
                    args: Some(InitializeArgs {
                        fee: Some(fee_to_proto(fee)),
                        withdrawal_fee: Some(fee_to_proto(withdrawal_fee)),
                        deposit_fee: Some(fee_to_proto(deposit_fee)),
                        referral_fee: referral_fee as u32,
                        max_validators,
                    }),
                })
            },

            StakePoolInstruction::AddValidatorToPool(raw_validator_seed) => {
                check_min_accounts_req(accounts_len, 13)?;

                Out::AddValidatorToPool(AddValidatorToPoolInstruction {
                    accounts: Some(AddValidatorToPoolAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        staker: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        funder: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        stake_pool_withdraw: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        validator_list: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        stake: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        validator: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                        rent: crate::PublicKey { value: ix.accounts[7].into_bytes().to_vec() },
                        clock: crate::PublicKey { value: ix.accounts[8].into_bytes().to_vec() },
                        sysvar_stake_history: crate::PublicKey { value: ix.accounts[9].into_bytes().to_vec() },
                        stake_config: crate::PublicKey { value: ix.accounts[10].into_bytes().to_vec() },
                        system_program: crate::PublicKey { value: ix.accounts[11].into_bytes().to_vec() },
                        stake_program: crate::PublicKey { value: ix.accounts[12].into_bytes().to_vec() },
                    }),
                    args: Some(AddValidatorToPoolArgs { raw_validator_seed }),
                })
            },

            StakePoolInstruction::RemoveValidatorFromPool => {
                check_min_accounts_req(accounts_len, 8)?;

                Out::RemoveValidatorFromPool(RemoveValidatorFromPoolInstruction {
                    accounts: Some(RemoveValidatorFromPoolAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        staker: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        stake_pool_withdraw: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        validator_list: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        stake_account: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        transient_stake_account: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        clock: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                        stake_program: crate::PublicKey { value: ix.accounts[7].into_bytes().to_vec() },
                    }),
                })
            },

            StakePoolInstruction::DecreaseValidatorStake {
                lamports,
                transient_stake_seed,
            } => {
                check_min_accounts_req(accounts_len, 10)?;

                Out::DecreaseValidatorStake(DecreaseValidatorStakeInstruction {
                    accounts: Some(DecreaseValidatorStakeAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        staker: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        stake_pool_withdraw_authority: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        validator_list: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        validator_stake: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        transient_stake: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        clock: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                        rent: crate::PublicKey { value: ix.accounts[7].into_bytes().to_vec() },
                        system_program: crate::PublicKey { value: ix.accounts[8].into_bytes().to_vec() },
                        stake_program: crate::PublicKey { value: ix.accounts[9].into_bytes().to_vec() },
                    }),
                    args: Some(DecreaseValidatorStakeArgs {
                        lamports,
                        transient_stake_seed,
                    }),
                })
            },

            StakePoolInstruction::IncreaseValidatorStake {
                lamports,
                transient_stake_seed,
            } => {
                check_min_accounts_req(accounts_len, 14)?;

                Out::IncreaseValidatorStake(IncreaseValidatorStakeInstruction {
                    accounts: Some(IncreaseValidatorStakeAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        staker: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        stake_pool_withdraw_authority: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        validator_list: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        reserve_stake: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        transient_stake: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        validator_stake: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                        validator: crate::PublicKey { value: ix.accounts[7].into_bytes().to_vec() },
                        clock: crate::PublicKey { value: ix.accounts[8].into_bytes().to_vec() },
                        rent: crate::PublicKey { value: ix.accounts[9].into_bytes().to_vec() },
                        sysvar_stake_history: crate::PublicKey { value: ix.accounts[10].into_bytes().to_vec() },
                        stake_config: crate::PublicKey { value: ix.accounts[11].into_bytes().to_vec() },
                        system_program: crate::PublicKey { value: ix.accounts[12].into_bytes().to_vec() },
                        stake_program: crate::PublicKey { value: ix.accounts[13].into_bytes().to_vec() },
                    }),
                    args: Some(IncreaseValidatorStakeArgs {
                        lamports,
                        transient_stake_seed,
                    }),
                })
            },

            StakePoolInstruction::SetPreferredValidator {
                validator_type,
                validator_vote_address,
            } => {
                check_min_accounts_req(accounts_len, 3)?;

                Out::SetPreferredValidator(SetPreferredValidatorInstruction {
                    accounts: Some(SetPreferredValidatorAccounts {
                        stake_pool_address: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        staker: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        validator_list_address: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                    }),
                    args: Some(SetPreferredValidatorArgs {
                        validator_type: preferred_validator_type_to_proto(validator_type) as i32,
                        // real Pubkey, not an account meta
                        validator_vote_address: validator_vote_address
                            .map(|p| crate::PublicKey { value: p.to_bytes().to_vec() }),
                    }),
                })
            },

            StakePoolInstruction::UpdateValidatorListBalance {
                start_index,
                no_merge,
            } => {
                check_min_accounts_req(accounts_len, 7)?;

                Out::UpdateValidatorListBalance(UpdateValidatorListBalanceInstruction {
                    accounts: Some(UpdateValidatorListBalanceAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        stake_pool_withdraw_authority: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        validator_list_address: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        reserve_stake: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        clock: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        sysvar_stake_history: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        stake_program: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                    }),
                    args: Some(UpdateValidatorListBalanceArgs {
                        start_index,
                        no_merge,
                    }),
                })
            },

            StakePoolInstruction::UpdateStakePoolBalance => {
                check_min_accounts_req(accounts_len, 7)?;

                Out::UpdateStakePoolBalance(UpdateStakePoolBalanceInstruction {
                    accounts: Some(UpdateStakePoolBalanceAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        withdraw_authority: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        validator_list_storage: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        reserve_stake: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        manager_fee_account: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        stake_pool_mint: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        token_program: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                    }),
                })
            },

            StakePoolInstruction::CleanupRemovedValidatorEntries => {
                check_min_accounts_req(accounts_len, 2)?;

                Out::CleanupRemovedValidatorEntries(CleanupRemovedValidatorEntriesInstruction {
                    accounts: Some(CleanupRemovedValidatorEntriesAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        validator_list_storage: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                    }),
                })
            },

            StakePoolInstruction::DepositStake => {
                check_min_accounts_req(accounts_len, 15)?;

                Out::DepositStake(DepositStakeInstruction {
                    accounts: Some(DepositStakeAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        validator_list_storage: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        stake_pool_deposit_authority: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        stake_pool_withdraw_authority: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        deposit_stake_address: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        validator_stake_account: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        reserve_stake_account: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                        pool_tokens_to: crate::PublicKey { value: ix.accounts[7].into_bytes().to_vec() },
                        manager_fee_account: crate::PublicKey { value: ix.accounts[8].into_bytes().to_vec() },
                        referrer_pool_tokens_account: crate::PublicKey { value: ix.accounts[9].into_bytes().to_vec() },
                        pool_mint: crate::PublicKey { value: ix.accounts[10].into_bytes().to_vec() },
                        clock: crate::PublicKey { value: ix.accounts[11].into_bytes().to_vec() },
                        sysvar_stake_history: crate::PublicKey { value: ix.accounts[12].into_bytes().to_vec() },
                        token_program: crate::PublicKey { value: ix.accounts[13].into_bytes().to_vec() },
                        stake_program: crate::PublicKey { value: ix.accounts[14].into_bytes().to_vec() },
                    }),
                })
            },

            StakePoolInstruction::WithdrawStake(amount) => {
                check_min_accounts_req(accounts_len, 13)?;

                Out::WithdrawStake(WithdrawStakeInstruction {
                    accounts: Some(WithdrawStakeAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        validator_list_storage: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        stake_pool_withdraw: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        stake_to_split: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        stake_to_receive: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        user_stake_authority: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        user_transfer_authority: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                        user_pool_token_account: crate::PublicKey { value: ix.accounts[7].into_bytes().to_vec() },
                        manager_fee_account: crate::PublicKey { value: ix.accounts[8].into_bytes().to_vec() },
                        pool_mint: crate::PublicKey { value: ix.accounts[9].into_bytes().to_vec() },
                        clock: crate::PublicKey { value: ix.accounts[10].into_bytes().to_vec() },
                        token_program: crate::PublicKey { value: ix.accounts[11].into_bytes().to_vec() },
                        stake_program: crate::PublicKey { value: ix.accounts[12].into_bytes().to_vec() },
                    }),
                    args: Some(WithdrawStakeArgs { amount }),
                })
            },

            StakePoolInstruction::SetManager => {
                check_min_accounts_req(accounts_len, 4)?;

                Out::SetManager(SetManagerInstruction {
                    accounts: Some(SetManagerAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        manager: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        new_manager: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        new_fee_receiver: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                    }),
                })
            },

            StakePoolInstruction::SetFee { fee } => {
                check_min_accounts_req(accounts_len, 2)?;

                Out::SetFee(SetFeeInstruction {
                    accounts: Some(SetFeeAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        manager: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                    }),
                    args: Some(SetFeeArgs {
                        fee: Some(fee_type_to_proto(fee)),
                    }),
                })
            },

            StakePoolInstruction::SetStaker => {
                check_min_accounts_req(accounts_len, 3)?;

                Out::SetStaker(SetStakerInstruction {
                    accounts: Some(SetStakerAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        set_staker_authority: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        new_staker: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                    }),
                })
            },

            StakePoolInstruction::DepositSol(amount) => {
                check_min_accounts_req(accounts_len, 10)?;

                Out::DepositSol(DepositSolInstruction {
                    accounts: Some(DepositSolAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        stake_pool_withdraw_authority: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        reserve_stake_account: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        lamports_from: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        pool_tokens_to: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        manager_fee_account: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        referrer_pool_tokens_account: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                        pool_mint: crate::PublicKey { value: ix.accounts[7].into_bytes().to_vec() },
                        system_program: crate::PublicKey { value: ix.accounts[8].into_bytes().to_vec() },
                        token_program: crate::PublicKey { value: ix.accounts[9].into_bytes().to_vec() },
                        deposit_authority: ix.accounts.get(10).map(|a| crate::PublicKey { value: a.into_bytes().to_vec() }),
                    }),
                    args: Some(DepositSolArgs { amount }),
                })
            },

            StakePoolInstruction::SetFundingAuthority(funding_type) => {
                check_min_accounts_req(accounts_len, 2)?;

                Out::SetFundingAuthority(SetFundingAuthorityInstruction {
                    accounts: Some(SetFundingAuthorityAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        manager: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        auth: ix.accounts.get(2).map(|a| crate::PublicKey { value: a.into_bytes().to_vec() }),
                    }),
                    args: Some(SetFundingAuthorityArgs {
                        funding_type: funding_type_to_proto(funding_type) as i32,
                    }),
                })
            },

            StakePoolInstruction::WithdrawSol(amount) => {
                check_min_accounts_req(accounts_len, 12)?;

                Out::WithdrawSol(WithdrawSolInstruction {
                    accounts: Some(WithdrawSolAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        stake_pool_withdraw_authority: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        user_transfer_authority: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        pool_tokens_from: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        reserve_stake_account: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        lamports_to: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        manager_fee_account: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                        pool_mint: crate::PublicKey { value: ix.accounts[7].into_bytes().to_vec() },
                        clock: crate::PublicKey { value: ix.accounts[8].into_bytes().to_vec() },
                        sysvar_stake_history: crate::PublicKey { value: ix.accounts[9].into_bytes().to_vec() },
                        stake_program: crate::PublicKey { value: ix.accounts[10].into_bytes().to_vec() },
                        token_program: crate::PublicKey { value: ix.accounts[11].into_bytes().to_vec() },
                        sol_withdraw_authority: ix
                            .accounts
                            .get(12)
                            .map(|a| crate::PublicKey { value: a.into_bytes().to_vec() }),
                    }),
                    args: Some(WithdrawSolArgs { amount }),
                })
            },

            StakePoolInstruction::CreateTokenMetadata { name, symbol, uri } => {
                check_min_accounts_req(accounts_len, 8)?;

                Out::CreateTokenMetadata(CreateTokenMetadataInstruction {
                    accounts: Some(CreateTokenMetadataAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        manager: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        stake_pool_withdraw_authority: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        pool_mint: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        payer: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        token_metadata: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        mpl_token_metadata: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                        system_program: crate::PublicKey { value: ix.accounts[7].into_bytes().to_vec() },
                    }),
                    args: Some(CreateTokenMetadataArgs { name, symbol, uri }),
                })
            },

            StakePoolInstruction::UpdateTokenMetadata { name, symbol, uri } => {
                check_min_accounts_req(accounts_len, 5)?;

                Out::UpdateTokenMetadata(UpdateTokenMetadataInstruction {
                    accounts: Some(UpdateTokenMetadataAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        manager: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        stake_pool_withdraw_authority: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        token_metadata: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        mpl_token_metadata: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                    }),
                    args: Some(UpdateTokenMetadataArgs { name, symbol, uri }),
                })
            },

            StakePoolInstruction::IncreaseAdditionalValidatorStake {
                lamports,
                transient_stake_seed,
                ephemeral_stake_seed,
            } => {
                check_min_accounts_req(accounts_len, 14)?;

                Out::IncreaseAdditionalValidatorStake(IncreaseAdditionalValidatorStakeInstruction {
                    accounts: Some(IncreaseAdditionalValidatorStakeAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        staker: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        stake_pool_withdraw_authority: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        validator_list: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        reserve_stake: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        ephemeral_stake: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        transient_stake: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                        validator_stake: crate::PublicKey { value: ix.accounts[7].into_bytes().to_vec() },
                        validator: crate::PublicKey { value: ix.accounts[8].into_bytes().to_vec() },
                        clock: crate::PublicKey { value: ix.accounts[9].into_bytes().to_vec() },
                        stake_history: crate::PublicKey { value: ix.accounts[10].into_bytes().to_vec() },
                        stake_config: crate::PublicKey { value: ix.accounts[11].into_bytes().to_vec() },
                        system_program: crate::PublicKey { value: ix.accounts[12].into_bytes().to_vec() },
                        stake_program: crate::PublicKey { value: ix.accounts[13].into_bytes().to_vec() },
                    }),
                    args: Some(IncreaseAdditionalValidatorStakeArgs {
                        lamports,
                        transient_stake_seed,
                        ephemeral_stake_seed,
                    }),
                })
            },

            StakePoolInstruction::DecreaseAdditionalValidatorStake {
                lamports,
                transient_stake_seed,
                ephemeral_stake_seed,
            } => {
                check_min_accounts_req(accounts_len, 12)?;

                Out::DecreaseAdditionalValidatorStake(DecreaseAdditionalValidatorStakeInstruction {
                    accounts: Some(DecreaseAdditionalValidatorStakeAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        staker: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        stake_pool_withdraw_authority: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        validator_list: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        reserve_stake: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        validator_stake: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        ephemeral_stake: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                        transient_stake: crate::PublicKey { value: ix.accounts[7].into_bytes().to_vec() },
                        clock: crate::PublicKey { value: ix.accounts[8].into_bytes().to_vec() },
                        stake_history: crate::PublicKey { value: ix.accounts[9].into_bytes().to_vec() },
                        system_program: crate::PublicKey { value: ix.accounts[10].into_bytes().to_vec() },
                        stake_program: crate::PublicKey { value: ix.accounts[11].into_bytes().to_vec() },
                    }),
                    args: Some(DecreaseAdditionalValidatorStakeArgs {
                        lamports,
                        transient_stake_seed,
                        ephemeral_stake_seed,
                    }),
                })
            },

            StakePoolInstruction::DecreaseValidatorStakeWithReserve {
                lamports,
                transient_stake_seed,
            } => {
                check_min_accounts_req(accounts_len, 11)?;

                Out::DecreaseValidatorStakeWithReserve(
                    DecreaseValidatorStakeWithReserveInstruction {
                        accounts: Some(DecreaseValidatorStakeWithReserveAccounts {
                            stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                            staker: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                            stake_pool_withdraw_authority: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                            validator_list: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                            reserve_stake: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                            validator_stake: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                            transient_stake: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                            clock: crate::PublicKey { value: ix.accounts[7].into_bytes().to_vec() },
                            stake_history: crate::PublicKey { value: ix.accounts[8].into_bytes().to_vec() },
                            system_program: crate::PublicKey { value: ix.accounts[9].into_bytes().to_vec() },
                            stake_program: crate::PublicKey { value: ix.accounts[10].into_bytes().to_vec() },
                        }),
                        args: Some(DecreaseValidatorStakeWithReserveArgs {
                            lamports,
                            transient_stake_seed,
                        }),
                    },
                )
            },

            StakePoolInstruction::DepositStakeWithSlippage {
                minimum_pool_tokens_out,
            } => {
                check_min_accounts_req(accounts_len, 15)?;

                Out::DepositStakeWithSlippage(DepositStakeWithSlippageInstruction {
                    accounts: Some(DepositStakeWithSlippageAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        validator_list_storage: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        stake_pool_deposit_authority: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        stake_pool_withdraw_authority: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        deposit_stake_address: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        validator_stake_account: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        reserve_stake_account: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                        pool_tokens_to: crate::PublicKey { value: ix.accounts[7].into_bytes().to_vec() },
                        manager_fee_account: crate::PublicKey { value: ix.accounts[8].into_bytes().to_vec() },
                        referrer_pool_tokens_account: crate::PublicKey { value: ix.accounts[9].into_bytes().to_vec() },
                        pool_mint: crate::PublicKey { value: ix.accounts[10].into_bytes().to_vec() },
                        clock: crate::PublicKey { value: ix.accounts[11].into_bytes().to_vec() },
                        sysvar_stake_history: crate::PublicKey { value: ix.accounts[12].into_bytes().to_vec() },
                        token_program: crate::PublicKey { value: ix.accounts[13].into_bytes().to_vec() },
                        stake_program: crate::PublicKey { value: ix.accounts[14].into_bytes().to_vec() },
                    }),
                    args: Some(DepositStakeWithSlippageArgs {
                        minimum_pool_tokens_out,
                    }),
                })
            },

            StakePoolInstruction::WithdrawStakeWithSlippage {
                pool_tokens_in,
                minimum_lamports_out,
            } => {
                check_min_accounts_req(accounts_len, 13)?;

                Out::WithdrawStakeWithSlippage(WithdrawStakeWithSlippageInstruction {
                    accounts: Some(WithdrawStakeWithSlippageAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        validator_list_storage: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        stake_pool_withdraw: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        stake_to_split: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        stake_to_receive: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        user_stake_authority: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        user_transfer_authority: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                        user_pool_token_account: crate::PublicKey { value: ix.accounts[7].into_bytes().to_vec() },
                        manager_fee_account: crate::PublicKey { value: ix.accounts[8].into_bytes().to_vec() },
                        pool_mint: crate::PublicKey { value: ix.accounts[9].into_bytes().to_vec() },
                        clock: crate::PublicKey { value: ix.accounts[10].into_bytes().to_vec() },
                        token_program: crate::PublicKey { value: ix.accounts[11].into_bytes().to_vec() },
                        stake_program: crate::PublicKey { value: ix.accounts[12].into_bytes().to_vec() },
                    }),
                    args: Some(WithdrawStakeWithSlippageArgs {
                        pool_tokens_in,
                        minimum_lamports_out,
                    }),
                })
            },

            StakePoolInstruction::DepositSolWithSlippage {
                lamports_in,
                minimum_pool_tokens_out,
            } => {
                check_min_accounts_req(accounts_len, 10)?;

                Out::DepositSolWithSlippage(DepositSolWithSlippageInstruction {
                    accounts: Some(DepositSolWithSlippageAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        stake_pool_withdraw_authority: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        reserve_stake_account: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        lamports_from: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        pool_tokens_to: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        manager_fee_account: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        referrer_pool_tokens_account: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                        pool_mint: crate::PublicKey { value: ix.accounts[7].into_bytes().to_vec() },
                        system_program: crate::PublicKey { value: ix.accounts[8].into_bytes().to_vec() },
                        token_program: crate::PublicKey { value: ix.accounts[9].into_bytes().to_vec() },
                        deposit_authority: ix.accounts.get(10).map(|a| crate::PublicKey { value: a.into_bytes().to_vec() }),
                    }),
                    args: Some(DepositSolWithSlippageArgs {
                        lamports_in,
                        minimum_pool_tokens_out,
                    }),
                })
            },

            StakePoolInstruction::WithdrawSolWithSlippage {
                pool_tokens_in,
                minimum_lamports_out,
            } => {
                check_min_accounts_req(accounts_len, 12)?;

                Out::WithdrawSolWithSlippage(WithdrawSolWithSlippageInstruction {
                    accounts: Some(WithdrawSolWithSlippageAccounts {
                        stake_pool: crate::PublicKey { value: ix.accounts[0].into_bytes().to_vec() },
                        stake_pool_withdraw_authority: crate::PublicKey { value: ix.accounts[1].into_bytes().to_vec() },
                        user_transfer_authority: crate::PublicKey { value: ix.accounts[2].into_bytes().to_vec() },
                        pool_tokens_from: crate::PublicKey { value: ix.accounts[3].into_bytes().to_vec() },
                        reserve_stake_account: crate::PublicKey { value: ix.accounts[4].into_bytes().to_vec() },
                        lamports_to: crate::PublicKey { value: ix.accounts[5].into_bytes().to_vec() },
                        manager_fee_account: crate::PublicKey { value: ix.accounts[6].into_bytes().to_vec() },
                        pool_mint: crate::PublicKey { value: ix.accounts[7].into_bytes().to_vec() },
                        clock: crate::PublicKey { value: ix.accounts[8].into_bytes().to_vec() },
                        sysvar_stake_history: crate::PublicKey { value: ix.accounts[9].into_bytes().to_vec() },
                        stake_program: crate::PublicKey { value: ix.accounts[10].into_bytes().to_vec() },
                        token_program: crate::PublicKey { value: ix.accounts[11].into_bytes().to_vec() },
                        sol_withdraw_authority: ix
                            .accounts
                            .get(12)
                            .map(|a| crate::PublicKey { value: a.into_bytes().to_vec() }),
                    }),
                    args: Some(WithdrawSolWithSlippageArgs {
                        pool_tokens_in,
                        minimum_lamports_out,
                    }),
                })
            },

            _ => {
                return Err(ParseError::from(
                    "Invalid Instruction discriminator".to_owned(),
                ));
            },
        };

        Ok(StakePoolProgram {
            instruction: Some(instruction),
        })
    }
}
