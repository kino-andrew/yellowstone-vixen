use codama_nodes::InstructionNode;

use crate::intermediate_representation::{
    helpers::build_fields_ir, FieldIr, FieldTypeIr, LabelIr, OneofIr, OneofVariantIr, ScalarIr,
    SchemaIr, TypeIr, TypeKindIr,
};

///
/// Converts Codama `instructions` into IR messages.
///
/// For each instruction in the IDL, we generate three messages:
///
///   1) <IxName>Accounts  → holds all account pubkeys
///   2) <IxName>Args      → holds instruction arguments
///   3) <IxName>Ix        → wrapper combining accounts + args
///
/// At the end, we also create a global:
///
///   ProgramInstruction { oneof ix { ... } }
///
/// This acts as the root enum for all instructions.
///
/// ─────────────────────────────────────────────────────────────
/// Example IDL instruction:
///
/// ```text
/// instruction open_position {
///   accounts {
///     owner: pubkey,
///     position: pubkey,
///     pool: pubkey,
///   }
///
///   args {
///     collateral: u64,
///     leverage: u32,
///   }
/// }
/// ```
///
/// ─────────────────────────────────────────────────────────────
/// IR result:
///
/// 1) Accounts message
///
/// ```rust, ignore
/// TypeIr {
///   name: "OpenPositionAccounts",
///   fields: [
///     { name: "owner",    tag: 1, field_type: PubkeyBytes },
///     { name: "position", tag: 2, field_type: PubkeyBytes },
///     { name: "pool",     tag: 3, field_type: PubkeyBytes },
///   ],
///   kind: Instruction
/// }
/// ```
///
/// 2) Args message
///
/// ```rust, ignore
/// TypeIr {
///   name: "OpenPositionArgs",
///   fields: [
///     { name: "collateral", tag: 1, field_type: Uint64 },
///     { name: "leverage",   tag: 2, field_type: Uint32 },
///   ],
///   kind: Instruction
/// }
/// ```
///
/// 3) Wrapper payload message
///
/// ```rust, ignore
/// TypeIr {
///   name: "OpenPositionIx",
///   fields: [
///     { name: "accounts", tag: 1, field_type: Message(OpenPositionAccounts), optional },
///     { name: "args",     tag: 2, field_type: Message(OpenPositionArgs),     optional },
///   ],
///   kind: Instruction
/// }
/// ```
///
/// ─────────────────────────────────────────────────────────────
/// Final global instruction enum:
///
/// ```rust, ignore
/// TypeIr {
///   name: "ProgramInstruction",
///   fields: [],
///   kind: Instruction
/// }
///
/// OneofIr {
///   parent_message: "ProgramInstruction",
///   field_name: "ix",
///   variants: [
///     { tag: 1, variant_name: "OpenPositionIx", message_type: "OpenPositionIx" },
///     { tag: 2, variant_name: "SetLimitsIx",    message_type: "SetLimitsIx" },
///     ...
///   ]
/// }
/// ```
///
/// This allows a single protobuf message to represent any instruction.
///
pub fn build_instructions_schema(instructions: &[InstructionNode], ir: &mut SchemaIr) {
    for ix in instructions {
        let ix_name = crate::utils::to_pascal_case(&ix.name);

        let accounts_name = format!("{}Accounts", ix_name);
        let args_name = format!("{}Args", ix_name);
        let payload_name = format!("{}Ix", ix_name);

        // Accounts: always PubkeyBytes, singular
        let mut acct_fields = Vec::new();

        for (i, a) in ix.accounts.iter().enumerate() {
            acct_fields.push(FieldIr {
                name: crate::utils::to_snake_case(&a.name),
                tag: (i + 1) as u32,
                label: LabelIr::Singular,
                field_type: FieldTypeIr::Scalar(ScalarIr::PubkeyBytes),
            });
        }

        ir.push_unique_type(TypeIr {
            name: accounts_name.clone(),
            fields: acct_fields,
            kind: TypeKindIr::Instruction,
        });

        let arg_fields = build_fields_ir(&args_name, &ix.arguments, ir, TypeKindIr::Helper);

        ir.push_unique_type(TypeIr {
            name: args_name.clone(),
            fields: arg_fields,
            kind: TypeKindIr::Instruction,
        });

        // Payload wrapper: accounts + args optional
        ir.push_unique_type(TypeIr {
            name: payload_name.clone(),
            fields: vec![
                FieldIr {
                    name: "accounts".to_string(),
                    tag: 1,
                    label: LabelIr::Optional,
                    field_type: FieldTypeIr::Message(accounts_name),
                },
                FieldIr {
                    name: "args".to_string(),
                    tag: 2,
                    label: LabelIr::Optional,
                    field_type: FieldTypeIr::Message(args_name),
                },
            ],
            kind: TypeKindIr::Instruction,
        });
    }

    // ProgramInstruction oneof
    ir.push_unique_type(TypeIr {
        name: "ProgramInstruction".to_string(),
        fields: vec![],
        kind: TypeKindIr::Instruction,
    });

    let variants = instructions
        .iter()
        .enumerate()
        .map(|(i, ix)| {
            let ix_name = crate::utils::to_pascal_case(&ix.name);
            let payload_name = format!("{}Ix", ix_name);

            OneofVariantIr {
                tag: (i + 1) as u32,
                variant_name: payload_name.clone(),
                message_type: payload_name,
            }
        })
        .collect::<Vec<_>>();

    ir.oneofs.push(OneofIr {
        parent_message: "ProgramInstruction".to_string(),
        field_name: "ix".to_string(),
        variants,
        kind: crate::intermediate_representation::OneofKindIr::InstructionDispatch,
    });
}
