use base64::{engine::general_purpose::STANDARD, Engine};
use codama_nodes::{
    CamelCaseString, DiscriminatorNode, InstructionInputValueNode, Number, TypeNode, ValueNode,
};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

/**
 * Build the *instruction parser* for a program.
 */
pub fn parser(
    program_name_camel: &CamelCaseString,
    instructions: &[codama_nodes::InstructionNode],
) -> TokenStream {
    let program_name = crate::utils::to_pascal_case(program_name_camel);

    let instruction_parser_id = format!("{}::InstructionParser", program_name);

    let wrapper_ident = format_ident!("ProgramInstruction");
    let oneof_mod_ident = format_ident!("program_instruction_oneof");
    let oneof_ident = format_ident!("Ix");

    let instruction_matches = instructions.iter().filter_map(|instruction| {
        let discriminator = instruction.discriminators.first()?;

        let ix_name_pascal = crate::utils::to_pascal_case(&instruction.name);

        let payload_ident = format_ident!("{}Ix", ix_name_pascal);
        let accounts_ident = format_ident!("{}Accounts", ix_name_pascal);

        let accounts_fields = instruction.accounts.iter().enumerate().map(|(idx, account)| {
            let field_name = format_ident!("{}", crate::utils::to_snake_case(&account.name));
            let error_msg = format!("Account does not exist at index {idx}");

            quote! { #field_name: accounts.get(#idx).ok_or(ParseError::from(#error_msg))?.to_vec() }
        });

        let accounts_value = quote! {
            #accounts_ident { #(#accounts_fields),* }
        };

        Some(match discriminator {
            // Handle 1-byte discriminator (simple programs / custom formats)
            DiscriminatorNode::Constant(node) => {
                let offset = node.offset;

                // Skip if discriminator isn't an unsigned integer
                let ValueNode::Number(nn) = node.constant.value.as_ref() else {
                    return None;
                };
                let Number::UnsignedInteger(value) = nn.number else {
                    return None;
                };

                quote! {
                    if let Some(d) = data.get(#offset) {
                        if d == #value {
                            return Ok(#wrapper_ident {
                                ix: ::core::option::Option::Some(
                                    #oneof_mod_ident::#oneof_ident::#payload_ident(
                                        #payload_ident {
                                            accounts: ::core::option::Option::Some(#accounts_value),
                                            args: ::core::option::Option::Some(data.to_vec()),
                                        }
                                    )
                                ),
                            });
                        }
                    }
                }
            }

            // Handle multi-byte discriminator stored in a field (Anchor-style 8 bytes)
            DiscriminatorNode::Field(node) => {
                let offset = node.offset;

                // Find the discriminator argument by name
                let Some(field) = instruction.arguments.iter().find(|f| f.name == node.name) else {
                    return None;
                };

                // Discriminator field must be fixed-size
                let TypeNode::FixedSize(fixed_size_node) = &field.r#type else {
                    return None;
                };
                let size = fixed_size_node.size;

                // Must have a byte default value we can compare against
                let Some(default_value) = field.default_value.as_ref() else {
                    return None;
                };

                let InstructionInputValueNode::Bytes(bytes) = default_value else {
                    return None;
                };

                // Decode expected discriminator bytes
                let discriminator: Vec<u8> = match bytes.encoding {
                    codama_nodes::BytesEncoding::Base16 => {
                        hex::decode(&bytes.data).expect("Failed to decode base16 (hex) bytes")
                    }
                    codama_nodes::BytesEncoding::Base58 => bs58::decode(&bytes.data)
                        .into_vec()
                        .expect("Failed to decode base58 bytes"),
                    codama_nodes::BytesEncoding::Base64 => STANDARD
                        .decode(&bytes.data)
                        .expect("Failed to decode base64 bytes"),
                    codama_nodes::BytesEncoding::Utf8 => bytes.data.as_bytes().to_vec(),
                };

                let end = offset + size;

                quote! {
                    if let Some(slice) = data.get(#offset..#end) {
                        if slice == &[#(#discriminator),*] {
                            return Ok(#wrapper_ident {
                                ix: ::core::option::Option::Some(
                                    #oneof_mod_ident::#oneof_ident::#payload_ident(
                                        #payload_ident {
                                            accounts: ::core::option::Option::Some(#accounts_value),
                                            args: ::core::option::Option::Some(data[#end..].to_vec()),
                                        }
                                    )
                                ),
                            });
                        }
                    }
                }
            }

            // Handle discriminator by total size only (e.g the account is 558 Bytes long)
            DiscriminatorNode::Size(node) => {
                let size = node.size;

                quote! {
                    if data.len() == #size {
                        return Ok(#wrapper_ident {
                            ix: ::core::option::Option::Some(
                                #oneof_mod_ident::#oneof_ident::#payload_ident(
                                    #payload_ident {
                                        accounts: ::core::option::Option::Some(#accounts_value),
                                        args: ::core::option::Option::Some(data.to_vec()),
                                    }
                                )
                            ),
                        });
                    }
                }
            }
        })
    });

    quote! {
        #[derive(Debug, Copy, Clone)]
        pub struct InstructionParser;

        impl Parser for InstructionParser {
            type Input = instruction::InstructionUpdate;
            type Output = #wrapper_ident;

            fn id(&self) -> std::borrow::Cow<'static, str> {
                #instruction_parser_id.into()
            }

            fn prefilter(&self) -> Prefilter {
                Prefilter::builder()
                    .transaction_accounts([ID_BYTES])
                    .build()
                    .unwrap()
            }

            async fn parse(
                &self,
                ix_update: &instruction::InstructionUpdate,
            ) -> ParseResult<Self::Output> {
                let data = &ix_update.data;
                let accounts = &ix_update.accounts;

                #(#instruction_matches)*

                Err(ParseError::from("Invalid Instruction discriminator".to_owned()))
            }
        }
    }
}
