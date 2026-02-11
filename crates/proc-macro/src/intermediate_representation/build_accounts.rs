use codama_nodes::AccountNode;

use crate::intermediate_representation::{
    helpers::{build_fields_ir, unwrap_nested_struct},
    SchemaIr, TypeIr, TypeKindIr,
};

/// Converts Codama account definitions into IR messages.
///
/// Each `AccountNode` in the IDL represents a real on-chain account
///
/// Example:
///
/// IDL:
///
/// ```text
///   account BondingCurve {
///     virtual_token_reserves: u64,
///     complete: bool,
///   }
/// ``````
///
/// IR result:
///
/// ```rust,ignore
///   TypeIr {
///     name: "BondingCurve",
///     fields: [...],
///     kind: Account { len: Some(82) }
///   }
/// ``````
pub fn build_accounts(accounts: &[AccountNode], ir: &mut SchemaIr) {
    for account in accounts {
        let name = crate::utils::to_pascal_case(&account.name);

        // AccountNode.data is NestedTypeNode<StructTypeNode> in Codama.
        let struct_type_node = unwrap_nested_struct(&account.data);

        let fields = build_fields_ir(&name, &struct_type_node.fields, ir, TypeKindIr::Helper);

        ir.types.push(TypeIr {
            name,
            fields,
            kind: TypeKindIr::Account { len: account.size },
        });
    }
}
