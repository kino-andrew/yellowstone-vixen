use crate::intermediate_representation::{
    FieldIr, FieldTypeIr, LabelIr, ScalarIr, SchemaIr, TypeIr, TypeKindIr,
};

/// Common interface for field-like nodes from Codama (both struct fields and instruction arguments).
pub trait FieldLikeNode {
    fn name(&self) -> &codama_nodes::CamelCaseString;
    fn r#type(&self) -> &codama_nodes::TypeNode;
    fn default_value_strategy(&self) -> Option<codama_nodes::DefaultValueStrategy>;
}

impl FieldLikeNode for codama_nodes::StructFieldTypeNode {
    fn name(&self) -> &codama_nodes::CamelCaseString { &self.name }

    fn r#type(&self) -> &codama_nodes::TypeNode { &self.r#type }

    fn default_value_strategy(&self) -> Option<codama_nodes::DefaultValueStrategy> {
        self.default_value_strategy
    }
}

impl FieldLikeNode for codama_nodes::InstructionArgumentNode {
    fn name(&self) -> &codama_nodes::CamelCaseString { &self.name }

    fn r#type(&self) -> &codama_nodes::TypeNode { &self.r#type }

    fn default_value_strategy(&self) -> Option<codama_nodes::DefaultValueStrategy> {
        self.default_value_strategy
    }
}

///
/// Build IR fields from any field-like nodes, handling inline tuple definitions by materializing new messages for them.
///
/// E.g. for a struct like:
///
/// ```rust, ignore
///   struct MyStruct {
///     field1: u64,
///     field2: (u64, String),
///   }
/// ```
///
/// We materialize a new message `MyStructField2Tuple` with fields `item_0: u64` and `item_1: String`, and the original struct's IR fields become:
///
///   field1: uint64
///   field2: MyStructField2Tuple
///
pub fn build_fields_ir(
    parent_name: &str,
    fields: &[impl FieldLikeNode],
    ir: &mut SchemaIr,
    tuple_msg_kind: TypeKindIr,
) -> Vec<FieldIr> {
    let mut out = Vec::new();
    let mut tag: u32 = 0;

    for f in fields {
        if f.default_value_strategy() == Some(codama_nodes::DefaultValueStrategy::Omitted) {
            continue;
        }

        tag += 1;

        let name = crate::utils::to_snake_case(f.name());

        match f.r#type() {
            codama_nodes::TypeNode::Tuple(tuple) => {
                // Inline tuple => synth message Parent + Field + Tuple, and field is Option<Msg>
                let tuple_name = format!(
                    "{}{}Tuple",
                    parent_name,
                    crate::utils::to_pascal_case(&name)
                );

                // Materialize the tuple message now (and recurse for nested tuple elements via build_fields below)
                let mut tuple_fields = Vec::new();

                for (j, item) in tuple.items.iter().enumerate() {
                    let item_tag = (j + 1) as u32;
                    let item_name = format!("item_{}", j);

                    match item {
                        codama_nodes::TypeNode::Tuple(inner_tuple) => {
                            let inner_tuple_name = format!(
                                "{}{}Tuple",
                                tuple_name,
                                crate::utils::to_pascal_case(&item_name)
                            );

                            // recurse: create inner tuple message first
                            let mut inner_fields = Vec::new();

                            for (k, inner_item) in inner_tuple.items.iter().enumerate() {
                                let (label, field_type) = map_type_with_label(inner_item);

                                inner_fields.push(FieldIr {
                                    name: format!("item_{}", k),
                                    tag: (k + 1) as u32,
                                    label,
                                    field_type,
                                });
                            }

                            ir.push_unique_type(TypeIr {
                                name: inner_tuple_name.clone(),
                                fields: inner_fields,
                                kind: tuple_msg_kind.clone(),
                            });

                            tuple_fields.push(FieldIr {
                                name: item_name,
                                tag: item_tag,
                                label: LabelIr::Optional,
                                field_type: FieldTypeIr::Message(inner_tuple_name),
                            });
                        },

                        other => {
                            let (label, field_type) = map_type_with_label(other);

                            tuple_fields.push(FieldIr {
                                name: item_name,
                                tag: item_tag,
                                label,
                                field_type,
                            });
                        },
                    }
                }

                ir.push_unique_type(TypeIr {
                    name: tuple_name.clone(),
                    fields: tuple_fields,
                    kind: tuple_msg_kind.clone(),
                });

                out.push(FieldIr {
                    name,
                    tag,
                    label: LabelIr::Optional,
                    field_type: FieldTypeIr::Message(tuple_name),
                });
            },

            other => {
                let (label, field_type) = map_type_with_label(other);

                out.push(FieldIr {
                    name,
                    tag,
                    label,
                    field_type,
                });
            },
        }
    }

    out
}

pub fn map_type_with_label(type_node: &codama_nodes::TypeNode) -> (LabelIr, FieldTypeIr) {
    use codama_nodes::TypeNode as T;

    match type_node {
        T::Option(option) => (LabelIr::Optional, map_type(&option.item)),
        T::Array(array) => (LabelIr::Repeated, map_type(&array.item)),
        _ => (LabelIr::Singular, map_type(type_node)),
    }
}

// Helper to map codama type nodes to our IR types
fn map_type(t: &codama_nodes::TypeNode) -> FieldTypeIr {
    use codama_nodes::TypeNode as T;

    match t {
        T::String(_) | T::SizePrefix(_) => FieldTypeIr::Scalar(ScalarIr::String),
        T::Bytes(_) => FieldTypeIr::Scalar(ScalarIr::Bytes),
        T::PublicKey(_) => FieldTypeIr::Scalar(ScalarIr::PubkeyBytes),
        T::Boolean(_) => FieldTypeIr::Scalar(ScalarIr::Bool),

        T::Number(n) => match n.format {
            codama_nodes::NumberFormat::U8
            | codama_nodes::NumberFormat::U16
            | codama_nodes::NumberFormat::U32
            | codama_nodes::NumberFormat::ShortU16 => FieldTypeIr::Scalar(ScalarIr::Uint32),

            codama_nodes::NumberFormat::U64 => FieldTypeIr::Scalar(ScalarIr::Uint64),

            codama_nodes::NumberFormat::I8
            | codama_nodes::NumberFormat::I16
            | codama_nodes::NumberFormat::I32 => FieldTypeIr::Scalar(ScalarIr::Int32),

            codama_nodes::NumberFormat::I64 => FieldTypeIr::Scalar(ScalarIr::Int64),

            codama_nodes::NumberFormat::F32 => FieldTypeIr::Scalar(ScalarIr::Float),
            codama_nodes::NumberFormat::F64 => FieldTypeIr::Scalar(ScalarIr::Double),

            codama_nodes::NumberFormat::U128 | codama_nodes::NumberFormat::I128 => {
                FieldTypeIr::Scalar(ScalarIr::Bytes)
            },
        },

        T::Link(link) => FieldTypeIr::Message(crate::utils::to_pascal_case(&link.name)),

        // FixedSize bytes/string => bytes
        T::FixedSize(node) => match *node.r#type {
            T::Bytes(_) | T::String(_) => FieldTypeIr::Scalar(ScalarIr::Bytes),
            _ => panic!("map_type not implemented for FixedSize {:?}", node.r#type),
        },

        // Tuple must be handled at call-site for naming/materialization.
        T::Tuple(_) => panic!("map_type() called with Tuple; handle tuple wrapper at call site"),

        T::Option(o) => map_type(&o.item),
        T::Array(a) => map_type(&a.item),

        other => panic!("map_type not implemented for {:?}", other),
    }
}

/// Unwrap Codama's NestedTypeNode wrappers to get to the underlying StructTypeNode for accounts.
///
/// We expect the account data to always be a struct, but Codama wraps it in various ways (e.g. for fixed-size arrays) so we need to unwrap those.
pub fn unwrap_nested_struct(
    node: &codama_nodes::NestedTypeNode<codama_nodes::StructTypeNode>,
) -> &codama_nodes::StructTypeNode {
    use codama_nodes::NestedTypeNode as N;

    match node {
        N::Value(v) => v,

        N::FixedSize(w) => unwrap_nested_struct(&w.r#type),
        N::HiddenPrefix(w) => unwrap_nested_struct(&w.r#type),
        N::HiddenSuffix(w) => unwrap_nested_struct(&w.r#type),
        N::SizePrefix(w) => unwrap_nested_struct(&w.r#type),

        // If Codama adds more wrappers later, we want to fail loudly.
        other => panic!("Unsupported AccountNode.data wrapper: {:?}", other),
    }
}
