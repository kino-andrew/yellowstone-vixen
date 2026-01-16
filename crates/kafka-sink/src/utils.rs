use yellowstone_vixen_core::instruction::InstructionUpdate;

/// Iterate through an instruction and all its inner instructions (CPIs),
/// yielding each instruction along with its path (e.g., [0], [0, 1], [0, 1, 2]).
pub fn visit_instructions_with_path(
    ix: &InstructionUpdate,
    base_index: usize,
) -> Vec<(Vec<usize>, &InstructionUpdate)> {
    let mut result = Vec::new();
    let path = vec![base_index];
    visit_recursive(ix, path, &mut result);
    result
}

fn visit_recursive<'a>(
    ix: &'a InstructionUpdate,
    path: Vec<usize>,
    result: &mut Vec<(Vec<usize>, &'a InstructionUpdate)>,
) {
    result.push((path.clone(), ix));
    for (i, inner) in ix.inner.iter().enumerate() {
        let mut inner_path = path.clone();
        inner_path.push(i);
        visit_recursive(inner, inner_path, result);
    }
}

/// Format a path as a dot-separated string (e.g., "0.1.2").
pub fn format_path(path: &[usize]) -> String {
    path.iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(".")
}

/// Generate a unique Kafka key for deduplication.
/// Format: `{signature}:{ix_index}`
/// The signature is unique per transaction, and ix_index identifies the instruction within it.
pub fn make_record_key(signature: &str, ix_index: &str) -> String {
    format!("{}:{}", signature, ix_index)
}
