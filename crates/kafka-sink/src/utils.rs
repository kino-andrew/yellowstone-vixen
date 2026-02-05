use std::fmt::Write;

use yellowstone_vixen_core::instruction::InstructionUpdate;

/// Get all instructions (main + inner CPIs) with their index path.
/// Index path example: [0], [0, 1], [0, 1, 2] for nested CPIs.
pub fn get_all_ix_with_index(
    ix: &InstructionUpdate,
    base_index: usize,
) -> Vec<(Vec<usize>, &InstructionUpdate)> {
    let mut result = Vec::new();
    let mut path = vec![base_index];
    collect_recursive(ix, &mut path, &mut result);
    result
}

fn collect_recursive<'a>(
    ix: &'a InstructionUpdate,
    path: &mut Vec<usize>,
    result: &mut Vec<(Vec<usize>, &'a InstructionUpdate)>,
) {
    result.push((path.clone(), ix));
    for (i, inner) in ix.inner.iter().enumerate() {
        path.push(i);
        collect_recursive(inner, path, result);
        path.pop();
    }
}

/// Format a path as a dot-separated string (e.g., "0.1.2").
pub fn format_path(path: &[usize]) -> String {
    let mut out = String::new();
    for (i, idx) in path.iter().enumerate() {
        if i > 0 {
            out.push('.');
        }
        write!(out, "{idx}").unwrap();
    }
    out
}

/// Generate a unique Kafka key for deduplication.
/// Format: `{slot}:{signature}:{ix_index}`
pub fn make_record_key(slot: u64, signature: &str, ix_index: &str) -> String {
    format!("{slot}:{signature}:{ix_index}")
}
