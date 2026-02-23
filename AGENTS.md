# Yellowstone Vixen

Program-aware, real-time Solana data pipeline framework by ABK Labs / Triton One.
Transforms raw on-chain events into structured protobuf data via pluggable parsers, handlers, and sinks.

## Architecture

```
Sources (gRPC, Fumarole, Jetstream, RPC, Snapshot)
  → Runtime (filter, buffer, coordinate)
    → Parsers (program-specific: IDL → Rust structs + proto)
      → Handlers (Logger, Kafka sink, gRPC stream, custom)
```

**Core pattern**: `Pipeline::new(Parser, [Handlers])` — the runtime routes events to parsers by program ID, parsers decode raw bytes into typed structs, handlers consume the output.

## Ecosystem context

- **Richat**: Geyser stream provider (Triton infrastructure). Vixen connects via Yellowstone gRPC.
- **Yellowstone**: Solana Geyser plugin specification. Dragon's Mouth (gRPC) and Fumarole (persistent streams).
- **Kafka / RedPanda**: Downstream consumers read protobuf-encoded events from Kafka topics via the kafka-sink crate.
- **Schema Registry**: Confluent-compatible. Kafka sink wraps protobuf payloads in Confluent wire format.

## Crate map

| Crate | Path | Role |
|-------|------|------|
| `yellowstone-vixen` | `crates/runtime/` | Runtime: builder, pipelines, handler trait, metrics |
| `yellowstone-vixen-core` | `crates/core/` | `Parser`, `ProgramParser`, `Prefilter`, `Pubkey`, update types |
| `yellowstone-vixen-proc-macro` | `crates/proc-macro/` | `include_vixen_parser!` (Codama IDL → code), `#[vixen_proto]` attr macro |
| `yellowstone-vixen-parser` | `crates/parser/` | Parser utilities, error types, prelude |
| `yellowstone-vixen-proto` | `crates/proto/` | Generated protobuf modules (prost) |
| `yellowstone-vixen-mock` | `crates/mock/` | Offline testing: `account_fixture!`, `tx_fixture!` macros |
| `yellowstone-vixen-stream` | `crates/stream/` | gRPC server for streaming parsed events |
| `yellowstone-vixen-kafka-sink` | `crates/kafka-sink/` | Kafka producer with schema registry, slot ordering |
| `yellowstone-vixen-block-coordinator` | `crates/block-coordinator/` | Slot ordering, fork handling, block completion |
| Sources | `crates/*-source/` | gRPC, Fumarole, Jetstream, RPC, Snapshot |
| Parsers | `crates/*-parser/` | SPL Token, Token Extensions, Block Meta, Slot, Stake Pool |

## Proc-macro internals (`crates/proc-macro/`)

This is where most active development happens. Pipeline:

```
Codama JSON IDL
  → parse.rs (deserialize IDL into codama_nodes)
    → intermediate_representation/ (IDL → SchemaIr)
      → build_schema_ir.rs (orchestrator)
      → build_accounts.rs, build_defined_types.rs, build_instructions_schema.rs
      → schema_ir.rs (TypeIr, FieldIr, OneofIr, etc.)
    → render/ (SchemaIr → TokenStream)
      → vixen_parser.rs (top-level module codegen)
      → rust_types_from_ir.rs (prost-annotated Rust structs from IR)
      → proto_schema_string.rs (proto schema string constant)
      → instruction_parser.rs (Parser impls, discriminator matching, InstructionResolver)
      → account_parser.rs (account parsing codegen)
      → program_pubkey.rs (PROGRAM_ID constant)
    → utils.rs (case conversion, hex padding)
```

**Key generated items per program**:
- `mod program_name { ... }` with `PROGRAM_ID`, `PROTOBUF_SCHEMA`
- `pub mod instruction { ... }` — prost-annotated structs (accounts, args, oneof dispatch)
- `InstructionParser` (unit struct) — default discriminator-based parsing
- `CustomInstructionParser<R>` — user-provided resolver for ambiguous discriminators
- `parse_*()` helpers — per-instruction public functions
- `resolve_instruction_default()` — default discriminator matching
- `AccountParser` — account data parsing

## Key traits

```rust
// crates/core/src/lib.rs
trait Parser {
    type Input;   // InstructionUpdate, AccountUpdate, etc.
    type Output;  // Parsed protobuf struct
    fn id(&self) -> Cow<'static, str>;
    fn prefilter(&self) -> Prefilter;
    async fn parse(&self, value: &Self::Input) -> ParseResult<Self::Output>;
}

trait ProgramParser: Parser {
    fn program_id(&self) -> Pubkey;
}

// crates/runtime/src/handler.rs
trait Handler<T, R: Sync> {
    async fn handle(&self, value: &T, raw_event: &R) -> HandlerResult<()>;
}
```

## Testing

```bash
# Run all proc-macro integration tests
cargo test -p proc-macro-test

# Run a specific test
cargo test -p proc-macro-test -- raydium_amm_v4_with_swapv2

# Update insta snapshots
INSTA_UPDATE=always cargo test -p proc-macro-test

# Full workspace build
cargo build --workspace

# Custom RPC endpoint for fixture fetching
RPC_ENDPOINT=https://your-rpc-url.com cargo test -p proc-macro-test
```

Test IDLs live in `tests/proc-macro/idls/`, test files in `tests/proc-macro/tests/`, snapshots in `tests/proc-macro/tests/snapshots/`.

Current test programs: perpetuals, pump_fun, dynamic_bonding_curve, order_engine, raydium_amm_v4_with_swapv2.

### Fixtures

Tests use `tx_fixture!("hash", &parser)` and `account_fixture!("pubkey", &parser)` macros from the mock crate. Fixture files live in `tests/proc-macro/fixtures/` and are named `<tx_hash>_tx.json` or `<pubkey>_account.json`.

If a fixture is missing, the mock crate fetches it from an RPC node at test time. Set `RPC_ENDPOINT` to a Solana mainnet URL (Helius, Triton, QuickNode, etc.) before running tests.

### Snapshot testing

Tests use [insta](https://insta.rs/) for snapshot assertions. When snapshots mismatch:
- Review the diff to confirm the change is intentional
- Update with: `INSTA_UPDATE=always cargo test -p proc-macro-test`

## Conventions

- Rust edition 2024, toolchain 1.90.0
- Formatting: `rustfmt` with nightly features, 100 char max width, 4-space indent
- Imports: grouped (std → external → crate), crate-level granularity
- Serialization: prost for protobuf, borsh for on-chain args, serde for config/fixtures
- `Pubkey` = `KeyBytes<32>` (32-byte array wrapper), serialized as `bytes` in proto
- Error handling: `ParseError::Filtered` for non-matching programs, `ParseError::Other` for real errors
- Prefer Rust code over config files for complex logic (e.g., instruction disambiguation)

## Code style

- **Scoped bindings**: Use `{ ... }` blocks to limit variable scope whenever possible:
  ```rust
  let tags_lit = {
      let tags_list = (1..=accounts.len())
          .map(|t| t.to_string())
          .collect::<Vec<_>>()
          .join(", ");

      LitStr::new(&tags_list, Span::call_site())
  };
  ```

- **Early returns / guard clauses**: Return early when something doesn't match rather than nesting deeper. Use `let...else { return }` and early `match` + `return`:
  ```rust
  let NestedTypeNode::Value(struct_node) = &account.data else {
      return None;
  };

  let field = match struct_node.fields.iter().find(|f| f.name == node.name) {
      Some(f) => f,
      None => return None,
  };
  ```

- **Doc comments with examples**: For complex functions, include `/// Example output:` with a code block showing what the function produces. Use `rust, ignore` (with a space after the comma). Add a blank `///` line at the start and end of the doc comment so it "breathes" when more than 3 lines:
  ```rust
  ///
  /// Generate a public parse helper function for a single instruction.
  ///
  /// Example output:
  /// ```rust, ignore
  /// pub fn parse_swap_base_in(
  ///     accounts: &[Pubkey],
  ///     data: &[u8],
  /// ) -> ParseResult<Instructions> { ... }
  /// ```
  ///
  fn single_instruction_helper_fn(...) -> Option<TokenStream> {
  ```

- **Breathing room in code**: Add blank lines between logical groups of statements — don't pack everything together. Separate setup, loops, and closing logic:
  ```rust
  // Good — breathes
  writeln!(out, "message {} {{", oneof.parent_message).unwrap();
  writeln!(out, "  oneof {} {{", oneof.field_name).unwrap();

  for v in &oneof.variants {
      let field_name = to_snake_case(&v.variant_name);

      writeln!(out, "    {} {} = {};", v.message_type, field_name, v.tag).unwrap();
  }

  writeln!(out, "  }}").unwrap();
  writeln!(out, "}}").unwrap();
  ```
