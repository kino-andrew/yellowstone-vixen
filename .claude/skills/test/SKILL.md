---
name: test
description: Run tests for the project. Handles missing fixtures by prompting for RPC_ENDPOINT.
allowed-tools: Bash, Read, Glob, Grep, AskUserQuestion
---

# Run Tests

Refer to AGENTS.md "Testing" section for commands, fixture conventions, and snapshot workflow.

## Claude-specific orchestration

1. **Pre-flight**: Check test files (`tests/proc-macro/tests/*.rs`) for `tx_fixture!` / `account_fixture!` calls and verify the corresponding files exist in `tests/proc-macro/fixtures/`. If any are missing, check `RPC_ENDPOINT` env var — if unset, ask the user for one before proceeding.

2. **Run**: Execute `cargo test -p proc-macro-test` (or scoped/workspace variant per user request).

3. **Snapshot mismatches**: If insta assertions fail, show the diff and ask if the user wants to update. If yes, re-run with `INSTA_UPDATE=always`.

4. **Report**: Summarize pass/fail counts. For failures, show error + file path + line number and suggest fixes if obvious.