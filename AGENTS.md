# Repository Guidelines

## Project Structure & Module Organization
- `src/main.rs` is the main Tokio executable. It orchestrates Excel image extraction, 1688 candidate retrieval, VLM verification, and result export.
- `Cargo.toml` and `Cargo.lock` define crate metadata and dependency versions.
- Runtime artifacts are `1.xlsx` (input), `result.xlsx` (output), `temp_images/` (intermediate files), and `target/` (build output).
- If complexity increases, move domain logic into `src/` modules (for example `src/excel.rs`, `src/search.rs`, `src/vlm.rs`) and keep `main.rs` focused on flow control.

## Build, Test, and Development Commands
- `cargo check` - Fast compile validation without building a release binary.
- `cargo run` - Run the local pipeline using `1.xlsx` and generate `result.xlsx`.
- `cargo run --release` - Optimized execution for larger spreadsheets and longer network batches.
- `cargo test` - Run unit and integration tests.
- `cargo fmt && cargo clippy -- -D warnings` - Format code and enforce lint cleanliness before review.

## Coding Style & Naming Conventions
- Use Rust 2021 idioms and default `rustfmt` formatting (4-space indentation).
- Use `snake_case` for functions and variables, `PascalCase` for structs and enums, and `SCREAMING_SNAKE_CASE` for constants.
- Keep parsing and transform logic in small helper functions.
- Prefer `Result` for recoverable errors and reserve `expect` for startup-fatal failures.

## Testing Guidelines
- Use Rust test primitives (`#[test]`, `#[tokio::test]`).
- Keep quick unit tests near helper functions, and place broader flow tests in `tests/`.
- Name tests by behavior, for example `find_cheapest_returns_lowest_numeric_price`.
- Prioritize cases around regex extraction, price parsing and sorting, and empty or malformed candidate data.

## Commit & Pull Request Guidelines
- Recent commits use concise conventional prefixes (for example `fix: bugs`); keep `type: short-summary` style (`fix: handle empty candidate chunks`).
- Keep commits scoped to one logical change.
- PRs should include purpose, key changes, verification commands run, and any output-impact notes (such as `result.xlsx` column behavior).
- Link related issues or task IDs when available.

## Security & Configuration Tips
- Provide `DASHSCOPE_API_KEY` through local environment variables and never commit secrets.
- The app depends on a local Node service at `http://127.0.0.1:8266/search`; document API contract changes in PR descriptions.
