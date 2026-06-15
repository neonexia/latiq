//! latiq-engine-duckdb — DuckDB + DuckLake implementation of QueryEngine.
pub mod duck_engine;
pub mod exec;
pub mod instance;
pub use duck_engine::DuckEngine;
pub use instance::{ensure_standard_extensions, warm_optional_extensions};
