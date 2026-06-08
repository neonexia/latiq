//! latiq-engine-duckdb — DuckDB + DuckLake implementation of QueryEngine.
pub mod duck_engine;
pub mod exec;
pub mod instance;
pub mod latiq_schema;
pub use duck_engine::DuckEngine;
