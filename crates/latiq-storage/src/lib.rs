//! latiq-storage — pluggable pond storage (PondStorage trait + backends).
pub mod location;
pub mod storage;
pub use location::PondLocation;
pub use storage::{PondStorage, StorageError};
