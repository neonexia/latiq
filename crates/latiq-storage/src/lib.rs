//! latiq-storage — pluggable pond storage (PondStorage trait + backends).
pub mod local_fs;
pub mod location;
pub mod storage;
pub use local_fs::LocalFs;
pub use location::PondLocation;
pub use storage::{PondStorage, StorageError};
