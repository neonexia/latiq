//! latiq-control-plane — registry + Control/Admin gRPC surfaces.
pub mod admin_service;
pub mod control_service;
pub mod error;
pub mod migrations;
pub mod registry;
pub use error::ControlPlaneError;
