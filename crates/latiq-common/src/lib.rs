// Copyright 2026 Neonexia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! latiq-common — shared kernel (ids, identity, errors, results, config).
pub mod id;
pub use id::PondId;
pub mod identity;
pub use identity::Identity;
pub mod error;
pub use error::{ErrorEnvelope, ErrorKind, Location};
pub mod meta;
pub use meta::{DatasetField, DatasetRef, QueryMeta, Warning, WarningKind};
pub mod timeout;
pub use timeout::{QueryTimeouts, DEFAULT_QUERY_TIMEOUT_MS, MAX_QUERY_TIMEOUT_MS};
pub mod tier;
pub use tier::{PondTier, ResourceLimits};
pub mod catalog;
pub mod extensions;
