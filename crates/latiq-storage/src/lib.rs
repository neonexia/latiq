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

//! latiq-storage — pluggable pond storage (PondStorage trait + backends).
pub mod local_fs;
pub mod location;
pub mod storage;
pub mod temp_fs;
pub use local_fs::LocalFs;
pub use location::PondLocation;
pub use storage::{PondStorage, StorageError};
pub use temp_fs::TempFs;
