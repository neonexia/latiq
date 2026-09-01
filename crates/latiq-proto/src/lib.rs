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

//! Generated gRPC contracts for the Control and Admin surfaces.
pub mod v1 {
    // `clippy::result_large_err` fires on every generated client/server method
    // because tonic returns `Result<_, tonic::Status>` and `Status` is ~176
    // bytes. The signature is tonic's, not ours — we cannot box the error
    // without hand-editing generated code — and the lint is advisory (a
    // perf hint about move sizes), not a correctness one. Scoped to this
    // module only: the lint stays live for our hand-written crates, where
    // acting on it is possible and useful.
    #![allow(clippy::result_large_err)]
    tonic::include_proto!("latiq.v1");
}
