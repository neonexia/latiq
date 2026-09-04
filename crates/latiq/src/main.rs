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

//! The `latiq` executable. Everything it does lives in the library half of this
//! crate (`src/lib.rs`) so the SAME clap layer can be driven from somewhere that
//! is not this `main` — specifically the `latiq` console script the PyPI wheel
//! installs, which calls [`latiq::run_from_env`] through PyO3. Two entry points,
//! one argument parser: `latiq serve` from the wheel and `latiq serve` from the
//! image cannot drift.
fn main() -> anyhow::Result<()> {
    latiq::run_from_env()
}
