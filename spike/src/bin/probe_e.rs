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

//! Probe E: Query cancellation via Connection::interrupt_handle()
//!
//! Key finding: `Connection` is NOT Send (uses RefCell internally).
//! The pattern is:
//!   1. Call `conn.interrupt_handle()` BEFORE moving `conn` into thread
//!   2. `interrupt_handle()` returns Arc<InterruptHandle> which IS Send+Sync
//!   3. Call `interrupt_handle.interrupt()` from any thread

use duckdb::Connection;
use std::time::{Duration, Instant};

fn main() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;

    // Get the interrupt handle BEFORE moving conn into thread
    let interrupt_handle = conn.interrupt_handle();

    let t0 = Instant::now();

    // Move conn into thread for the heavy query
    let handle = std::thread::spawn(move || {
        conn.execute_batch("SELECT count(*) FROM range(100000000000) t1, range(1000) t2;")
    });

    std::thread::sleep(Duration::from_millis(200));

    // Interrupt from main thread using the Arc<InterruptHandle>
    interrupt_handle.interrupt();

    let res = handle.join().unwrap();
    let elapsed = t0.elapsed();
    println!("elapsed={elapsed:?} err={}", res.is_err());
    if let Err(ref e) = res {
        println!("error={e}");
    }
    assert!(res.is_err(), "Expected error after interrupt");
    assert!(elapsed < Duration::from_secs(5), "Expected abort within 5s, got {elapsed:?}");
    println!("Probe E: CONFIRMED - interrupt_handle().interrupt() aborted query in {elapsed:?}");
    Ok(())
}
