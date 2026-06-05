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
