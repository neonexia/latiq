//! Probe A: DuckLake round-trip with duckdb-rs bundled
//! Probe B: Native attribution via set_commit_message
//! Run with: cargo run

use duckdb::Connection;

fn main() -> anyhow::Result<()> {
    // ---- Probe A: DuckLake round-trip ----
    println!("=== Probe A: DuckLake round-trip ===");

    let dir = std::env::temp_dir().join("latiq-spike-a");
    // Clean up from previous runs
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("data"))?;
    let catalog = dir.join("catalog.duckdb");
    let data = dir.join("data");

    println!("Using catalog: {}", catalog.display());
    println!("Using data dir: {}", data.display());

    let conn = Connection::open_in_memory()?;

    // Install and load ducklake
    println!("Installing and loading ducklake extension...");
    match conn.execute_batch("INSTALL ducklake; LOAD ducklake;") {
        Ok(_) => println!("ducklake installed and loaded successfully"),
        Err(e) => {
            println!("BLOCKER: Failed to install/load ducklake: {e}");
            return Err(e.into());
        }
    }

    // Try ATTACH with ducklake:duckdb: prefix
    let attach_sql = format!(
        "ATTACH 'ducklake:duckdb:{}' AS pond (DATA_PATH '{}');",
        catalog.display(),
        data.display()
    );
    println!("Trying ATTACH: {attach_sql}");

    match conn.execute_batch(&attach_sql) {
        Ok(_) => println!("ATTACH succeeded with 'ducklake:duckdb:' prefix"),
        Err(e) => {
            println!("ATTACH with 'ducklake:duckdb:' failed: {e}");
            // Try alternative form
            let attach_alt = format!(
                "ATTACH '{}' AS pond (TYPE ducklake, DATA_PATH '{}');",
                catalog.display(),
                data.display()
            );
            println!("Trying alternative ATTACH: {attach_alt}");
            conn.execute_batch(&attach_alt)?;
            println!("ATTACH succeeded with TYPE ducklake form");
        }
    }

    // Create table and insert
    conn.execute_batch("CREATE TABLE pond.events(id INTEGER, sev VARCHAR);")?;
    conn.execute_batch("INSERT INTO pond.events VALUES (1,'high'),(2,'low');")?;

    let n: i64 = conn.query_row("SELECT count(*) FROM pond.events", [], |r| r.get(0))?;
    println!("row count = {n}");
    assert_eq!(n, 2, "Expected 2 rows");
    println!("Probe A: round-trip CONFIRMED");

    // ---- Probe B: Native attribution via set_commit_message ----
    println!("\n=== Probe B: Native attribution ===");

    // Try set_commit_message in a transaction
    match conn.execute_batch(
        "BEGIN; \
         INSERT INTO pond.events VALUES (3,'critical'); \
         CALL pond.set_commit_message('agent-spike', 'write_query', extra_info => '{\"verified\":false}'); \
         COMMIT;"
    ) {
        Ok(_) => println!("set_commit_message SUCCEEDED"),
        Err(e) => {
            println!("set_commit_message with pond-qualified form failed: {e}");
            // Try without extra_info
            match conn.execute_batch(
                "BEGIN; \
                 INSERT INTO pond.events VALUES (3,'critical'); \
                 CALL pond.set_commit_message('agent-spike', 'write_query'); \
                 COMMIT;"
            ) {
                Ok(_) => println!("set_commit_message without extra_info SUCCEEDED"),
                Err(e2) => println!("set_commit_message without extra_info also failed: {e2}"),
            }
        }
    }

    // Try pond.snapshots()
    // NOTE: column_names() panics if called before query_map() executes the stmt
    // (schema is None until executed). Must call column_names() inside the row closure.
    println!("\nTrying pond.snapshots()...");
    match conn.prepare("SELECT snapshot_id, author, commit_message FROM pond.snapshots()") {
        Ok(mut stmt) => {
            match stmt.query_map([], |r| {
                // column_names() is available on r.as_ref() after execution
                let snapshot_id: Result<i64, _> = r.get(0);
                let author: Result<Option<String>, _> = r.get(1);
                let commit_message: Result<Option<String>, _> = r.get(2);
                // Get actual column names from row stmt (available after execution)
                let cols = r.as_ref().column_names();
                Ok((
                    snapshot_id.unwrap_or(-1),
                    author.unwrap_or(None),
                    commit_message.unwrap_or(None),
                    cols,
                ))
            }) {
                Ok(rows) => {
                    let mut first = true;
                    for row in rows {
                        let (id, author, msg, cols) = row?;
                        if first {
                            println!("pond.snapshots() actual columns: {:?}", cols);
                            first = false;
                        }
                        println!("snapshot: id={id}, author={author:?}, message={msg:?}");
                        if author.as_deref() == Some("agent-spike") {
                            println!("  --> pond.snapshots() ATTRIBUTION CONFIRMED: author='agent-spike'");
                        }
                    }
                }
                Err(e) => println!("Error querying pond.snapshots(): {e}"),
            }
        }
        Err(e) => println!("pond.snapshots() prepare failed: {e}"),
    }

    // Try ducklake_snapshots('pond')
    println!("\nTrying ducklake_snapshots('pond')...");
    match conn.prepare("SELECT snapshot_id, author, commit_message FROM ducklake_snapshots('pond')") {
        Ok(mut stmt) => {
            match stmt.query_map([], |r| {
                let snapshot_id: Result<i64, _> = r.get(0);
                let author: Result<Option<String>, _> = r.get(1);
                let commit_message: Result<Option<String>, _> = r.get(2);
                let cols = r.as_ref().column_names();
                Ok((
                    snapshot_id.unwrap_or(-1),
                    author.unwrap_or(None),
                    commit_message.unwrap_or(None),
                    cols,
                ))
            }) {
                Ok(rows) => {
                    let mut first = true;
                    for row in rows {
                        let (id, author, msg, cols) = row?;
                        if first {
                            println!("ducklake_snapshots('pond') actual columns: {:?}", cols);
                            first = false;
                        }
                        println!("snapshot: id={id}, author={author:?}, message={msg:?}");
                        if author.as_deref() == Some("agent-spike") {
                            println!("  --> ducklake_snapshots() ATTRIBUTION CONFIRMED: author='agent-spike'");
                        } else {
                            println!("  --> ATTRIBUTION ISSUE: expected 'agent-spike', got {author:?}");
                        }
                    }
                }
                Err(e) => println!("Error querying ducklake_snapshots('pond'): {e}"),
            }
        }
        Err(e) => println!("ducklake_snapshots('pond') prepare failed: {e}"),
    }

    println!("\nProbes A+B complete.");
    Ok(())
}
