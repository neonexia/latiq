//! End-to-end pond lifecycle through the public seams only (PondStorage +
//! QueryEngine). Proves storage + engine compose: create → init → attributed
//! write → read → attribution (native DuckLake pond.snapshots()) + schema → drop.
use latiq_common::{Identity, PondId};
use latiq_engine::{AbortToken, QueryEngine};
use latiq_engine_duckdb::DuckEngine;
use latiq_storage::{PondStorage, TempFs};

#[test]
fn pond_lifecycle_end_to_end() {
    let fs = TempFs::new();
    let eng = DuckEngine::new();
    let id = PondId::new();
    let loc = fs.create_pond(id, false).unwrap();
    eng.init_pond(&loc).unwrap();

    let agent = Identity::claimed(Some("agent-e2e"));
    eng.write_query(
        &loc,
        "CREATE TABLE events(id INTEGER, sev VARCHAR)",
        &agent,
        AbortToken::new(),
    )
    .unwrap();
    let w = eng
        .write_query(
            &loc,
            "INSERT INTO events VALUES (1,'high'),(2,'critical')",
            &agent,
            AbortToken::new(),
        )
        .unwrap();
    assert!(
        w.meta.snapshot_id.is_some(),
        "write should record a snapshot id"
    );

    let r = eng
        .read_query(
            &loc,
            "SELECT id, sev FROM events ORDER BY id",
            AbortToken::new(),
        )
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[1][1], serde_json::json!("critical"));

    let attr = eng
        .read_query(
            &loc,
            "SELECT DISTINCT author FROM pond.snapshots()",
            AbortToken::new(),
        )
        .unwrap();
    let authors: Vec<_> = attr.rows.iter().filter_map(|row| row[0].as_str()).collect();
    assert!(
        authors.contains(&"agent-e2e"),
        "attribution must name the writer; got {authors:?}"
    );

    let schema = eng.describe_schema(&loc).unwrap();
    assert!(
        schema.tables.iter().any(|t| t.name == "events"),
        "describe_schema should list 'events'; got {:?}",
        schema.tables
    );

    fs.drop_pond(id).unwrap();
    assert!(!fs.pond_exists(id));
}

#[test]
fn attribution_records_the_verified_subject_not_only_the_claimed_leaf() {
    // A caller with a valid token for `svc-lowpriv` claims a flattering leaf id.
    // History must make the verified subject visible so the claim cannot pass
    // itself off as authenticated identity.
    let fs = TempFs::new();
    let eng = DuckEngine::new();
    let pond = PondId::new();
    let loc = fs.create_pond(pond, false).unwrap();
    eng.init_pond(&loc).unwrap();

    let id = Identity::verified(
        "svc-lowpriv",
        "https://idp.example/realms/latiq",
        Some("svc-admin"),
    );
    eng.write_query(
        &loc,
        "CREATE TABLE events(id INTEGER)",
        &id,
        AbortToken::new(),
    )
    .unwrap();

    let attr = eng
        .read_query(
            &loc,
            "SELECT * FROM pond.snapshots() ORDER BY snapshot_id DESC LIMIT 1",
            AbortToken::new(),
        )
        .unwrap();
    let msg = serde_json::to_string(&attr.rows[0]).unwrap();
    assert!(
        msg.contains("svc-lowpriv"),
        "must carry the verified subject: {msg}"
    );
    assert!(
        msg.contains("https://idp.example/realms/latiq"),
        "must carry the issuer: {msg}"
    );
    // The claimed leaf is still recorded -- but as a claim, in its own field, so
    // a reader can never mistake it for the authenticated identity. Parsed, not
    // substring-matched: a substring hit would also pass if the value leaked
    // into an unrelated column.
    let extra = latest_extra_info(&eng, &loc);
    assert_eq!(extra["agent_id"], serde_json::json!("svc-admin"));
    assert_eq!(extra["verified"], serde_json::json!(true));
    assert_eq!(
        extra["issuer"],
        serde_json::json!("https://idp.example/realms/latiq")
    );
    let author_ix = attr
        .columns
        .iter()
        .position(|c| c == "author")
        .expect("snapshots() must expose an author column");
    assert_eq!(
        attr.rows[0][author_ix],
        serde_json::json!("svc-lowpriv"),
        "the author must be the verified subject, not the claimed leaf"
    );

    fs.drop_pond(pond).unwrap();
}

/// The latest snapshot's `author` and parsed `commit_extra_info`, read the
/// native DuckLake way (`pond.snapshots()`) — no Latiq objects in the catalog.
fn latest_attribution(
    eng: &DuckEngine,
    loc: &latiq_storage::PondLocation,
) -> (String, serde_json::Value) {
    let r = eng
        .read_query(
            loc,
            "SELECT author, commit_extra_info FROM pond.snapshots() \
             ORDER BY snapshot_id DESC LIMIT 1",
            AbortToken::new(),
        )
        .unwrap();
    let row = &r.rows[0];
    let author = row[0].as_str().expect("author is text").to_string();
    let extra = serde_json::from_str(row[1].as_str().expect("commit_extra_info is text"))
        .expect("commit_extra_info must be the JSON we wrote");
    (author, extra)
}

fn latest_extra_info(eng: &DuckEngine, loc: &latiq_storage::PondLocation) -> serde_json::Value {
    latest_attribution(eng, loc).1
}

#[test]
fn attribution_escapes_hostile_identity_values() {
    // Identity values reach a `CALL … set_commit_message('…')` by interpolation:
    // `agent_id` is caller-supplied and `subject`/`issuer` come from a token, so
    // neither is trustworthy text. This pins the escaping — a refactor that
    // escapes before serializing, drops one `.replace`, or hand-builds the JSON
    // must fail here.
    const INJECT: &str = "x'); DROP TABLE t; --";
    const DOUBLED: &str = "a''b";
    const BACKSLASH: &str = r"back\slash\'quote";

    let fs = TempFs::new();
    let eng = DuckEngine::new();
    let pond = PondId::new();
    let loc = fs.create_pond(pond, false).unwrap();
    eng.init_pond(&loc).unwrap();

    let write = |sql: &str, id: &Identity| {
        eng.write_query(&loc, sql, id, AbortToken::new()).unwrap();
    };
    let benign = Identity::claimed(Some("agent-setup"));
    write("CREATE TABLE t(id INTEGER)", &benign);
    write("INSERT INTO t VALUES (1),(2)", &benign);

    // 1. Hostile value as the CLAIMED leaf (unverified caller → it is the author).
    write("INSERT INTO t VALUES (3)", &Identity::claimed(Some(INJECT)));
    let (author, extra) = latest_attribution(&eng, &loc);
    assert_eq!(author, INJECT, "claimed leaf must round-trip verbatim");
    assert_eq!(extra["agent_id"], serde_json::json!(INJECT));
    assert_eq!(extra["verified"], serde_json::json!(false));

    // 2. The same string as a VERIFIED subject (→ it is the author), with a
    //    doubled quote in the claimed leaf and a backslash in the issuer.
    write(
        "INSERT INTO t VALUES (4)",
        &Identity::verified(INJECT, BACKSLASH, Some(DOUBLED)),
    );
    let (author, extra) = latest_attribution(&eng, &loc);
    assert_eq!(author, INJECT, "verified subject must round-trip verbatim");
    assert_eq!(extra["agent_id"], serde_json::json!(DOUBLED));
    assert_eq!(extra["issuer"], serde_json::json!(BACKSLASH));
    assert_eq!(extra["verified"], serde_json::json!(true));

    // 3. A doubled quote as the author itself — naive escaping mangles this.
    write(
        "INSERT INTO t VALUES (5)",
        &Identity::claimed(Some(DOUBLED)),
    );
    let (author, _) = latest_attribution(&eng, &loc);
    assert_eq!(author, DOUBLED, "a doubled quote must not be re-escaped");

    // The injected DROP never executed: the table is intact with every row.
    let rows = eng
        .read_query(&loc, "SELECT count(*) AS c FROM t", AbortToken::new())
        .unwrap();
    assert_eq!(
        rows.rows[0][0],
        serde_json::json!(5),
        "the injected DROP TABLE must not have executed"
    );

    fs.drop_pond(pond).unwrap();
}

#[test]
fn attribution_unverified_caller_has_empty_verified_fields() {
    // The unverified shape is a compatible superset of the old one: the claimed
    // leaf is still the author, and `verified` is explicitly false so no reader
    // can take the claim for an authenticated identity.
    let fs = TempFs::new();
    let eng = DuckEngine::new();
    let pond = PondId::new();
    let loc = fs.create_pond(pond, false).unwrap();
    eng.init_pond(&loc).unwrap();

    let id = Identity::claimed(Some("agent-plain"));
    eng.write_query(&loc, "CREATE TABLE t(id INTEGER)", &id, AbortToken::new())
        .unwrap();

    let (author, extra) = latest_attribution(&eng, &loc);
    assert_eq!(author, "agent-plain");
    assert_eq!(extra["agent_id"], serde_json::json!("agent-plain"));
    assert_eq!(extra["verified"], serde_json::json!(false));
    assert_eq!(extra["issuer"], serde_json::json!(""));

    fs.drop_pond(pond).unwrap();
}

/// Provenance extraction: what a statement reads and writes, taken from
/// DuckDB's **bound** plan (`exec::referenced_tables`). Everything here is our
/// integration with the serialiser, never DuckDB's SQL semantics: the shapes we
/// read out of the plan, the three properties that make the extraction safe
/// (never fails, never executes, never concatenates), and the gate that keeps a
/// lineage-disabled pond from paying for any of it.
mod lineage {
    use super::*;
    use latiq_common::DatasetRef;
    use latiq_engine_duckdb::exec::{in_read_txn, referenced_tables, run_read};
    use latiq_engine_duckdb::instance::PondInstance;

    /// A pond with `a`, `b` and a view `vw` that joins them.
    fn pond_with_a_view() -> (TempFs, PondId, PondInstance) {
        let fs = TempFs::new();
        let pond = PondId::new();
        let loc = fs.create_pond(pond, true).unwrap();
        let inst = PondInstance::open(&loc).unwrap();
        for sql in [
            "CREATE TABLE a(id INTEGER, v VARCHAR)",
            "INSERT INTO a VALUES (1,'x')",
            "CREATE TABLE b(id INTEGER, w VARCHAR)",
            "INSERT INTO b VALUES (1,'y')",
            "CREATE VIEW vw AS SELECT a.id, a.v, b.w FROM a JOIN b USING (id)",
        ] {
            inst.conn.execute_batch(sql).unwrap();
        }
        (fs, pond, inst)
    }

    /// `referenced_tables` with the pond label its diagnostics carry (the
    /// catalog name, `pond` for a `TempFs` pond).
    fn refs(inst: &PondInstance, sql: &str) -> (Vec<DatasetRef>, Vec<DatasetRef>) {
        referenced_tables(inst, sql, "pond")
    }

    fn names(datasets: &[DatasetRef]) -> Vec<&str> {
        let mut out: Vec<&str> = datasets.iter().map(|d| d.name.as_str()).collect();
        out.sort_unstable();
        out
    }

    fn scalar(inst: &PondInstance, sql: &str) -> i64 {
        inst.conn.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    #[test]
    fn lineage_extracts_base_tables_through_a_view() {
        // THE deciding case for using the bound plan over the parse tree: the
        // parse tree of `SELECT * FROM vw` says only `vw`, and the base tables
        // are unrecoverable from it. A consumer told a query read `vw` learns
        // nothing about which data it actually depended on.
        let (_fs, _id, inst) = pond_with_a_view();
        let (inputs, outputs) = refs(&inst, "SELECT * FROM vw");
        assert_eq!(
            names(&inputs),
            vec!["pond.main.a", "pond.main.b"],
            "a view must resolve to the base tables it reads"
        );
        assert!(
            !names(&inputs).contains(&"pond.main.vw"),
            "the view itself is not a dataset anything read"
        );
        assert!(outputs.is_empty(), "a SELECT writes nothing");

        // A CTE resolves the same way — same mechanism, and the pin costs a line.
        let (cte_inputs, _) = refs(&inst, "WITH c AS (SELECT * FROM a) SELECT * FROM c");
        assert_eq!(names(&cte_inputs), vec!["pond.main.a"]);
    }

    #[test]
    fn lineage_distinguishes_same_named_tables_in_different_catalogs() {
        // Two catalogs can both hold `main.events`. A bare table name would
        // conflate them into one dataset in every consumer — which is worse
        // than reporting nothing, because it invents a dependency.
        let (_fs, _id, inst) = pond_with_a_view();
        let side = tempfile::tempdir().unwrap();
        let side_db = side.path().join("side.duckdb");
        inst.conn
            .execute_batch(&format!(
                "ATTACH '{}' AS side; CREATE TABLE side.main.a(id INTEGER);",
                side_db.display()
            ))
            .unwrap();

        let (inputs, _) = refs(
            &inst,
            "SELECT a.id FROM a JOIN side.main.a AS s ON a.id = s.id",
        );
        assert_eq!(
            names(&inputs),
            vec!["pond.main.a", "side.main.a"],
            "the catalog must be part of a dataset's identity"
        );
    }

    #[test]
    fn lineage_separates_write_targets_from_read_inputs() {
        // `INSERT INTO a SELECT * FROM b` is one dataset read and a different
        // one written. Reporting a flat "tables touched" list would make `b`
        // look like it was written, reversing the direction of the edge every
        // lineage graph is built from.
        let (_fs, _id, inst) = pond_with_a_view();
        let (inputs, outputs) = refs(&inst, "INSERT INTO a SELECT * FROM b");
        assert_eq!(names(&inputs), vec!["pond.main.b"]);
        assert_eq!(names(&outputs), vec!["pond.main.a"]);

        // A DELETE both reads and writes its target: it must appear on BOTH
        // sides, not be silently classified as one of them.
        let (del_in, del_out) = refs(&inst, "DELETE FROM a WHERE id = 1");
        assert_eq!(names(&del_in), vec!["pond.main.a"]);
        assert_eq!(names(&del_out), vec!["pond.main.a"]);

        // DDL targets are outputs too, or a pond's tables would appear in the
        // graph only once something inserted into them.
        let (_, created) = refs(&inst, "CREATE TABLE c AS SELECT * FROM a");
        assert_eq!(names(&created), vec!["pond.main.c"]);
        let (_, dropped) = refs(&inst, "DROP TABLE b");
        assert_eq!(names(&dropped), vec!["pond.main.b"]);
    }

    #[test]
    fn lineage_records_the_snapshot_each_input_was_read_at() {
        // "Which version did this run consume" is the question a dataset
        // version facet exists to answer; a missing or stale one makes two
        // different reads of a changing table indistinguishable.
        let (_fs, _id, inst) = pond_with_a_view();
        let latest =
            |inst: &PondInstance| scalar(inst, "SELECT max(snapshot_id) FROM pond.snapshots()");

        let before = latest(&inst);
        let (inputs, _) = refs(&inst, "SELECT * FROM a");
        assert_eq!(
            inputs[0].version,
            Some(before),
            "an input must carry the snapshot it would be read at"
        );

        // After a write, the same query reads a NEWER snapshot — so the version
        // tracks the data and is not a constant that happened to match.
        inst.conn
            .execute_batch("INSERT INTO a VALUES (2,'z')")
            .unwrap();
        let after = latest(&inst);
        assert!(after > before, "the write must advance the snapshot");
        let (inputs, _) = refs(&inst, "SELECT * FROM a");
        assert_eq!(inputs[0].version, Some(after));

        // Nothing outside DuckLake gets a version invented for it.
        inst.conn
            .execute_batch("CREATE TEMP TABLE t(id INTEGER)")
            .unwrap();
        let (temp_inputs, _) = refs(&inst, "SELECT * FROM t");
        assert_eq!(names(&temp_inputs), vec!["temp.main.t"]);
        assert_eq!(
            temp_inputs[0].version, None,
            "a table with no snapshot must not be given one"
        );
    }

    #[test]
    fn lineage_read_reports_the_snapshot_it_actually_saw() {
        // The claim a version facet makes is "this run consumed THAT state".
        // Unbracketed, the extraction, the read and the snapshot accessor are
        // three separate implicit transactions, so a commit landing between any
        // two of them makes the recorded version describe a state the query
        // never read — under-reported provenance that still looks complete.
        //
        // `in_read_txn` is what `read_query`/`read_arrow` bracket their work
        // with, and this drives it with the same closure body they use, with a
        // second connection committing in the middle of it.
        let (_fs, _id, writer) = pond_with_a_view();
        let reader = writer.clone_for_read().unwrap();
        let latest = |i: &PondInstance| scalar(i, "SELECT max(snapshot_id) FROM pond.snapshots()");
        let pinned = latest(&writer);

        let (rows, inputs) = in_read_txn(&reader, |i| {
            let (inputs, _) = refs(i, "SELECT * FROM a");
            // Between the extraction and the read — the exact interleaving.
            writer
                .conn
                .execute_batch("INSERT INTO a VALUES (2,'z')")
                .unwrap();
            let res = run_read(i, "SELECT * FROM a")?;
            Ok((res.rows.len(), inputs))
        })
        .unwrap();

        // Anti-vacuity: a newer snapshot really does exist by now, so agreeing
        // on `pinned` below is a pin and not an absence of anything to disagree
        // with.
        assert!(
            latest(&writer) > pinned,
            "the interleaved commit must have advanced the catalog"
        );
        // The rows the read returned are the state at `pinned` (one row), and
        // the version recorded for the input names that same snapshot. Those
        // two agreeing is the whole claim: unbracketed, the extraction would
        // still report `pinned` while the read returned the newer snapshot's
        // two rows.
        assert_eq!(rows, 1, "the read must see the snapshot it was pinned at");
        assert_eq!(
            inputs[0].version,
            Some(pinned),
            "the input's version must be the snapshot the rows came from"
        );
    }

    #[test]
    fn lineage_a_read_cannot_close_the_transaction_that_pins_its_version() {
        // The bracket is only worth something if user SQL cannot open it from
        // the inside. `SELECT … ; COMMIT; BEGIN TRANSACTION` ends OUR pinned
        // transaction after the rows are read and leaves a fresh one for our
        // COMMIT to close successfully — so the read would report a version it
        // never observed, which is the exact defect the transaction exists to
        // remove. Verified reachable before the guard: `run_read` ran it, and
        // our COMMIT then succeeded against a transaction the statement had
        // opened.
        let (_fs, _id, inst) = pond_with_a_view();
        for sql in [
            "SELECT * FROM a; COMMIT; BEGIN TRANSACTION",
            "SELECT * FROM a;COMMIT;BEGIN TRANSACTION",
            "SELECT * FROM a; ROLLBACK",
        ] {
            assert!(
                matches!(
                    run_read(&inst, sql),
                    Err(latiq_engine::EngineError::ReadOnlyViolation)
                ),
                "transaction control must be refused as a read-only violation, \
                 not left to corrupt the bracket: {sql:?}"
            );
        }
        // The same statement inside the bracket leaves it intact: the read is
        // refused, and the transaction the caller opened is still ours to
        // commit. (Without the guard this COMMIT would have closed a
        // transaction the STATEMENT opened.)
        let outcome = in_read_txn(&inst, |i| {
            let refused = run_read(i, "SELECT * FROM a; COMMIT; BEGIN TRANSACTION");
            assert!(matches!(
                refused,
                Err(latiq_engine::EngineError::ReadOnlyViolation)
            ));
            // Still pinned, so a normal read still works in here.
            Ok(run_read(i, "SELECT * FROM a")?.rows.len())
        });
        assert_eq!(outcome.unwrap(), 1);
    }

    #[test]
    fn lineage_read_only_transaction_never_outlives_a_failed_read() {
        // These connections are pooled and reused: an open transaction left
        // behind by an error path wedges the connection for every later reader.
        // Both exits are covered — the closure returning `Err`, and the caller's
        // `?` unwinding out of it.
        let (_fs, _id, inst) = pond_with_a_view();
        let failed: Result<(), _> = in_read_txn(&inst, |i| {
            let _ = run_read(i, "SELECT * FROM a")?;
            Err(latiq_engine::EngineError::Cancelled)
        });
        assert!(matches!(failed, Err(latiq_engine::EngineError::Cancelled)));
        // A second transaction can only begin if the first one closed — DuckDB
        // rejects a nested BEGIN, so this is the whole assertion.
        let n = in_read_txn(&inst, |i| Ok(run_read(i, "SELECT * FROM a")?.rows.len()))
            .expect("a rolled-back transaction must leave the connection usable");
        assert_eq!(n, 1);
    }

    #[test]
    fn lineage_unparseable_sql_yields_no_tables_rather_than_failing() {
        // Extraction is best-effort provenance riding on a real query: it may
        // never turn a working query into a failure, and `json_serialize_plan`
        // reports these IN BAND (`{"error":true,…}`) rather than raising, so a
        // caller that only handled a raised error would hand the caller a plan
        // document that is really an error object.
        let (_fs, _id, inst) = pond_with_a_view();
        for sql in [
            "SELEC bogus",                     // syntax error
            "SELECT * FROM nonexistent_table", // binder error
            "",                                // nothing at all
        ] {
            let (inputs, outputs) = refs(&inst, sql);
            assert!(
                inputs.is_empty() && outputs.is_empty(),
                "{sql:?} must yield no datasets, got {inputs:?} / {outputs:?}"
            );
        }
        // A batch whose LATER statement cannot bind against the catalog AS IT
        // STANDS (the table its first statement would create does not exist
        // yet) yields nothing rather than failing. A known, accepted limit of
        // planning without executing: no lineage beats fabricated lineage.
        let (batch_in, batch_out) = refs(
            &inst,
            "CREATE TABLE later(i INTEGER); INSERT INTO later VALUES (1)",
        );
        assert!(
            batch_in.is_empty() && batch_out.is_empty(),
            "an unbindable batch must yield nothing, got {batch_in:?} / {batch_out:?}"
        );

        // Anti-vacuity: the same pond, a statement that does resolve.
        let (inputs, _) = refs(&inst, "SELECT * FROM a");
        assert_eq!(names(&inputs), vec!["pond.main.a"]);

        // And a multi-statement batch that DOES bind reports every statement's
        // datasets — a batch is one operation, not only its first statement.
        let (multi_in, _) = refs(&inst, "SELECT * FROM a; SELECT * FROM b");
        assert_eq!(names(&multi_in), vec!["pond.main.a", "pond.main.b"]);
    }

    #[test]
    fn lineage_extraction_does_not_execute_the_statement() {
        // The property that disqualified every alternative (`EXPLAIN ANALYZE`,
        // profiling): planning a statement must not RUN it. This is the test
        // that stops a future refactor reaching for one of them — if it does,
        // rows appear, snapshots advance and a file gets written.
        let (_fs, _id, inst) = pond_with_a_view();
        let out = tempfile::tempdir().unwrap();
        let target = out.path().join("copied.parquet");

        let rows_before = scalar(&inst, "SELECT count(*) FROM a");
        let snapshot_before = scalar(&inst, "SELECT max(snapshot_id) FROM pond.snapshots()");

        refs(&inst, "INSERT INTO a VALUES (99,'inserted')");
        refs(&inst, "DELETE FROM a");
        refs(&inst, "DROP TABLE b");
        refs(
            &inst,
            &format!(
                "COPY (SELECT * FROM a) TO '{}' (FORMAT PARQUET)",
                target.display()
            ),
        );

        assert_eq!(
            scalar(&inst, "SELECT count(*) FROM a"),
            rows_before,
            "no row may be inserted or deleted by planning a statement"
        );
        assert_eq!(
            scalar(&inst, "SELECT max(snapshot_id) FROM pond.snapshots()"),
            snapshot_before,
            "planning must not commit anything"
        );
        assert!(
            !target.exists(),
            "planning a COPY … TO must not write its target"
        );
        // And `b`, whose DROP was planned, is still queryable.
        assert_eq!(scalar(&inst, "SELECT count(*) FROM b"), 1);
    }

    #[test]
    fn lineage_a_plan_over_the_size_cap_is_skipped_not_parsed() {
        // Plan JSON scales with the number of LITERALS, not tables: a 1000-
        // element `IN` list serialises to hundreds of KB. Over the cap we
        // record nothing rather than spend a multi-megabyte JSON parse on
        // provenance — the query itself is unaffected either way.
        let (_fs, _id, inst) = pond_with_a_view();
        let list = (0..40_000)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let big = format!("SELECT * FROM a WHERE id IN ({list})");
        let (inputs, outputs) = refs(&inst, &big);
        assert!(
            inputs.is_empty() && outputs.is_empty(),
            "an oversized plan must be skipped, got {inputs:?}"
        );
        // Anti-vacuity: a small IN list over the same shape IS extracted, so
        // the emptiness above is the cap and not a query the walker cannot read.
        let (small_inputs, _) = refs(&inst, "SELECT * FROM a WHERE id IN (1,2,3)");
        assert_eq!(names(&small_inputs), vec!["pond.main.a"]);
    }

    #[test]
    fn lineage_the_plan_size_cap_counts_bytes_not_characters() {
        // The cap exists to bound what crosses the FFI boundary and reaches
        // serde_json, and that budget is in BYTES. DuckDB's `length()` counts
        // characters, so measuring with it would let a plan full of non-ASCII
        // literals through at up to ~4x the cap — on the query hot path.
        //
        // The two queries below have the SAME character count and differ only
        // in bytes per character, so only a byte-counted cap tells them apart.
        let (_fs, _id, inst) = pond_with_a_view();
        let query_with = |literal: String| format!("SELECT * FROM a WHERE a.v = '{literal}'");
        let wide = query_with("\u{1F600}".repeat(150_000)); // 4 bytes/char -> ~600 KB
        let narrow = query_with("x".repeat(150_000)); // 1 byte/char  -> ~150 KB

        let (over, _) = refs(&inst, &wide);
        assert!(
            over.is_empty(),
            "a plan over the byte cap must be skipped, got {over:?}"
        );
        let (under, _) = refs(&inst, &narrow);
        assert_eq!(
            names(&under),
            vec!["pond.main.a"],
            "the same number of CHARACTERS, under the byte cap, must still be extracted"
        );
    }

    #[test]
    fn lineage_records_a_copy_export_as_an_output() {
        // `COPY … TO` is how data leaves a pond. Without this the export's
        // event shows what it read and nothing it produced — the edge out of
        // the pond is missing while the event still looks complete, which is
        // the silent under-reporting this whole feature is built to avoid.
        let (_fs, _id, inst) = pond_with_a_view();
        let (inputs, outputs) = refs(
            &inst,
            "COPY (SELECT * FROM a) TO '/tmp/lineage_export.parquet' (FORMAT PARQUET)",
        );
        assert_eq!(names(&inputs), vec!["pond.main.a"], "the export read `a`");
        assert_eq!(names(&outputs), vec!["/tmp/lineage_export.parquet"]);
        assert_eq!(
            outputs[0].namespace.as_deref(),
            Some("file"),
            "an export target keeps its standard scheme, like an external input"
        );

        // A remote target keeps `s3://{bucket}` — the identifier another
        // tool's lineage joins the same object on.
        let (_, remote) = refs(
            &inst,
            "COPY a TO 's3://warehouse/exports/a.parquet' (FORMAT PARQUET)",
        );
        assert_eq!(remote[0].namespace.as_deref(), Some("s3://warehouse"));
        assert_eq!(names(&remote), vec!["exports/a.parquet"]);
    }

    #[test]
    fn lineage_time_travel_reads_of_one_table_are_distinct_datasets() {
        // Two reads of one table at two snapshots are two different states of
        // the data. Collapsing them (deduping on name alone) would report a
        // single version for both and lose the older dependency entirely.
        let (_fs, _id, inst) = pond_with_a_view();
        inst.conn
            .execute_batch("INSERT INTO a VALUES (2,'z')")
            .unwrap();
        let latest: i64 = inst
            .conn
            .query_row("SELECT max(snapshot_id) FROM pond.snapshots()", [], |r| {
                r.get(0)
            })
            .unwrap();
        let earlier = latest - 1;

        let (inputs, _) = refs(
            &inst,
            &format!("SELECT * FROM a AT (VERSION => {earlier}) UNION ALL SELECT * FROM a"),
        );
        let mut versions: Vec<Option<i64>> = inputs.iter().map(|d| d.version).collect();
        versions.sort_unstable();
        assert_eq!(
            versions,
            vec![Some(earlier), Some(latest)],
            "both snapshots of `a` must survive as separate datasets: {inputs:?}"
        );

        // Anti-vacuity: the same table read TWICE AT THE SAME version is still
        // one dataset — dedup must not have simply stopped deduping.
        let (self_join, _) = refs(&inst, "SELECT * FROM a AS x JOIN a AS y ON x.id = y.id");
        assert_eq!(
            self_join.len(),
            1,
            "a self-join is one dataset: {self_join:?}"
        );
    }

    #[test]
    fn lineage_plan_key_names_still_match_this_duckdb_version() {
        // The plan's key names are serialisation internals with NO stability
        // guarantee, and they differ per table function. A DuckDB upgrade that
        // renames one would make extraction silently return nothing for that
        // scan kind — under-reporting, which is the worst failure mode for
        // provenance and the exact reason the purpose-built C API was rejected.
        // So: exercise one query per scan kind and assert the datasets, so an
        // upgrade fails HERE with a name, rather than quietly downstream.
        let (fs, id, inst) = pond_with_a_view();
        let parquet = fs.pond_location(id).unwrap().data_path.to_string() + "/probe.parquet";
        inst.conn
            .execute_batch(&format!(
                "COPY (SELECT 1 AS id) TO '{parquet}' (FORMAT PARQUET); \
                 CREATE TEMP TABLE t(id INTEGER);"
            ))
            .unwrap();

        // ducklake_scan: catalog_name / schema_name / table_name + snapshot.
        let (ducklake, _) = refs(&inst, "SELECT * FROM a");
        assert_eq!(
            names(&ducklake),
            vec!["pond.main.a"],
            "a DuckLake scan no longer resolves: its plan keys \
             (function_data.catalog_name/schema_name/table_name) have moved"
        );
        assert!(
            ducklake[0].version.is_some(),
            "the DuckLake scan's function_data.snapshot.snapshot_id has moved"
        );

        // seq_scan: catalog / schema / table — different key names entirely.
        let (seq, _) = refs(&inst, "SELECT * FROM t");
        assert_eq!(
            names(&seq),
            vec!["temp.main.t"],
            "a core table scan no longer resolves: its plan keys \
             (function_data.catalog/schema/table) have moved"
        );

        // read_parquet: `files`, and the standard `file` namespace kept intact.
        let (files, _) = refs(&inst, &format!("SELECT * FROM read_parquet('{parquet}')"));
        assert_eq!(
            names(&files),
            vec![parquet.as_str()],
            "a file scan no longer resolves: function_data.files has moved"
        );
        assert_eq!(
            files[0].namespace.as_deref(),
            Some("file"),
            "an external source must keep its standard scheme"
        );
    }

    #[test]
    fn lineage_disabled_pond_pays_for_no_extraction() {
        // The per-pond opt-in exists because extraction costs a second bind of
        // every statement. A pond that records no lineage must therefore not
        // just emit nothing — it must not extract, which is observable as an
        // empty meta on exactly the same queries.
        let fs = TempFs::new();
        let eng = DuckEngine::new();
        let id = Identity::claimed(Some("agent-a"));
        let mut off = fs.create_pond(PondId::new(), false).unwrap();
        off.lineage = false;
        let mut on = fs.create_pond(PondId::new(), true).unwrap();
        on.lineage = true;

        for loc in [&off, &on] {
            eng.write_query(loc, "CREATE TABLE t(i INTEGER)", &id, AbortToken::new())
                .unwrap();
        }
        let quiet = eng
            .read_query(&off, "SELECT * FROM t", AbortToken::new())
            .unwrap();
        assert!(
            quiet.meta.tables_touched.is_empty() && quiet.meta.inputs.is_empty(),
            "a lineage-disabled pond must not extract, got {:?}",
            quiet.meta
        );
        let loud = eng
            .read_query(&on, "SELECT * FROM t", AbortToken::new())
            .unwrap();
        assert_eq!(
            loud.meta.tables_touched,
            vec!["pond.main.t"],
            "the opted-in pond gets its inputs — so the silence above is the flag"
        );
    }

    #[test]
    fn lineage_every_query_path_reports_its_datasets() {
        // Three engine entry points produce a meta, and each is the read path
        // for a different surface: `read_arrow` is what the CLI and SDK use, so
        // a meta missing there means the primary read path has no provenance at
        // all. Pinned per path, because they are three different code paths.
        use arrow::datatypes::SchemaRef;
        use arrow::record_batch::RecordBatch;
        use latiq_engine::ArrowSink;
        use std::ops::ControlFlow;

        let fs = TempFs::new();
        let eng = DuckEngine::new();
        let id = Identity::claimed(Some("agent-a"));
        let mut loc = fs.create_pond(PondId::new(), true).unwrap();
        loc.lineage = true;
        eng.write_query(&loc, "CREATE TABLE t(i INTEGER)", &id, AbortToken::new())
            .unwrap();

        let write = eng
            .write_query(&loc, "INSERT INTO t VALUES (1)", &id, AbortToken::new())
            .unwrap();
        assert_eq!(write.meta.outputs.len(), 1, "a write reports its target");
        assert_eq!(write.meta.outputs[0].name, "pond.main.t");
        assert!(write.meta.inputs.is_empty(), "VALUES reads no dataset");

        let read = eng
            .read_query(&loc, "SELECT i FROM t", AbortToken::new())
            .unwrap();
        assert_eq!(read.meta.tables_touched, vec!["pond.main.t"]);
        // The observed read version rides on the INPUT, never on the meta's
        // `snapshot_id`: `latiq-pond-node`'s wire encoder labels a statement
        // `write_query` exactly when `snapshot_id` is set, so a read that
        // recorded one there would be labelled a write on the Data gRPC wire.
        assert!(
            read.meta.snapshot_id.is_none(),
            "a read must not claim a snapshot id — the wire reads it as a write"
        );
        assert!(
            read.meta.inputs[0].version.is_some(),
            "and the version it observed must still be recorded, on the input"
        );

        struct Silent;
        impl ArrowSink for Silent {
            fn schema(&mut self, _: SchemaRef) -> ControlFlow<()> {
                ControlFlow::Continue(())
            }
            fn batch(&mut self, _: RecordBatch) -> ControlFlow<()> {
                ControlFlow::Continue(())
            }
        }
        let streamed = eng
            .read_arrow(&loc, "SELECT i FROM t", AbortToken::new(), &mut Silent)
            .unwrap();
        assert_eq!(
            streamed.tables_touched,
            vec!["pond.main.t"],
            "the streaming read path must report its inputs too"
        );
        assert_eq!(streamed.rows, 1, "and the rows it streamed");
    }

    #[test]
    fn lineage_engine_reports_its_real_version() {
        // The processing-engine facet claims what ran the query. A hard-coded
        // string would be wrong the first time the bundled DuckDB is bumped,
        // and nothing would notice.
        let eng = DuckEngine::new();
        let reported = eng.version();
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new(), false).unwrap();
        let actual = eng
            .read_query(&loc, "SELECT version() AS v", AbortToken::new())
            .unwrap();
        assert_eq!(
            serde_json::json!(reported),
            actual.rows[0][0],
            "the reported version must be what the engine itself says"
        );
        assert!(reported.starts_with('v'), "got {reported:?}");
    }
}
