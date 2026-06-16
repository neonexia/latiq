//! Shared metrics plumbing: a global Prometheus recorder, a `/metrics` HTTP
//! server, and a process CPU/memory sampler. Counters/gauges are recorded
//! throughout the codebase via the `metrics` facade (`metrics::counter!` etc.);
//! this crate installs the recorder and exposes the rendered text. We serve
//! `/metrics` ourselves (axum) rather than the exporter's hyper listener, so no
//! extra HTTP stack / tonic version enters the tree.
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::net::SocketAddr;
use std::sync::OnceLock;

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install the global Prometheus recorder once and return its handle. Idempotent:
/// in single-process setups (e.g. the full-stack tests run control plane + node
/// together) the first call installs, later calls return the same handle.
/// Latency buckets (seconds) for our histograms. Explicit buckets make histograms
/// render as Prometheus `_bucket` series (not summaries), so `histogram_quantile`
/// works AND aggregates **across nodes** — summaries can't be merged across pods.
const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

pub fn init_recorder() -> PrometheusHandle {
    HANDLE
        .get_or_init(|| {
            PrometheusBuilder::new()
                .set_buckets(LATENCY_BUCKETS)
                .expect("set histogram buckets")
                .install_recorder()
                .expect("install prometheus recorder")
        })
        .clone()
}

/// Serve `GET /metrics` (Prometheus text) on `addr` until shut down.
pub async fn serve_metrics(
    addr: SocketAddr,
    handle: PrometheusHandle,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = axum::Router::new().route(
        "/metrics",
        axum::routing::get(move || {
            let h = handle.clone();
            async move { h.render() }
        }),
    );
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

/// Samples this process's CPU% and resident memory. Keep one per process and
/// call `sample()` periodically — CPU is usage since the previous refresh.
pub struct ProcessSampler {
    sys: sysinfo::System,
    pid: sysinfo::Pid,
}

impl ProcessSampler {
    pub fn new() -> Self {
        let pid = sysinfo::get_current_pid().expect("current pid");
        let mut sys = sysinfo::System::new();
        // Prime so the first real sample has a CPU delta to compare against.
        sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
        Self { sys, pid }
    }

    /// `(cpu_percent, resident_bytes)`. CPU can exceed 100% across cores.
    pub fn sample(&mut self) -> (f64, u64) {
        self.sys
            .refresh_processes(sysinfo::ProcessesToUpdate::Some(&[self.pid]), true);
        match self.sys.process(self.pid) {
            Some(p) => (p.cpu_usage() as f64, p.memory()),
            None => (0.0, 0),
        }
    }
}

impl Default for ProcessSampler {
    fn default() -> Self {
        Self::new()
    }
}

/// Record this process's CPU/memory gauges. Called by each process's collector.
pub fn record_process_gauges(sampler: &mut ProcessSampler) {
    let (cpu, mem) = sampler.sample();
    metrics::gauge!("latiq_process_cpu_percent").set(cpu);
    metrics::gauge!("latiq_process_memory_bytes").set(mem as f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_includes_recorded_series() {
        let h = init_recorder();
        metrics::counter!("latiq_test_total", "k" => "v").increment(3);
        metrics::gauge!("latiq_test_gauge").set(7.0);
        metrics::histogram!("latiq_test_seconds").record(0.5);
        let out = h.render();
        assert!(out.contains("latiq_test_total"), "counter missing:\n{out}");
        assert!(out.contains("latiq_test_gauge"), "gauge missing");
        // Histograms render as Prometheus _bucket/_sum/_count series — the type
        // our query-latency metric uses (latiq_pond_query_duration_seconds).
        assert!(
            out.contains("latiq_test_seconds_bucket"),
            "histogram buckets missing:\n{out}"
        );
    }

    #[test]
    fn process_sampler_reports_memory() {
        let (_cpu, mem) = ProcessSampler::new().sample();
        assert!(mem > 0, "process RSS should be > 0");
    }
}
