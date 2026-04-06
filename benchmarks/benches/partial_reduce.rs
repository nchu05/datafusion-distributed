/// Benchmarks the partial-reduce pre-aggregation optimization.
///
/// # What it measures
///
/// Measures query wall-time at several `target_partitions` settings over a real (localhost)
/// network with `AggregateExec(PartialReduce)` inserted above the hash `RepartitionExec`
/// in each producer stage.
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::physical_plan::collect as df_collect;
use datafusion_distributed::test_utils::benchmarks_common;
use datafusion_distributed::test_utils::localhost::start_localhost_context;
use datafusion_distributed::test_utils::tpch;
use datafusion_distributed::{
    DefaultSessionBuilder, DistributedConfig, DistributedMetricsFormat,
    display_plan_ascii, rewrite_distributed_plan_with_metrics,
};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::runtime::Builder as RuntimeBuilder;

// ── TPC-H data setup ──────────────────────────────────────────────────────────

/// Scale factor. SF=0.1 ≈ 600K lineitem rows — fast to generate, ≈500 distinct l_suppkeys.
const TPCH_SF: f64 = 0.1;

/// Number of parquet files per table. Must be a multiple of [`N_WORKERS`] so each worker gets
/// an equal share. `TPCH_PARTS / N_WORKERS = 4` files per task → `input_partitions = 4`.
const TPCH_PARTS: i32 = 16;

/// Number of localhost gRPC workers. Must divide [`TPCH_PARTS`] evenly.
const N_WORKERS: usize = 4;

/// Files assigned to each producer task. This is the number of input partitions available
/// to PartialReduce: with 4 files per task, PartialReduce merges 4 partial-aggregate streams
/// into 1 per group key, reducing network traffic by up to 4×.
///
/// Overrides the `integration`-feature default of `files_per_task = 1`, which would make
/// each task read a single file and leave PartialReduce with nothing to merge.
const FILES_PER_TASK: usize = TPCH_PARTS as usize / N_WORKERS;

static TPCH_DATA_DIR: OnceLock<PathBuf> = OnceLock::new();

/// Returns the path to TPC-H data, generating it on first call.
fn ensure_tpch_data() -> &'static PathBuf {
    TPCH_DATA_DIR.get_or_init(|| {
        let data_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("testdata/tpch/benchmark_sf{TPCH_SF}"));
        if !data_dir.exists() {
            eprintln!(
                "Generating TPC-H SF={TPCH_SF} data ({TPCH_PARTS} parts) into {} …",
                data_dir.display()
            );
            tpch::generate_tpch_data(&data_dir, TPCH_SF, TPCH_PARTS);
        }
        data_dir
    })
}

// ── Partition counts ──────────────────────────────────────────────────────────

/// Each entry drives `SessionConfig::with_target_partitions`.
const PARTITION_COUNTS: &[usize] = &[4, 16, 64];

// ── Queries ───────────────────────────────────────────────────────────────────

// ~500 distinct l_suppkey values at Scale Factor=0.1 (5000 at SF=1).
// Each of the 4 input partitions per task contributes up to 500 rows after Partial.
// PartialReduce merges them down to ~500 rows per task before the wire (~3.25×
// row reduction), which is visible in the bytes_transferred metric.
const QUERY_LABEL: &str = "lineitem_suppkey";
const QUERY: &str = "SELECT l_suppkey, COUNT(*) AS cnt, SUM(l_extendedprice) AS revenue \
                     FROM lineitem \
                     GROUP BY l_suppkey \
                     ORDER BY l_suppkey";

// ── Helper: build a fresh session context for one benchmark cell ──────────────

fn make_ctx(
    rt: &tokio::runtime::Runtime,
    data_dir: &Path,
    n_partitions: usize,
) -> (datafusion::prelude::SessionContext, datafusion::common::runtime::JoinSet<()>) {
    let (ctx, guard, _) =
        rt.block_on(start_localhost_context(N_WORKERS, DefaultSessionBuilder));

    // files_per_task controls how many parquet files each producer task reads.
    // Setting it to FILES_PER_TASK = TPCH_PARTS / N_WORKERS ensures 1 task per worker,
    // each reading FILES_PER_TASK files → input_partitions = FILES_PER_TASK.
    // The integration-mode default of files_per_task=1 would give 16 tasks each reading
    // 1 file, leaving PartialReduce with a single input partition and nothing to merge.
    {
        let state = ctx.state_ref();
        let mut write_guard = state.write();
        let cfg = DistributedConfig::from_config_options_mut(
            write_guard.config_mut().options_mut(),
        )
        .unwrap();
        cfg.files_per_task = FILES_PER_TASK;
    }

    ctx.state_ref()
        .write()
        .config_mut()
        .options_mut()
        .execution
        .target_partitions = n_partitions;

    rt.block_on(benchmarks_common::register_tables(&ctx, data_dir))
        .unwrap();

    (ctx, guard)
}

// ── Timing benchmark ──────────────────────────────────────────────────────────

fn bench_partial_reduce(c: &mut Criterion) {
    let data_dir = ensure_tpch_data();

    let rt = RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("partial_reduce");
    group.sample_size(10);

    for &n_partitions in PARTITION_COUNTS {
        let (ctx, _guard) = make_ctx(&rt, data_dir, n_partitions);

        let bench_id = BenchmarkId::new(QUERY_LABEL, format!("N={n_partitions}"));

        group.bench_function(bench_id, |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    rt.block_on(async {
                        ctx.sql(QUERY).await.unwrap().collect().await.unwrap()
                    });
                    total += start.elapsed();
                }
                total
            });
        });
    }

    group.finish();

    // Print the data-reduction metrics for the lineitem_suppkey query at N=4.
    // Timing on localhost can't show network savings because loopback bandwidth makes even
    // large byte-count differences sub-millisecond. The metrics — especially `output_rows`
    // at PartialReduce and `bytes_transferred` at NetworkShuffleExec — directly show whether
    // the optimization is reducing data before the wire.
    print_metrics(&rt, data_dir);
}

// ── Metrics ───────────────────────────────────────────────────────────────────

/// Runs the `lineitem_suppkey` query once with N=4, then prints the distributed plan
/// annotated with per-node metrics from all tasks.
///
/// Look for:
/// - `output_rows` at `AggregateExec(mode=Partial)` vs `AggregateExec(mode=PartialReduce)`:
///   the ratio should be close to `input_partitions` (= [`FILES_PER_TASK`] = 4).
/// - `bytes_transferred` at `NetworkShuffleExec`: reduced relative to the row count savings.
fn print_metrics(rt: &tokio::runtime::Runtime, data_dir: &Path) {
    let n_partitions = 4;

    println!(
        "\n\
         ════════════════════════════════════════════════════════════════════\n\
         Data-reduction metrics for `{QUERY_LABEL}` (N={n_partitions})\n\
         \n\
         Key nodes to inspect:\n\
           • AggregateExec(mode=Partial):       rows entering the network stage\n\
           • AggregateExec(mode=PartialReduce): rows after pre-aggregation (should be ~4× fewer)\n\
           • NetworkShuffleExec → bytes_transferred: bytes actually sent over the wire\n\
         ════════════════════════════════════════════════════════════════════"
    );

    let (ctx, _guard) = make_ctx(rt, data_dir, n_partitions);

    let plan = rt
        .block_on(async { ctx.sql(QUERY).await?.create_physical_plan().await })
        .expect("plan creation");

    rt.block_on(df_collect(plan.clone(), ctx.task_ctx()))
        .expect("execution");

    let plan_with_metrics =
        rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated)
            .expect("metrics rewrite");

    println!("{}", display_plan_ascii(plan_with_metrics.as_ref(), true));
}

criterion_group!(benches, bench_partial_reduce);
criterion_main!(benches);
