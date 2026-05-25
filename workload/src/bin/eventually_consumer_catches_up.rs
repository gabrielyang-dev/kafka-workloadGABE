use anyhow::{Context, Result};
use antithesis_kafka_workload::config::WorkloadConfig;
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    ClientConfig, Offset, TopicPartitionList,
};
use serde_json::json;
use std::{collections::HashMap, env, time::{Duration, Instant}};
use tokio::time::sleep;
use tracing::{info, level_filters::LevelFilter, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, Layer, Registry};

/// A single (group, topic, partition) tuple where the consumer is behind the
/// high-water mark. Both HWM and committed_offset use Kafka's "next offset"
/// semantics, so lag = hwm - committed_offset and 0 means fully caught up.
#[derive(Debug, serde::Serialize)]
struct PartitionLag {
    group_id: String,
    topic: String,
    partition: i32,
    committed_offset: i64,
    high_water_mark: i64,
    lag: i64,
}

fn setup_logging() -> Result<()> {
    let global_log_subscriber =
        Registry::default().with(fmt::layer().json().with_filter(LevelFilter::INFO));
    tracing::subscriber::set_global_default(global_log_subscriber)
        .context("failed to register global log subscriber")?;
    Ok(())
}

/// Wait for all Kafka brokers to respond to a metadata request, with a hard
/// timeout. Returns true when the cluster reports at least 3 brokers; false
/// if the timeout elapses first. Retries with exponential backoff (capped 10 s).
///
/// Note: TestAdminClient::wait_on_cluster loops indefinitely — we need a
/// bounded timeout here so we can skip assertions when the cluster truly cannot
/// recover, which is what the "or_unreachable" part of assert_always_or_unreachable
/// is designed for.
async fn wait_for_cluster_health(bootstrap_servers: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    let mut attempt: u32 = 0;

    loop {
        attempt += 1;

        let healthy: bool = (|| -> bool {
            let consumer: BaseConsumer = match ClientConfig::new()
                .set("bootstrap.servers", bootstrap_servers)
                .set("group.id", "antithesis-drain-check")
                .create()
            {
                Ok(c) => c,
                Err(err) => {
                    warn!(
                        timestamp = chrono::Utc::now()
                            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                        event = "cluster_health_check_error",
                        attempt = attempt,
                        error = format!("{:#}", err).as_str(),
                        "failed to create consumer for health check"
                    );
                    return false;
                }
            };
            match consumer.fetch_metadata(None, Duration::from_secs(10)) {
                Ok(meta) => {
                    info!(
                        timestamp = chrono::Utc::now()
                            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                        event = "cluster_health_check",
                        attempt = attempt,
                        broker_count = meta.brokers().len(),
                        elapsed_secs = start.elapsed().as_secs(),
                        "cluster health probe"
                    );
                    meta.brokers().len() >= 3
                }
                Err(err) => {
                    warn!(
                        timestamp = chrono::Utc::now()
                            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                        event = "cluster_health_check_error",
                        attempt = attempt,
                        elapsed_secs = start.elapsed().as_secs(),
                        error = format!("{:#}", err).as_str(),
                        "metadata fetch failed during health check"
                    );
                    false
                }
            }
        })();

        if healthy {
            info!(
                timestamp = chrono::Utc::now()
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                event = "cluster_healthy",
                attempts = attempt,
                elapsed_secs = start.elapsed().as_secs(),
                "cluster is healthy"
            );
            return true;
        }

        if start.elapsed() >= timeout {
            warn!(
                timestamp = chrono::Utc::now()
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                event = "cluster_health_timeout",
                attempts = attempt,
                timeout_secs = timeout.as_secs(),
                "cluster did not become healthy within timeout"
            );
            return false;
        }

        // Exponential backoff, capped at 10 s to avoid long idle periods.
        let backoff_secs = 10u64.min(2u64.saturating_pow(attempt - 1));
        sleep(Duration::from_secs(backoff_secs)).await;
    }
}

/// Query consumer lag for every Stable consumer group across all application topics.
///
/// Returns one PartitionLag entry per (group, topic, partition) tuple where the
/// committed offset is behind the high-water mark. An empty Vec means all groups
/// are fully caught up (or there are no application topics / no Stable groups).
///
/// Groups in non-Stable states (Dead, Empty, PreparingRebalance, …) are skipped
/// because they are typically stale groups from prior workload runs whose lag
/// will never converge — treating them as lagging would produce false failures.
fn query_consumer_lag(bootstrap_servers: &str) -> Result<Vec<PartitionLag>> {
    // ── Enumerate application topics and their high-water marks ──────────────
    let meta_consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", "antithesis-drain-check")
        .create()
        .context("failed to create metadata consumer")?;

    let metadata = meta_consumer
        .fetch_metadata(None, Duration::from_secs(15))
        .context("failed to fetch cluster metadata")?;

    let mut tpl = TopicPartitionList::new();
    let mut hwms: HashMap<(String, i32), i64> = HashMap::new();

    for topic in metadata.topics() {
        // Skip internal Kafka topics (__consumer_offsets, __transaction_state, etc.).
        if topic.name().starts_with('_') {
            continue;
        }
        for partition in topic.partitions() {
            tpl.add_partition(topic.name(), partition.id());
            match meta_consumer.fetch_watermarks(
                topic.name(),
                partition.id(),
                Duration::from_secs(10),
            ) {
                Ok((_low, high)) => {
                    hwms.insert((topic.name().to_string(), partition.id()), high);
                }
                Err(err) => {
                    warn!(
                        timestamp = chrono::Utc::now()
                            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                        event = "watermark_fetch_failed",
                        topic = topic.name(),
                        partition = partition.id(),
                        error = format!("{:#}", err).as_str(),
                        "could not fetch watermark for partition; skipping"
                    );
                }
            }
        }
    }

    if tpl.count() == 0 {
        info!(
            timestamp = chrono::Utc::now()
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            event = "no_application_topics",
            "no application topics found in cluster"
        );
        return Ok(vec![]);
    }

    info!(
        timestamp = chrono::Utc::now()
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        event = "topics_enumerated",
        topic_partition_count = tpl.count(),
        "enumerated application topics and partitions"
    );

    // ── Enumerate consumer groups, keeping only Stable ones ──────────────────
    let groups = meta_consumer
        .fetch_group_list(None, Duration::from_secs(15))
        .context("failed to list consumer groups")?;

    let mut all_lags: Vec<PartitionLag> = Vec::new();

    for group in groups.groups() {
        // Skip internal groups, our own ephemeral query group, and non-Stable
        // groups (Dead, Empty, PreparingRebalance) which may be stale from prior
        // workload runs and will never have their lag converge.
        if group.name().starts_with('_')
            || group.name() == "antithesis-drain-check"
            || group.state() != "Stable"
        {
            continue;
        }

        // ── Query committed offsets for this group ────────────────────────────
        let group_consumer: BaseConsumer = match ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", group.name())
            .create()
        {
            Ok(c) => c,
            Err(err) => {
                warn!(
                    timestamp = chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    event = "group_consumer_create_failed",
                    group_id = group.name(),
                    error = format!("{:#}", err).as_str(),
                    "failed to create consumer for group; skipping"
                );
                continue;
            }
        };

        if let Err(err) = group_consumer.assign(&tpl) {
            warn!(
                timestamp = chrono::Utc::now()
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                event = "group_assign_failed",
                group_id = group.name(),
                error = format!("{:#}", err).as_str(),
                "failed to assign partitions for group; skipping"
            );
            continue;
        }

        let committed = match group_consumer.committed(Duration::from_secs(15)) {
            Ok(c) => c,
            Err(err) => {
                warn!(
                    timestamp = chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    event = "committed_fetch_failed",
                    group_id = group.name(),
                    error = format!("{:#}", err).as_str(),
                    "failed to fetch committed offsets for group; skipping"
                );
                continue;
            }
        };

        for elem in committed.elements() {
            // Offset::Invalid (-1001) or Offset::Beginning (-2) means the group
            // has never committed for this partition — skip it.
            if let Offset::Offset(committed_offset) = elem.offset() {
                // Both hwm ("next offset to write") and committed_offset ("next offset
                // to fetch") use the same "next" semantics, so lag = hwm - committed_offset
                // and 0 means the consumer has read everything the producer wrote.
                let hwm = match hwms.get(&(elem.topic().to_string(), elem.partition())) {
                    Some(&h) => h,
                    None => {
                        warn!(
                            timestamp = chrono::Utc::now()
                                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                            event = "missing_hwm",
                            topic = elem.topic(),
                            partition = elem.partition(),
                            group_id = group.name(),
                            "no high-water mark recorded for partition; skipping"
                        );
                        continue;
                    }
                };
                let lag = hwm - committed_offset;
                if lag > 0 {
                    all_lags.push(PartitionLag {
                        group_id: group.name().to_string(),
                        topic: elem.topic().to_string(),
                        partition: elem.partition(),
                        committed_offset,
                        high_water_mark: hwm,
                        lag,
                    });
                }
            }
        }
    }

    info!(
        timestamp = chrono::Utc::now()
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        event = "lag_query_complete",
        lagging_partition_count = all_lags.len(),
        total_lag = all_lags.iter().map(|l| l.lag).sum::<i64>(),
        "consumer lag query complete"
    );

    Ok(all_lags)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    antithesis_sdk::antithesis_init();
    setup_logging()?;

    let args: Vec<String> = env::args().collect();
    let config_path = args.get(1).expect("no config path provided");
    let config = WorkloadConfig::new(config_path).context("failed to read configuration file")?;
    let bootstrap_servers = &config.bootstrap_servers;

    info!(
        timestamp = chrono::Utc::now()
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        event = "drain_check_started",
        bootstrap_servers = bootstrap_servers.as_str(),
        "eventually_consumer_catches_up started"
    );

    // ── Phase 1: Wait for the cluster to become reachable ────────────────────
    // Antithesis pauses fault injection before invoking an `eventually` command,
    // but brokers take time to become operational again after faults stop.
    // We retry with exponential backoff for up to 120 s.
    let cluster_healthy =
        wait_for_cluster_health(bootstrap_servers, Duration::from_secs(120)).await;

    // Bounded timeout keeps the eventually command from hanging forever.
    // If the cluster does not recover, the command reports that explicitly
    // rather than silently skipping the liveness property.
    if !cluster_healthy {
        info!(
            timestamp = chrono::Utc::now()
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            event = "drain_check_aborted",
            reason = "cluster_unreachable",
            "cluster did not recover within timeout"
        );

        antithesis_sdk::assert_always_or_unreachable!(
            false,
            "Kafka cluster recovered before consumer drain check",
            &json!({
                "bootstrap_servers": bootstrap_servers,
                "cluster_health_timeout_secs": 120,
                "reason": "cluster_unreachable"
            })
        );

        return Ok(());
    }

    // Assert 1: confirms the command was scheduled and the cluster recovered.
    antithesis_sdk::assert_reachable!(
        "Eventually command reached drain check",
        &json!({"bootstrap_servers": bootstrap_servers})
    );

    // Phase 2: Wait for consumers to drain 
    let initial_lags = query_consumer_lag(bootstrap_servers)
        .context("failed to query initial consumer lag")?;

    info!(
        timestamp = chrono::Utc::now()
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        event = "initial_lag_sampled",
        initial_lag_total = initial_lags.iter().map(|l| l.lag).sum::<i64>(),
        lagging_partition_count = initial_lags.len(),
        "initial consumer lag snapshot taken"
    );

    let drain_timeout = Duration::from_secs(120);
    let drain_start = Instant::now();
    let mut current_lags = initial_lags;
    let mut drain_attempt: u32 = 0;

    loop {
        let total_lag: i64 = current_lags.iter().map(|l| l.lag).sum();

        // Assert 2: guard against trivially-empty runs. Antithesis aggregates
        // assert_sometimes across the entire test session — any single iteration
        // where lag > 0 counts as a hit. Firing it here (inside the loop, while
        // lag is observed) correctly captures "chaos caused real consumer backlog
        // at least once," without the timing race of sampling after the cluster
        // has already had up to 120 s to recover.
        if total_lag > 0 {
            antithesis_sdk::assert_sometimes!(
                true,
                "Consumer groups had outstanding lag during drain check",
                &json!({
                    "observed_lag": total_lag,
                    "lagging_partition_count": current_lags.len()
                })
            );
        }

        if total_lag == 0 {
            info!(
                timestamp = chrono::Utc::now()
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                event = "drain_complete",
                drain_attempts = drain_attempt,
                "all consumer groups have caught up to their high-water marks"
            );
            break;
        }

        if drain_start.elapsed() >= drain_timeout {
            info!(
                timestamp = chrono::Utc::now()
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                event = "drain_timeout",
                drain_attempts = drain_attempt,
                remaining_lag = total_lag,
                lagging_partition_count = current_lags.len(),
                "drain wait timed out with outstanding consumer lag"
            );
            break;
        }

        drain_attempt += 1;
        sleep(Duration::from_secs(5)).await;

        info!(
            timestamp = chrono::Utc::now()
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            event = "drain_check_attempt",
            attempt = drain_attempt,
            previous_lag = total_lag,
            "re-checking consumer lag"
        );

        current_lags = match query_consumer_lag(bootstrap_servers) {
            Ok(lags) => lags,
            Err(err) => {
                warn!(
                    timestamp = chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    event = "lag_query_failed",
                    error = format!("{:#}", err).as_str(),
                    "failed to re-query consumer lag; retaining previous reading"
                );
                current_lags
            }
        };
    }

    // Phase 3: Assert liveness convergence 
    let final_lag_total: i64 = current_lags.iter().map(|l| l.lag).sum();
    let all_caught_up = final_lag_total == 0;

    info!(
        timestamp = chrono::Utc::now()
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        event = "drain_check_complete",
        all_caught_up = all_caught_up,
        final_lag_total = final_lag_total,
        lagging_partition_count = current_lags.len(),
        "drain check completed"
    );

    // Assert 3: headline liveness property. Every Stable consumer group must
    // reach the topic high-water marks after fault injection stops.
    // always_or_unreachable (not always) so a genuinely unrecoverable cluster
    // is reported via the recovery assertion above, not as a violation here.
    antithesis_sdk::assert_always_or_unreachable!(
        all_caught_up,
        "All consumer groups drained the topic after chaos",
        &json!({
            "final_lag_total": final_lag_total,
            "lagging_partitions": current_lags
        })
    );

    Ok(())
}
