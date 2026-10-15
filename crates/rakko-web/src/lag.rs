//! Consumer-group lag over time. Kafka only knows the current lag, so the server samples
//! groups someone has looked at recently and keeps the history in memory (it starts when a
//! group is first viewed and is lost on restart).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rakko_core::kafka::group_offsets::describe_group;
use serde::Serialize;

use crate::state::AppState;

pub const SAMPLE_EVERY: Duration = Duration::from_secs(30);
/// A group nobody has viewed for this long stops being sampled and is forgotten.
const IDLE_TTL: Duration = Duration::from_secs(30 * 60);
/// 24 hours at the sampling interval.
const MAX_SAMPLES: usize = 24 * 60 * 2;
const MAX_SERIES: usize = 64;

#[derive(Debug, Clone, Serialize)]
pub struct LagSample {
    /// Epoch milliseconds.
    pub t: i64,
    pub total: i64,
    /// Lag per topic.
    pub topics: BTreeMap<String, i64>,
}

struct Series {
    samples: VecDeque<LagSample>,
    last_viewed: Instant,
}

#[derive(Default)]
pub struct LagHistory {
    series: Mutex<HashMap<(String, String), Series>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

impl LagHistory {
    /// Marks the group as being watched and returns what has been collected so far.
    pub fn view(&self, profile: &str, group: &str) -> Vec<LagSample> {
        let mut all = lock(&self.series);
        if all.len() >= MAX_SERIES && !all.contains_key(&(profile.to_string(), group.to_string())) {
            if let Some(oldest) = all.iter().min_by_key(|(_, s)| s.last_viewed).map(|(k, _)| k.clone()) {
                all.remove(&oldest);
            }
        }
        let series = all
            .entry((profile.to_string(), group.to_string()))
            .or_insert_with(|| Series { samples: VecDeque::new(), last_viewed: Instant::now() });
        series.last_viewed = Instant::now();
        series.samples.iter().cloned().collect()
    }

    pub fn record(&self, profile: &str, group: &str, sample: LagSample) {
        if let Some(series) = lock(&self.series).get_mut(&(profile.to_string(), group.to_string())) {
            series.samples.push_back(sample);
            while series.samples.len() > MAX_SAMPLES {
                series.samples.pop_front();
            }
        }
    }

    /// Series still being watched; forgets the idle ones.
    fn watched(&self) -> Vec<(String, String)> {
        let mut all = lock(&self.series);
        all.retain(|_, s| s.last_viewed.elapsed() < IDLE_TTL);
        all.keys().cloned().collect()
    }
}

/// Takes one sample of `group` (a describe, as the group page does).
pub async fn sample(state: &AppState, profile: &str, group: &str) -> Option<LagSample> {
    let rt = state.profile(profile)?;
    let d = describe_group(&rt.profile, group).await.ok()?;
    let mut topics: BTreeMap<String, i64> = BTreeMap::new();
    for l in &d.lags {
        *topics.entry(l.topic.clone()).or_default() += l.lag.unwrap_or(0).max(0);
    }
    Some(LagSample { t: now_ms(), total: d.total_lag(), topics })
}

/// Samples every watched group on a fixed cadence.
pub fn spawn_sampler(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(SAMPLE_EVERY);
        loop {
            tick.tick().await;
            for (profile, group) in state.lag.watched() {
                if let Some(s) = sample(&state, &profile, &group).await {
                    state.lag.record(&profile, &group, s);
                }
            }
        }
    });
}
