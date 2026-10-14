//! Decoded sample values per topic, for filter autocomplete. Refreshed lazily (at most
//! once a minute per topic) from the newest few messages of each partition, so a
//! keystroke never touches Kafka.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rakko_core::error::AppResult;
use rakko_core::kafka::consumer::{partition_watermarks, read_seek_page, SeekPageRequest};
use rakko_core::query_complete::Root;
use rakko_core::serde_detect::decode_json_value;
use serde_json::Value;

use crate::runtime::{schema_ids, ProfileRuntime};

const TTL: Duration = Duration::from_secs(60);
const PER_PARTITION: usize = 25;
const MAX_PARTITIONS: usize = 16;
/// Messages bigger than this aren't sampled: the sample only has to show the field
/// names, and decoding a huge payload per refresh isn't worth it.
const MAX_SAMPLE_BYTES: usize = 256 * 1024;

#[derive(Default)]
pub struct Samples {
    keys: Vec<Value>,
    values: Vec<Value>,
}

impl Samples {
    pub fn for_root(&self, root: Root) -> Vec<Value> {
        match root {
            Root::Key => self.keys.clone(),
            Root::Value => self.values.clone(),
        }
    }
}

type Slot = Arc<tokio::sync::Mutex<Option<(Instant, Arc<Samples>)>>>;

#[derive(Default)]
pub struct SampleCache {
    slots: Mutex<HashMap<(String, String), Slot>>,
}

impl SampleCache {
    pub async fn get(&self, rt: &Arc<ProfileRuntime>, topic: &str) -> AppResult<Arc<Samples>> {
        let slot = {
            let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
            slots.entry((rt.name.clone(), topic.to_string())).or_default().clone()
        };
        // Holding the slot for the refresh makes concurrent keystrokes wait for one load.
        let mut guard = slot.lock().await;
        if let Some((at, samples)) = guard.as_ref() {
            if at.elapsed() < TTL {
                return Ok(samples.clone());
            }
        }
        let samples = Arc::new(load(rt, topic).await?);
        *guard = Some((Instant::now(), samples.clone()));
        Ok(samples)
    }

    pub fn sweep(&self) {
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        slots.retain(|_, slot| match slot.try_lock() {
            Ok(g) => g.as_ref().is_some_and(|(at, _)| at.elapsed() < TTL * 10),
            Err(_) => true,
        });
    }
}

async fn load(rt: &Arc<ProfileRuntime>, topic: &str) -> AppResult<Samples> {
    let marks = partition_watermarks(&rt.profile, topic).await?;
    let pages = futures::future::join_all(
        marks
            .iter()
            .filter(|m| m.message_count() > 0)
            .take(MAX_PARTITIONS)
            .map(|m| read_seek_page(&rt.profile, topic, m.partition, SeekPageRequest::Latest { page_size: PER_PARTITION })),
    )
    .await;

    let mut messages = Vec::new();
    // One unreadable partition shouldn't blank the suggestions for the rest.
    for (msgs, _) in pages.into_iter().flatten() {
        messages.extend(msgs);
    }
    messages.retain(|m| {
        m.key.as_ref().map_or(0, Vec::len) <= MAX_SAMPLE_BYTES && m.value.as_ref().map_or(0, Vec::len) <= MAX_SAMPLE_BYTES
    });
    let ids: Vec<u32> = messages.iter().flat_map(schema_ids).collect();
    rt.ensure_schemas(ids).await;

    let rt = rt.clone();
    tokio::task::spawn_blocking(move || {
        rt.with_registry(|reg| {
            let mut out = Samples::default();
            for m in &messages {
                if let Some(v) = m.key.as_deref().and_then(|b| decode_json_value(b, reg)) {
                    out.keys.push(v);
                }
                if let Some(v) = m.value.as_deref().and_then(|b| decode_json_value(b, reg)) {
                    out.values.push(v);
                }
            }
            out
        })
    })
    .await
    .map_err(|e| rakko_core::error::AppError::Other(format!("sample decode panicked: {e}")))
}
