//! Per-profile state shared by every session: the Schema Registry cache and its
//! fetch/retry bookkeeping. Schemas are not user data, so one cache serves everyone.

use std::sync::{Mutex, RwLock};
use std::time::Instant;

use rakko_core::config::Profile;
use rakko_core::error::AppError;
use rakko_core::kafka::schema_registry::{fetch_schema, SchemaFetchTracker, SchemaRegistry};
use tokio::runtime::Handle;

pub struct ProfileRuntime {
    pub name: String,
    pub profile: Profile,
    registry: Option<RwLock<SchemaRegistry>>,
    tracker: Mutex<SchemaFetchTracker>,
    /// Serialises fetches so a concurrent caller waits for the in-flight fetch of the
    /// same id instead of seeing "not cached" and treating the message as undecodable.
    fetch_lock: tokio::sync::Mutex<()>,
}

/// Schema Registry ids in a message's key and/or value headers.
pub fn schema_ids(message: &rakko_core::raw_message::RawMessage) -> Vec<u32> {
    let mut ids = Vec::new();
    for bytes in [message.key.as_deref(), message.value.as_deref()].into_iter().flatten() {
        if let Some(id) = rakko_core::serde_detect::avro_schema_id(bytes) {
            ids.push(id);
        }
    }
    ids
}

fn unpoison<T>(r: Result<T, std::sync::PoisonError<T>>) -> T {
    r.unwrap_or_else(|e| e.into_inner())
}

impl ProfileRuntime {
    pub fn new(profile: Profile) -> Self {
        let registry = profile.schema_registry_url.as_deref().and_then(|url| match SchemaRegistry::new(url) {
            Ok(sr) => Some(RwLock::new(sr)),
            Err(err) => {
                tracing::warn!("profile {}: schema registry unavailable: {err}", profile.name);
                None
            }
        });
        Self {
            name: profile.name.clone(),
            profile,
            registry,
            tracker: Mutex::new(SchemaFetchTracker::default()),
            fetch_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn has_registry(&self) -> bool {
        self.registry.is_some()
    }

    /// Runs `f` with the registry cache (or `None` if the profile has no registry).
    pub fn with_registry<R>(&self, f: impl FnOnce(Option<&SchemaRegistry>) -> R) -> R {
        match &self.registry {
            Some(lock) => {
                let guard = unpoison(lock.read());
                f(Some(&*guard))
            }
            None => f(None),
        }
    }

    fn is_cached(&self, id: u32) -> bool {
        self.with_registry(|r| r.is_some_and(|r| r.cached_schema(id).is_some()))
    }

    /// Makes sure schema `id` is cached, fetching it if needed. Returns whether it is
    /// available afterwards (false while a failed fetch is backing off).
    pub async fn ensure_schema(&self, id: u32) -> bool {
        let Some(lock) = &self.registry else { return false };
        if self.is_cached(id) {
            return true;
        }
        let _one_fetch_at_a_time = self.fetch_lock.lock().await;
        if self.is_cached(id) {
            return true;
        }
        {
            let mut tracker = unpoison(self.tracker.lock());
            if !tracker.should_fetch(id, Instant::now()) {
                return false;
            }
            tracker.mark_started(id);
        }
        let base_url = unpoison(lock.read()).base_url().to_string();
        match fetch_schema(&base_url, id).await {
            Ok(schema) => {
                unpoison(lock.write()).insert_cached(id, schema);
                unpoison(self.tracker.lock()).mark_loaded(id);
                true
            }
            Err(err) => {
                let transient = matches!(err, AppError::SchemaRegistryTransient(_));
                tracing::warn!("profile {}: schema id {id} fetch failed: {err}", self.name);
                unpoison(self.tracker.lock()).mark_failed(id, transient, Instant::now());
                false
            }
        }
    }

    pub async fn ensure_schemas(&self, ids: impl IntoIterator<Item = u32>) {
        let mut ids: Vec<u32> = ids.into_iter().collect();
        ids.sort_unstable();
        ids.dedup();
        futures::future::join_all(ids.into_iter().map(|id| self.ensure_schema(id))).await;
    }

    /// [`ensure_schema`](Self::ensure_schema) for a `spawn_blocking` thread.
    pub fn ensure_schema_blocking(&self, handle: &Handle, id: u32) -> bool {
        handle.block_on(self.ensure_schema(id))
    }
}
