//! Shared helpers for the docker-compose-gated integration test tier.
//!
//! Every test that uses these lives behind `#[ignore]` in the relevant `kafka/*.rs`
//! module (alongside that module's regular unit tests) and assumes the stack from the
//! repo-root `docker-compose.yml` is reachable at `localhost:9092` / `localhost:8081`:
//!
//! ```bash
//! docker compose up -d
//! cargo test -- --ignored
//! ```
//!
//! `cargo test` (no args) never runs these, so the default fast/hermetic run is
//! unaffected. See AGENTS.md for the full rationale.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::{AuthMode, Profile};

pub(crate) const BOOTSTRAP: &str = "localhost:9092";
pub(crate) const SCHEMA_REGISTRY_URL: &str = "http://localhost:8081";

/// A PLAINTEXT profile pointed at the local docker-compose stack.
pub(crate) fn local_profile() -> Profile {
    Profile {
        name: "integration-test".into(),
        bootstrap_servers: BOOTSTRAP.into(),
        tls_enabled: false,
        auth: AuthMode::None,
        schema_registry_url: Some(SCHEMA_REGISTRY_URL.into()),
        message_max_bytes: None,
        extra_producer_config: HashMap::new(),
    }
}

/// Process-unique name (topic/group/subject) so `#[ignore]`d tests sharing one broker
/// don't collide, including across concurrent `cargo test -- --ignored` runs.
pub(crate) fn unique_name(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("rakko-it-{prefix}-{}-{nanos}-{n}", std::process::id())
}
