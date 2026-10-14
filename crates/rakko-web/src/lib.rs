//! rakko's web server: a JSON API over `rakko-core` plus the static frontend.

pub mod details;
pub mod dto;
pub mod error;
pub mod ops;
pub mod routes;
pub mod runtime;
pub mod samples;
pub mod scan;
pub mod state;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use state::AppState;
use tower_http::compression::CompressionLayer;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;

/// The full application: `/api/*` plus (if `static_dir` exists) the single-page frontend.
pub fn build_router(state: Arc<AppState>, static_dir: Option<&Path>) -> Router {
    let mut app = Router::new().nest("/api", routes::api_router());
    if let Some(dir) = static_dir.filter(|d| d.join("index.html").is_file()) {
        let index = ServeFile::new(dir.join("index.html"));
        app = app.fallback_service(ServeDir::new(dir).fallback(index));
    }
    app.with_state(state).layer(CompressionLayer::new()).layer(TraceLayer::new_for_http())
}

/// Periodic housekeeping: drop idle scans and stale autocomplete samples.
pub fn spawn_sweeper(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        loop {
            tick.tick().await;
            let dropped = state.scans.sweep();
            state.samples.sweep();
            if dropped > 0 {
                tracing::info!("dropped {dropped} idle scan(s); {} active", state.scans.len());
            }
        }
    });
}
