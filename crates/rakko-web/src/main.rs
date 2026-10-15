use std::path::PathBuf;

use clap::Parser;
use rakko_core::config;
use rakko_web::state::AppState;

#[derive(Parser)]
#[command(name = "rakko-web", about = "Web UI for rakko")]
struct Args {
    /// Address to listen on.
    #[arg(long, default_value = "0.0.0.0:8080", env = "RAKKO_BIND")]
    bind: String,
    /// rakko config.toml with the cluster profiles (default: ~/.config/rakko/config.toml).
    #[arg(long, env = "RAKKO_CONFIG")]
    config: Option<PathBuf>,
    /// Built frontend to serve (the `web/dist` directory).
    #[arg(long, default_value = "web/dist", env = "RAKKO_STATIC_DIR")]
    static_dir: PathBuf,
    /// Offsets read per partition per scan step.
    #[arg(long, default_value_t = 1000, env = "RAKKO_SCAN_WINDOW")]
    scan_window: usize,
}

#[tokio::main]
async fn main() -> anyhow_free::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,tower_http=info".into()))
        .init();
    let args = Args::parse();

    let path = match args.config {
        Some(p) => p,
        None => config::config_dir()?.join("config.toml"),
    };
    let cfg = config::load(&path)?;
    tracing::info!("loaded {} profile(s) from {}", cfg.profiles.len(), path.display());
    if cfg.profiles.is_empty() {
        tracing::warn!("no profiles configured; the UI will have nothing to connect to");
    }

    let state = AppState::new(cfg, args.scan_window);
    rakko_web::spawn_sweeper(state.clone());
    rakko_web::lag::spawn_sampler(state.clone());
    let app = rakko_web::build_router(state, Some(&args.static_dir));

    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    tracing::info!("rakko-web listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

/// `main` returns a boxed error so `?` works on every error type used above without
/// pulling in `anyhow` for four call sites.
mod anyhow_free {
    pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
}
