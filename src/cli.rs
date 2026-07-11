use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "kaf-tui", about = "Terminal UI for Kafka cluster monitoring")]
pub struct Cli {
    /// Override the config directory (defaults to ~/.config/kaf-tui).
    #[arg(long, value_name = "PATH")]
    pub config_dir: Option<PathBuf>,

    /// Pre-select a profile by name, skipping the profile picker.
    #[arg(long, value_name = "NAME")]
    pub profile: Option<String>,
}
