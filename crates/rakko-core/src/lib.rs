//! Frontend-agnostic core of rakko: everything that talks to Kafka or the Schema
//! Registry, decodes and filters messages, and reads/writes config. No terminal or
//! web types live here — `rakko` (the TUI, repo root) and future frontends depend on
//! this crate and own their own state and rendering.

pub mod config;
pub mod error;
pub mod export;
pub mod kafka;
pub mod message_filter;
pub mod preview;
pub mod query_complete;
pub mod query_filter;
pub mod raw_message;
pub mod ring_buffer;
pub mod serde_detect;
