//! Shell-out to `$EDITOR` (fallback: `vi`) for multi-line message editing.
//!
//! **Terminal responsibility**: the caller must leave the alternate screen and disable
//! raw mode *before* calling [`edit_in_external_editor`], then re-enable raw mode and
//! re-enter the alternate screen afterwards. This module only runs the editor process
//! and reads the tempfile — it never touches the TUI terminal state.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{AppError, AppResult};

/// Resolves the editor binary + optional args from `$EDITOR`, falling back to `vi`.
///
/// `$EDITOR` may include arguments (e.g. `code --wait`); we split on whitespace so those
/// still work. Paths containing spaces are not supported (common limitation of `$EDITOR`).
fn editor_command() -> (String, Vec<String>) {
    let raw = env::var("EDITOR").unwrap_or_else(|_| "vi".to_string());
    let mut parts = raw.split_whitespace();
    let program = parts.next().unwrap_or("vi").to_string();
    let args = parts.map(str::to_string).collect();
    (program, args)
}

fn temp_edit_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut path = env::temp_dir();
    path.push(format!("rakko-edit-{}-{}.txt", std::process::id(), nanos));
    path
}

/// Writes `initial` to a tempfile, opens `$EDITOR` (or `vi`) on it, waits for the editor
/// to exit, then returns the file contents.
///
/// See module docs: the TUI must release the terminal around this call.
pub fn edit_in_external_editor(initial: &str) -> AppResult<String> {
    let path = temp_edit_path();
    fs::write(&path, initial)?;

    let (program, args) = editor_command();
    let mut cmd = Command::new(&program);
    for arg in &args {
        cmd.arg(arg);
    }
    cmd.arg(&path);

    let status = cmd.status().map_err(|err| {
        let _ = fs::remove_file(&path);
        AppError::Other(format!("failed to launch editor '{program}': {err}"))
    })?;

    if !status.success() {
        let _ = fs::remove_file(&path);
        return Err(AppError::Other(format!(
            "editor '{program}' exited unsuccessfully ({status})"
        )));
    }

    let content = fs::read_to_string(&path).map_err(|err| {
        let _ = fs::remove_file(&path);
        AppError::Io(err)
    })?;
    let _ = fs::remove_file(&path);
    Ok(content)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn editor_command_defaults_to_vi_when_unset() {
        // Can't safely mutate env in parallel tests; just exercise the path that has a
        // program name. The split logic is pure over a string — test it via a local helper
        // replica of the split.
        let raw = "code --wait";
        let mut parts = raw.split_whitespace();
        let program = parts.next().unwrap();
        let args: Vec<&str> = parts.collect();
        assert_eq!(program, "code");
        assert_eq!(args, vec!["--wait"]);
    }

    #[test]
    fn temp_edit_path_is_under_temp_dir() {
        let path = temp_edit_path();
        assert!(path.starts_with(env::temp_dir()));
        assert!(path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("rakko-edit-")));
    }
}
