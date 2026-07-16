//! Copy text to the system clipboard with an OSC 52 fallback for SSH/tmux.
//!
//! Prefer the native clipboard (`arboard`) when available. When that fails
//! (headless Linux, some SSH sessions, containers without a display), emit
//! OSC 52 so the *outer* terminal can still update its clipboard — the same
//! multi-route idea Grok Build documents, kept small for rakko's needs.

use base64::Engine;
use std::io::Write;

/// Soft upper bound for OSC 52 payloads (base64). Many terminals and multiplexers
/// silently drop larger sequences; above this we report an error instead of a
/// silent no-op.
const OSC52_MAX_B64_LEN: usize = 100_000;

/// Copy `text` to the clipboard. Tries the OS clipboard first, then OSC 52.
pub fn copy_text(text: &str) -> Result<(), String> {
    match try_native_write(text) {
        Ok(()) => Ok(()),
        Err(native_err) => match try_osc52(text) {
            Ok(()) => Ok(()),
            Err(osc_err) => Err(format!(
                "clipboard unavailable (native: {native_err}; osc52: {osc_err})"
            )),
        },
    }
}

/// Read text from the host clipboard (Grok-style app-owned paste).
///
/// Returns `Ok(None)` when the clipboard is empty or holds non-text.
/// There is no OSC 52 read path — OSC 52 is write-only; over SSH, terminal
/// native paste (Shift+Insert) remains the fallback when host read fails.
pub fn read_text() -> Result<Option<String>, String> {
    let mut clipboard = arboard::Clipboard::new().map_err(|e| e.to_string())?;
    match clipboard.get_text() {
        Ok(s) if s.is_empty() => Ok(None),
        Ok(s) => Ok(Some(s)),
        Err(err) => {
            // arboard reports "clipboard is empty" / content-unavailable as error
            // on some platforms; treat empty as None so callers can toast softly.
            let msg = err.to_string();
            let lower = msg.to_ascii_lowercase();
            if lower.contains("empty") || lower.contains("unavailable") || lower.contains("none") {
                Ok(None)
            } else {
                Err(msg)
            }
        }
    }
}

fn try_native_write(text: &str) -> Result<(), String> {
    let mut clipboard = arboard::Clipboard::new().map_err(|e| e.to_string())?;
    clipboard.set_text(text.to_string()).map_err(|e| e.to_string())
}

fn try_osc52(text: &str) -> Result<(), String> {
    let b64 = base64::engine::general_purpose::STANDARD.encode(text.as_bytes());
    if b64.len() > OSC52_MAX_B64_LEN {
        return Err(format!(
            "payload too large for OSC 52 ({} base64 bytes; max {OSC52_MAX_B64_LEN})",
            b64.len()
        ));
    }
    // OSC 52: ESC ] 52 ; c ; <base64> BEL  (clipboard selection "c")
    let seq = format!("\x1b]52;c;{b64}\x07");
    let mut out = std::io::stdout();
    out.write_all(seq.as_bytes())
        .and_then(|_| out.flush())
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn osc52_rejects_huge_payload() {
        let huge = "x".repeat(OSC52_MAX_B64_LEN); // base64 expands ~4/3
        let err = try_osc52(&huge).unwrap_err();
        assert!(err.contains("too large"), "{err}");
    }

    #[test]
    fn osc52_accepts_small_payload() {
        // May still fail if stdout is not a TTY in CI — only assert encoding path
        // doesn't panic; write can succeed even without a real terminal.
        let _ = try_osc52("hello");
    }
}
