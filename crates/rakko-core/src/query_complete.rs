//! Autocomplete for the structured query filter (`key.a.b = "x" AND value.c > 1`).
//!
//! Pure: the caller supplies sample values (decoded messages) and gets back the
//! candidates for the token at the end of the input. The terminal and web frontends
//! both call [`suggest`]; each keeps its own presentation (Tab cycling vs a dropdown).

use std::collections::BTreeSet;

use serde_json::Value;

/// Which side of the record a path starts at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Root {
    Key,
    Value,
}

impl Root {
    pub fn as_str(self) -> &'static str {
        match self {
            Root::Key => "key",
            Root::Value => "value",
        }
    }
}

/// Candidates for the token at the end of the input. A frontend replaces the trailing
/// token with `prefix + candidate`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suggestions {
    /// Text that stays in front of the completed segment (everything before the token,
    /// plus the already-typed path segments).
    pub prefix: String,
    /// Sorted, deduped candidate segments (root words or field names).
    pub candidates: Vec<String>,
}

/// Child field names reachable from `value` at `path`, transparently fanning out over
/// arrays exactly like `query_filter`'s path evaluation does — so what's offered here
/// always matches what would actually work if typed. `path` empty at an object yields
/// that object's own keys; arrays never consume a path segment; leaves have no children.
pub fn candidate_children(value: &Value, path: &[String]) -> BTreeSet<String> {
    match value {
        Value::Array(items) => {
            let mut out = BTreeSet::new();
            for item in items {
                out.extend(candidate_children(item, path));
            }
            out
        }
        Value::Object(map) => match path.split_first() {
            Some((head, rest)) => map
                .get(head)
                .map(|next| candidate_children(next, rest))
                .unwrap_or_default(),
            None => map.keys().cloned().collect(),
        },
        _leaf => BTreeSet::new(),
    }
}

/// What a trailing path-like token is asking to complete.
enum Target {
    /// Still typing the root word itself (`val` → `key`/`value`).
    Root,
    /// Completing one path segment under `root`, `path_so_far` deep, with `partial`
    /// typed so far for the segment being completed (may be empty after a trailing `.`).
    Path { root: Root, path_so_far: Vec<String>, partial: String },
}

/// The path-like token at the very end of `input` (letters/digits/underscore/dot) —
/// e.g. `"value.hou"` out of `"key.x = 1 AND value.hou"`.
pub fn trailing_path_token(input: &str) -> &str {
    let is_path_char = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '.';
    match input.char_indices().rev().find(|(_, c)| !is_path_char(*c)) {
        Some((i, c)) => &input[i + c.len_utf8()..],
        None => input,
    }
}

/// True where a new condition may begin: empty input, or right after `AND`/`OR`/`(`.
fn starts_expression(input: &str) -> bool {
    let trimmed = input.trim_end();
    if trimmed.is_empty() {
        return true;
    }
    // A condition still being typed (`value.x =`) must not offer roots.
    if trimmed.len() == input.len() && !trimmed.ends_with('(') {
        return false;
    }
    let last = trimmed.rsplit(|c: char| c.is_whitespace() || c == '(').next().unwrap_or("");
    trimmed.ends_with('(') || last.eq_ignore_ascii_case("and") || last.eq_ignore_ascii_case("or")
}

/// `None` means "not a key/value path at all" (an empty token, or a root word that isn't
/// `key`/`value`).
fn classify(token: &str) -> Option<Target> {
    if token.is_empty() {
        return None;
    }
    match token.split_once('.') {
        None => Some(Target::Root),
        Some((root, rest)) => {
            let root = match root {
                "key" => Root::Key,
                "value" => Root::Value,
                _ => return None,
            };
            let mut segments: Vec<String> = rest.split('.').map(str::to_string).collect();
            let partial = segments.pop().unwrap_or_default();
            Some(Target::Path { root, path_so_far: segments, partial })
        }
    }
}

/// Candidates for the token at the end of `input`, or `None` if there is nothing to
/// complete there. `samples` is asked for decoded values on the key or value side only
/// when a path under that root is being completed.
pub fn suggest(input: &str, mut samples: impl FnMut(Root) -> Vec<Value>) -> Option<Suggestions> {
    let token = trailing_path_token(input);
    if token.is_empty() && starts_expression(input) {
        return Some(Suggestions {
            prefix: input.to_string(),
            candidates: vec!["key".to_string(), "value".to_string()],
        });
    }
    let target = classify(token)?;
    let outer_prefix = &input[..input.len() - token.len()];

    Some(match target {
        Target::Root => Suggestions {
            prefix: outer_prefix.to_string(),
            candidates: ["key", "value"]
                .into_iter()
                .filter(|r| r.starts_with(token))
                .map(str::to_string)
                .collect(),
        },
        Target::Path { root, path_so_far, partial } => {
            let mut set = BTreeSet::new();
            for decoded in samples(root) {
                set.extend(candidate_children(&decoded, &path_so_far));
            }
            let mut prefix = format!("{outer_prefix}{}.", root.as_str());
            for segment in &path_so_far {
                prefix.push_str(segment);
                prefix.push('.');
            }
            Suggestions {
                prefix,
                candidates: set.into_iter().filter(|c| c.starts_with(partial.as_str())).collect(),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn value_samples(v: Vec<Value>) -> impl FnMut(Root) -> Vec<Value> {
        move |root| if root == Root::Value { v.clone() } else { vec![] }
    }

    #[test]
    fn roots_offered_where_a_condition_can_start() {
        for input in ["", "value.a = 1 AND ", "value.a = 1 or ", "(", "value.a = 1 AND ("] {
            let s = suggest(input, |_| vec![]).unwrap_or_else(|| panic!("{input:?}"));
            assert_eq!(s.prefix, input);
            assert_eq!(s.candidates, vec!["key", "value"]);
        }
        for input in ["value.a ", "value.a = ", "value.a = 1 ", "value.a = 1 AND value.b "] {
            assert!(suggest(input, |_| vec![]).is_none(), "{input:?}");
        }
    }

    #[test]
    fn root_word_completes_to_key_and_value() {
        let s = suggest("val", |_| vec![]).unwrap();
        assert_eq!(s.prefix, "");
        assert_eq!(s.candidates, vec!["value"]);
        let s = suggest("k", |_| vec![]).unwrap();
        assert_eq!(s.candidates, vec!["key"]);
    }

    #[test]
    fn path_segments_come_from_samples_filtered_by_partial() {
        let s = suggest(
            "value.o",
            value_samples(vec![json!({"order_id": 1, "owner": "a", "tags": []})]),
        )
        .unwrap();
        assert_eq!(s.prefix, "value.");
        assert_eq!(s.candidates, vec!["order_id", "owner"]);
    }

    #[test]
    fn trailing_dot_lists_every_child_and_nested_paths_keep_their_prefix() {
        let sample = json!({"customer": {"name": "a", "tier": "gold"}});
        let s = suggest("key.x = 1 AND value.customer.", value_samples(vec![sample])).unwrap();
        assert_eq!(s.prefix, "key.x = 1 AND value.customer.");
        assert_eq!(s.candidates, vec!["name", "tier"]);
    }

    #[test]
    fn arrays_are_transparent_and_candidates_are_merged_across_samples() {
        let samples = vec![
            json!({"items": [{"sku": "a"}, {"sku": "b", "qty": 1}]}),
            json!({"items": [{"note": "x"}]}),
        ];
        let s = suggest("value.items.", value_samples(samples)).unwrap();
        assert_eq!(s.candidates, vec!["note", "qty", "sku"]);
    }

    #[test]
    fn not_a_path_or_unknown_root_yields_nothing() {
        assert!(suggest("value.a = ", |_| vec![]).is_none());
        assert!(suggest("other.a", |_| vec![]).is_none());
    }
}
