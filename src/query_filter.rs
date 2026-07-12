//! Advanced structured filter for the message browser: `key.a.b = "x" AND value.c != 5`.
//!
//! Parses to a small AST and evaluates against the decoded Avro/JSON
//! `serde_json::Value` for a message's key/value (see
//! `serde_detect::decode_json_value`). Separate from the plain substring filter (`/`)
//! — the two are independent and, when both applied, AND-combined by the caller.

use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Root {
    Key,
    Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Eq,
    Ne,
}

#[derive(Debug, Clone, PartialEq)]
enum Literal {
    Str(String),
    Num(f64),
    Bool(bool),
}

#[derive(Debug, Clone)]
struct Condition {
    root: Root,
    path: Vec<String>,
    op: Op,
    literal: Literal,
}

/// A parsed, AND-combined set of conditions plus the original text (for display in the
/// topic-detail header).
#[derive(Debug, Clone)]
pub struct QueryFilter {
    pub raw: String,
    conditions: Vec<Condition>,
}

impl QueryFilter {
    /// True if every condition matches. `key`/`value` are the structured decode of the
    /// message's key/value bytes — `None` when not JSON/Avro-decodable (raw text, or an
    /// Avro payload whose schema isn't cached yet), in which case every condition
    /// against that root is treated as "field not found" (see `path_matches`).
    pub fn matches(&self, key: Option<&Value>, value: Option<&Value>) -> bool {
        self.conditions.iter().all(|c| {
            let root_value = match c.root {
                Root::Key => key,
                Root::Value => value,
            };
            let eq = root_value.is_some_and(|v| path_matches(v, &c.path, &c.literal));
            match c.op {
                Op::Eq => eq,
                Op::Ne => !eq,
            }
        })
    }
}

/// Recursively resolves `path` against `value`, fanning out over arrays at any depth —
/// matches if ANY element along the way satisfies the rest of the path (same implicit
/// semantics as MongoDB's dot-notation array matching), so a path can walk through any
/// number of nested arrays without index syntax.
fn path_matches(value: &Value, path: &[String], literal: &Literal) -> bool {
    match value {
        Value::Array(items) => items.iter().any(|item| path_matches(item, path, literal)),
        Value::Object(map) => match path.split_first() {
            Some((head, rest)) => map.get(head).is_some_and(|next| path_matches(next, rest, literal)),
            None => false, // path exhausted at an object — nothing to compare against
        },
        leaf => path.is_empty() && literal_matches(leaf, literal),
    }
}

fn literal_matches(value: &Value, literal: &Literal) -> bool {
    match (value, literal) {
        (Value::String(s), Literal::Str(l)) => s.to_lowercase() == l.to_lowercase(),
        (Value::Number(n), Literal::Num(l)) => {
            n.as_f64().is_some_and(|v| (v - l).abs() < 1e-9)
        }
        (Value::Bool(b), Literal::Bool(l)) => b == l,
        _ => false,
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Word(String),
    Str(String),
    Eq,
    Ne,
}

fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let chars: Vec<char> = input.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c == '"' {
            let mut s = String::new();
            i += 1;
            let mut closed = false;
            while i < chars.len() {
                let ch = chars[i];
                if ch == '\\' && i + 1 < chars.len() {
                    s.push(chars[i + 1]);
                    i += 2;
                    continue;
                }
                if ch == '"' {
                    closed = true;
                    i += 1;
                    break;
                }
                s.push(ch);
                i += 1;
            }
            if !closed {
                return Err("unterminated string literal".to_string());
            }
            tokens.push(Token::Str(s));
            continue;
        }
        if c == '!' && chars.get(i + 1) == Some(&'=') {
            tokens.push(Token::Ne);
            i += 2;
            continue;
        }
        if c == '=' {
            tokens.push(Token::Eq);
            i += 1;
            continue;
        }
        if c.is_alphanumeric() || c == '_' || c == '-' || c == '.' {
            let start = i;
            while i < chars.len() {
                let ch = chars[i];
                if ch.is_alphanumeric() || ch == '_' || ch == '-' || ch == '.' {
                    i += 1;
                } else {
                    break;
                }
            }
            tokens.push(Token::Word(chars[start..i].iter().collect()));
            continue;
        }
        return Err(format!("unexpected character '{c}'"));
    }
    Ok(tokens)
}

/// Parses `key.a.b = "x" AND value.c != 5`-style queries. Errors are short and
/// user-facing (shown in the status line while the query-filter wizard is open).
pub fn parse(input: &str) -> Result<QueryFilter, String> {
    let raw = input.trim().to_string();
    if raw.is_empty() {
        return Err("empty query".to_string());
    }
    let tokens = tokenize(&raw)?;
    let mut pos = 0;
    let mut conditions = Vec::new();
    loop {
        conditions.push(parse_condition(&tokens, &mut pos)?);
        match tokens.get(pos) {
            None => break,
            Some(Token::Word(w)) if w.eq_ignore_ascii_case("and") => {
                pos += 1;
            }
            Some(other) => return Err(format!("expected AND, got {other:?}")),
        }
    }
    Ok(QueryFilter { raw, conditions })
}

fn parse_condition(tokens: &[Token], pos: &mut usize) -> Result<Condition, String> {
    let (root, path) = match tokens.get(*pos) {
        Some(Token::Word(w)) => parse_path(w)?,
        _ => return Err("expected a field path (e.g. key.name)".to_string()),
    };
    *pos += 1;

    let op = match tokens.get(*pos) {
        Some(Token::Eq) => Op::Eq,
        Some(Token::Ne) => Op::Ne,
        _ => return Err("expected '=' or '!='".to_string()),
    };
    *pos += 1;

    let literal = match tokens.get(*pos) {
        Some(Token::Str(s)) => Literal::Str(s.clone()),
        Some(Token::Word(w)) => parse_bareword_literal(w),
        _ => return Err("expected a value after the operator".to_string()),
    };
    *pos += 1;

    Ok(Condition { root, path, op, literal })
}

fn parse_path(s: &str) -> Result<(Root, Vec<String>), String> {
    let mut parts = s.split('.');
    let root_str = parts.next().filter(|s| !s.is_empty()).ok_or("empty field path")?;
    let root = match root_str.to_ascii_lowercase().as_str() {
        "key" => Root::Key,
        "value" => Root::Value,
        _ => {
            return Err(format!(
                "field path must start with 'key.' or 'value.', got '{root_str}'"
            ))
        }
    };
    let path: Vec<String> = parts.map(str::to_string).collect();
    if path.is_empty() || path.iter().any(String::is_empty) {
        return Err(format!("'{s}' needs at least one field after '{root_str}.'"));
    }
    Ok((root, path))
}

fn parse_bareword_literal(word: &str) -> Literal {
    if word.eq_ignore_ascii_case("true") {
        return Literal::Bool(true);
    }
    if word.eq_ignore_ascii_case("false") {
        return Literal::Bool(false);
    }
    if let Ok(n) = word.parse::<f64>() {
        return Literal::Num(n);
    }
    Literal::Str(word.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn matches_simple_string_field() {
        let q = parse(r#"key.name = jxhui"#).unwrap();
        let key = json!({"name": "jxhui"});
        assert!(q.matches(Some(&key), None));
        let other = json!({"name": "someone-else"});
        assert!(!q.matches(Some(&other), None));
    }

    #[test]
    fn matches_is_case_insensitive_for_strings() {
        let q = parse(r#"key.name = JXHUI"#).unwrap();
        assert!(q.matches(Some(&json!({"name": "jxhui"})), None));
    }

    #[test]
    fn matches_quoted_string_with_spaces() {
        let q = parse(r#"value.title = "hello world""#).unwrap();
        assert!(q.matches(None, Some(&json!({"title": "hello world"}))));
    }

    #[test]
    fn matches_numeric_and_bool_literals() {
        let q = parse("key.age = 20 AND key.active = true").unwrap();
        assert!(q.matches(Some(&json!({"age": 20, "active": true})), None));
        assert!(!q.matches(Some(&json!({"age": 21, "active": true})), None));
        assert!(!q.matches(Some(&json!({"age": 20, "active": false})), None));
    }

    #[test]
    fn matches_deeply_nested_path() {
        let q = parse(r#"value.parent.child.name = "jxhui""#).unwrap();
        let value = json!({"parent": {"child": {"name": "jxhui"}}});
        assert!(q.matches(None, Some(&value)));
        let miss = json!({"parent": {"child": {"name": "someone-else"}}});
        assert!(!q.matches(None, Some(&miss)));
    }

    #[test]
    fn and_combines_key_and_value_conditions() {
        let q = parse(
            r#"key.person.name = jxhui AND key.person.age = 20 AND value.house.owner = jxhui"#,
        )
        .unwrap();
        let key = json!({"person": {"name": "jxhui", "age": 20}});
        let value = json!({"house": {"owner": "jxhui"}});
        assert!(q.matches(Some(&key), Some(&value)));

        let wrong_owner = json!({"house": {"owner": "someone-else"}});
        assert!(!q.matches(Some(&key), Some(&wrong_owner)));
    }

    #[test]
    fn array_of_primitives_any_match() {
        let q = parse(r#"value.tags = "urgent""#).unwrap();
        assert!(q.matches(None, Some(&json!({"tags": ["a", "urgent", "b"]}))));
        assert!(!q.matches(None, Some(&json!({"tags": ["a", "b"]}))));
    }

    #[test]
    fn array_of_objects_any_match() {
        let q = parse(r#"value.items.sku = "ABC123""#).unwrap();
        let value = json!({"items": [{"sku": "X"}, {"sku": "ABC123"}]});
        assert!(q.matches(None, Some(&value)));
        let miss = json!({"items": [{"sku": "X"}, {"sku": "Y"}]});
        assert!(!q.matches(None, Some(&miss)));
    }

    #[test]
    fn nested_arrays_at_multiple_path_levels() {
        // orders: array; each order.items: array; each item.sku compared.
        let q = parse(r#"value.orders.items.sku = "ABC123""#).unwrap();
        let value = json!({
            "orders": [
                {"items": [{"sku": "X"}]},
                {"items": [{"sku": "Y"}, {"sku": "ABC123"}]}
            ]
        });
        assert!(q.matches(None, Some(&value)));
    }

    #[test]
    fn not_equal_matches_when_field_missing() {
        let q = parse(r#"key.missing != "x""#).unwrap();
        assert!(q.matches(Some(&json!({"other": 1})), None));
    }

    #[test]
    fn not_equal_is_false_when_field_equals() {
        let q = parse(r#"key.name != "jxhui""#).unwrap();
        assert!(!q.matches(Some(&json!({"name": "jxhui"})), None));
        assert!(q.matches(Some(&json!({"name": "someone-else"})), None));
    }

    #[test]
    fn not_equal_on_array_is_true_only_when_no_element_matches() {
        // "tags != urgent" should NOT match if urgent is one of the tags (universal
        // negation), even though some *other* element differs from "urgent".
        let q = parse(r#"value.tags != "urgent""#).unwrap();
        assert!(!q.matches(None, Some(&json!({"tags": ["urgent", "other"]}))));
        assert!(q.matches(None, Some(&json!({"tags": ["a", "b"]}))));
    }

    #[test]
    fn undecodable_root_never_matches_eq_and_always_matches_ne() {
        let q = parse(r#"key.name = "jxhui" AND value.x != "y""#).unwrap();
        // key/value None simulates raw/undecodable payloads or an uncached Avro schema.
        assert!(!q.matches(None, None));
        let ne_only = parse(r#"value.x != "y""#).unwrap();
        assert!(ne_only.matches(None, None));
    }

    #[test]
    fn parse_error_missing_root_prefix() {
        assert!(parse("name = jxhui").is_err());
    }

    #[test]
    fn parse_error_missing_operator() {
        assert!(parse("key.name jxhui").is_err());
    }

    #[test]
    fn parse_error_missing_literal() {
        assert!(parse("key.name =").is_err());
    }

    #[test]
    fn parse_error_empty_query() {
        assert!(parse("").is_err());
        assert!(parse("   ").is_err());
    }

    #[test]
    fn parse_error_unterminated_string() {
        assert!(parse(r#"key.name = "unterminated"#).is_err());
    }

    #[test]
    fn parse_error_bad_combinator() {
        assert!(parse(r#"key.name = jxhui OR key.age = 20"#).is_err());
    }

    #[test]
    fn raw_preserves_original_text_for_display() {
        let q = parse(r#"  key.name = jxhui  "#).unwrap();
        assert_eq!(q.raw, "key.name = jxhui");
    }
}
