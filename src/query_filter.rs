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
    Gt,
    Lt,
    Ge,
    Le,
}

/// The comparison actually threaded through path/array traversal. `Op::Ne` isn't one
/// of these — it's computed as `NOT(Comparator::Eq)` at the top level (see
/// `QueryFilter::matches`) rather than recursed into, so array fan-out gives "no
/// element equals" instead of "some element differs" (see module docs on `!=`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Comparator {
    Eq,
    Gt,
    Lt,
    Ge,
    Le,
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
            let (cmp, negate) = match c.op {
                Op::Eq => (Comparator::Eq, false),
                Op::Ne => (Comparator::Eq, true),
                Op::Gt => (Comparator::Gt, false),
                Op::Lt => (Comparator::Lt, false),
                Op::Ge => (Comparator::Ge, false),
                Op::Le => (Comparator::Le, false),
            };
            let found = root_value.is_some_and(|v| path_matches(v, &c.path, cmp, &c.literal));
            if negate {
                !found
            } else {
                found
            }
        })
    }
}

/// Recursively resolves `path` against `value`, fanning out over arrays at any depth —
/// matches if ANY element along the way satisfies the rest of the path (same implicit
/// semantics as MongoDB's dot-notation array matching), so a path can walk through any
/// number of nested arrays without index syntax.
fn path_matches(value: &Value, path: &[String], cmp: Comparator, literal: &Literal) -> bool {
    match value {
        Value::Array(items) => items.iter().any(|item| path_matches(item, path, cmp, literal)),
        Value::Object(map) => match path.split_first() {
            Some((head, rest)) => {
                map.get(head).is_some_and(|next| path_matches(next, rest, cmp, literal))
            }
            None => false, // path exhausted at an object — nothing to compare against
        },
        leaf => path.is_empty() && leaf_matches(leaf, cmp, literal),
    }
}

fn leaf_matches(value: &Value, cmp: Comparator, literal: &Literal) -> bool {
    match cmp {
        Comparator::Eq => match (value, literal) {
            (Value::String(s), Literal::Str(l)) => s.to_lowercase() == l.to_lowercase(),
            (Value::Number(n), Literal::Num(l)) => {
                n.as_f64().is_some_and(|v| (v - l).abs() < 1e-9)
            }
            (Value::Bool(b), Literal::Bool(l)) => b == l,
            _ => false,
        },
        Comparator::Gt | Comparator::Lt | Comparator::Ge | Comparator::Le => {
            // Parser guarantees a numeric literal for these operators; only a numeric
            // leaf can ever satisfy one (see `parse_condition`).
            let (Value::Number(n), Literal::Num(l)) = (value, literal) else {
                return false;
            };
            let Some(v) = n.as_f64() else { return false };
            match cmp {
                Comparator::Gt => v > *l,
                Comparator::Lt => v < *l,
                Comparator::Ge => v >= *l,
                Comparator::Le => v <= *l,
                Comparator::Eq => unreachable!(),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Word(String),
    Str(String),
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
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
        if c == '>' && chars.get(i + 1) == Some(&'=') {
            tokens.push(Token::Ge);
            i += 2;
            continue;
        }
        if c == '>' {
            tokens.push(Token::Gt);
            i += 1;
            continue;
        }
        if c == '<' && chars.get(i + 1) == Some(&'=') {
            tokens.push(Token::Le);
            i += 2;
            continue;
        }
        if c == '<' {
            tokens.push(Token::Lt);
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
        Some(Token::Gt) => Op::Gt,
        Some(Token::Lt) => Op::Lt,
        Some(Token::Ge) => Op::Ge,
        Some(Token::Le) => Op::Le,
        _ => return Err("expected '=', '!=', '>', '<', '>=', or '<='".to_string()),
    };
    *pos += 1;

    let literal = match tokens.get(*pos) {
        Some(Token::Str(s)) => Literal::Str(s.clone()),
        Some(Token::Word(w)) => parse_bareword_literal(w),
        _ => return Err("expected a value after the operator".to_string()),
    };
    *pos += 1;

    if matches!(op, Op::Gt | Op::Lt | Op::Ge | Op::Le) && !matches!(literal, Literal::Num(_)) {
        return Err("'>', '<', '>=', and '<=' need a numeric value, e.g. value.timestamp > 23434".to_string());
    }

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
    fn greater_than_and_less_than_compare_numeric_fields() {
        let q = parse("value.timestamp > 23434").unwrap();
        assert!(q.matches(None, Some(&json!({"timestamp": 23435}))));
        assert!(!q.matches(None, Some(&json!({"timestamp": 23434}))));
        assert!(!q.matches(None, Some(&json!({"timestamp": 100}))));

        let q = parse("value.timestamp < 100").unwrap();
        assert!(q.matches(None, Some(&json!({"timestamp": 50}))));
        assert!(!q.matches(None, Some(&json!({"timestamp": 100}))));
    }

    #[test]
    fn greater_equal_and_less_equal_are_inclusive() {
        let q = parse("key.age >= 20").unwrap();
        assert!(q.matches(Some(&json!({"age": 20})), None));
        assert!(q.matches(Some(&json!({"age": 21})), None));
        assert!(!q.matches(Some(&json!({"age": 19})), None));

        let q = parse("key.age <= 20").unwrap();
        assert!(q.matches(Some(&json!({"age": 20})), None));
        assert!(!q.matches(Some(&json!({"age": 21})), None));
    }

    #[test]
    fn comparison_operators_any_match_across_arrays() {
        let q = parse("value.scores > 90").unwrap();
        assert!(q.matches(None, Some(&json!({"scores": [10, 95, 20]}))));
        assert!(!q.matches(None, Some(&json!({"scores": [10, 20, 30]}))));
    }

    #[test]
    fn comparison_operator_never_matches_non_numeric_field() {
        let q = parse(r#"key.name > 5"#).unwrap();
        assert!(!q.matches(Some(&json!({"name": "jxhui"})), None));
    }

    #[test]
    fn parse_error_comparison_operator_requires_numeric_literal() {
        assert!(parse(r#"key.name > "abc""#).is_err());
        assert!(parse(r#"key.name > true"#).is_err());
    }

    #[test]
    fn raw_preserves_original_text_for_display() {
        let q = parse(r#"  key.name = jxhui  "#).unwrap();
        assert_eq!(q.raw, "key.name = jxhui");
    }
}
