//! Schema Registry management over its REST API: browse subjects/versions, register a
//! new version (with an optional compatibility check), and delete versions/subjects.
//!
//! Separate from the decode path in `schema_registry.rs`: that one only ever reads
//! schemas by id; this one changes registry state.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::error::{AppError, AppResult};

const CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

/// A Confluent schema reference: `name` is how the referencing schema spells the type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SubjectVersion {
    pub subject: String,
    pub version: i64,
    pub id: i64,
    /// `AVRO` when the registry omits it (older registries only knew Avro).
    #[serde(default = "avro", rename = "schemaType")]
    pub schema_type: String,
    pub schema: String,
    #[serde(default)]
    pub references: Vec<SchemaReference>,
}

fn avro() -> String {
    "AVRO".into()
}

fn client() -> AppResult<reqwest::Client> {
    reqwest::Client::builder()
        .build()
        .map_err(|err| AppError::SchemaRegistry(format!("http client: {err}")))
}

fn url(base_url: &str, segments: &[&str]) -> AppResult<reqwest::Url> {
    let mut url = reqwest::Url::parse(base_url.trim().trim_end_matches('/'))
        .map_err(|err| AppError::SchemaRegistry(format!("invalid registry URL '{base_url}': {err}")))?;
    url.path_segments_mut()
        .map_err(|_| AppError::SchemaRegistry(format!("registry URL '{base_url}' cannot be a base")))?
        .pop_if_empty()
        .extend(segments);
    Ok(url)
}

/// The registry's own error text (`{"error_code":…,"message":…}`) when it sent one.
fn registry_message(status: reqwest::StatusCode, body: &str) -> String {
    match serde_json::from_str::<Value>(body).ok().and_then(|v| v.get("message")?.as_str().map(str::to_string)) {
        Some(message) => format!("{message} (HTTP {status})"),
        None => format!("HTTP {status}: {body}"),
    }
}

async fn send(builder: reqwest::RequestBuilder) -> AppResult<(reqwest::StatusCode, Value)> {
    let response = builder
        .header("accept", CONTENT_TYPE)
        .send()
        .await
        .map_err(|err| AppError::SchemaRegistryTransient(format!("schema registry request failed: {err}")))?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let message = registry_message(status, &body);
        return Err(if status.is_server_error() {
            AppError::SchemaRegistryTransient(message)
        } else {
            AppError::SchemaRegistry(message)
        });
    }
    Ok((status, serde_json::from_str(&body).unwrap_or(Value::Null)))
}

fn from_value<T: for<'de> Deserialize<'de>>(value: Value, what: &str) -> AppResult<T> {
    serde_json::from_value(value).map_err(|err| AppError::SchemaRegistry(format!("{what}: unexpected response: {err}")))
}

pub async fn list_subjects(base_url: &str) -> AppResult<Vec<String>> {
    let (_, v) = send(client()?.get(url(base_url, &["subjects"])?)).await?;
    let mut subjects: Vec<String> = from_value(v, "subjects")?;
    subjects.sort();
    Ok(subjects)
}

pub async fn list_versions(base_url: &str, subject: &str) -> AppResult<Vec<i64>> {
    let (_, v) = send(client()?.get(url(base_url, &["subjects", subject, "versions"])?)).await?;
    from_value(v, "versions")
}

/// `version` is a number or `latest`.
pub async fn get_version(base_url: &str, subject: &str, version: &str) -> AppResult<SubjectVersion> {
    let (_, v) = send(client()?.get(url(base_url, &["subjects", subject, "versions", version])?)).await?;
    from_value(v, "schema version")
}

fn schema_body(schema: &str, schema_type: &str, references: &[SchemaReference]) -> Value {
    let mut body = json!({ "schema": schema, "schemaType": schema_type });
    if !references.is_empty() {
        body["references"] = json!(references);
    }
    body
}

/// Registers `schema` under `subject`; returns its global id (the existing id if this
/// exact schema is already registered).
pub async fn register(
    base_url: &str,
    subject: &str,
    schema_type: &str,
    schema: &str,
    references: &[SchemaReference],
) -> AppResult<i64> {
    let request = client()?
        .post(url(base_url, &["subjects", subject, "versions"])?)
        .header("content-type", CONTENT_TYPE)
        .body(schema_body(schema, schema_type, references).to_string());
    let (_, v) = send(request).await?;
    v.get("id").and_then(Value::as_i64).ok_or_else(|| AppError::SchemaRegistry("register: response had no id".into()))
}

/// Whether `schema` is compatible with the subject's latest version under the
/// subject's compatibility level. A subject with no versions yet is trivially so.
pub async fn check_compatibility(
    base_url: &str,
    subject: &str,
    schema_type: &str,
    schema: &str,
    references: &[SchemaReference],
) -> AppResult<bool> {
    let request = client()?
        .post(url(base_url, &["compatibility", "subjects", subject, "versions", "latest"])?)
        .header("content-type", CONTENT_TYPE)
        .body(schema_body(schema, schema_type, references).to_string());
    match send(request).await {
        Ok((_, v)) => Ok(v.get("is_compatible").and_then(Value::as_bool).unwrap_or(false)),
        // 404: no such subject/version yet.
        Err(AppError::SchemaRegistry(m)) if m.contains("HTTP 404") => Ok(true),
        Err(err) => Err(err),
    }
}

/// Compatibility level for `subject` (falling back to the global default), or the global
/// level when `subject` is `None`.
pub async fn get_compatibility(base_url: &str, subject: Option<&str>) -> AppResult<String> {
    let mut endpoint = match subject {
        Some(s) => url(base_url, &["config", s])?,
        None => url(base_url, &["config"])?,
    };
    endpoint.query_pairs_mut().append_pair("defaultToGlobal", "true");
    let (_, v) = send(client()?.get(endpoint)).await?;
    v.get("compatibilityLevel")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| AppError::SchemaRegistry("config: response had no compatibilityLevel".into()))
}

pub async fn set_compatibility(base_url: &str, subject: Option<&str>, level: &str) -> AppResult<()> {
    let endpoint = match subject {
        Some(s) => url(base_url, &["config", s])?,
        None => url(base_url, &["config"])?,
    };
    let request = client()?
        .put(endpoint)
        .header("content-type", CONTENT_TYPE)
        .body(json!({ "compatibility": level }).to_string());
    send(request).await?;
    Ok(())
}

/// Deletes one version. Soft by default (the registry keeps it hidden but recoverable);
/// `permanent` soft-deletes first if needed, then removes it for good.
pub async fn delete_version(base_url: &str, subject: &str, version: &str, permanent: bool) -> AppResult<()> {
    let client = client()?;
    let endpoint = url(base_url, &["subjects", subject, "versions", version])?;
    if permanent {
        // Already soft-deleted is fine; the hard delete below is the point.
        let _ = send(client.delete(endpoint.clone())).await;
        let mut hard = endpoint;
        hard.query_pairs_mut().append_pair("permanent", "true");
        send(client.delete(hard)).await?;
    } else {
        send(client.delete(endpoint)).await?;
    }
    Ok(())
}

/// Deletes a subject and all its versions (soft, or permanent as above).
pub async fn delete_subject(base_url: &str, subject: &str, permanent: bool) -> AppResult<()> {
    let client = client()?;
    let endpoint = url(base_url, &["subjects", subject])?;
    if permanent {
        let _ = send(client.delete(endpoint.clone())).await;
        let mut hard = endpoint;
        hard.query_pairs_mut().append_pair("permanent", "true");
        send(client.delete(hard)).await?;
    } else {
        send(client.delete(endpoint)).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_escape_subject_names() {
        let u = url("http://sr:8081/", &["subjects", "a/b c", "versions"]).unwrap();
        assert_eq!(u.as_str(), "http://sr:8081/subjects/a%2Fb%20c/versions");
    }

    #[test]
    fn registry_error_text_is_surfaced() {
        let m = registry_message(
            reqwest::StatusCode::CONFLICT,
            r#"{"error_code":409,"message":"Schema being registered is incompatible"}"#,
        );
        assert!(m.starts_with("Schema being registered is incompatible"));
        assert!(registry_message(reqwest::StatusCode::BAD_GATEWAY, "oops").contains("oops"));
    }

    #[test]
    fn schema_body_includes_references_only_when_present() {
        assert!(schema_body("{}", "AVRO", &[]).get("references").is_none());
        let r = SchemaReference { name: "n".into(), subject: "s".into(), version: 2 };
        assert_eq!(schema_body("{}", "AVRO", &[r])["references"][0]["subject"], "s");
    }
}
