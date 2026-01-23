//! Schema Registry client for registering Protobuf schemas.
//!
//! Uses the Confluent Schema Registry REST API (compatible with Redpanda).

use serde::{Deserialize, Serialize};

use crate::config::KafkaSinkConfig;

/// Schema definition to register.
pub struct SchemaDefinition {
    /// Subject name (typically "<topic>-value" or "<topic>-key").
    pub subject: String,
    /// The protobuf schema content.
    pub schema: &'static str,
}

#[derive(Serialize)]
struct SchemaReference {
    name: String,
    subject: String,
    version: i32,
}

#[derive(Serialize)]
struct RegisterSchemaRequest<'a> {
    #[serde(rename = "schemaType")]
    schema_type: &'a str,
    schema: &'a str,
    references: Vec<SchemaReference>,
}

#[derive(Deserialize)]
struct RegisterSchemaResponse {
    id: i32,
}

#[derive(Deserialize)]
struct SchemaRegistryError {
    error_code: i32,
    message: String,
}

/// Register a protobuf schema with the Schema Registry.
/// Returns the schema ID on success.
fn register_schema(
    client: &reqwest::blocking::Client,
    base_url: &str,
    subject: &str,
    schema: &str,
) -> Result<i32, String> {
    let url = format!("{}/subjects/{}/versions", base_url, subject);

    let request = RegisterSchemaRequest {
        schema_type: "PROTOBUF",
        schema,
        references: vec![],
    };

    let response = client
        .post(&url)
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .json(&request)
        .send()
        .map_err(|e| format!("HTTP request failed: {}", e))?;

    let status = response.status();

    if status.is_success() {
        let result: RegisterSchemaResponse = response
            .json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;
        Ok(result.id)
    } else if status.as_u16() == 409 {
        // Schema already exists or is incompatible
        let error: SchemaRegistryError = response
            .json()
            .map_err(|e| format!("Failed to parse error response: {}", e))?;
        Err(format!(
            "Schema conflict (code {}): {}",
            error.error_code, error.message
        ))
    } else {
        let error_text = response
            .text()
            .unwrap_or_else(|_| "Unknown error".to_string());
        Err(format!("HTTP {}: {}", status, error_text))
    }
}

/// Check if Schema Registry is available.
fn check_schema_registry(client: &reqwest::blocking::Client, base_url: &str) -> bool {
    client
        .get(format!("{}/subjects", base_url))
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

/// Register all provided schemas with the Schema Registry.
pub fn ensure_schemas_registered(config: &KafkaSinkConfig, schemas: &[SchemaDefinition]) {
    let base_url = &config.schema_registry_url;

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("Failed to create HTTP client");

    // Check if Schema Registry is available
    if !check_schema_registry(&client, base_url) {
        tracing::warn!(
            url = %base_url,
            "Schema Registry not available, skipping schema registration"
        );
        return;
    }

    tracing::info!(url = %base_url, "Registering schemas with Schema Registry");

    for schema_def in schemas {
        match register_schema(&client, base_url, &schema_def.subject, schema_def.schema) {
            Ok(id) => {
                tracing::info!(
                    subject = %schema_def.subject,
                    schema_id = id,
                    "Schema registered successfully"
                );
            }
            Err(e) => {
                // Log as warning, not error - schema might already exist
                tracing::warn!(
                    subject = %schema_def.subject,
                    error = %e,
                    "Failed to register schema"
                );
            }
        }
    }
}
