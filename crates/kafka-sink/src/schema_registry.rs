//! Schema Registry client for registering Protobuf schemas.
//!
//! Uses the Confluent Schema Registry REST API (compatible with Redpanda).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::config::KafkaSinkConfig;

/// Schema definition to register.
pub struct SchemaDefinition {
    /// Subject name (typically "<topic>-value" or "<topic>-key").
    pub subject: String,
    /// The protobuf schema content.
    pub schema: &'static str,
    /// Index of the message type within the schema (0-indexed from the last message).
    /// For a schema with only one message, this should be 0.
    /// For our TokenProgramInstruction which is the last message, this is 0.
    pub message_index: i32,
}

/// Registered schema info including ID and message index.
#[derive(Clone, Debug)]
pub struct RegisteredSchema {
    pub schema_id: i32,
    pub message_index: i32,
}

/// Encodes a payload with the Confluent wire format for protobuf.
/// Format: [0x00][4-byte schema ID BE][varint message index...][protobuf payload]
pub fn encode_with_schema_id(schema_id: i32, message_indices: &[i32], payload: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(5 + message_indices.len() + payload.len());

    // Magic byte
    result.push(0x00);

    // Schema ID (big-endian 4 bytes)
    result.extend_from_slice(&schema_id.to_be_bytes());

    // Message indices as varints (for protobuf)
    // First, the count of indices
    encode_varint(&mut result, message_indices.len() as i64);
    // Then each index
    for &idx in message_indices {
        encode_varint(&mut result, idx as i64);
    }

    // Actual protobuf payload
    result.extend_from_slice(payload);

    result
}

/// Encode a varint (used for message indices in protobuf wire format).
fn encode_varint(buf: &mut Vec<u8>, mut value: i64) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
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
/// Returns a map of subject -> RegisteredSchema for encoding messages.
pub fn ensure_schemas_registered(
    config: &KafkaSinkConfig,
    schemas: &[SchemaDefinition],
) -> HashMap<String, RegisteredSchema> {
    let base_url = &config.schema_registry_url;
    let mut registered = HashMap::new();

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
        return registered;
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
                registered.insert(
                    schema_def.subject.clone(),
                    RegisteredSchema {
                        schema_id: id,
                        message_index: schema_def.message_index,
                    },
                );
            }
            Err(e) => {
                // Try to get existing schema ID
                if let Some(id) = get_schema_id(&client, base_url, &schema_def.subject) {
                    tracing::info!(
                        subject = %schema_def.subject,
                        schema_id = id,
                        "Using existing schema"
                    );
                    registered.insert(
                        schema_def.subject.clone(),
                        RegisteredSchema {
                            schema_id: id,
                            message_index: schema_def.message_index,
                        },
                    );
                } else {
                    tracing::warn!(
                        subject = %schema_def.subject,
                        error = %e,
                        "Failed to register schema"
                    );
                }
            }
        }
    }

    registered
}

/// Get the latest schema ID for a subject.
fn get_schema_id(
    client: &reqwest::blocking::Client,
    base_url: &str,
    subject: &str,
) -> Option<i32> {
    let url = format!("{}/subjects/{}/versions/latest", base_url, subject);
    let response = client.get(&url).send().ok()?;

    if response.status().is_success() {
        #[derive(Deserialize)]
        struct SchemaVersion {
            id: i32,
        }
        let version: SchemaVersion = response.json().ok()?;
        Some(version.id)
    } else {
        None
    }
}
