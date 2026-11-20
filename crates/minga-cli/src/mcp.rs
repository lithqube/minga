//! Tenant-scoped MCP server over stdio.
//!
//! The server forwards fixed typed reads and agent-only change proposals
//! through a tenant client certificate. It never accepts arbitrary HTTP paths.
use std::io::BufReader;
use std::path::Path;

use minga_api::auth::{ApiRole, ClientIdentity};
use serde_json::{json, Map, Value};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader as AsyncBufReader,
};

use crate::McpServeArgs;

const API_VERSION: &str = "v1.41";
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
const MAX_LOG_TAIL: u64 = 200;
const MAX_LOG_BYTES: usize = 128 * 1024;
const USAGE_LIMIT: u64 = 100;

struct McpServer {
    client: reqwest::Client,
    root: String,
    tenant_id: String,
    role: ApiRole,
}

pub async fn serve(args: McpServeArgs) -> Result<(), String> {
    let server = McpServer::connect(args)?;
    let stdin = tokio::io::stdin();
    let mut input = AsyncBufReader::new(stdin);
    let mut stdout = tokio::io::stdout();
    let mut line = Vec::new();

    loop {
        match read_message(&mut input, &mut line)
            .await
            .map_err(|error| format!("read MCP input: {error}"))?
        {
            Incoming::Eof => return Ok(()),
            Incoming::TooLarge => {
                write_response(
                    &mut stdout,
                    jsonrpc_error(Value::Null, -32700, "message exceeds 1 MiB"),
                )
                .await?;
                continue;
            }
            Incoming::Message => {}
        }

        let response = match serde_json::from_slice::<Value>(&line) {
            Ok(request) => server.handle(request).await,
            Err(_) => Some(jsonrpc_error(Value::Null, -32700, "parse error")),
        };
        if let Some(response) = response {
            write_response(&mut stdout, response).await?;
        }
    }
}

enum Incoming {
    Eof,
    Message,
    TooLarge,
}

/// Reads one newline-delimited message into `line`, holding at most
/// `MAX_MESSAGE_BYTES + 1` bytes: an oversized line is discarded as it streams.
async fn read_message<R: AsyncBufRead + Unpin>(
    input: &mut R,
    line: &mut Vec<u8>,
) -> std::io::Result<Incoming> {
    let limit = MAX_MESSAGE_BYTES as u64 + 1;
    line.clear();
    if (&mut *input).take(limit).read_until(b'\n', line).await? == 0 {
        return Ok(Incoming::Eof);
    }
    if line.len() <= MAX_MESSAGE_BYTES {
        return Ok(Incoming::Message);
    }
    while !line.ends_with(b"\n") {
        line.clear();
        if (&mut *input).take(limit).read_until(b'\n', line).await? == 0 {
            break;
        }
    }
    line.clear();
    Ok(Incoming::TooLarge)
}

async fn write_response(stdout: &mut tokio::io::Stdout, response: Value) -> Result<(), String> {
    let mut bytes = serde_json::to_vec(&response).map_err(|_| "encode MCP response".to_owned())?;
    bytes.push(b'\n');
    stdout
        .write_all(&bytes)
        .await
        .map_err(|error| format!("write MCP response: {error}"))?;
    stdout
        .flush()
        .await
        .map_err(|error| format!("flush MCP response: {error}"))
}

impl McpServer {
    fn connect(args: McpServeArgs) -> Result<Self, String> {
        let mut root =
            reqwest::Url::parse(&args.api_url).map_err(|_| "invalid --api-url".to_owned())?;
        if root.scheme() != "https"
            || root.host_str().is_none()
            || !root.username().is_empty()
            || root.password().is_some()
            || !matches!(root.path(), "" | "/")
            || root.query().is_some()
            || root.fragment().is_some()
        {
            return Err(
                "--api-url must be an HTTPS origin without credentials, path, query or fragment"
                    .to_owned(),
            );
        }
        root.set_path("");

        let certificate_pem = read_file(&args.client_cert, "client certificate")?;
        check_private_key_mode(&args.client_key)?;
        let private_key_pem = read_file(&args.client_key, "client private key")?;
        let identity_pem = [
            certificate_pem.as_slice(),
            b"\n",
            private_key_pem.as_slice(),
        ]
        .concat();
        let identity = reqwest::Identity::from_pem(&identity_pem)
            .map_err(|_| "client certificate or private key is not valid PEM".to_owned())?;
        let certs = rustls_pemfile::certs(&mut BufReader::new(certificate_pem.as_slice()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "client certificate is not valid PEM".to_owned())?;
        let client_certificate = certs
            .first()
            .ok_or_else(|| "client certificate file contains no certificate".to_owned())?;
        let client_identity =
            ClientIdentity::from_certificate(client_certificate).ok_or_else(|| {
                "client certificate must contain one CN, one supported OU and one tenant O"
                    .to_owned()
            })?;
        let role = client_identity
            .role
            .ok_or_else(|| "client certificate role is unsupported".to_owned())?;
        let tenant_id = client_identity
            .tenant
            .filter(|tenant| tenant != "*")
            .ok_or_else(|| {
                "MCP requires a tenant-scoped client certificate; O=* is not accepted".to_owned()
            })?;
        minga_state::types::Tenant::validate_id(&tenant_id)
            .map_err(|_| "client certificate tenant ID is invalid".to_owned())?;

        let ca_pem = read_file(&args.ca_cert, "API CA certificate")?;
        let ca = reqwest::Certificate::from_pem(&ca_pem)
            .map_err(|_| "API CA certificate is not valid PEM".to_owned())?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(10))
            .tls_built_in_root_certs(false)
            .add_root_certificate(ca)
            .identity(identity)
            .build()
            .map_err(|_| "cannot create the HTTPS client".to_owned())?;

        Ok(Self {
            client,
            root: root.as_str().trim_end_matches('/').to_owned(),
            tenant_id,
            role,
        })
    }

    async fn handle(&self, request: Value) -> Option<Value> {
        if request.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return Some(jsonrpc_error(
                Value::Null,
                -32600,
                "invalid JSON-RPC request",
            ));
        }
        let Some(method) = request.get("method").and_then(Value::as_str) else {
            return Some(jsonrpc_error(
                Value::Null,
                -32600,
                "invalid JSON-RPC request",
            ));
        };
        let id = match request.get("id") {
            Some(id) if id.is_string() || id.is_number() || id.is_null() => id.clone(),
            Some(_) => return Some(jsonrpc_error(Value::Null, -32600, "invalid request id")),
            None if method.starts_with("notifications/") => return None,
            None => return Some(jsonrpc_error(Value::Null, -32600, "request id is required")),
        };
        let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
        let result = match method {
            "initialize" => Ok(json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "minga", "version": env!("CARGO_PKG_VERSION")}
            })),
            "ping" => Ok(json!({})),
            "tools/list" => Ok(json!({"tools": tool_definitions(self.role)})),
            "tools/call" => self.call_tool(&params).await,
            _ => return Some(jsonrpc_error(id, -32601, "method not found")),
        };
        Some(match result {
            Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
            Err(message) => jsonrpc_error(id, -32602, message),
        })
    }

    async fn call_tool(&self, params: &Value) -> Result<Value, &'static str> {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .ok_or("tool name is required")?;
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        if ![
            "status",
            "tenants",
            "nodes",
            "services",
            "tasks",
            "logs",
            "events",
            "usage",
            "diagnostics",
            "request_change",
        ]
        .contains(&name)
            || (name == "request_change" && self.role != ApiRole::Agent)
        {
            return Err("unknown tool");
        }
        if name != "request_change" && !matches!(name, "logs" | "usage") {
            reject_unknown_arguments(&arguments, &[])?;
        }
        let output = match name {
            "request_change" => self.request_change(&arguments).await?,
            "status" => self.status().await?,
            "tenants" => json!([{"id": self.tenant_id}]),
            "nodes" => project_nodes(self.get("nodes", &[]).await?),
            "services" => project_services(self.get("services", &[]).await?),
            "tasks" => project_tasks(self.get("tasks", &[]).await?),
            "logs" => self.logs(&arguments).await?,
            "events" => project_events(self.get("agent/events", &[]).await?),
            "usage" => self.usage(&arguments).await?,
            "diagnostics" => self.diagnostics().await?,
            _ => return Err("unknown tool"),
        };
        let text = serde_json::to_string(&output).map_err(|_| "cannot encode tool result")?;
        Ok(json!({
            "content": [{"type": "text", "text": text}],
            "isError": false
        }))
    }

    async fn status(&self) -> Result<Value, &'static str> {
        let services = project_services(self.get("services", &[]).await?);
        let tasks = project_tasks(self.get("tasks", &[]).await?);
        let running = tasks
            .as_array()
            .into_iter()
            .flatten()
            .filter(|task| task.get("state").and_then(Value::as_str) == Some("running"))
            .count();
        Ok(json!({
            "tenant_id": self.tenant_id,
            "service_count": services.as_array().map_or(0, Vec::len),
            "task_count": tasks.as_array().map_or(0, Vec::len),
            "running_task_count": running
        }))
    }

    async fn logs(&self, arguments: &Value) -> Result<Value, &'static str> {
        reject_unknown_arguments(arguments, &["task_id", "tail"])?;
        let task_id = required_id(arguments, "task_id")?;
        let tail = arguments
            .get("tail")
            .map(|value| value.as_u64().ok_or("tail must be an unsigned integer"))
            .transpose()?
            .unwrap_or(100);
        if !(1..=MAX_LOG_TAIL).contains(&tail) {
            return Err("tail must be between 1 and 200");
        }
        let path = format!("tasks/{task_id}/logs");
        let query = [("tail", tail.to_string())];
        let value = self.get(&path, &query).await?;
        let entries = value
            .as_array()
            .ok_or("log response has an invalid shape")?;
        let mut result = Vec::with_capacity(entries.len());
        let mut remaining = MAX_LOG_BYTES;
        for entry in entries {
            if remaining == 0 {
                break;
            }
            let data = entry
                .get("data")
                .and_then(Value::as_array)
                .map(|bytes| {
                    bytes
                        .iter()
                        .filter_map(Value::as_u64)
                        .filter_map(|byte| u8::try_from(byte).ok())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let take = data.len().min(remaining);
            remaining -= take;
            result.push(json!({
                "task_id": entry.get("task_id"),
                "stream": entry.get("stream"),
                "timestamp": entry.get("timestamp"),
                "data": String::from_utf8_lossy(&data[..take]),
                "truncated": take < data.len()
            }));
        }
        Ok(Value::Array(result))
    }

    async fn usage(&self, arguments: &Value) -> Result<Value, &'static str> {
        reject_unknown_arguments(arguments, &["since_unix_ms"])?;
        let since = arguments
            .get("since_unix_ms")
            .map(|value| {
                value
                    .as_u64()
                    .ok_or("since_unix_ms must be an unsigned integer")
            })
            .transpose()?
            .unwrap_or(0);
        let query = [
            ("since_unix_ms", since.to_string()),
            ("limit", USAGE_LIMIT.to_string()),
        ];
        let response = self.get("agent/usage", &query).await?;
        let events = response
            .get("events")
            .and_then(Value::as_array)
            .ok_or("usage response has an invalid shape")?;
        Ok(json!({"events": events.iter().map(project_usage_event).collect::<Vec<_>>() }))
    }

    async fn request_change(&self, arguments: &Value) -> Result<Value, &'static str> {
        reject_unknown_arguments(arguments, &["action", "service_id", "image", "replicas"])?;
        let action = arguments
            .get("action")
            .and_then(Value::as_str)
            .ok_or("action is required")?;
        let service_id = required_id(arguments, "service_id")?;
        let proposal = match action {
            "deploy" => {
                if arguments.get("replicas").is_some() {
                    return Err("replicas is only valid for scale");
                }
                let image = arguments
                    .get("image")
                    .and_then(Value::as_str)
                    .ok_or("image is required for deploy")?;
                if image.trim().is_empty()
                    || image.len() > 512
                    || image.chars().any(char::is_control)
                {
                    return Err("image must contain 1–512 printable bytes");
                }
                json!({"action":"deploy","service_id":service_id,"image":image})
            }
            "scale" => {
                if arguments.get("image").is_some() {
                    return Err("image is only valid for deploy");
                }
                let replicas = arguments
                    .get("replicas")
                    .and_then(Value::as_u64)
                    .and_then(|value| u32::try_from(value).ok())
                    .filter(|value| (1..=1000).contains(value))
                    .ok_or("replicas must be between 1 and 1000")?;
                json!({"action":"scale","service_id":service_id,"replicas":replicas})
            }
            "rollback" | "stop" => {
                if arguments.get("image").is_some() || arguments.get("replicas").is_some() {
                    return Err("image and replicas are not valid for this action");
                }
                json!({"action":action,"service_id":service_id})
            }
            _ => return Err("action must be deploy, scale, rollback or stop"),
        };
        let url = format!("{}/minga/change-requests", self.root);
        let response = self
            .client
            .post(url)
            .json(&proposal)
            .send()
            .await
            .map_err(|_| "API request failed")?;
        if !response.status().is_success() {
            return Err("change request was denied or unavailable");
        }
        let request = response
            .json::<Value>()
            .await
            .map_err(|_| "change request response is invalid")?;
        Ok(project_change_request(&request))
    }

    async fn diagnostics(&self) -> Result<Value, &'static str> {
        let tasks = project_tasks(self.get("tasks", &[]).await?);
        let task_rows = tasks
            .as_array()
            .ok_or("task response has an invalid shape")?;
        let findings = task_rows
            .iter()
            .filter(|task| {
                task.get("desired_state").and_then(Value::as_str) == Some("running")
                    && task.get("state").and_then(Value::as_str) != Some("running")
            })
            .map(|task| {
                json!({
                    "id": "task-not-running",
                    "severity": "warning",
                    "task_id": task.get("id"),
                    "summary": "task is not in its desired running state"
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({"tenant_id": self.tenant_id, "findings": findings}))
    }

    async fn get(&self, path: &str, query: &[(&str, String)]) -> Result<Value, &'static str> {
        let url = format!("{}/{API_VERSION}/{path}", self.root);
        let response = self
            .client
            .get(url)
            .query(query)
            .send()
            .await
            .map_err(|_| "API request failed")?;
        if !response.status().is_success() {
            return Err("API request was denied or unavailable");
        }
        response
            .json::<Value>()
            .await
            .map_err(|_| "API returned invalid JSON")
    }
}

fn read_file(path: &Path, description: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|_| format!("cannot read {description}"))
}

fn check_private_key_mode(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata =
            std::fs::metadata(path).map_err(|_| "cannot inspect client private key".to_owned())?;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(
                "client private key must not be readable by group or others (chmod 600)".to_owned(),
            );
        }
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn jsonrpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn reject_unknown_arguments(arguments: &Value, allowed: &[&str]) -> Result<(), &'static str> {
    let object = arguments
        .as_object()
        .ok_or("tool arguments must be an object")?;
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err("tool arguments contain an unknown field");
    }
    Ok(())
}

fn required_id<'a>(arguments: &'a Value, key: &str) -> Result<&'a str, &'static str> {
    let value = arguments
        .get(key)
        .and_then(Value::as_str)
        .ok_or("task_id is required")?;
    if value.is_empty()
        || value.bytes().all(|byte| byte == b'.')
        || value.len() > 255
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err("task_id contains unsupported characters");
    }
    Ok(value)
}

fn array(value: Value) -> Vec<Value> {
    value.as_array().cloned().unwrap_or_default()
}

fn project_fields(value: &Value, fields: &[(&str, &str)]) -> Value {
    let mut result = Map::new();
    for (output, path) in fields {
        if let Some(field) = value.pointer(path) {
            result.insert((*output).to_owned(), field.clone());
        }
    }
    Value::Object(result)
}

fn project_services(value: Value) -> Value {
    let mut services = array(value)
        .iter()
        .map(|service| {
            project_fields(
                service,
                &[
                    ("id", "/ID"),
                    ("name", "/Spec/Name"),
                    ("image", "/Spec/TaskTemplate/ContainerSpec/Image"),
                    ("desired_replicas", "/Spec/Mode/Replicated/Replicas"),
                ],
            )
        })
        .collect::<Vec<_>>();
    services.sort_by(|left, right| left["id"].as_str().cmp(&right["id"].as_str()));
    Value::Array(services)
}

fn project_tasks(value: Value) -> Value {
    let mut tasks = array(value)
        .iter()
        .map(|task| {
            project_fields(
                task,
                &[
                    ("id", "/ID"),
                    ("service_id", "/ServiceID"),
                    ("node_id", "/NodeID"),
                    ("desired_state", "/DesiredState"),
                    ("state", "/Status/State"),
                    ("updated_at", "/Status/UpdatedAt"),
                ],
            )
        })
        .collect::<Vec<_>>();
    tasks.sort_by(|left, right| left["id"].as_str().cmp(&right["id"].as_str()));
    Value::Array(tasks)
}

fn project_nodes(value: Value) -> Value {
    let mut nodes = array(value)
        .iter()
        .map(|node| {
            project_fields(
                node,
                &[
                    ("id", "/ID"),
                    ("hostname", "/Description/Hostname"),
                    ("role", "/Spec/Role"),
                    ("availability", "/Spec/Availability"),
                    ("state", "/Status/State"),
                ],
            )
        })
        .collect::<Vec<_>>();
    nodes.sort_by(|left, right| left["id"].as_str().cmp(&right["id"].as_str()));
    Value::Array(nodes)
}

fn project_events(value: Value) -> Value {
    Value::Array(
        array(value)
            .iter()
            .map(|event| {
                project_fields(
                    event,
                    &[
                        ("subject", "/subject"),
                        ("observed_at_unix_ms", "/observed_at_unix_ms"),
                        ("payload_size_bytes", "/payload_size_bytes"),
                    ],
                )
            })
            .collect(),
    )
}
fn project_change_request(value: &Value) -> Value {
    project_fields(
        value,
        &[
            ("id", "/id"),
            ("service_id", "/service_id"),
            ("kind", "/kind"),
            ("status", "/status"),
            ("expires_at_unix", "/expires_at_unix"),
            ("before_image", "/before/image"),
            ("before_replicas", "/before/replicas"),
            ("after_image", "/after/image"),
            ("after_replicas", "/after/replicas"),
        ],
    )
}
fn project_usage_event(event: &Value) -> Value {
    project_fields(
        event,
        &[
            ("request_id", "/request_id"),
            ("service_id", "/service_id"),
            ("task_id", "/task_id"),
            ("customer_id", "/customer_id"),
            ("route", "/route"),
            ("requested_model", "/requested_model"),
            ("answered_model", "/answered_model"),
            ("input_tokens", "/input_tokens"),
            ("output_tokens", "/output_tokens"),
            ("cache_hit", "/cache_hit"),
            ("latency_ms", "/latency_ms"),
            ("status", "/status"),
            ("occurred_at_unix_ms", "/occurred_at_unix_ms"),
            ("cost_micros", "/cost_micros"),
        ],
    )
}

fn tool_definitions(role: ApiRole) -> Vec<Value> {
    let mut tools = vec![
        tool(
            "status",
            "Summarize services and tasks visible to the caller's tenant.",
            json!({"type": "object", "properties": {}, "additionalProperties": false}),
        ),
        tool(
            "tenants",
            "Return the one tenant named by the caller's client certificate.",
            json!({"type": "object", "properties": {}, "additionalProperties": false}),
        ),
        tool(
            "nodes",
            "List nodes assigned to the caller's tenant.",
            json!({"type": "object", "properties": {}, "additionalProperties": false}),
        ),
        tool(
            "services",
            "List services visible to the caller's tenant; environment values are omitted.",
            json!({"type": "object", "properties": {}, "additionalProperties": false}),
        ),
        tool(
            "tasks",
            "List tasks visible to the caller's tenant; task error text is omitted.",
            json!({"type": "object", "properties": {}, "additionalProperties": false}),
        ),
        tool(
            "logs",
            "Read bounded retained logs for a task in the caller's tenant.",
            json!({"type": "object", "properties": {"task_id": {"type": "string"}, "tail": {"type": "integer", "minimum": 1, "maximum": 200}}, "required": ["task_id"], "additionalProperties": false}),
        ),
        tool(
            "events",
            "Read bounded retained event metadata for the caller's tenant; event payloads are omitted.",
            json!({"type": "object", "properties": {}, "additionalProperties": false}),
        ),
        tool(
            "usage",
            "Read bounded agent usage events in the caller's tenant.",
            json!({"type": "object", "properties": {"since_unix_ms": {"type": "integer", "minimum": 0}}, "additionalProperties": false}),
        ),
        tool(
            "diagnostics",
            "Report non-running tasks visible to the caller's tenant without task error text.",
            json!({"type": "object", "properties": {}, "additionalProperties": false}),
        ),
    ];
    if role == ApiRole::Agent {
        let mut proposal = tool(
            "request_change",
            "Propose a service change for same-tenant deployer/admin review; never applies it.",
            // MCP requires an object at the root; `oneOf` narrows it per action.
            json!({"type":"object","oneOf":[
                {"type":"object","properties":{"action":{"const":"deploy"},"service_id":{"type":"string"},"image":{"type":"string","maxLength":512}},"required":["action","service_id","image"],"additionalProperties":false},
                {"type":"object","properties":{"action":{"const":"scale"},"service_id":{"type":"string"},"replicas":{"type":"integer","minimum":1,"maximum":1000}},"required":["action","service_id","replicas"],"additionalProperties":false},
                {"type":"object","properties":{"action":{"enum":["rollback","stop"]},"service_id":{"type":"string"}},"required":["action","service_id"],"additionalProperties":false}
            ]}),
        );
        proposal["annotations"]["readOnlyHint"] = json!(false);
        tools.push(proposal);
    }
    tools
}

fn tool(name: &str, description: &str, input_schema: Value) -> Value {
    json!({
        "name": name,
        "description": description,
        "inputSchema": input_schema,
        "annotations": {"readOnlyHint": true, "openWorldHint": false}
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offline_server(role: ApiRole) -> McpServer {
        McpServer {
            client: reqwest::Client::new(),
            root: "https://127.0.0.1:1".to_owned(),
            tenant_id: "tenant-a".to_owned(),
            role,
        }
    }

    async fn call(server: &McpServer, name: &str, arguments: Value) -> Value {
        server
            .handle(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments}
            }))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn tool_calls_outside_the_allow_list_are_refused() {
        let viewer = offline_server(ApiRole::Viewer);
        for name in ["deploy", "scale", "../services", "services/x", ""] {
            let response = call(&viewer, name, json!({})).await;
            assert_eq!(response["error"]["message"], "unknown tool", "{name}");
        }
        // The proposal tool exists only for agent certificates.
        let response = call(&viewer, "request_change", json!({})).await;
        assert_eq!(response["error"]["message"], "unknown tool");
    }

    #[tokio::test]
    async fn the_tenant_comes_from_the_certificate_not_the_arguments() {
        let server = offline_server(ApiRole::Agent);
        let response = call(&server, "tenants", json!({})).await;
        let text = response["result"]["content"][0]["text"].as_str().unwrap();
        assert_eq!(text, r#"[{"id":"tenant-a"}]"#);
        let response = call(&server, "tenants", json!({"tenant_id": "tenant-b"})).await;
        assert!(response.get("error").is_some());
        let response = call(&server, "logs", json!({"task_id": ".."})).await;
        assert_eq!(
            response["error"]["message"],
            "task_id contains unsupported characters"
        );
    }

    #[tokio::test]
    async fn an_oversized_message_is_skipped_without_buffering_it() {
        let mut input = vec![b'a'; MAX_MESSAGE_BYTES + 10];
        input.extend_from_slice(b"\n{\"jsonrpc\":\"2.0\"}\n");
        let mut reader = input.as_slice();
        let mut line = Vec::new();
        assert!(matches!(
            read_message(&mut reader, &mut line).await.unwrap(),
            Incoming::TooLarge
        ));
        assert!(line.capacity() <= 2 * (MAX_MESSAGE_BYTES + 1));
        assert!(matches!(
            read_message(&mut reader, &mut line).await.unwrap(),
            Incoming::Message
        ));
        assert_eq!(line, b"{\"jsonrpc\":\"2.0\"}\n");
        assert!(matches!(
            read_message(&mut reader, &mut line).await.unwrap(),
            Incoming::Eof
        ));
    }

    #[test]
    fn mcp_tools_are_read_only_and_have_closed_argument_schemas() {
        let tools = tool_definitions(ApiRole::Viewer);
        let names = tools
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "status",
                "tenants",
                "nodes",
                "services",
                "tasks",
                "logs",
                "events",
                "usage",
                "diagnostics"
            ]
        );
        assert!(tools
            .iter()
            .all(|tool| tool["inputSchema"]["additionalProperties"] == false));
        assert!(names
            .iter()
            .all(|name| !["deploy", "scale", "stop", "rollback"].contains(name)));
    }
    #[test]
    fn agent_gets_only_a_proposal_write_tool() {
        let tools = tool_definitions(ApiRole::Agent);
        let proposal = tools
            .iter()
            .find(|tool| tool["name"] == "request_change")
            .unwrap();
        assert_eq!(proposal["annotations"]["readOnlyHint"], false);
        assert_eq!(proposal["annotations"]["openWorldHint"], false);
        assert_eq!(tools.len(), 10);
        // MCP 2024-11-05: every inputSchema is an object schema.
        assert!(tools
            .iter()
            .all(|tool| tool["inputSchema"]["type"] == "object"));
        assert!(tool_definitions(ApiRole::Deployer)
            .iter()
            .all(|tool| tool["name"] != "request_change"));
    }

    #[test]
    fn output_projections_omit_environment_and_task_errors_and_sort_rows() {
        let services = project_services(json!([
            {"ID":"b", "Spec":{"Name":"b","TaskTemplate":{"ContainerSpec":{"Image":"img:b","Env":["TOKEN=secret"]}}}},
            {"ID":"a", "Spec":{"Name":"a","TaskTemplate":{"ContainerSpec":{"Image":"img:a"}}}}
        ]));
        assert_eq!(services[0]["id"], "a");
        assert!(services.to_string().find("secret").is_none());

        let tasks = project_tasks(json!([{
            "ID":"task-a", "ServiceID":"service-a", "NodeID":"3", "DesiredState":"running",
            "Status":{"State":"failed", "Err":"private task error", "UpdatedAt":"now"}
        }]));
        assert_eq!(tasks[0]["state"], "failed");
        assert!(tasks.to_string().find("private task error").is_none());
    }
    #[test]
    fn event_projection_keeps_metadata_and_omits_payload() {
        let events = project_events(json!([{
            "subject": "minga.tenant-a.task.id.event",
            "observed_at_unix_ms": 1,
            "payload_size_bytes": 12,
            "payload": "sensitive event body"
        }]));
        assert_eq!(events[0]["subject"], "minga.tenant-a.task.id.event");
        assert_eq!(events[0]["payload_size_bytes"], 12);
        assert!(events.to_string().find("sensitive event body").is_none());
    }

    #[test]
    fn task_arguments_reject_path_injection_and_unknown_fields() {
        assert_eq!(
            required_id(&json!({"task_id":"tenant-a.task-1"}), "task_id"),
            Ok("tenant-a.task-1")
        );
        assert!(required_id(&json!({"task_id":"../../other"}), "task_id").is_err());
        assert!(reject_unknown_arguments(
            &json!({"task_id":"x", "path":"/minga/tenants"}),
            &["task_id"]
        )
        .is_err());
    }
    fn test_server() -> McpServer {
        McpServer {
            client: reqwest::Client::new(),
            root: "https://127.0.0.1".to_owned(),
            tenant_id: "tenant-a".to_owned(),
            role: ApiRole::Viewer,
        }
    }

    #[tokio::test]
    async fn initialize_and_tools_list_use_mcp_jsonrpc_shapes() {
        let server = test_server();
        let initialized = server
            .handle(json!({"jsonrpc":"2.0", "id":1, "method":"initialize"}))
            .await
            .unwrap();
        assert_eq!(initialized["result"]["protocolVersion"], "2024-11-05");

        let listed = server
            .handle(json!({"jsonrpc":"2.0", "id":"tools", "method":"tools/list"}))
            .await
            .unwrap();
        assert_eq!(listed["result"]["tools"].as_array().unwrap().len(), 9);
        assert!(server
            .handle(json!({"jsonrpc":"2.0", "method":"notifications/initialized"}))
            .await
            .is_none());
    }

    #[tokio::test]
    async fn tool_arguments_cannot_add_unlisted_reads() {
        let error = test_server()
            .call_tool(&json!({
                "name": "status",
                "arguments": {"path": "/minga/tenants"}
            }))
            .await
            .unwrap_err();
        assert_eq!(error, "tool arguments contain an unknown field");
    }
}
