//! Errors and their exit codes.
//!
//! Exit codes follow the plan: 0 success, 1 a user or configuration error,
//! 2 an API or transport error, 3 a named thing that does not exist.

use std::fmt;

/// A failure that stops the command.
#[derive(Debug, thiserror::Error)]
pub enum CtlError {
    /// The user asked for something that cannot work. Exit 1.
    #[error("{0}")]
    Usage(String),
    /// The configuration is wrong. Exit 1.
    #[error("{0}")]
    Config(String),
    /// A helper binary is not on `PATH`. Exit 1.
    #[error("`{bin}` not found on PATH (needed for `{command}`)")]
    MissingBinary { bin: String, command: String },
    /// The API could not be reached. Exit 2.
    #[error("cannot reach the Minga API at {endpoint}: {detail}")]
    Unreachable { endpoint: String, detail: String },
    /// The SSH tunnel failed. Exit 2.
    #[error("ssh to {target} failed: {detail}")]
    Ssh { target: String, detail: String },
    /// The API rejected the token. Exit 2.
    #[error("the Minga API at {endpoint} rejected the credentials (401 {message})")]
    Auth { endpoint: String, message: String },
    /// The API answered with an error. Exit 2.
    #[error("API error {status}: {message}")]
    Api { status: u16, message: String },
    /// `health` ran and at least one check was not `ok`. Exit 2.
    #[error("{failed} health check(s) not ok")]
    HealthFailed { failed: usize },
    /// A named stack, service or revision does not exist. Exit 3.
    #[error("{kind} `{name}` not found in context `{context}`")]
    NotFound {
        kind: &'static str,
        name: String,
        context: String,
    },
    /// A context name does not exist. Exit 3.
    #[error("context `{name}` not found")]
    ContextNotFound { name: String },
    /// A plain-HTTP endpoint that is not on the loopback interface. Exit 1.
    #[error(
        "refusing plain HTTP to {host}: the token and specs would cross the network unencrypted"
    )]
    PlainHttp { host: String },
    /// The config file can be read by group or others. Exit 1.
    #[error("{path} is readable by group or others (mode {mode:04o})")]
    ConfigPermissions { path: String, mode: u32 },
    /// The config file has a key that is not known. Exit 1.
    #[error("{path}: unknown field `{field}`")]
    ConfigUnknownField { path: String, field: String },
    /// The config file could not be read, written or parsed. Exit 1.
    #[error("{path}: {detail}")]
    ConfigInvalid { path: String, detail: String },
    /// The output could not be rendered. Exit 1.
    #[error("cannot render the output: {0}")]
    Output(String),
}

impl From<serde_json::Error> for CtlError {
    fn from(err: serde_json::Error) -> Self {
        CtlError::Output(err.to_string())
    }
}

impl CtlError {
    /// The exit code this failure produces.
    pub fn exit_code(&self) -> u8 {
        match self {
            CtlError::Usage(_)
            | CtlError::Config(_)
            | CtlError::MissingBinary { .. }
            | CtlError::PlainHttp { .. }
            | CtlError::ConfigPermissions { .. }
            | CtlError::ConfigUnknownField { .. }
            | CtlError::ConfigInvalid { .. }
            | CtlError::Output(_) => 1,
            CtlError::Unreachable { .. }
            | CtlError::Ssh { .. }
            | CtlError::Auth { .. }
            | CtlError::HealthFailed { .. }
            | CtlError::Api { .. } => 2,
            CtlError::NotFound { .. } | CtlError::ContextNotFound { .. } => 3,
        }
    }

    /// A next step the user can take, printed after the error.
    pub fn hint(&self) -> Option<String> {
        match self {
            CtlError::Usage(_) => None,
            CtlError::Config(_) => None,
            CtlError::MissingBinary { .. } => Some("install minga or set MINGA_BIN".to_string()),
            CtlError::Unreachable { .. } => {
                Some("is `minga api` running there? see `minga-ctl context list`".to_string())
            }
            CtlError::Ssh { .. } => None,
            CtlError::Auth { .. } => Some(
                "check remote_token_file, token_command or MINGA_CTL_TOKEN for the context"
                    .to_string(),
            ),
            CtlError::Api { .. } => None,
            CtlError::HealthFailed { .. } => None,
            CtlError::NotFound { .. } => None,
            CtlError::ContextNotFound { .. } => Some("see `minga-ctl context list`".to_string()),
            CtlError::PlainHttp { .. } => {
                Some("use --ssh user@HOST, or an https:// endpoint with --tls-cert".to_string())
            }
            CtlError::ConfigPermissions { path, .. } => Some(format!("chmod 600 {path}")),
            CtlError::ConfigUnknownField { field, .. } => Some(
                if ["token", "password", "secret"]
                    .iter()
                    .any(|word| field.to_ascii_lowercase().contains(word))
                {
                    "tokens are not stored in the config; use remote_token_file or token_command"
                } else {
                    "check the key name; `minga-ctl context add --help` lists the context keys"
                }
                .to_string(),
            ),
            CtlError::ConfigInvalid { .. } => None,
            CtlError::Output(_) => None,
        }
    }
}

/// A failure printed as `error: …` plus its hint.
#[derive(Debug)]
pub struct Reported {
    pub error: CtlError,
}

impl fmt::Display for Reported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "error: {}", self.error)?;
        if let Some(hint) = self.error.hint() {
            write!(f, "\nhint: {hint}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_errors_exit_one() {
        assert_eq!(CtlError::Usage("x".into()).exit_code(), 1);
    }

    #[test]
    fn transport_errors_exit_two() {
        assert_eq!(
            CtlError::Unreachable {
                endpoint: "http://127.0.0.1:2375".into(),
                detail: "connection refused".into(),
            }
            .exit_code(),
            2
        );
        assert_eq!(
            CtlError::Auth {
                endpoint: "http://127.0.0.1:2375".into(),
                message: "authentication required".into(),
            }
            .exit_code(),
            2
        );
    }

    #[test]
    fn a_failed_health_check_exits_two() {
        assert_eq!(CtlError::HealthFailed { failed: 1 }.exit_code(), 2);
    }

    #[test]
    fn missing_things_exit_three() {
        assert_eq!(
            CtlError::NotFound {
                kind: "stack",
                name: "myapp".into(),
                context: "prod".into(),
            }
            .exit_code(),
            3
        );
        assert_eq!(
            CtlError::ContextNotFound {
                name: "nope".into()
            }
            .exit_code(),
            3
        );
    }

    #[test]
    fn a_readable_config_reports_the_fix() {
        let reported = Reported {
            error: CtlError::ConfigPermissions {
                path: "~/.config/minga-ctl/config.yaml".into(),
                mode: 0o644,
            },
        };
        let text = reported.to_string();
        assert!(text.contains("mode 0644"), "{text}");
        assert!(text.contains("chmod 600"), "{text}");
    }

    #[test]
    fn a_plain_http_endpoint_reports_the_alternative() {
        let reported = Reported {
            error: CtlError::PlainHttp {
                host: "10.0.0.1".into(),
            },
        };
        let text = reported.to_string();
        assert!(text.contains("refusing plain HTTP to 10.0.0.1"), "{text}");
        assert!(text.contains("--ssh"), "{text}");
    }

    #[test]
    fn the_token_hint_is_only_for_token_fields() {
        let hint = |field: &str| {
            CtlError::ConfigUnknownField {
                path: "config.yaml".into(),
                field: field.into(),
            }
            .hint()
            .unwrap()
        };
        assert!(hint("api_token").contains("tokens are not stored"));
        assert!(hint("Password").contains("tokens are not stored"));
        assert!(hint("metrics_endpointt").contains("check the key name"));
    }

    #[test]
    fn an_unreachable_api_reports_the_hint() {
        let reported = Reported {
            error: CtlError::Unreachable {
                endpoint: "http://127.0.0.1:2375".into(),
                detail: "connection refused".into(),
            },
        };
        let text = reported.to_string();
        assert!(text.contains("cannot reach the Minga API"), "{text}");
        assert!(text.contains("is `minga api` running there?"), "{text}");
    }
}
