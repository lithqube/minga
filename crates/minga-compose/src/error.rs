use std::fmt;

/// One problem found while loading a Compose file. `path` is a dotted location
/// such as `services.web.ports[0]`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ComposeError {
    #[error("invalid YAML: {0}")]
    Yaml(String),

    #[error("{path}: unknown key `{key}`{}", did_you_mean(.suggestion))]
    UnknownKey {
        path: String,
        key: String,
        suggestion: Option<String>,
    },

    #[error("{path}: `{key}` is part of the Compose spec but not supported yet{}", with_note(.note))]
    Unsupported {
        path: String,
        key: String,
        note: Option<String>,
    },

    #[error("{path}: {message}")]
    Invalid { path: String, message: String },

    #[error("{path}: {message}")]
    Interpolation { path: String, message: String },
}

impl ComposeError {
    pub(crate) fn invalid(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Invalid {
            path: path.into(),
            message: message.into(),
        }
    }
}

fn did_you_mean(suggestion: &Option<String>) -> String {
    suggestion
        .as_ref()
        .map(|s| format!(" (did you mean `{s}`?)"))
        .unwrap_or_default()
}

fn with_note(note: &Option<String>) -> String {
    note.as_ref().map(|n| format!(": {n}")).unwrap_or_default()
}

/// Every error found in one load. Loading reports all problems it can find at
/// once instead of stopping at the first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Errors(pub Vec<ComposeError>);

impl Errors {
    pub fn iter(&self) -> impl Iterator<Item = &ComposeError> {
        self.0.iter()
    }
}

impl fmt::Display for Errors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, e) in self.0.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "{e}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Errors {}

impl From<ComposeError> for Errors {
    fn from(e: ComposeError) -> Self {
        Self(vec![e])
    }
}
