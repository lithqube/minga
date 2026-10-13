//! Compose Specification parser and validator.
//!
//! [`load`] runs the whole pipeline: YAML parsing with merge keys (`<<`),
//! variable interpolation, key checks (unknown, unsupported, `x-`
//! extensions), typed parsing, profile filtering and project validation. It
//! reports every problem it finds at once.

mod error;
mod interpolate;
mod keys;
mod model;
mod project;

use std::collections::HashMap;
use std::path::Path;

use indexmap::IndexMap;
use serde_yaml::{Mapping, Value};

pub use error::{ComposeError, Errors};
pub use interpolate::interpolate_str;
pub use model::*;
pub use project::{Project, DEFAULT_NETWORK};

#[derive(Debug, Clone, Default)]
pub struct LoadOptions {
    /// Variables for interpolation, usually the process environment.
    pub env: HashMap<String, String>,
    /// Active profiles; `*` enables every profile.
    pub profiles: Vec<String>,
}

impl LoadOptions {
    /// Options using the current process environment and no profiles.
    pub fn from_process_env() -> Self {
        Self {
            env: std::env::vars().collect(),
            profiles: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Loaded {
    pub project: Project,
    pub warnings: Vec<String>,
}

/// Loads a Compose file from disk.
pub fn load_path(path: impl AsRef<Path>, opts: &LoadOptions) -> Result<Loaded, Errors> {
    let path = path.as_ref();
    let yaml = std::fs::read_to_string(path).map_err(|e| {
        ComposeError::invalid(path.display().to_string(), format!("cannot read file: {e}"))
    })?;
    load(&yaml, opts)
}

/// Loads a Compose file from a string.
pub fn load(yaml: &str, opts: &LoadOptions) -> Result<Loaded, Errors> {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    let mut value: Value =
        serde_yaml::from_str(yaml).map_err(|e| ComposeError::Yaml(e.to_string()))?;
    value
        .apply_merge()
        .map_err(|e| ComposeError::Yaml(e.to_string()))?;
    if !value.is_mapping() {
        return Err(ComposeError::invalid("(root)", "a Compose file must be a mapping").into());
    }

    let lookup = |k: &str| opts.env.get(k).cloned();
    interpolate::interpolate_value(&mut value, "", &lookup, &mut errors, &mut warnings);
    let Value::Mapping(mut root) = value else {
        unreachable!("checked above")
    };

    let (top_ext, service_ext) = keys::check_and_strip(&mut root, &mut errors);

    // Services are parsed one by one so a mistake in one does not hide
    // mistakes in the others.
    let raw_services = match root.remove("services") {
        Some(Value::Mapping(m)) => m,
        Some(Value::Null) | None => Mapping::new(),
        Some(_) => {
            errors.push(ComposeError::invalid(
                "services",
                "must be a mapping of service names",
            ));
            Mapping::new()
        }
    };
    let mut services = IndexMap::new();
    for (name, raw) in raw_services {
        let Some(name) = name.as_str().map(str::to_string) else {
            errors.push(ComposeError::invalid(
                "services",
                format!("non-string service name {name:?}"),
            ));
            continue;
        };
        let raw = if raw.is_null() {
            Value::Mapping(Mapping::new())
        } else {
            raw
        };
        match deserialize::<Service>(raw, &format!("services.{name}")) {
            Ok(mut svc) => {
                svc.extensions = service_ext.get(&name).cloned().unwrap_or_default();
                services.insert(name, svc);
            }
            Err(e) => errors.push(e),
        }
    }

    let mut file = match deserialize::<ComposeFile>(Value::Mapping(root), "") {
        Ok(f) => f,
        Err(e) => {
            errors.push(e);
            ComposeFile::default()
        }
    };
    file.services = services;
    file.extensions = top_ext;

    let project = Project::build(file, &opts.profiles, &mut errors, &mut warnings);
    if errors.is_empty() {
        Ok(Loaded { project, warnings })
    } else {
        Err(Errors(errors))
    }
}

/// Typed parsing that reports where in the document it failed.
fn deserialize<T: serde::de::DeserializeOwned>(
    value: Value,
    prefix: &str,
) -> Result<T, ComposeError> {
    serde_path_to_error::deserialize(value).map_err(|e| {
        let inner = e.path().to_string();
        let path = match (prefix.is_empty(), inner.as_str()) {
            (true, ".") => "(root)".to_string(),
            (true, _) => inner,
            (false, ".") => prefix.to_string(),
            (false, _) => format!("{prefix}.{inner}"),
        };
        ComposeError::invalid(path, e.into_inner().to_string())
    })
}
