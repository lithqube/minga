//! Variable interpolation as defined by the Compose Specification:
//! `$VAR`, `${VAR}`, `${VAR:-default}`, `${VAR-default}`, `${VAR:?err}`,
//! `${VAR?err}`, `${VAR:+alt}`, `${VAR+alt}` and `$$` for a literal `$`.
//! Defaults and alternatives may themselves contain interpolations.

use serde_yaml::Value;

use crate::error::ComposeError;

/// Interpolates every string value in the tree. Mapping keys are left alone,
/// as the spec only interpolates values.
pub(crate) fn interpolate_value(
    value: &mut Value,
    path: &str,
    lookup: &dyn Fn(&str) -> Option<String>,
    errors: &mut Vec<ComposeError>,
    warnings: &mut Vec<String>,
) {
    match value {
        Value::String(s) => match interpolate_str(s, lookup, warnings) {
            Ok(out) => *s = out,
            Err(message) => errors.push(ComposeError::Interpolation {
                path: path.to_string(),
                message,
            }),
        },
        Value::Sequence(items) => {
            for (i, item) in items.iter_mut().enumerate() {
                interpolate_value(item, &format!("{path}[{i}]"), lookup, errors, warnings);
            }
        }
        Value::Mapping(map) => {
            for (k, v) in map.iter_mut() {
                let key = k
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("{k:?}"));
                let child = if path.is_empty() {
                    key
                } else {
                    format!("{path}.{key}")
                };
                interpolate_value(v, &child, lookup, errors, warnings);
            }
        }
        Value::Tagged(tagged) => {
            interpolate_value(&mut tagged.value, path, lookup, errors, warnings)
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// Interpolates one string. Unset variables without a default become empty
/// and produce a warning, matching `docker compose`.
pub fn interpolate_str(
    input: &str,
    lookup: &dyn Fn(&str) -> Option<String>,
    warnings: &mut Vec<String>,
) -> Result<String, String> {
    let chars: Vec<char> = input.chars().collect();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '$' {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        match chars.get(i + 1) {
            Some('$') => {
                out.push('$');
                i += 2;
            }
            Some('{') => {
                let close = matching_brace(&chars, i + 1)
                    .ok_or_else(|| format!("unclosed `${{` in {input:?}"))?;
                let inner: String = chars[i + 2..close].iter().collect();
                out.push_str(&expand_braced(&inner, lookup, warnings)?);
                i = close + 1;
            }
            Some(c) if is_name_start(*c) => {
                let start = i + 1;
                let mut end = start;
                while end < chars.len() && is_name_char(chars[end]) {
                    end += 1;
                }
                let name: String = chars[start..end].iter().collect();
                out.push_str(&resolve_plain(&name, lookup, warnings));
                i = end;
            }
            // A lone `$` (end of string, or before a non-name character) is literal.
            _ => {
                out.push('$');
                i += 1;
            }
        }
    }
    Ok(out)
}

fn expand_braced(
    inner: &str,
    lookup: &dyn Fn(&str) -> Option<String>,
    warnings: &mut Vec<String>,
) -> Result<String, String> {
    let name_len = inner.chars().take_while(|c| is_name_char(*c)).count();
    let name = &inner[..name_len];
    if name.is_empty() || !name.starts_with(is_name_start) {
        return Err(format!("invalid variable name in `${{{inner}}}`"));
    }
    let rest = &inner[name_len..];
    let value = lookup(name);
    let is_set = value.is_some();
    let is_non_empty = value.as_deref().is_some_and(|v| !v.is_empty());

    let (op, arg) = if rest.is_empty() {
        return Ok(resolve_plain(name, lookup, warnings));
    } else if let Some(arg) = rest.strip_prefix(":-") {
        (":-", arg)
    } else if let Some(arg) = rest.strip_prefix(":?") {
        (":?", arg)
    } else if let Some(arg) = rest.strip_prefix(":+") {
        (":+", arg)
    } else if let Some(arg) = rest.strip_prefix('-') {
        ("-", arg)
    } else if let Some(arg) = rest.strip_prefix('?') {
        ("?", arg)
    } else if let Some(arg) = rest.strip_prefix('+') {
        ("+", arg)
    } else {
        return Err(format!("invalid interpolation format `${{{inner}}}`"));
    };

    let mut arg = || interpolate_str(arg, lookup, warnings);
    match op {
        ":-" if is_non_empty => Ok(value.unwrap_or_default()),
        ":-" => arg(),
        "-" if is_set => Ok(value.unwrap_or_default()),
        "-" => arg(),
        ":?" if is_non_empty => Ok(value.unwrap_or_default()),
        "?" if is_set => Ok(value.unwrap_or_default()),
        ":?" | "?" => {
            let msg = arg()?;
            Err(if msg.is_empty() {
                format!("required variable {name} is missing a value")
            } else {
                format!("required variable {name} is missing a value: {msg}")
            })
        }
        ":+" if is_non_empty => arg(),
        "+" if is_set => arg(),
        _ => Ok(String::new()),
    }
}

fn resolve_plain(
    name: &str,
    lookup: &dyn Fn(&str) -> Option<String>,
    warnings: &mut Vec<String>,
) -> String {
    lookup(name).unwrap_or_else(|| {
        let w = format!("variable {name} is not set, defaulting to a blank string");
        if !warnings.contains(&w) {
            warnings.push(w);
        }
        String::new()
    })
}

/// Index of the `}` closing the `{` at `open`, honouring nested `${...}`.
fn matching_brace(chars: &[char], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (i, c) in chars.iter().enumerate().skip(open) {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

fn is_name_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn run(input: &str, vars: &[(&str, &str)]) -> (Result<String, String>, Vec<String>) {
        let env: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let mut warnings = Vec::new();
        let r = interpolate_str(input, &|k| env.get(k).cloned(), &mut warnings);
        (r, warnings)
    }

    fn ok(input: &str, vars: &[(&str, &str)]) -> String {
        run(input, vars).0.unwrap()
    }

    #[test]
    fn plain_and_braced() {
        assert_eq!(ok("$TAG", &[("TAG", "1.2")]), "1.2");
        assert_eq!(ok("nginx:${TAG}", &[("TAG", "1.27")]), "nginx:1.27");
        assert_eq!(ok("${A}${B}", &[("A", "x"), ("B", "y")]), "xy");
    }

    #[test]
    fn escapes_and_lone_dollar() {
        assert_eq!(ok("$$HOME", &[]), "$HOME");
        assert_eq!(ok("cost: 5$", &[]), "cost: 5$");
        assert_eq!(ok("a $ b", &[]), "a $ b");
    }

    #[test]
    fn defaults() {
        assert_eq!(ok("${PORT:-8080}", &[]), "8080");
        assert_eq!(ok("${PORT:-8080}", &[("PORT", "")]), "8080");
        assert_eq!(ok("${PORT-8080}", &[("PORT", "")]), "");
        assert_eq!(ok("${PORT-8080}", &[("PORT", "9")]), "9");
    }

    #[test]
    fn nested_default() {
        assert_eq!(ok("${A:-${B:-z}}", &[("B", "b")]), "b");
        assert_eq!(ok("${A:-${B:-z}}", &[]), "z");
    }

    #[test]
    fn alternatives() {
        assert_eq!(ok("${DEBUG:+--verbose}", &[("DEBUG", "1")]), "--verbose");
        assert_eq!(ok("${DEBUG:+--verbose}", &[("DEBUG", "")]), "");
        assert_eq!(ok("${DEBUG+--verbose}", &[("DEBUG", "")]), "--verbose");
    }

    #[test]
    fn required() {
        let (r, _) = run("${DB_PASSWORD:?set it in .env}", &[]);
        assert_eq!(
            r.unwrap_err(),
            "required variable DB_PASSWORD is missing a value: set it in .env"
        );
        let (r, _) = run("${X?}", &[("X", "")]);
        assert_eq!(r.unwrap(), "");
    }

    #[test]
    fn unset_warns_once() {
        let (r, w) = run("$NOPE-$NOPE", &[]);
        assert_eq!(r.unwrap(), "-");
        assert_eq!(w.len(), 1);
    }

    #[test]
    fn malformed() {
        assert!(run("${OOPS", &[]).0.is_err());
        assert!(run("${1BAD}", &[]).0.is_err());
        assert!(run("${A/b}", &[]).0.is_err());
    }
}
