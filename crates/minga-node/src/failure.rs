//! The one-line reason a task ended, kept in `Task::last_error`.
//!
//! The text reaches the Docker API and `minga-ctl`, so it never holds a
//! secret: runtime errors can carry `ctr` stderr, which may echo registry
//! credentials.

use std::fmt::Display;

/// Longest reason kept, in characters.
pub(crate) const MAX_LEN: usize = 300;

/// How long a task must stay `Running` before its last error is cleared.
pub(crate) const CLEAR_AFTER_SECS: u64 = 60;

/// Reason recorded when the supervisor kills a container for failing its
/// healthcheck; the exit that follows would otherwise read as a plain SIGKILL.
pub(crate) const HEALTHCHECK_KILL: &str = "healthcheck failed; container killed";

/// Why a task ended or failed to start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Failure {
    pub error: String,
    pub exit_code: Option<i32>,
}

/// A container exit. `prior` is the reason already on the task, so a
/// healthcheck kill keeps its cause.
pub(crate) fn exit(code: Option<i32>, prior: Option<&str>) -> Failure {
    let error = match code {
        None => "exited (the runtime reported no exit status)".to_string(),
        Some(c) if prior.is_some_and(|p| p.starts_with(HEALTHCHECK_KILL)) => {
            format!("{HEALTHCHECK_KILL} (exit code {c})")
        }
        // Runtimes report a signal death as 128 + the signal number. The
        // runtime says nothing about why (an OOM kill looks like SIGKILL),
        // so no cause is guessed.
        Some(c @ 129..=159) => match signal_name(c - 128) {
            Some(name) => format!("exited with code {c} (killed by signal {} {name})", c - 128),
            None => format!("exited with code {c} (killed by signal {})", c - 128),
        },
        Some(c) => format!("exited with code {c}"),
    };
    Failure {
        error,
        exit_code: code,
    }
}

fn signal_name(n: i32) -> Option<&'static str> {
    Some(match n {
        1 => "SIGHUP",
        2 => "SIGINT",
        6 => "SIGABRT",
        9 => "SIGKILL",
        11 => "SIGSEGV",
        13 => "SIGPIPE",
        15 => "SIGTERM",
        _ => return None,
    })
}

/// A runtime error from starting a container or agent. `secrets` are exact
/// values to blank out, such as the registry credentials of the image.
pub(crate) fn start_error(err: &dyn Display, secrets: &[String]) -> Failure {
    Failure {
        error: sanitize(&err.to_string(), secrets),
        exit_code: None,
    }
}

/// The registry credentials the node would use for `image`: the `user:token`
/// pair and the token alone.
pub(crate) fn registry_secrets(image: &str) -> Vec<String> {
    let Some(pair) = crate::containerd::runtime::registry_auth(image) else {
        return Vec::new();
    };
    let token = pair.split_once(':').map(|(_, t)| t.to_string());
    std::iter::once(pair).chain(token).collect()
}

/// One line of at most [`MAX_LEN`] characters with credentials blanked out.
///
/// shortcut: finds secrets by exact value, URL userinfo, `key=value` pairs
/// `Bearer`/`Basic`/`--user` arguments; a secret in another shape passes through.
/// Upgrade by capping the text harder if a runtime starts echoing more.
pub(crate) fn sanitize(raw: &str, secrets: &[String]) -> String {
    let mut text = raw.to_string();
    // Longest first, so a credential pair goes before its token.
    let mut secrets: Vec<&String> = secrets.iter().filter(|s| s.chars().count() >= 4).collect();
    secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
    for secret in secrets {
        text = text.replace(secret.as_str(), "***");
    }
    let text: String = text
        .chars()
        .filter(|c| !c.is_control() || c.is_whitespace())
        .collect();

    let mut words: Vec<String> = Vec::new();
    let mut hide_next = false;
    for word in text.split_whitespace() {
        let lower = word.to_ascii_lowercase();
        let shown = if hide_next {
            "***".to_string()
        } else {
            redact_word(word)
        };
        hide_next = matches!(
            lower.trim_end_matches(':'),
            "bearer" | "basic" | "authorization" | "--user" | "-u" | "--password"
        );
        words.push(shown);
    }
    let line = words.join(" ");
    if line.chars().count() <= MAX_LEN {
        return line;
    }
    let mut cut: String = line.chars().take(MAX_LEN - 1).collect();
    cut.push('…');
    cut
}

fn redact_word(word: &str) -> String {
    // `scheme://user:pass@host/...`
    if let Some((scheme, rest)) = word.split_once("://") {
        let end = rest.find('/').unwrap_or(rest.len());
        if let Some(at) = rest[..end].rfind('@') {
            return format!("{scheme}://***@{}", &rest[at + 1..]);
        }
    }
    // `token=abc`, `--password=abc`, `api_key=abc`
    if let Some((key, _)) = word.split_once('=') {
        let lower = key.to_ascii_lowercase();
        if [
            "token",
            "password",
            "passwd",
            "secret",
            "auth",
            "key",
            "credential",
        ]
        .iter()
        .any(|k| lower.contains(k))
        {
            return format!("{key}=***");
        }
    }
    word.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_reasons_name_the_code_and_a_signal() {
        assert_eq!(exit(Some(1), None).error, "exited with code 1");
        assert_eq!(exit(Some(1), None).exit_code, Some(1));
        assert_eq!(
            exit(Some(137), None).error,
            "exited with code 137 (killed by signal 9 SIGKILL)"
        );
        assert_eq!(
            exit(Some(150), None).error,
            "exited with code 150 (killed by signal 22)"
        );
        assert!(exit(None, None).error.starts_with("exited"));
    }

    #[test]
    fn an_exit_after_a_healthcheck_kill_keeps_the_cause() {
        let f = exit(Some(137), Some(HEALTHCHECK_KILL));
        assert_eq!(
            f.error,
            "healthcheck failed; container killed (exit code 137)"
        );
        assert_eq!(f.exit_code, Some(137));
    }

    #[test]
    fn a_reason_is_one_line_capped_at_a_character_boundary() {
        let long = format!("failed\n{}", "é".repeat(1000));
        let line = sanitize(&long, &[]);
        assert!(!line.contains('\n'));
        assert_eq!(line.chars().count(), MAX_LEN);
        assert!(line.ends_with('…'));
        assert_eq!(sanitize("a\n\tb   c", &[]), "a b c");
    }

    #[test]
    fn credentials_in_a_pull_error_never_survive() {
        let token = "ghp_FAKE0123456789tokenvalue";
        let stderr = format!(
            "ctr pull ghcr.io/acme/web:1 failed (exit status: 1): \
             ctr: failed to resolve reference: pulling from https://deploy:{token}@ghcr.io/v2/acme/web \
             Authorization: Bearer {token} retry with password={token} --user deploy:{token}"
        );
        let secrets = [format!("deploy:{token}"), token.to_string()];
        // With the known credentials, and with only the generic patterns.
        for secrets in [&secrets[..], &[]] {
            let line = sanitize(&stderr, secrets);
            assert!(!line.contains(token), "leaked: {line}");
            assert!(line.contains("ghcr.io/acme/web"), "lost the cause: {line}");
        }
    }

    #[test]
    fn a_start_error_is_sanitized_and_has_no_exit_code() {
        let err = crate::runtime::RuntimeError::Start("image pull failed\nx".into());
        let f = start_error(&err, &[]);
        assert_eq!(f.error, "failed to start container: image pull failed x");
        assert_eq!(f.exit_code, None);
    }
}
