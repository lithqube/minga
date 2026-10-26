//! Secret and PII redaction for logs and error messages.
//!
//! The [`SecretRedactor`] applies regex-based redaction rules to text,
//! replacing matched secrets with a placeholder. Built-in rules cover
//! common secret patterns: API keys, Bearer tokens, AWS keys, private
//! keys, credit card numbers, and email addresses.

use regex::Regex;
use serde::{Deserialize, Serialize};

/// A redaction rule: a named regex pattern and its replacement text.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactionRule {
    /// Human-readable name for the rule.
    pub name: String,
    /// The regex pattern to match.
    pub pattern: String,
    /// Replacement text (supports capture groups, e.g. `${1}`).
    pub replacement: String,
}

impl RedactionRule {
    /// Compiles the rule into a usable form.
    pub fn compile(&self) -> Result<CompiledRule, regex::Error> {
        Ok(CompiledRule {
            name: self.name.clone(),
            regex: Regex::new(&self.pattern)?,
            replacement: self.replacement.clone(),
        })
    }
}

/// A compiled redaction rule.
#[derive(Debug, Clone)]
pub struct CompiledRule {
    pub name: String,
    pub regex: Regex,
    pub replacement: String,
}

/// A secret/PII redactor that applies multiple redaction rules.
#[derive(Debug, Clone)]
pub struct SecretRedactor {
    rules: Vec<CompiledRule>,
}

impl SecretRedactor {
    /// Creates a redactor with the built-in rules for common secret patterns.
    pub fn with_defaults() -> Self {
        let default_rules = vec![
            // OpenAI API keys: sk-...
            RedactionRule {
                name: "openai-key".into(),
                pattern: r"sk-[a-zA-Z0-9]{20,}[a-zA-Z0-9]*".into(),
                replacement: "sk-***REDACTED***".into(),
            },
            // Anthropic API keys: sk-ant-...
            RedactionRule {
                name: "anthropic-key".into(),
                pattern: r"sk-ant-[a-zA-Z0-9_-]{20,}".into(),
                replacement: "sk-ant-***REDACTED***".into(),
            },
            // Generic API keys in headers/JSON: api_key=..., "api_key": ...
            RedactionRule {
                name: "api-key-param".into(),
                pattern: r#"(?i)["']?(api[_-]?key|apikey|access[_-]?token|auth[_-]?token|secret[_-]?key|password|passwd|pwd)["']?(\s*[=:]\s*["']?)[^\s"']+"#.into(),
                replacement: "${1}${2}***REDACTED***".into(),
            },
            // Bearer tokens
            RedactionRule {
                name: "bearer-token".into(),
                pattern: r"(?i)bearer\s+[a-zA-Z0-9._~+/=-]+".into(),
                replacement: "Bearer ***REDACTED***".into(),
            },
            // AWS Access Key ID: AKIA...
            RedactionRule {
                name: "aws-access-key".into(),
                pattern: r"AKIA[0-9A-Z]{16}".into(),
                replacement: "AKIA***REDACTED***".into(),
            },
            // AWS Secret Access Key (40-char base64-like)
            RedactionRule {
                name: "aws-secret-key".into(),
                pattern: r#"(?i)aws.{0,20}secret.{0,20}["']?([a-zA-Z0-9/+=]{40})["']?"#.into(),
                replacement: "aws_secret=***REDACTED***".into(),
            },
            // Private keys (PEM format)
            RedactionRule {
                name: "private-key".into(),
                pattern: r"-----BEGIN (?:RSA |EC |OPENSSH |DSA |PGP )?PRIVATE KEY(?: BLOCK)?-----[\s\S]*?-----END (?:RSA |EC |OPENSSH |DSA |PGP )?PRIVATE KEY(?: BLOCK)?-----".into(),
                replacement: "-----BEGIN PRIVATE KEY-----\n***REDACTED***\n-----END PRIVATE KEY-----".into(),
            },
            // JWT tokens: xxx.yyy.zzz
            RedactionRule {
                name: "jwt".into(),
                pattern: r"eyJ[a-zA-Z0-9_-]+\.eyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+".into(),
                replacement: "***JWT_REDACTED***".into(),
            },
            // Credit card numbers (Visa, MC, Amex, Discover)
            RedactionRule {
                name: "credit-card".into(),
                pattern: r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})\b".into(),
                replacement: "***CC_REDACTED***".into(),
            },
            // Email addresses
            RedactionRule {
                name: "email".into(),
                pattern: r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}".into(),
                replacement: "***EMAIL_REDACTED***".into(),
            },
            // Generic long hex strings (32+ chars, could be secrets)
            RedactionRule {
                name: "hex-secret".into(),
                pattern: r"\b[0-9a-fA-F]{32,}\b".into(),
                replacement: "***HEX_REDACTED***".into(),
            },
        ];

        Self::new(default_rules)
    }

    /// Creates a redactor from a list of rules. Invalid regex patterns
    /// are silently skipped.
    pub fn new(rules: Vec<RedactionRule>) -> Self {
        let compiled: Vec<CompiledRule> = rules
            .into_iter()
            .filter_map(|r| match r.compile() {
                Ok(c) => Some(c),
                Err(e) => {
                    tracing::warn!(rule = %r.name, error = %e, "invalid redaction rule, skipping");
                    None
                }
            })
            .collect();
        Self { rules: compiled }
    }

    /// Creates a redactor with custom compiled rules.
    pub fn from_compiled(rules: Vec<CompiledRule>) -> Self {
        Self { rules }
    }

    /// Redacts all matched secrets in the input text.
    pub fn redact(&self, text: &str) -> String {
        let mut result = text.to_string();
        for rule in &self.rules {
            result = rule
                .regex
                .replace_all(&result, rule.replacement.as_str())
                .into_owned();
        }
        result
    }

    /// Redacts secrets in the input and returns the result along with
    /// the names of rules that matched.
    pub fn redact_with_matches(&self, text: &str) -> (String, Vec<String>) {
        let mut result = text.to_string();
        let mut matched = Vec::new();
        for rule in &self.rules {
            if rule.regex.is_match(&result) {
                matched.push(rule.name.clone());
                result = rule
                    .regex
                    .replace_all(&result, rule.replacement.as_str())
                    .into_owned();
            }
        }
        (result, matched)
    }

    /// Returns the number of active redaction rules.
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }

    /// Returns the names of all active rules.
    pub fn rule_names(&self) -> Vec<&str> {
        self.rules.iter().map(|r| r.name.as_str()).collect()
    }
}

impl Default for SecretRedactor {
    fn default() -> Self {
        Self::with_defaults()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_openai_key() {
        let r = SecretRedactor::with_defaults();
        let input = "my key is sk-abc123def456ghi789jkl012mno345pqr";
        let output = r.redact(input);
        assert!(!output.contains("sk-abc123"));
        assert!(output.contains("REDACTED"));
    }

    #[test]
    fn redact_anthropic_key() {
        let r = SecretRedactor::with_defaults();
        let input = "key: sk-ant-api03-abc123def456ghi789jkl012";
        let output = r.redact(input);
        assert!(!output.contains("sk-ant-api03"));
        assert!(output.contains("REDACTED"));
    }

    #[test]
    fn redact_bearer_token() {
        let r = SecretRedactor::with_defaults();
        let input = "Authorization: Bearer abc123.def456.ghi789";
        let output = r.redact(input);
        assert!(!output.contains("abc123.def456"));
        assert!(output.contains("REDACTED"));
    }

    #[test]
    fn redact_aws_access_key() {
        let r = SecretRedactor::with_defaults();
        let input = "aws key: AKIAIOSFODNN7EXAMPLE";
        let output = r.redact(input);
        assert!(!output.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(output.contains("REDACTED"));
    }

    #[test]
    fn redact_private_key() {
        let r = SecretRedactor::with_defaults();
        let input =
            "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA...\n-----END RSA PRIVATE KEY-----";
        let output = r.redact(input);
        assert!(!output.contains("MIIEpAIBAAKCAQEA"));
        assert!(output.contains("REDACTED"));
    }

    #[test]
    fn redact_jwt() {
        let r = SecretRedactor::with_defaults();
        let input =
            "token: eyJhbGciOiJIUzI1NiIs.eyJzdWIiOiIxMjM0NTY3ODkwIn0.eyJhbGciOiJIUzI1NiJ9.abc123";
        let output = r.redact(input);
        assert!(!output.contains("eyJhbGciOiJIUzI1NiIs"));
        assert!(output.contains("JWT_REDACTED"));
    }

    #[test]
    fn redact_credit_card() {
        let r = SecretRedactor::with_defaults();
        // Note: with spaces, the regex won't match. Test without spaces.
        let input = "card: 4111111111111111";
        let output = r.redact(input);
        assert!(!output.contains("4111111111111111"));
        assert!(output.contains("CC_REDACTED"));
    }

    #[test]
    fn redact_email() {
        let r = SecretRedactor::with_defaults();
        let input = "contact: john.doe@example.com";
        let output = r.redact(input);
        assert!(!output.contains("john.doe@example.com"));
        assert!(output.contains("EMAIL_REDACTED"));
    }

    #[test]
    fn redact_api_key_param() {
        let r = SecretRedactor::with_defaults();
        let input = r#"{"api_key": "secret123", "model": "gpt-4"}"#;
        let output = r.redact(input);
        assert!(!output.contains("secret123"));
        assert!(output.contains("REDACTED"));
        assert!(output.contains("gpt-4")); // non-secret preserved
    }

    #[test]
    fn redact_hex_secret() {
        let r = SecretRedactor::with_defaults();
        let input = "hash: abcdef1234567890abcdef1234567890abcdef1234567890";
        let output = r.redact(input);
        assert!(!output.contains("abcdef1234567890"));
        assert!(output.contains("HEX_REDACTED"));
    }

    #[test]
    fn no_redaction_needed() {
        let r = SecretRedactor::with_defaults();
        let input = "Hello, world! This is a normal message.";
        let output = r.redact(input);
        assert_eq!(output, input);
    }

    #[test]
    fn redact_with_matches() {
        let r = SecretRedactor::with_defaults();
        let input = "key: sk-abc123def456ghi789jkl012 email: test@example.com";
        let (output, matched) = r.redact_with_matches(input);
        assert!(!output.contains("sk-abc123"));
        assert!(!output.contains("test@example.com"));
        assert!(matched.contains(&"openai-key".to_string()));
        assert!(matched.contains(&"email".to_string()));
    }

    #[test]
    fn custom_rule() {
        let rules = vec![RedactionRule {
            name: "custom".into(),
            pattern: r"secret-\d+".into(),
            replacement: "***".into(),
        }];
        let r = SecretRedactor::new(rules);
        let output = r.redact("the code is secret-42 ok?");
        assert_eq!(output, "the code is *** ok?");
    }

    #[test]
    fn invalid_regex_skipped() {
        let rules = vec![
            RedactionRule {
                name: "bad".into(),
                pattern: r"(unclosed".into(),
                replacement: "x".into(),
            },
            RedactionRule {
                name: "good".into(),
                pattern: r"hello".into(),
                replacement: "hi".into(),
            },
        ];
        let r = SecretRedactor::new(rules);
        assert_eq!(r.rule_count(), 1); // only the valid rule
        assert_eq!(r.redact("hello world"), "hi world");
    }

    #[test]
    fn rule_names() {
        let r = SecretRedactor::with_defaults();
        let names = r.rule_names();
        assert!(names.contains(&"openai-key"));
        assert!(names.contains(&"bearer-token"));
        assert!(names.contains(&"jwt"));
        assert!(names.contains(&"email"));
    }

    #[test]
    fn multiple_secrets_in_one_text() {
        let r = SecretRedactor::with_defaults();
        let input =
            "key=sk-abc123def456ghi789jkl012 and email=user@test.com and AKIAIOSFODNN7EXAMPLE";
        let output = r.redact(input);
        assert!(!output.contains("sk-abc123"));
        assert!(!output.contains("user@test.com"));
        assert!(!output.contains("AKIAIOSFODNN7EXAMPLE"));
        assert_eq!(output.matches("REDACTED").count(), 3);
    }

    #[test]
    fn default_is_with_defaults() {
        let r = SecretRedactor::default();
        assert!(r.rule_count() > 5);
    }
}
