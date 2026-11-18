//! Short-lived, manager-signed authorization for a node to join one tenant.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use thiserror::Error;

const TOKEN_VERSION: u8 = 1;
const SIGNATURE_BYTES: usize = 32;

/// HMAC signer for tenant-bound node join tokens.
///
/// Managers in one cluster must be provisioned with the same 32-byte key.
/// Keep it owner-only and separate from the API bearer token.
pub struct JoinTokenKey([u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinTokenClaims {
    version: u8,
    pub node_id: u64,
    pub tenant_id: String,
    pub issued_at: u64,
    pub expires_at: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum JoinTokenError {
    #[error("join-token key must be exactly 32 bytes")]
    InvalidKey,
    #[error("join token is malformed")]
    Malformed,
    #[error("join token signature is invalid")]
    InvalidSignature,
    #[error("join token is expired or not yet valid")]
    Expired,
    #[error("join token is for node {actual}, not node {expected}")]
    WrongNode { expected: u64, actual: u64 },
    #[error("join token tenant is invalid: {0}")]
    InvalidTenant(String),
}

impl JoinTokenKey {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, JoinTokenError> {
        let key: [u8; 32] = bytes.try_into().map_err(|_| JoinTokenError::InvalidKey)?;
        Ok(Self(key))
    }

    /// Issue a versioned token whose signed claims bind one node to one tenant.
    pub fn issue(
        &self,
        node_id: u64,
        tenant_id: impl Into<String>,
        issued_at: u64,
        expires_at: u64,
    ) -> Result<String, JoinTokenError> {
        let claims = JoinTokenClaims {
            version: TOKEN_VERSION,
            node_id,
            tenant_id: tenant_id.into(),
            issued_at,
            expires_at,
        };
        minga_state_tenant_id_validate(&claims.tenant_id)?;
        if expires_at <= issued_at {
            return Err(JoinTokenError::Expired);
        }
        let payload = serde_json::to_vec(&claims).map_err(|_| JoinTokenError::Malformed)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0).expect("HMAC accepts any key size");
        mac.update(&payload);
        let signature = mac.finalize().into_bytes();
        Ok(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(payload),
            URL_SAFE_NO_PAD.encode(signature)
        ))
    }

    /// Verify signature, node binding, tenant ID and time bounds before use.
    pub fn verify(
        &self,
        token: &str,
        expected_node_id: u64,
        now: u64,
    ) -> Result<JoinTokenClaims, JoinTokenError> {
        let (payload, signature) = token.split_once('.').ok_or(JoinTokenError::Malformed)?;
        let payload = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| JoinTokenError::Malformed)?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| JoinTokenError::Malformed)?;
        if signature.len() != SIGNATURE_BYTES {
            return Err(JoinTokenError::Malformed);
        }
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0).expect("HMAC accepts any key size");
        mac.update(&payload);
        mac.verify_slice(&signature)
            .map_err(|_| JoinTokenError::InvalidSignature)?;
        let claims: JoinTokenClaims =
            serde_json::from_slice(&payload).map_err(|_| JoinTokenError::Malformed)?;
        if claims.version != TOKEN_VERSION {
            return Err(JoinTokenError::Malformed);
        }
        minga_state_tenant_id_validate(&claims.tenant_id)?;
        if claims.node_id != expected_node_id {
            return Err(JoinTokenError::WrongNode {
                expected: expected_node_id,
                actual: claims.node_id,
            });
        }
        if claims.issued_at > now.saturating_add(60) || claims.expires_at <= now {
            return Err(JoinTokenError::Expired);
        }
        Ok(claims)
    }
}

fn minga_state_tenant_id_validate(tenant_id: &str) -> Result<(), JoinTokenError> {
    crate::types::Tenant::validate_id(tenant_id)
        .map_err(|error| JoinTokenError::InvalidTenant(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(fill: u8) -> JoinTokenKey {
        JoinTokenKey::from_bytes(&[fill; 32]).unwrap()
    }

    #[test]
    fn token_binds_node_tenant_and_expiry() {
        let token = key(7).issue(12, "acme", 100, 200).unwrap();
        let claims = key(7).verify(&token, 12, 150).unwrap();
        assert_eq!(claims.tenant_id, "acme");
        assert_eq!(claims.node_id, 12);
        assert_eq!(
            key(7).verify(&token, 13, 150),
            Err(JoinTokenError::WrongNode {
                expected: 13,
                actual: 12
            })
        );
        assert_eq!(key(7).verify(&token, 12, 200), Err(JoinTokenError::Expired));
        assert_eq!(
            key(8).verify(&token, 12, 150),
            Err(JoinTokenError::InvalidSignature)
        );
    }

    #[test]
    fn token_rejects_malformed_and_invalid_tenant() {
        assert_eq!(
            key(7).verify("invalid", 1, 100),
            Err(JoinTokenError::Malformed)
        );
        assert!(matches!(
            key(7).issue(1, "Tenant-A", 100, 200),
            Err(JoinTokenError::InvalidTenant(_))
        ));
        assert!(matches!(
            JoinTokenKey::from_bytes(&[0; 31]),
            Err(JoinTokenError::InvalidKey)
        ));
    }
}
