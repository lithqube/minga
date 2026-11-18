//! Tenant-scoped NATS user credentials for dedicated worker nodes.

use std::collections::BTreeMap;

use nats_jwt_rs::{account::Account, operator::Operator, types::ScopeType, user::User, Claims};
use nkeys::KeyPair;

/// A manager-local map of validated tenant signing keys. Seed material is never serialized.
pub struct NatsCredentialIssuer {
    account_public_key: String,
    account_signer: KeyPair,
    account_claim_jwt: String,
    tenant_signers: BTreeMap<String, KeyPair>,
}

impl NatsCredentialIssuer {
    /// Build the issuer from owner-only operator/account JWT and NKey seed files.
    pub fn new(
        operator_jwt: &str,
        account_jwt: &str,
        account_seed: &str,
        tenant_signer_seeds: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, String> {
        let operator = Claims::<Operator>::decode(operator_jwt)
            .map_err(|error| format!("invalid NATS operator JWT: {error}"))?;
        let account_claim = Claims::<Account>::decode(account_jwt)
            .map_err(|error| format!("invalid NATS account JWT: {error}"))?;
        let account_signer = KeyPair::from_seed(account_seed)
            .map_err(|error| format!("invalid NATS account signing seed: {error}"))?;
        if account_signer.public_key() != account_claim.sub {
            return Err("NATS account seed does not match account JWT subject".to_string());
        }
        let operator_signers = operator.nats.signing_keys.as_deref().unwrap_or_default();
        if account_claim.iss != operator.sub && !operator_signers.contains(&account_claim.iss) {
            return Err("NATS account JWT is not signed by the configured operator".to_string());
        }
        let account_public_key = account_claim.sub.clone();
        let account_signing_keys = account_claim
            .nats
            .signing_keys
            .as_ref()
            .ok_or_else(|| "NATS account JWT has no tenant signing keys".to_string())?;
        let mut tenant_signers = BTreeMap::new();
        for (tenant_id, seed) in tenant_signer_seeds {
            minga_state::types::Tenant::validate_id(&tenant_id)
                .map_err(|error| format!("invalid tenant signer id `{tenant_id}`: {error}"))?;
            let key = KeyPair::from_seed(&seed)
                .map_err(|error| format!("invalid NATS signing seed for `{tenant_id}`: {error}"))?;
            let scope = account_signing_keys
                .iter()
                .find(|signing_key| signing_key.key == key.public_key())
                .and_then(|signing_key| signing_key.scope.as_ref())
                .ok_or_else(|| {
                    format!("tenant signer for `{tenant_id}` is not scoped in the account JWT")
                })?;
            if scope.kind != ScopeType::UserScope {
                return Err(format!(
                    "tenant signer for `{tenant_id}` is not a user signer"
                ));
            }
            let template = scope.template.as_ref().ok_or_else(|| {
                format!("tenant signer for `{tenant_id}` has no permission template")
            })?;
            let (publish, subscribe) = worker_permission_template(&tenant_id);
            if template.permissions.publish.allow != publish
                || template.permissions.subscribe.allow != subscribe
            {
                return Err(format!(
                    "tenant signer for `{tenant_id}` has an unexpected permission template"
                ));
            }
            if tenant_signers.insert(tenant_id.clone(), key).is_some() {
                return Err(format!("duplicate NATS signing key for `{tenant_id}`"));
            }
        }
        if tenant_signers.is_empty() {
            return Err("no tenant-scoped NATS signing keys configured".to_string());
        }
        Ok(Self {
            account_public_key,
            account_signer,
            account_claim_jwt: account_jwt.to_owned(),
            tenant_signers,
        })
    }
    /// Sign a claim built from the complete replicated revocation set.
    pub fn account_claim_with_revocations(
        &self,
        user_public_keys: &[String],
        revoked_at: u64,
    ) -> Result<String, String> {
        let mut claim = Claims::<Account>::decode(&self.account_claim_jwt)
            .map_err(|error| format!("invalid NATS account JWT: {error}"))?;
        let revocations = claim.nats.revocations.get_or_insert_with(BTreeMap::new);
        for user_public_key in user_public_keys {
            nkeys::KeyPair::from_public_key(user_public_key)
                .map_err(|error| format!("invalid revoked NATS user public key: {error}"))?;
            revocations.insert(user_public_key.clone(), revoked_at);
        }
        claim
            .encode(&self.account_signer)
            .map_err(|error| format!("cannot sign NATS account revocation: {error}"))
    }
    /// Validates a worker NKey before its tombstone is committed to Raft.
    pub fn validate_user_public_key(&self, user_public_key: &str) -> Result<(), String> {
        nkeys::KeyPair::from_public_key(user_public_key)
            .map_err(|error| format!("invalid revoked NATS user public key: {error}"))?;
        Ok(())
    }

    /// Public NKey of the account whose claim signs this tenant's users.
    pub fn account_public_key(&self) -> &str {
        &self.account_public_key
    }

    /// Mint a user JWT and standard `.creds` file for one tenant-bound node.
    pub fn issue(&self, tenant_id: &str, node_id: u64) -> Result<(String, String), String> {
        let signer = self
            .tenant_signers
            .get(tenant_id)
            .ok_or_else(|| format!("no NATS signing key for tenant `{tenant_id}`"))?;
        let user_key = KeyPair::new_user();
        let public_key = user_key.public_key();
        let mut claim = User::new_claims(format!("minga-worker-{node_id}"), public_key.clone());
        claim.nats.issuer_account = Some(self.account_public_key.clone());
        let (template_publish, template_subscribe) = worker_permission_template(tenant_id);
        claim.nats.permissions.permissions.publish.allow = template_publish
            .iter()
            .map(|subject| subject.replace("{{tag(node)}}", &node_id.to_string()))
            .collect();
        claim.nats.permissions.permissions.subscribe.allow = template_subscribe
            .iter()
            .map(|subject| subject.replace("{{tag(node)}}", &node_id.to_string()))
            .collect();
        claim.nats.generic_fields.tags = Some(vec![format!("node:{node_id}")]);
        let jwt = claim
            .encode(signer)
            .map_err(|error| format!("cannot sign worker JWT: {error}"))?;
        let seed = user_key
            .seed()
            .map_err(|error| format!("cannot encode worker NKey seed: {error}"))?;
        let credentials = format!(
            "-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------\n\n************************* IMPORTANT *************************\nNKEY Seed printed below can be used to sign and authenticate\nto the NATS server. Protect and distribute it securely.\n\n-----BEGIN USER NKEY SEED-----\n{seed}\n------END USER NKEY SEED------\n"
        );
        Ok((public_key, credentials))
    }
}

fn worker_permission_template(tenant_id: &str) -> (Vec<String>, Vec<String>) {
    let node = "{{tag(node)}}";
    let publish = vec![
        format!("minga.{tenant_id}.log.node.{node}.>"),
        format!("$KV.MINGA_STATUS_{tenant_id}.node.{node}.task.>"),
        format!("$KV.MINGA_LEASES.{tenant_id}.lease.node.{node}"),
        "$JS.API.STREAM.INFO.KV_MINGA_ASSIGN".to_string(),
        format!("$JS.API.STREAM.INFO.KV_MINGA_STATUS_{tenant_id}"),
        "$JS.API.STREAM.INFO.KV_MINGA_LEASES".to_string(),
        "$JS.FC.KV_MINGA_ASSIGN.>".to_string(),
        format!("$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.*.$KV.MINGA_ASSIGN.{tenant_id}.node.{node}.task.>"),
        format!("$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.*.$KV.MINGA_ASSIGN.{tenant_id}.node.{node}.ingress.>"),
        format!("$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.*.$KV.MINGA_ASSIGN.{tenant_id}.node.{node}.net.>"),
        "$JS.API.CONSUMER.INFO.KV_MINGA_ASSIGN.*".to_string(),
        format!("$JS.API.DIRECT.GET.KV_MINGA_ASSIGN.$KV.MINGA_ASSIGN.{tenant_id}.node.{node}.>"),
        format!("$JS.API.DIRECT.GET.KV_MINGA_STATUS_{tenant_id}.$KV.MINGA_STATUS_{tenant_id}.node.{node}.task.>"),
        format!("$JS.API.DIRECT.GET.KV_MINGA_LEASES.$KV.MINGA_LEASES.{tenant_id}.lease.node.{node}"),
        "_INBOX.>".to_string(),
    ];
    let subscribe = vec![
        format!("$KV.MINGA_ASSIGN.{tenant_id}.node.{node}.task.>"),
        format!("$KV.MINGA_ASSIGN.{tenant_id}.node.{node}.ingress.>"),
        format!("$KV.MINGA_ASSIGN.{tenant_id}.node.{node}.net.>"),
        format!("$KV.MINGA_ASSIGN.{tenant_id}.node.{node}.wg"),
        format!("$KV.MINGA_STATUS_{tenant_id}.node.{node}.task.>"),
        format!("$KV.MINGA_LEASES.{tenant_id}.lease.node.{node}"),
        format!("minga.{tenant_id}.ctl.node.{node}.ping"),
        format!("minga.{tenant_id}.ctl.node.{node}.drain"),
        "_INBOX.>".to_string(),
    ];
    (publish, subscribe)
}

#[cfg(test)]
pub(crate) fn test_issuer(tenant_id: &str) -> NatsCredentialIssuer {
    use nats_jwt_rs::{
        account::Account,
        operator::Operator,
        types::{Permission, Permissions, ScopeType, SigningKey, UserScope},
        user::UserPermissionLimits,
    };

    let operator_key = KeyPair::new_operator();
    let account_key = KeyPair::new_account();
    let tenant_signer = KeyPair::new_account();
    let (publish, subscribe) = worker_permission_template(tenant_id);
    let template = UserPermissionLimits {
        permissions: Permissions {
            publish: Permission {
                allow: publish,
                ..Default::default()
            },
            subscribe: Permission {
                allow: subscribe,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let scope = UserScope {
        kind: ScopeType::UserScope,
        key: tenant_signer.public_key(),
        role: Some("worker".to_string()),
        template: Some(template),
        description: None,
    };
    let mut account = Account::new_claims("Minga".to_string(), account_key.public_key());
    account.nats.signing_keys = Some(
        [SigningKey {
            key: tenant_signer.public_key(),
            scope: Some(scope),
        }]
        .into_iter()
        .collect(),
    );
    let account_jwt = account.encode(&operator_key).unwrap();
    let operator = Operator::new_claims("Minga".to_string(), operator_key.public_key());
    let operator_jwt = operator.encode(&operator_key).unwrap();
    NatsCredentialIssuer::new(
        &operator_jwt,
        &account_jwt,
        &account_key.seed().unwrap(),
        [(tenant_id.to_string(), tenant_signer.seed().unwrap())],
    )
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use nats_jwt_rs::Claims;

    #[test]
    fn issued_credentials_are_node_and_tenant_scoped() {
        let issuer = test_issuer("tenant-a");
        let (public_key, credentials) = issuer.issue("tenant-a", 7).unwrap();
        let jwt = credentials
            .split("-----BEGIN NATS USER JWT-----\n")
            .nth(1)
            .unwrap()
            .split('\n')
            .next()
            .unwrap();
        let claim = Claims::<User>::decode(jwt).unwrap();
        assert_eq!(claim.sub, public_key);
        let publish = &claim.nats.permissions.permissions.publish.allow;
        let subscribe = &claim.nats.permissions.permissions.subscribe.allow;
        assert!(publish.iter().all(|subject| !subject.contains("tenant-b")));
        assert!(subscribe.iter().all(|subject| !subject.contains("node.8")));
        assert!(publish.contains(&"$KV.MINGA_STATUS_tenant-a.node.7.task.>".to_string()));
        assert!(publish.contains(&"$JS.API.CONSUMER.INFO.KV_MINGA_ASSIGN.*".to_string()));
        let seed = credentials
            .split("-----BEGIN USER NKEY SEED-----\n")
            .nth(1)
            .unwrap()
            .split('\n')
            .next()
            .unwrap();
        assert_eq!(KeyPair::from_seed(seed).unwrap().public_key(), public_key);
    }

    #[test]
    fn issuer_rejects_missing_tenant_signer() {
        assert!(test_issuer("tenant-a").issue("tenant-b", 7).is_err());
    }

    #[test]
    fn revocation_claim_contains_the_complete_replicated_key_set() {
        let issuer = test_issuer("tenant-a");
        let (user_key, _) = issuer.issue("tenant-a", 7).unwrap();
        let (another_user_key, _) = issuer.issue("tenant-a", 8).unwrap();
        let updated = issuer
            .account_claim_with_revocations(&[user_key.clone(), another_user_key.clone()], 1234)
            .unwrap();
        let account = Claims::<Account>::decode(&updated).unwrap();
        let revocations = account.nats.revocations.unwrap();
        assert_eq!(revocations.get(&user_key), Some(&1234));
        assert_eq!(revocations.get(&another_user_key), Some(&1234));
    }
}
