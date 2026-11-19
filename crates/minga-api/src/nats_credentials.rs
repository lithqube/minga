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
            if !permission_lists_match(&template.permissions.publish.allow, &publish)
                || !permission_lists_match(&template.permissions.subscribe.allow, &subscribe)
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
        // NATS expands this scoped signer template from the node tag.
        claim.nats.generic_fields.tags = Some(vec![format!("node:{node_id}")]);
        claim.nats.permissions.limits = None;
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
/// Order-insensitive, but duplicates count: `[X, X]` does not match `[X, Y]`.
fn permission_lists_match(actual: &[String], expected: &[String]) -> bool {
    let mut actual = actual.to_vec();
    let mut expected = expected.to_vec();
    actual.sort();
    expected.sort();
    actual == expected
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
        format!("$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.*.$KV.MINGA_ASSIGN.{tenant_id}.node.{node}.wg"),
        "$JS.API.CONSUMER.INFO.KV_MINGA_ASSIGN.*".to_string(),
        format!("$JS.API.DIRECT.GET.KV_MINGA_ASSIGN.$KV.MINGA_ASSIGN.{tenant_id}.node.{node}.>"),
        format!("$JS.API.DIRECT.GET.KV_MINGA_STATUS_{tenant_id}.$KV.MINGA_STATUS_{tenant_id}.node.{node}.task.>"),
        format!("$JS.API.DIRECT.GET.KV_MINGA_LEASES.$KV.MINGA_LEASES.{tenant_id}.lease.node.{node}"),
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
        format!("_INBOX.{tenant_id}.{node}.>"),
    ];
    (publish, subscribe)
}

#[cfg(test)]
pub(crate) fn test_issuer(tenant_id: &str) -> NatsCredentialIssuer {
    test_issuer_with_template(tenant_id, |_, _| {}).unwrap()
}

#[cfg(test)]
fn test_issuer_with_template(
    tenant_id: &str,
    edit_template: fn(&mut Vec<String>, &mut Vec<String>),
) -> Result<NatsCredentialIssuer, String> {
    use nats_jwt_rs::{
        account::Account,
        operator::Operator,
        types::{Permission, Permissions, ScopeType, SigningKey, UserScope},
        user::UserPermissionLimits,
    };

    let operator_key = KeyPair::new_operator();
    let account_key = KeyPair::new_account();
    let tenant_signer = KeyPair::new_account();
    let (mut publish, mut subscribe) = worker_permission_template(tenant_id);
    edit_template(&mut publish, &mut subscribe);
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
        assert!(claim.nats.permissions.permissions.publish.allow.is_empty());
        assert!(claim
            .nats
            .permissions
            .permissions
            .subscribe
            .allow
            .is_empty());
        assert!(claim.nats.permissions.limits.as_ref().is_none_or(|limits| {
            limits.user_limits.is_none()
                && limits.nats_limits.as_ref().is_none_or(|nats_limits| {
                    nats_limits.subs.is_none()
                        && nats_limits.data.is_none()
                        && nats_limits.payload.is_none()
                })
        }));
        assert!(claim
            .nats
            .generic_fields
            .tags
            .as_ref()
            .is_some_and(|tags| tags == &["node:7"]));
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
    fn wireguard_watch_permissions_are_limited_to_the_tenant_node_key() {
        let (publish, subscribe) = worker_permission_template("tenant-a");
        let create_consumer =
            "$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.*.$KV.MINGA_ASSIGN.tenant-a.node.{{tag(node)}}.wg";

        assert!(publish
            .iter()
            .any(|permission| permission == create_consumer));
        assert!(subscribe
            .iter()
            .any(|permission| permission == "$KV.MINGA_ASSIGN.tenant-a.node.{{tag(node)}}.wg"));
    }

    #[test]
    fn issuer_accepts_sorted_signer_template_permissions() {
        let issuer = test_issuer_with_template("tenant-a", |publish, subscribe| {
            publish.sort();
            subscribe.sort();
        })
        .unwrap();

        assert!(issuer.issue("tenant-a", 7).is_ok());
    }

    #[test]
    fn issuer_rejects_a_template_that_repeats_one_permission_in_place_of_another() {
        let result = test_issuer_with_template("tenant-a", |_, subscribe| {
            subscribe[1] = subscribe[0].clone();
        });

        assert!(result.is_err_and(|error| error.contains("unexpected permission template")));
    }

    /// NATS subject matching: `*` is one token, a trailing `>` one or more.
    fn subject_matches(pattern: &str, subject: &str) -> bool {
        let (mut pattern, mut subject) = (pattern.split('.'), subject.split('.'));
        loop {
            match (pattern.next(), subject.next()) {
                (Some(">"), Some(_)) => return true,
                (Some(p), Some(s)) if p == "*" || p == s => {}
                (None, None) => return true,
                _ => return false,
            }
        }
    }

    /// What NATS grants the worker: the signer template expanded with the
    /// claim's `node:` tag, as `{{tag(node)}}` does on the server.
    fn effective_worker_permissions(
        issuer: &NatsCredentialIssuer,
        tenant_id: &str,
        node_id: u64,
    ) -> (Vec<String>, Vec<String>) {
        let (_, credentials) = issuer.issue(tenant_id, node_id).unwrap();
        let jwt = credentials
            .split("-----BEGIN NATS USER JWT-----\n")
            .nth(1)
            .and_then(|rest| rest.split('\n').next())
            .unwrap();
        let claim = Claims::<User>::decode(jwt).unwrap();
        let tags = claim.nats.generic_fields.tags.unwrap();
        let [node] = tags
            .iter()
            .filter_map(|tag| tag.strip_prefix("node:"))
            .collect::<Vec<_>>()[..]
        else {
            panic!("expected exactly one node tag, got {tags:?}");
        };
        let signer = issuer.tenant_signers[tenant_id].public_key();
        let account = Claims::<Account>::decode(&issuer.account_claim_jwt).unwrap();
        let template = account
            .nats
            .signing_keys
            .unwrap()
            .into_iter()
            .find(|key| key.key == signer)
            .and_then(|key| key.scope)
            .and_then(|scope| scope.template)
            .unwrap();
        let expand = |list: Vec<String>| {
            list.into_iter()
                .map(|permission| permission.replace("{{tag(node)}}", node))
                .collect::<Vec<_>>()
        };
        (
            expand(template.permissions.publish.allow),
            expand(template.permissions.subscribe.allow),
        )
    }

    #[test]
    fn effective_worker_permissions_cover_only_its_own_tenant_and_node() {
        let issuer = test_issuer("tenant-a");
        let (publish, subscribe) = effective_worker_permissions(&issuer, "tenant-a", 7);
        let allows = |list: &[String], subject: &str| {
            list.iter()
                .any(|permission| subject_matches(permission, subject))
        };

        assert!(publish
            .iter()
            .chain(&subscribe)
            .all(|permission| !permission.contains("{{")));
        for own in [
            "$KV.MINGA_ASSIGN.tenant-a.node.7.task.t1",
            "$KV.MINGA_ASSIGN.tenant-a.node.7.wg",
            "minga.tenant-a.ctl.node.7.drain",
        ] {
            assert!(allows(&subscribe, own), "{own} should be subscribable");
        }
        for own in [
            "$KV.MINGA_STATUS_tenant-a.node.7.task.t1",
            "minga.tenant-a.log.node.7.t1",
        ] {
            assert!(allows(&publish, own), "{own} should be publishable");
        }
        for foreign in [
            "$KV.MINGA_ASSIGN.tenant-a.node.8.task.t1",
            "$KV.MINGA_ASSIGN.tenant-b.node.7.task.t1",
            "$KV.MINGA_ASSIGN.tenant-a.node.8.wg",
            "$KV.MINGA_ASSIGN.tenant-b.node.7.wg",
            "$KV.MINGA_STATUS_tenant-a.node.8.task.t1",
            "$KV.MINGA_STATUS_tenant-b.node.7.task.t1",
            "$KV.MINGA_LEASES.tenant-a.lease.node.8",
            "$KV.MINGA_LEASES.tenant-b.lease.node.7",
            "minga.tenant-a.ctl.node.8.drain",
            "minga.tenant-b.ctl.node.7.drain",
            "minga.tenant-a.log.node.8.t1",
            "minga.tenant-b.log.node.7.t1",
            "_INBOX.tenant-a.8.reply",
            "_INBOX.tenant-b.7.reply",
            "$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.c.$KV.MINGA_ASSIGN.tenant-a.node.8.task.>",
            "$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.c.$KV.MINGA_ASSIGN.tenant-b.node.7.wg",
            "$JS.API.DIRECT.GET.KV_MINGA_ASSIGN.$KV.MINGA_ASSIGN.tenant-a.node.8.task.t1",
            "$JS.API.DIRECT.GET.KV_MINGA_STATUS_tenant-b.$KV.MINGA_STATUS_tenant-b.node.7.task.t1",
        ] {
            assert!(
                !allows(&publish, foreign) && !allows(&subscribe, foreign),
                "node 7 of tenant-a must not reach {foreign}"
            );
        }
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
