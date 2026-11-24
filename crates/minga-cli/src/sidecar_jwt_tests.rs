//! Manager restart in operator/JWT mode against a real `nats-server`
//! (issue #198): credentials changed while the manager is down must be
//! honoured by the restarted sidecar.
//!
//! Fails instead of skipping when `nats-server` is missing: a skipped
//! credential test reads as a passing one.

use std::path::{Path, PathBuf};

use minga_bus::tls::NatsTlsConfig;
use nats_jwt_rs::{
    account::Account,
    operator::Operator,
    types::{ScopeType, SigningKey, UserScope},
    user::{User, UserPermissionLimits},
    Claims,
};
use nkeys::KeyPair;

use super::*;

/// One CA, a `localhost` server leaf and one client leaf. In JWT mode the
/// certificate is transport identity only; the JWT is the principal.
struct Pki {
    ca: String,
    server: (String, String),
    client: (String, String),
}

fn make_pki() -> Pki {
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let leaf = |name: &str| {
        let key = rcgen::KeyPair::generate().unwrap();
        let params = rcgen::CertificateParams::new(vec![name.to_string()]).unwrap();
        let cert = params.signed_by(&key, &ca, &ca_key).unwrap();
        (cert.pem(), key.serialize_pem())
    };
    Pki {
        server: leaf("localhost"),
        client: leaf("minga-client"),
        ca: ca.pem(),
    }
}

fn write_private(path: &Path, contents: &str) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::write(path, contents).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Account JWT with one scoped signer per tenant, signed by the operator.
fn account_jwt(operator: &KeyPair, account: &KeyPair, signers: &[&KeyPair]) -> String {
    let mut claims = Account::new_claims("MINGA".to_string(), account.public_key());
    claims.nats.signing_keys = Some(
        signers
            .iter()
            .map(|signer| SigningKey {
                key: signer.public_key(),
                scope: Some(UserScope {
                    kind: ScopeType::UserScope,
                    key: signer.public_key(),
                    role: Some("worker".to_string()),
                    template: Some(UserPermissionLimits::default()),
                    description: None,
                }),
            })
            .collect(),
    );
    claims.encode(operator).unwrap()
}

/// A `.creds` file for a fresh user signed by `signer` on behalf of `account`.
fn user_creds(signer: &KeyPair, account: &KeyPair) -> String {
    let user = KeyPair::new_user();
    let mut claims = User::new_claims("minga-worker-7".to_string(), user.public_key());
    claims.nats.issuer_account = Some(account.public_key());
    // A scoped signer's users carry no limits of their own.
    claims.nats.permissions.limits = None;
    let jwt = claims.encode(signer).unwrap();
    let seed = user.seed().unwrap();
    format!(
        "-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------\n\n-----BEGIN USER NKEY SEED-----\n{seed}\n------END USER NKEY SEED------\n"
    )
}

/// Operator, account and SYS identities plus the on-disk layout the manager
/// passes to the sidecar.
struct Fixture {
    dir: tempfile::TempDir,
    pki: Pki,
    operator: KeyPair,
    account: KeyPair,
    auth: NatsJwtAuth,
}

impl Fixture {
    fn new(signers: &[&KeyPair]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let operator = KeyPair::new_operator();
        let account = KeyPair::new_account();
        let system = KeyPair::new_account();
        let mut operator_claims = Operator::new_claims("Minga".to_string(), operator.public_key());
        operator_claims.nats.system_account = Some(system.public_key());
        let resolver_dir = dir.path().join("resolver");
        let signer_dir = dir.path().join("signers");
        std::fs::create_dir(&resolver_dir).unwrap();
        std::fs::create_dir(&signer_dir).unwrap();
        // The full resolver needs the SYS account claim; the operator supplies it.
        let system_claims = Account::new_claims("SYS".to_string(), system.public_key());
        std::fs::write(
            resolver_dir.join(format!("{}.jwt", system.public_key())),
            system_claims.encode(&operator).unwrap(),
        )
        .unwrap();
        let auth = NatsJwtAuth {
            operator_jwt_file: dir.path().join("operator.jwt"),
            resolver_dir,
            account_jwt_file: dir.path().join("account.jwt"),
            account_seed_file: dir.path().join("account.seed"),
            tenant_signer_dir: signer_dir,
            account_public_key: account.public_key(),
            system_credentials_file: dir.path().join("sys.creds"),
        };
        write_private(
            &auth.operator_jwt_file,
            &operator_claims.encode(&operator).unwrap(),
        );
        write_private(&auth.account_seed_file, &account.seed().unwrap());
        write_private(&auth.system_credentials_file, &user_creds(&system, &system));
        let fixture = Self {
            dir,
            pki: make_pki(),
            operator,
            account,
            auth,
        };
        fixture.write_account_jwt(signers);
        fixture
    }

    /// Replace the provisioned account JWT, as an operator rotating signers does.
    fn write_account_jwt(&self, signers: &[&KeyPair]) {
        write_private(
            &self.auth.account_jwt_file,
            &account_jwt(&self.operator, &self.account, signers),
        );
    }

    /// Start a sidecar over the fixture's files, as a manager restart does.
    async fn start(&self) -> (NatsSidecar, u16) {
        let path = |name: &str| self.dir.path().join(name);
        write_private(&path("ca.pem"), &self.pki.ca);
        write_private(&path("server-cert.pem"), &self.pki.server.0);
        write_private(&path("server-key.pem"), &self.pki.server.1);
        let port = free_port();
        let sidecar = NatsSidecar::start(&SidecarOptions {
            data_dir: path("data"),
            client_port: port,
            monitor_port: free_port(),
            jwt_auth: Some(self.auth.clone()),
            tls: Some(ServerTls {
                cert_file: path("server-cert.pem"),
                key_file: path("server-key.pem"),
                ca_file: path("ca.pem"),
                verify: true,
                verify_and_map: false,
            }),
            ..SidecarOptions::default()
        })
        .await
        .expect("start JWT sidecar");
        (sidecar, port)
    }

    /// Connect with `creds`; the error text carries the server's refusal.
    async fn connect(&self, port: u16, creds: &str) -> Result<(), String> {
        let dir = self.dir.path();
        write_private(&dir.join("client-cert.pem"), &self.pki.client.0);
        write_private(&dir.join("client-key.pem"), &self.pki.client.1);
        let tls = NatsTlsConfig::new(
            dir.join("ca.pem"),
            dir.join("client-cert.pem"),
            dir.join("client-key.pem"),
            "localhost",
        );
        let options = tls
            .connect_options_with_credentials(creds)
            .map_err(|error| error.to_string())?;
        let client = async_nats::connect_with_options(format!("tls://localhost:{port}"), options)
            .await
            .map_err(|error| error.to_string())?;
        client.flush().await.map_err(|error| error.to_string())
    }
}

fn resolver_claim(fixture: &Fixture) -> PathBuf {
    fixture
        .auth
        .resolver_dir
        .join(format!("{}.jwt", fixture.auth.account_public_key))
}

#[tokio::test]
async fn restart_accepts_a_signer_added_by_a_rotated_account_jwt() {
    let first = KeyPair::new_account();
    let rotated = KeyPair::new_account();
    let fixture = Fixture::new(&[&first]);

    let (sidecar, port) = fixture.start().await;
    fixture
        .connect(port, &user_creds(&first, &fixture.account))
        .await
        .expect("a user from the provisioned signer connects");
    drop(sidecar);

    fixture.write_account_jwt(&[&rotated]);
    let (_sidecar, port) = fixture.start().await;
    let result = fixture
        .connect(port, &user_creds(&rotated, &fixture.account))
        .await;
    assert!(
        result.is_ok(),
        "user from the rotated signer refused after restart: {result:?}"
    );
}

#[tokio::test]
async fn restart_accepts_a_changed_manager_credentials_file() {
    let signer = KeyPair::new_account();
    let fixture = Fixture::new(&[&signer]);

    let (sidecar, port) = fixture.start().await;
    fixture
        .connect(port, &user_creds(&signer, &fixture.account))
        .await
        .expect("original credentials connect");
    drop(sidecar);

    let (_sidecar, port) = fixture.start().await;
    let result = fixture
        .connect(port, &user_creds(&signer, &fixture.account))
        .await;
    assert!(
        result.is_ok(),
        "changed credentials refused after restart: {result:?}"
    );
}

#[tokio::test]
async fn restart_with_an_unchanged_account_jwt_keeps_the_resolver_claim() {
    let signer = KeyPair::new_account();
    let fixture = Fixture::new(&[&signer]);

    let (sidecar, _) = fixture.start().await;
    drop(sidecar);
    let before = std::fs::read(resolver_claim(&fixture)).unwrap();
    let (_sidecar, _) = fixture.start().await;

    assert_eq!(before, std::fs::read(resolver_claim(&fixture)).unwrap());
}

#[tokio::test]
async fn restart_keeps_a_newer_resolver_claim_pushed_at_runtime() {
    let signer = KeyPair::new_account();
    let fixture = Fixture::new(&[&signer]);
    let (sidecar, _) = fixture.start().await;
    drop(sidecar);

    // A revocation push replaces the resolver claim with a newer one. JWT
    // timestamps have one-second resolution.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let revoked = KeyPair::new_user();
    let mut claims = Claims::<Account>::decode(
        std::fs::read_to_string(&fixture.auth.account_jwt_file)
            .unwrap()
            .trim(),
    )
    .unwrap();
    claims
        .nats
        .revocations
        .get_or_insert_with(Default::default)
        .insert(revoked.public_key(), 1);
    std::fs::write(
        resolver_claim(&fixture),
        claims.encode(&fixture.operator).unwrap(),
    )
    .unwrap();
    let pushed = std::fs::read(resolver_claim(&fixture)).unwrap();

    let (_sidecar, _) = fixture.start().await;

    assert_eq!(pushed, std::fs::read(resolver_claim(&fixture)).unwrap());
}
