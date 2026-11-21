//! Operator/JWT mode against a real `nats-server`: a worker signed by the
//! tenant signer, under the production permission template, must not learn
//! another tenant's keys through any JetStream API the template grants.
//!
//! Fails instead of skipping when `nats-server` is missing, like the bus
//! TLS suite: a skipped isolation test reads as a passing one.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use futures::StreamExt;
use minga_bus::{tls::NatsTlsConfig, EventBus, NatsBus};
use nats_jwt_rs::{
    account::{Account, JetStreamLimits, OperatorLimits},
    operator::Operator,
    types::{Permission, Permissions, ScopeType, SigningKey, UserScope},
    user::{User, UserPermissionLimits},
};
use nkeys::KeyPair;

use super::{worker_permission_template, NatsCredentialIssuer};

/// Same `.creds` layout `NatsCredentialIssuer::issue` writes.
fn creds_file(jwt: &str, seed: &str) -> String {
    format!(
        "-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------\n\n-----BEGIN USER NKEY SEED-----\n{seed}\n------END USER NKEY SEED------\n"
    )
}

fn write_file(path: &Path, contents: &str) {
    let mut file = std::fs::File::create(path).unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}

fn nats_server_bin() -> PathBuf {
    if let Ok(path) = std::env::var("MINGA_NATS_SERVER_BIN") {
        return PathBuf::from(path);
    }
    std::env::var("PATH")
        .unwrap_or_default()
        .split(':')
        .map(|dir| PathBuf::from(dir).join("nats-server"))
        .chain(
            [
                "/usr/local/bin/nats-server",
                "/opt/homebrew/bin/nats-server",
            ]
            .into_iter()
            .map(PathBuf::from),
        )
        .find(|path| path.exists())
        .expect("nats-server binary not found - install it to run the JWT isolation test")
}

struct Server {
    child: Child,
    port: u16,
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

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

/// Operator, `MINGA` account with JetStream and one scoped signer per tenant,
/// each carrying the production worker template.
struct Identity {
    operator_jwt: String,
    account_key: KeyPair,
    account_jwt: String,
    system_key: KeyPair,
    system_jwt: String,
    tenant_signers: Vec<(String, KeyPair)>,
}

fn scoped_signer(tenant: &str, signer: &KeyPair) -> SigningKey {
    let (publish, subscribe) = worker_permission_template(tenant);
    SigningKey {
        key: signer.public_key(),
        scope: Some(UserScope {
            kind: ScopeType::UserScope,
            key: signer.public_key(),
            role: Some("worker".to_string()),
            template: Some(UserPermissionLimits {
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
            }),
            description: None,
        }),
    }
}

fn make_identity(tenants: &[&str]) -> Identity {
    let operator_key = KeyPair::new_operator();
    let account_key = KeyPair::new_account();
    let system_key = KeyPair::new_account();
    let tenant_signers: Vec<(String, KeyPair)> = tenants
        .iter()
        .map(|tenant| (tenant.to_string(), KeyPair::new_account()))
        .collect();
    let mut account = Account::new_claims("MINGA".to_string(), account_key.public_key());
    account.nats.limits = Some(OperatorLimits {
        jetstream: Some(JetStreamLimits {
            memory_storage: Some(-1),
            disk_storage: Some(-1),
            streams: Some(-1),
            consumer: Some(-1),
            ..Default::default()
        }),
        ..Default::default()
    });
    account.nats.signing_keys = Some(
        tenant_signers
            .iter()
            .map(|(tenant, signer)| scoped_signer(tenant, signer))
            .collect(),
    );
    let system = Account::new_claims("SYS".to_string(), system_key.public_key());
    Identity {
        operator_jwt: Operator::new_claims("Minga".to_string(), operator_key.public_key())
            .encode(&operator_key)
            .unwrap(),
        account_jwt: account.encode(&operator_key).unwrap(),
        system_jwt: system.encode(&operator_key).unwrap(),
        account_key,
        system_key,
        tenant_signers,
    }
}

fn start_server(pki: &Pki, identity: &Identity) -> Server {
    let dir = tempfile::tempdir().unwrap();
    let path = |name: &str| dir.path().join(name);
    write_file(&path("ca.pem"), &pki.ca);
    write_file(&path("server-cert.pem"), &pki.server.0);
    write_file(&path("server-key.pem"), &pki.server.1);
    write_file(&path("operator.jwt"), &identity.operator_jwt);
    let config = format!(
        r#"listen: 127.0.0.1:-1
server_name: minga-jwt-test
operator: "{operator}"
system_account: {system}
resolver: MEMORY
resolver_preload: {{
    {account}: "{account_jwt}"
    {system}: "{system_jwt}"
}}
tls {{
    cert_file: "{cert}"
    key_file: "{key}"
    ca_file: "{ca}"
    verify: true
}}
jetstream {{
    store_dir: "{store}"
}}
"#,
        operator = path("operator.jwt").display(),
        system = identity.system_key.public_key(),
        account = identity.account_key.public_key(),
        account_jwt = identity.account_jwt,
        system_jwt = identity.system_jwt,
        cert = path("server-cert.pem").display(),
        key = path("server-key.pem").display(),
        ca = path("ca.pem").display(),
        store = path("jetstream").display(),
    );
    write_file(&path("server.conf"), &config);
    let mut child = Command::new(nats_server_bin())
        .arg("-c")
        .arg(path("server.conf"))
        .arg("--ports_file_dir")
        .arg(dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn nats-server");
    let deadline = Instant::now() + Duration::from_secs(20);
    let port = loop {
        if let Ok(Some(status)) = child.try_wait() {
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stderr.take() {
                use std::io::Read;
                let _ = pipe.read_to_string(&mut stderr);
            }
            panic!("nats-server exited early: {status}\n{stderr}");
        }
        let port = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .find(|path| path.extension().is_some_and(|ext| ext == "ports"))
            .and_then(|path| std::fs::read_to_string(path).ok())
            .and_then(|body| serde_json::from_str::<serde_json::Value>(&body).ok())
            .and_then(|ports| {
                ports["nats"][0]
                    .as_str()?
                    .rsplit(':')
                    .next()?
                    .parse::<u16>()
                    .ok()
            });
        if let Some(port) = port {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                break port;
            }
        }
        assert!(Instant::now() < deadline, "nats-server did not start");
        std::thread::sleep(Duration::from_millis(50));
    };
    Server {
        child,
        port,
        _dir: dir,
    }
}

fn client_tls(dir: &Path, pki: &Pki) -> NatsTlsConfig {
    write_file(&dir.join("ca.pem"), &pki.ca);
    write_file(&dir.join("client-cert.pem"), &pki.client.0);
    write_file(&dir.join("client-key.pem"), &pki.client.1);
    NatsTlsConfig::new(
        dir.join("ca.pem"),
        dir.join("client-cert.pem"),
        dir.join("client-key.pem"),
        "localhost",
    )
}

/// A raw worker client that records every server error it is sent, so a
/// missing answer can be told apart from a refused publish or subscription.
async fn raw_worker(
    url: &str,
    tls: &NatsTlsConfig,
    creds: &str,
) -> (
    async_nats::Client,
    tokio::sync::mpsc::UnboundedReceiver<String>,
) {
    let (errors_tx, errors) = tokio::sync::mpsc::unbounded_channel();
    let options = tls
        .connect_options_with_credentials(creds)
        .unwrap()
        .custom_inbox_prefix(minga_bus::subjects::worker_inbox_prefix("default", 7))
        .event_callback(move |event| {
            let errors_tx = errors_tx.clone();
            async move {
                if let async_nats::Event::ServerError(error) = event {
                    let _ = errors_tx.send(error.to_string());
                }
            }
        });
    (
        async_nats::connect_with_options(url, options)
            .await
            .unwrap(),
        errors,
    )
}

async fn expect_refusal(
    errors: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
    what: &str,
    subject: &str,
) {
    let error = tokio::time::timeout(Duration::from_secs(3), errors.recv())
        .await
        .unwrap_or_else(|_| panic!("{what}: the server reported no permissions violation"))
        .unwrap();
    assert!(
        error.contains("Permissions Violation") && error.contains(subject),
        "{what}: unexpected server error: {error}"
    );
}

#[tokio::test]
async fn jwt_worker_cannot_learn_other_tenants_keys() {
    let pki = make_pki();
    let identity = make_identity(&["default", "beta"]);
    let server = start_server(&pki, &identity);
    let url = format!("tls://localhost:{}", server.port);
    let dir = tempfile::tempdir().unwrap();
    let tls = client_tls(dir.path(), &pki);

    // The manager is an unrestricted user of the account, as in production.
    let manager_key = KeyPair::new_user();
    let manager_creds = creds_file(
        &User::new_claims("manager".to_string(), manager_key.public_key())
            .encode(&identity.account_key)
            .unwrap(),
        &manager_key.seed().unwrap(),
    );
    let manager = NatsBus::connect_secure_with_credentials(&url, &tls, &manager_creds)
        .await
        .unwrap();
    for tenant in ["default", "beta"] {
        manager.ensure_tenant_resources(tenant).await.unwrap();
    }
    manager
        .put_assignment("default.node.7.task.own", b"own")
        .await
        .unwrap();
    manager
        .put_assignment("beta.node.7.task.foreign", b"foreign")
        .await
        .unwrap();
    manager
        .refresh_tenant_lease("beta", 7, b"foreign-lease")
        .await
        .unwrap();
    let manager_client = async_nats::connect_with_options(
        &url,
        tls.connect_options_with_credentials(&manager_creds)
            .unwrap(),
    )
    .await
    .unwrap();
    let stream = async_nats::jetstream::new(manager_client)
        .get_stream("KV_MINGA_ASSIGN")
        .await
        .unwrap();
    stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            name: Some("betaprobe".into()),
            filter_subject: "$KV.MINGA_ASSIGN.beta.node.7.>".into(),
            ..Default::default()
        })
        .await
        .unwrap();

    // The worker of tenant `default`, node 7, signed by the production issuer.
    let issuer = NatsCredentialIssuer::new(
        &identity.operator_jwt,
        &identity.account_jwt,
        &identity.account_key.seed().unwrap(),
        identity
            .tenant_signers
            .iter()
            .map(|(tenant, signer)| (tenant.clone(), signer.seed().unwrap())),
    )
    .expect("the production template passes the issuer's own check");
    let (_, worker_creds) = issuer.issue("default", 7).unwrap();

    // Control: the production worker bus works under the template.
    let worker_bus =
        NatsBus::connect_worker_secure_with_credentials(&url, &tls, &worker_creds, 7, "default")
            .await
            .expect("a JWT worker opens the shared buckets");
    assert_eq!(
        worker_bus
            .get_assignment("default.node.7.task.own")
            .await
            .unwrap()
            .as_deref(),
        Some(&b"own"[..])
    );
    let mut watch = worker_bus
        .watch_assignments("default.node.7.task.")
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .expect("the JWT worker's watch delivers its own key")
        .unwrap();
    assert_eq!(first.key, "default.node.7.task.own");
    worker_bus
        .refresh_tenant_lease("default", 7, b"alive")
        .await
        .unwrap();

    let (worker, mut errors) = raw_worker(&url, &tls, &worker_creds).await;

    // APIs the template does not grant: refused by the server, not just silent.
    for (what, subject, body) in [
        (
            "STREAM.INFO listing assignment keys",
            "$JS.API.STREAM.INFO.KV_MINGA_ASSIGN",
            serde_json::json!({"subjects_filter": ">"}),
        ),
        (
            "STREAM.INFO listing lease keys",
            "$JS.API.STREAM.INFO.KV_MINGA_LEASES",
            serde_json::json!({"subjects_filter": ">"}),
        ),
        (
            "STREAM.INFO on another tenant's status bucket",
            "$JS.API.STREAM.INFO.KV_MINGA_STATUS_beta",
            serde_json::json!({"subjects_filter": ">"}),
        ),
        (
            "CONSUMER.INFO on another tenant's consumer",
            "$JS.API.CONSUMER.INFO.KV_MINGA_ASSIGN.betaprobe",
            serde_json::json!({}),
        ),
        (
            "CONSUMER.CREATE naming another tenant's prefix",
            "$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.x.$KV.MINGA_ASSIGN.beta.node.7.task.>",
            serde_json::json!({}),
        ),
        (
            "DIRECT.GET of another tenant's key",
            "$JS.API.DIRECT.GET.KV_MINGA_ASSIGN.$KV.MINGA_ASSIGN.beta.node.7.task.foreign",
            serde_json::json!({}),
        ),
        (
            "DIRECT.GET of another tenant's lease",
            "$JS.API.DIRECT.GET.KV_MINGA_LEASES.$KV.MINGA_LEASES.beta.lease.node.7",
            serde_json::json!({}),
        ),
    ] {
        let reply = tokio::time::timeout(
            Duration::from_secs(2),
            worker.request(subject, body.to_string().into()),
        )
        .await;
        assert!(
            !matches!(&reply, Ok(Ok(_))),
            "{what}: the worker got an answer: {reply:?}"
        );
        expect_refusal(&mut errors, what, subject).await;
    }

    // Granted APIs, asked through the body for tenant beta: answered, never
    // with a beta key.
    let leaks = |message: &async_nats::Message| {
        let subject = message
            .headers
            .as_ref()
            .and_then(|headers| headers.get("Nats-Subject"))
            .map(|value| value.as_str().to_owned())
            .unwrap_or_default();
        subject.contains(".beta.")
            || String::from_utf8_lossy(&message.payload).contains(".beta.")
            || message.payload.starts_with(b"foreign")
    };
    let inbox = "_INBOX.default.7.jwt-probe";
    let mut delivered = worker.subscribe(inbox).await.unwrap();
    for (name, filter) in [
        (
            "p1",
            serde_json::json!({"filter_subjects": ["$KV.MINGA_ASSIGN.beta.>"]}),
        ),
        (
            "p2",
            serde_json::json!({"filter_subject": "$KV.MINGA_ASSIGN.beta.>"}),
        ),
        ("p3", serde_json::json!({})),
    ] {
        let mut config = filter;
        config["name"] = name.into();
        config["deliver_subject"] = inbox.into();
        config["deliver_policy"] = "all".into();
        config["ack_policy"] = "none".into();
        let body = serde_json::json!({"stream_name": "KV_MINGA_ASSIGN", "config": config});
        let subject = format!(
            "$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.{name}.$KV.MINGA_ASSIGN.default.node.7.task.>"
        );
        let reply = worker
            .request(subject, body.to_string().into())
            .await
            .unwrap_or_else(|error| panic!("consumer {name}: no answer: {error}"));
        let text = String::from_utf8_lossy(&reply.payload);
        assert!(
            text.contains("\"error\""),
            "consumer {name} was created: {text}"
        );
    }
    for subject in [
        "$JS.API.DIRECT.GET.KV_MINGA_ASSIGN.$KV.MINGA_ASSIGN.default.node.7.task.own",
        "$JS.API.DIRECT.GET.KV_MINGA_LEASES.$KV.MINGA_LEASES.default.lease.node.7",
    ] {
        for body in [
            serde_json::json!({"seq": 1}),
            serde_json::json!({"next_by_subj": "$KV.MINGA_ASSIGN.beta.>", "seq": 1}),
            serde_json::json!({"last_by_subj": "$KV.MINGA_ASSIGN.beta.node.7.task.foreign"}),
            serde_json::json!({"multi_last": [">"], "batch": 10}),
        ] {
            let reply = worker
                .request(subject.to_string(), body.to_string().into())
                .await
                .unwrap_or_else(|error| panic!("direct get {subject} {body}: {error}"));
            assert!(
                !leaks(&reply),
                "direct get {subject} {body} returned a beta key: {reply:?}"
            );
        }
    }

    // Subscribing to another tenant's KV subjects is refused, and a new beta
    // write reaches nothing the worker holds.
    let foreign = "$KV.MINGA_ASSIGN.beta.node.7.task.>";
    let mut foreign_sub = worker.subscribe(foreign).await.unwrap();
    worker.flush().await.unwrap();
    expect_refusal(&mut errors, "subscription to beta keys", foreign).await;
    manager
        .put_assignment("beta.node.7.task.later", b"foreign-later")
        .await
        .unwrap();
    if let Ok(Some(message)) =
        tokio::time::timeout(Duration::from_millis(500), foreign_sub.next()).await
    {
        panic!("worker received a beta key: {message:?}");
    }
    while let Ok(Some(message)) =
        tokio::time::timeout(Duration::from_millis(300), delivered.next()).await
    {
        assert!(
            !leaks(&message),
            "a worker consumer delivered a beta key: {message:?}"
        );
    }
}
