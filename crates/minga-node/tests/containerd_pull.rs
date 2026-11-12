//! Authenticated image pulls through containerd's Transfer service, against a
//! real daemon and an htpasswd-protected `registry:2`.
//!
//! The environment names the registry and the image seeded into it:
//!   MINGA_TEST_REGISTRY_IMAGE=localhost:5000/minga/private:1
//!   MINGA_TEST_REGISTRY_USER / MINGA_TEST_REGISTRY_PASSWORD
//! With `MINGA_REQUIRE_CONTAINERD=1` (CI) a missing daemon or registry fails
//! the test instead of skipping it.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use minga_node::containerd::client::ContainerdClient;
use minga_node::containerd::{pull_image, DEFAULT_SNAPSHOTTER};

fn required() -> bool {
    std::env::var_os("MINGA_REQUIRE_CONTAINERD").is_some()
}

fn skip(why: &str) {
    assert!(!required(), "required by MINGA_REQUIRE_CONTAINERD: {why}");
    eprintln!("SKIP: {why}");
}

fn write_config(dir: &Path, json: serde_json::Value) {
    std::fs::write(dir.join("config.json"), json.to_string()).unwrap();
}

fn write_helper(dir: &Path, name: &str, user: &str, secret: &str) {
    let path = dir.join(format!("docker-credential-{name}"));
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\ncat >/dev/null\nprintf '{{\"Username\":\"{user}\",\"Secret\":\"{secret}\"}}'\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[tokio::test]
async fn private_registry_pull_uses_the_docker_config_and_credential_helpers() {
    let socket = std::env::var("MINGA_CONTAINERD_SOCKET")
        .unwrap_or_else(|_| "/run/containerd/containerd.sock".into());
    if !Path::new(&socket).exists() {
        return skip(&format!("no containerd socket at {socket}"));
    }
    let (Ok(image), Ok(user), Ok(password)) = (
        std::env::var("MINGA_TEST_REGISTRY_IMAGE"),
        std::env::var("MINGA_TEST_REGISTRY_USER"),
        std::env::var("MINGA_TEST_REGISTRY_PASSWORD"),
    ) else {
        return skip("MINGA_TEST_REGISTRY_IMAGE/USER/PASSWORD are not set");
    };
    let registry = image.split('/').next().unwrap().to_string();
    let client = ContainerdClient::connect(std::path::PathBuf::from(&socket))
        .await
        .expect("connect to containerd");

    let config_dir = tempfile::tempdir().unwrap();
    let bin_dir = tempfile::tempdir().unwrap();
    let old_path = std::env::var_os("PATH").unwrap_or_default();
    let mut path = std::env::split_paths(&old_path).collect::<Vec<_>>();
    path.insert(0, bin_dir.path().to_path_buf());
    std::env::set_var("PATH", std::env::join_paths(path).unwrap());
    std::env::set_var("DOCKER_CONFIG", config_dir.path());
    use base64::Engine as _;
    let auth = |user: &str, secret: &str| {
        base64::engine::general_purpose::STANDARD.encode(format!("{user}:{secret}"))
    };

    // No login configured: the registry refuses.
    write_config(config_dir.path(), serde_json::json!({}));
    pull_image(&client, &image, DEFAULT_SNAPSHOTTER)
        .await
        .expect_err("anonymous pull of a private image must fail");

    // A wrong password fails and the error does not repeat it.
    let wrong = "not-the-password-0451";
    write_config(
        config_dir.path(),
        serde_json::json!({"auths": {&registry: {"auth": auth(&user, wrong)}}}),
    );
    let err = pull_image(&client, &image, DEFAULT_SNAPSHOTTER)
        .await
        .expect_err("wrong password must fail");
    assert!(!err.to_string().contains(wrong), "{err}");

    // Inline credentials work.
    write_config(
        config_dir.path(),
        serde_json::json!({"auths": {&registry: {"auth": auth(&user, &password)}}}),
    );
    pull_image(&client, &image, DEFAULT_SNAPSHOTTER)
        .await
        .expect("pull with inline credentials");
    let mut inspect = ContainerdClient::connect(std::path::PathBuf::from(&socket))
        .await
        .unwrap();
    assert!(
        inspect.get_image(&image).await.unwrap().is_some(),
        "pulled image is in the minga namespace"
    );

    // A credential helper works, with nothing inline to fall back on...
    write_helper(bin_dir.path(), "good", &user, &password);
    write_config(config_dir.path(), serde_json::json!({"credsStore": "good"}));
    pull_image(&client, &image, DEFAULT_SNAPSHOTTER)
        .await
        .expect("pull with a credsStore helper");

    // ...and one that answers wrongly is refused, not retried anonymously.
    write_helper(bin_dir.path(), "wrong", &user, wrong);
    write_config(
        config_dir.path(),
        serde_json::json!({"credHelpers": {&registry: "wrong"}}),
    );
    let err = pull_image(&client, &image, DEFAULT_SNAPSHOTTER)
        .await
        .expect_err("a helper's wrong password must fail");
    assert!(!err.to_string().contains(wrong), "{err}");

    // A configured helper that is not installed is an error naming it.
    write_config(
        config_dir.path(),
        serde_json::json!({"credsStore": "absent"}),
    );
    let err = pull_image(&client, &image, DEFAULT_SNAPSHOTTER)
        .await
        .expect_err("missing helper must fail");
    assert!(
        err.to_string().contains("docker-credential-absent"),
        "{err}"
    );
}
