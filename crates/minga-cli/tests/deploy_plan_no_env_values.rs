//! Regression test for issue #195: `minga deploy` must not print env values.
//!
//! The deploy plan (printed before the API call) must contain env key names
//! but no env values.  A marker value that could not appear in normal output
//! is used so the assertion catches every shape a leaked value could take:
//! a `KEY=VALUE` pair, a JSON dump, a summary line, or an error message.

use std::io::Write;
use std::net::TcpListener;
use std::process::{Command, Stdio};

const MARKER_KEY: &str = "MINGA_TEST_SECRET_KEY";
const MARKER_VALUE: &str = "xSECRET_xMARKER_x195x";

/// Runs `minga deploy` against a closed port so it prints the plan and then
/// fails to connect. Returns `(stdout, stderr)`.
fn deploy_plan(compose: &str) -> (String, String) {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(compose.as_bytes()).unwrap();

    // Bind then drop: the port is closed by the time `minga deploy` connects.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let closed_addr = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);

    let output = Command::new(env!("CARGO_BIN_EXE_minga"))
        .args([
            "deploy",
            "-c",
            file.path().to_str().unwrap(),
            "test",
            "--api-url",
            &closed_addr,
            "--wait",
            "0",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("minga binary should run");

    (
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// AC1 + AC3: `minga deploy` plan output must not contain env values; keys
/// must still appear.
#[test]
fn deploy_plan_hides_env_value_and_shows_key() {
    let compose = format!(
        "services:\n  svc:\n    image: alpine:3\n    environment:\n      {MARKER_KEY}: {MARKER_VALUE}\n"
    );
    let (stdout, stderr) = deploy_plan(&compose);
    let all = format!("{stdout}{stderr}");

    assert!(
        !all.contains(MARKER_VALUE),
        "env value leaked into deploy output (issue #195):\n{all}"
    );
    assert!(
        stdout.contains(MARKER_KEY),
        "env key missing from deploy plan:\n{all}"
    );
}
