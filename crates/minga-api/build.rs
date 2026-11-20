use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=MINGA_GIT_SHA");

    if let Ok(revision) = std::env::var("MINGA_GIT_SHA") {
        if !revision.is_empty() {
            println!("cargo:rustc-env=MINGA_GIT_SHA={revision}");
            return;
        }
    }

    // Rerun when HEAD moves; otherwise Cargo keeps reporting the revision of
    // the first build. Only existing paths: a missing one would rerun always.
    let mut watched = vec![git(&["rev-parse", "--git-path", "HEAD"])];
    if let Some(reference) = git(&["symbolic-ref", "-q", "HEAD"]) {
        watched.push(git(&["rev-parse", "--git-path", &reference]));
    }
    watched.push(git(&["rev-parse", "--git-path", "packed-refs"]));
    for path in watched.into_iter().flatten() {
        if std::path::Path::new(&path).exists() {
            println!("cargo:rerun-if-changed={path}");
        }
    }

    if let Some(revision) = git(&["rev-parse", "--short=12", "HEAD"]) {
        println!("cargo:rustc-env=MINGA_GIT_SHA={revision}");
    }
}

/// A git command's trimmed stdout, or `None` outside a checkout (a source archive).
fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    let text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (output.status.success() && !text.is_empty()).then_some(text)
}
