use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=MINGA_GIT_SHA");

    if let Ok(revision) = std::env::var("MINGA_GIT_SHA") {
        if !revision.is_empty() {
            println!("cargo:rustc-env=MINGA_GIT_SHA={revision}");
            return;
        }
    }

    if let Ok(output) = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
    {
        if output.status.success() {
            let revision = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if !revision.is_empty() {
                println!("cargo:rustc-env=MINGA_GIT_SHA={revision}");
            }
        }
    }
}
