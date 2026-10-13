use std::collections::HashMap;
use std::path::PathBuf;

use minga_compose::{
    load, load_path, ComposeError, Condition, DeployMode, DeviceCount, LoadOptions, PortRange,
    StringOrList, Ulimit, VolumeKind,
};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn opts(vars: &[(&str, &str)], profiles: &[&str]) -> LoadOptions {
    LoadOptions {
        env: vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<_, _>>(),
        profiles: profiles.iter().map(|p| p.to_string()).collect(),
    }
}

/// Loads an inline file that must fail, returning its error messages.
fn errors(yaml: &str) -> Vec<String> {
    load(yaml, &opts(&[], &[]))
        .expect_err("expected the file to be rejected")
        .iter()
        .map(ToString::to_string)
        .collect()
}

#[test]
fn wordpress_tutorial_stack() {
    let loaded = load_path(
        fixture("wordpress.yaml"),
        &opts(
            &[("DB_ROOT_PASSWORD", "root-pw"), ("DB_PASSWORD", "wp-pw")],
            &[],
        ),
    )
    .unwrap();
    let p = &loaded.project;

    assert_eq!(p.startup_order(), ["db", "wordpress"]);
    let wp = &p.services["wordpress"];
    assert_eq!(wp.image.as_deref(), Some("wordpress:6.6"));
    assert_eq!(wp.ports[0].published, Some(PortRange::single(8000)));
    assert_eq!(
        wp.environment.0["WORDPRESS_DB_PASSWORD"].as_deref(),
        Some("wp-pw")
    );
    let db = &p.services["db"];
    assert_eq!(db.volumes[0].kind, VolumeKind::Volume);
    assert_eq!(
        db.environment.0["MYSQL_ROOT_PASSWORD"].as_deref(),
        Some("root-pw")
    );
    assert_eq!(loaded.warnings, ["`version` is obsolete and ignored"]);
}

#[test]
fn wordpress_requires_its_passwords() {
    let errs = load_path(fixture("wordpress.yaml"), &opts(&[], &[])).unwrap_err();
    let msgs: Vec<String> = errs.iter().map(ToString::to_string).collect();
    assert_eq!(msgs.len(), 2, "{msgs:#?}");
    assert!(msgs[0].starts_with("services.db.environment.MYSQL_ROOT_PASSWORD: required variable"));
}

#[test]
fn edge_ai_stack_with_features_swarm_ignores() {
    let loaded = load_path(
        fixture("edge-ai.yaml"),
        &opts(&[("INFERENCE_REPLICAS", "2")], &[]),
    )
    .unwrap();
    let p = &loaded.project;
    assert_eq!(p.name.as_deref(), Some("plant-7"));

    // The debug profile is off, so debug-ui is left out.
    assert_eq!(p.disabled_services, ["debug-ui"]);
    assert_eq!(p.startup_order(), ["inference", "agent-api", "sensors"]);

    // Anchors and merge keys apply, and the `x-defaults` block itself is not a service key.
    let inference = &p.services["inference"];
    assert_eq!(inference.restart.as_deref(), Some("unless-stopped"));
    assert!(inference.networks.contains_key("backplane"));
    assert!(p.extensions.contains_key("x-defaults"));
    assert!(inference.extensions.contains_key("x-minga-budget"));

    // Interpolated numbers still parse as numbers.
    let deploy = inference.deploy.as_ref().unwrap();
    assert_eq!(deploy.replicas, Some(2));
    let reservations = deploy
        .resources
        .as_ref()
        .unwrap()
        .reservations
        .as_ref()
        .unwrap();
    assert_eq!(reservations.cpus, Some(4.0));
    let gpu = &reservations.devices[0];
    assert_eq!(gpu.count, Some(DeviceCount::Count(1)));
    assert_eq!(gpu.capabilities, ["gpu"]);
    assert_eq!(inference.ulimits["memlock"], Ulimit::Single(-1));
    assert_eq!(
        inference.ulimits["nofile"],
        Ulimit::Range {
            soft: 65536,
            hard: 65536
        }
    );
    assert!(matches!(inference.command, Some(StringOrList::List(ref args)) if args.len() == 4));

    let api = &p.services["agent-api"];
    let dep = &api.depends_on["inference"];
    assert_eq!(
        (dep.condition, dep.restart, dep.required),
        (Condition::ServiceHealthy, true, true)
    );
    assert_eq!(api.ports[0].published, Some(PortRange::single(443)));
    assert_eq!(api.secrets[0].mode.as_deref(), Some("0400"));

    let sensors = &p.services["sensors"];
    assert_eq!(sensors.deploy.as_ref().unwrap().mode, DeployMode::Global);
    assert_eq!(sensors.devices[0].permissions.as_deref(), Some("rw"));
    assert_eq!(
        sensors.sysctls.0["net.core.somaxconn"].as_deref(),
        Some("1024")
    );
}

#[test]
fn edge_ai_profile_enables_debug_ui() {
    let loaded = load_path(fixture("edge-ai.yaml"), &opts(&[], &["debug"])).unwrap();
    assert!(loaded.project.services.contains_key("debug-ui"));
    assert!(loaded.project.disabled_services.is_empty());
}

#[test]
fn typo_gets_a_suggestion() {
    let errs = errors("services:\n  web:\n    imgae: nginx\n");
    assert!(
        errs.contains(&"services.web: unknown key `imgae` (did you mean `image`?)".to_string()),
        "{errs:#?}"
    );
}

#[test]
fn unsupported_spec_keys_say_why() {
    let errs = errors("services:\n  web:\n    build: .\n    image: web\n");
    assert_eq!(
        errs,
        [
            "services.web: `build` is part of the Compose spec but not supported yet: \
          build and push images with BuildKit, then reference them with `image`"
        ]
    );
}

#[test]
fn reports_every_problem_at_once() {
    let errs = errors(
        "services:
  a:
    image: a
    depends_on: [b]
    networks: [missing]
  b:
    image: b
    ports: ['70000:80']
  c:
    volumes: ['undeclared:/data']
",
    );
    let joined = errs.join("\n");
    assert!(
        joined.contains("services.b.ports[0]: invalid port \"70000\""),
        "{joined}"
    );
    assert!(
        joined.contains("network `missing` is not declared"),
        "{joined}"
    );
    assert!(
        joined.contains("services.c: `image` is required"),
        "{joined}"
    );
    assert!(
        joined.contains("volume `undeclared` is not declared"),
        "{joined}"
    );
}

#[test]
fn dependency_cycles_are_rejected() {
    let errs =
        errors("services:\n  a: {image: a, depends_on: [b]}\n  b: {image: b, depends_on: [a]}\n");
    assert_eq!(errs, ["services: dependency cycle between: a, b"]);
}

#[test]
fn optional_dependency_on_missing_service_only_warns() {
    let loaded = load(
        "services:\n  web:\n    image: web\n    depends_on:\n      cache: {condition: service_started, required: false}\n",
        &opts(&[], &[]),
    )
    .unwrap();
    assert!(loaded.project.services["web"].depends_on.is_empty());
    assert_eq!(loaded.warnings.len(), 1);
}

#[test]
fn dependency_on_disabled_profile_fails() {
    let errs = errors("services:\n  web: {image: web, depends_on: [debug]}\n  debug: {image: d, profiles: [debug]}\n");
    assert_eq!(
        errs,
        ["services.web.depends_on: depends on `debug`, which no active profile enables"]
    );
}

#[test]
fn published_port_conflicts() {
    let errs = errors(
        "services:\n  a: {image: a, ports: ['80:80']}\n  b: {image: b, ports: ['80:8080']}\n",
    );
    assert_eq!(
        errs,
        ["services.b.ports: port 80/tcp is already published by `a`"]
    );
}

#[test]
fn global_mode_cannot_set_replicas() {
    let errs = errors("services:\n  a:\n    image: a\n    deploy: {mode: global, replicas: 3}\n");
    assert_eq!(
        errs,
        ["services.a.deploy.replicas: cannot be set when mode is global"]
    );
}

#[test]
fn secret_needs_exactly_one_source() {
    let errs = errors("services: {}\nsecrets:\n  s: {file: ./a, environment: B}\n");
    assert_eq!(
        errs,
        ["secrets.s: set exactly one of `file`, `environment`, `content` or `external: true`"]
    );
}

#[test]
fn not_a_mapping() {
    let errs = errors("- just\n- a list\n");
    assert_eq!(errs, ["(root): a Compose file must be a mapping"]);
}

#[test]
fn error_type_is_matchable() {
    let err = load(
        "services:\n  w:\n    image: w\n    container_name: x\n",
        &opts(&[], &[]),
    )
    .unwrap_err();
    assert!(matches!(&err.0[0], ComposeError::Unsupported { key, .. } if key == "container_name"));
}
