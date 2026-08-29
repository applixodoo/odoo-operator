//! Tests for the droggol fork's additions to OdooInstance:
//! `spec.sourceVolume`, `spec.runAsUser` / `spec.runAsGroup`,
//! `spec.adminPasswordSecretRef`, and the namespaced-CNPG credential mapping.
//!
//! The load-bearing assertions here are the *negative* ones: with none of the
//! new fields set, every constructed command, volume list and security context
//! must be byte-identical to what upstream emitted. Those are the tests that
//! fail if the fork ever breaks its compatibility contract.

use std::collections::BTreeMap;

use kube::api::ObjectMeta;

use odoo_operator::controller::helpers::*;
use odoo_operator::controller::odoo_instance::{cnpg_app_secret_name, cnpg_config_from_app_secret};
use odoo_operator::controller::states::build_init_job;
use odoo_operator::crd::odoo_init_job::{OdooInitJob, OdooInitJobSpec};
use odoo_operator::crd::odoo_instance::{
    AdminPasswordSecretRef, CronSpec, IngressSpec, OdooInstance, OdooInstanceSpec,
    SourceVolumeMount, SourceVolumeSpec,
};
use odoo_operator::crd::shared::OdooInstanceRef;

// ── Fixtures ────────────────────────────────────────────────────────────────

fn base_instance(name: &str) -> OdooInstance {
    OdooInstance {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("tenant".to_string()),
            uid: Some("uid-1234".to_string()),
            ..Default::default()
        },
        spec: OdooInstanceSpec {
            image: None,
            image_pull_secret: None,
            admin_password: Some("admin".to_string()),
            admin_password_secret_ref: None,
            replicas: 1,
            cron: CronSpec {
                replicas: 1,
                resources: None,
            },
            ingress: IngressSpec {
                hosts: vec!["test.example.com".to_string()],
                issuer: None,
                class: None,
                gateway_ref: None,
            },
            resources: None,
            filestore: None,
            config_options: None,
            database: None,
            init: Default::default(),
            environment: Default::default(),
            production_instance_ref: None,
            strategy: None,
            webhook: None,
            probes: None,
            affinity: None,
            tolerations: vec![],
            read_only_sql_access: None,
            extra_env: vec![],
            extra_env_from: vec![],
            source_volume: None,
            run_as_user: None,
            run_as_group: None,
        },
        status: None,
    }
}

/// The layout the droggol platform writes: whole volume at `/work`, the
/// `pip install --target` tree at `/build` via a subPath, odoo-bin under the
/// instance home.
fn with_source_volume(mut inst: OdooInstance) -> OdooInstance {
    inst.spec.source_volume = Some(SourceVolumeSpec {
        claim_name: "prod-src".to_string(),
        mounts: vec![
            SourceVolumeMount {
                mount_path: "/work".to_string(),
                sub_path: None,
                read_only: false,
            },
            SourceVolumeMount {
                mount_path: "/build".to_string(),
                sub_path: Some("build".to_string()),
                read_only: false,
            },
        ],
        odoo_bin: "/work/instances/prod/odoo/odoo-bin".to_string(),
    });
    inst
}

fn test_init_job(name: &str, demo: bool) -> OdooInitJob {
    OdooInitJob {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("tenant".to_string()),
            uid: Some("init-uid".to_string()),
            ..Default::default()
        },
        spec: OdooInitJobSpec {
            odoo_instance_ref: OdooInstanceRef {
                name: "prod".to_string(),
                namespace: Some("tenant".to_string()),
            },
            modules: vec!["base".to_string()],
            demo,
            webhook: None,
        },
        status: None,
    }
}

// ── The command that launches Odoo ──────────────────────────────────────────

#[test]
fn entrypoint_without_source_volume_is_the_official_image_convention() {
    let inst = base_instance("prod");
    assert_eq!(odoo_entrypoint(&inst), vec!["/entrypoint.sh", "odoo"]);
}

#[test]
fn entrypoint_with_source_volume_runs_odoo_bin_with_an_explicit_config() {
    let inst = with_source_volume(base_instance("prod"));
    assert_eq!(
        odoo_entrypoint(&inst),
        vec![
            "python3",
            "/work/instances/prod/odoo/odoo-bin",
            "-c",
            "/etc/odoo/odoo.conf",
        ]
    );
}

#[test]
fn odoo_command_appends_args_after_the_entrypoint() {
    // The web deployment's exact command, both ways.
    let plain = base_instance("prod");
    assert_eq!(
        odoo_command(&plain, &["--max-cron-threads", "0"]),
        vec!["/entrypoint.sh", "odoo", "--max-cron-threads", "0"]
    );

    let sourced = with_source_volume(base_instance("prod"));
    assert_eq!(
        odoo_command(&sourced, &["--max-cron-threads", "0"]),
        vec![
            "python3",
            "/work/instances/prod/odoo/odoo-bin",
            "-c",
            "/etc/odoo/odoo.conf",
            "--max-cron-threads",
            "0",
        ]
    );
}

#[test]
fn probe_command_bypasses_the_entrypoint_wrapper() {
    // `odoo --version` must reach the binary directly — behind the image
    // entrypoint it would block on wait-for-psql.py first.
    assert_eq!(odoo_probe_command(&base_instance("prod")), "odoo");
    assert_eq!(
        odoo_probe_command(&with_source_volume(base_instance("prod"))),
        "python3 /work/instances/prod/odoo/odoo-bin"
    );
}

#[test]
fn odoo_cmd_env_is_empty_without_a_source_volume() {
    // Empty means the neutralize scripts keep their `${ODOO_CMD:-odoo}`
    // default, i.e. behave exactly as before.
    assert!(odoo_cmd_env(&base_instance("prod")).is_empty());
}

#[test]
fn odoo_cmd_env_carries_the_full_invocation_when_sourced() {
    let envs = odoo_cmd_env(&with_source_volume(base_instance("prod")));
    assert_eq!(envs.len(), 1);
    assert_eq!(envs[0].name, "ODOO_CMD");
    assert_eq!(
        envs[0].value.as_deref(),
        Some("python3 /work/instances/prod/odoo/odoo-bin -c /etc/odoo/odoo.conf")
    );
}

// ── Init job: the two command shapes ────────────────────────────────────────

fn init_container_command(inst: &OdooInstance, demo: bool) -> (Vec<String>, Vec<String>) {
    let cr = test_init_job("prod-init", demo);
    let job = build_init_job(
        "prod-init",
        "tenant",
        "odoo:18.0",
        "odoo_db",
        &["base".to_string()],
        inst,
        &cr,
    );
    let c = job
        .spec
        .unwrap()
        .template
        .spec
        .unwrap()
        .containers
        .remove(0);
    (c.command.unwrap(), c.args.unwrap())
}

#[test]
fn init_job_without_source_volume_is_byte_identical_to_upstream() {
    let (command, args) = init_container_command(&base_instance("prod"), false);
    assert_eq!(command, vec!["/entrypoint.sh", "odoo"]);
    assert_eq!(
        args,
        vec![
            "-i",
            "base",
            "-d",
            "odoo_db",
            "--no-http",
            "--stop-after-init",
            "--without-demo=all",
        ]
    );
}

#[test]
fn init_job_demo_script_without_source_volume_is_byte_identical_to_upstream() {
    // This exact string is what upstream emitted; the fork renders it through
    // a format! and must produce the same bytes.
    let (command, args) = init_container_command(&base_instance("prod"), true);
    assert_eq!(command, vec!["/bin/sh", "-c"]);
    assert_eq!(
        args[0],
        "maj=$(odoo --version 2>/dev/null | grep -oE '[0-9]+' | head -n1); \
         flag=''; [ \"${maj:-0}\" -ge 19 ] && flag='--with-demo'; \
         exec /entrypoint.sh odoo \"$@\" $flag"
    );
    assert_eq!(args[1], "sh", "$0 for the sh -c invocation");
}

#[test]
fn init_job_demo_script_substitutes_both_invocations_when_sourced() {
    let (_, args) = init_container_command(&with_source_volume(base_instance("prod")), true);
    let script = &args[0];
    // The probe is the bare binary...
    assert!(
        script.contains("maj=$(python3 /work/instances/prod/odoo/odoo-bin --version"),
        "version probe must not go through an entrypoint: {script}"
    );
    // ...while the real run carries the config file.
    assert!(
        script.contains(
            "exec python3 /work/instances/prod/odoo/odoo-bin -c /etc/odoo/odoo.conf \"$@\" $flag"
        ),
        "exec must use the full launch command: {script}"
    );
    assert!(!script.contains("/entrypoint.sh"));
}

#[test]
fn init_job_mounts_the_source_volume_only_when_configured() {
    let cr = test_init_job("prod-init", false);

    let plain = base_instance("prod");
    let job = build_init_job(
        "prod-init",
        "tenant",
        "odoo:18.0",
        "odoo_db",
        &["base".to_string()],
        &plain,
        &cr,
    );
    let spec = job.spec.unwrap().template.spec.unwrap();
    let vol_names: Vec<_> = spec
        .volumes
        .unwrap()
        .iter()
        .map(|v| v.name.clone())
        .collect();
    assert_eq!(vol_names, vec!["filestore", "odoo-conf"]);
    assert_eq!(spec.containers[0].volume_mounts.as_ref().unwrap().len(), 2);

    let sourced = with_source_volume(base_instance("prod"));
    let job = build_init_job(
        "prod-init",
        "tenant",
        "odoo:18.0",
        "odoo_db",
        &["base".to_string()],
        &sourced,
        &cr,
    );
    let spec = job.spec.unwrap().template.spec.unwrap();
    let vols = spec.volumes.unwrap();
    let vol_names: Vec<_> = vols.iter().map(|v| v.name.clone()).collect();
    assert_eq!(vol_names, vec!["filestore", "odoo-conf", "odoo-source"]);
    assert_eq!(
        vols[2].persistent_volume_claim.as_ref().unwrap().claim_name,
        "prod-src"
    );
    let mounts = spec.containers[0].volume_mounts.as_ref().unwrap();
    assert_eq!(mounts.len(), 4, "filestore + odoo-conf + /work + /build");
}

// ── Volumes and mounts ──────────────────────────────────────────────────────

#[test]
fn source_volume_helpers_are_empty_when_unset() {
    let inst = base_instance("prod");
    assert!(source_volumes(&inst).is_empty());
    assert!(source_volume_mounts(&inst).is_empty());
    assert_eq!(odoo_volume_mounts_for(&inst), odoo_volume_mounts());
}

#[test]
fn source_volume_mounts_reproduce_the_declared_absolute_paths() {
    let inst = with_source_volume(base_instance("prod"));
    let mounts = source_volume_mounts(&inst);
    assert_eq!(mounts.len(), 2);

    assert_eq!(mounts[0].name, "odoo-source");
    assert_eq!(mounts[0].mount_path, "/work");
    assert_eq!(mounts[0].sub_path, None);
    assert_eq!(mounts[0].read_only, None, "default false omits the field");

    assert_eq!(mounts[1].name, "odoo-source");
    assert_eq!(mounts[1].mount_path, "/build");
    assert_eq!(mounts[1].sub_path.as_deref(), Some("build"));
}

#[test]
fn source_volume_mount_honours_read_only() {
    let mut inst = with_source_volume(base_instance("prod"));
    inst.spec.source_volume.as_mut().unwrap().mounts[0].read_only = true;
    assert_eq!(source_volume_mounts(&inst)[0].read_only, Some(true));
}

// ── Security context ────────────────────────────────────────────────────────

#[test]
fn security_context_defaults_to_the_official_image_identity() {
    let sc = odoo_security_context(&base_instance("prod"));
    assert_eq!(sc.run_as_user, Some(100));
    assert_eq!(sc.run_as_group, Some(101));
    assert_eq!(sc.fs_group, Some(101), "fsGroup tracks runAsGroup");
}

#[test]
fn security_context_honours_overrides() {
    let mut inst = base_instance("prod");
    inst.spec.run_as_user = Some(10001);
    inst.spec.run_as_group = Some(10001);
    let sc = odoo_security_context(&inst);
    assert_eq!(sc.run_as_user, Some(10001));
    assert_eq!(sc.run_as_group, Some(10001));
    assert_eq!(sc.fs_group, Some(10001));
}

#[test]
fn security_context_overrides_are_independent() {
    let mut inst = base_instance("prod");
    inst.spec.run_as_user = Some(10001);
    let sc = odoo_security_context(&inst);
    assert_eq!(sc.run_as_user, Some(10001));
    assert_eq!(sc.run_as_group, Some(101), "group keeps its default");
    assert_eq!(sc.fs_group, Some(101));
}

#[test]
fn job_pods_carry_the_instance_security_context() {
    let mut inst = base_instance("prod");
    inst.spec.run_as_user = Some(10001);
    inst.spec.run_as_group = Some(10001);
    let cr = test_init_job("prod-init", false);
    let job = build_init_job(
        "prod-init",
        "tenant",
        "odoo:18.0",
        "odoo_db",
        &["base".to_string()],
        &inst,
        &cr,
    );
    let sc = job
        .spec
        .unwrap()
        .template
        .spec
        .unwrap()
        .security_context
        .unwrap();
    assert_eq!(sc.run_as_user, Some(10001));
    assert_eq!(sc.fs_group, Some(10001));
}

// ── odoo.conf object: ConfigMap vs Secret ───────────────────────────────────

#[test]
fn odoo_conf_is_a_config_map_by_default() {
    let inst = base_instance("prod");
    assert!(!odoo_conf_in_secret(&inst));
    let vol = odoo_conf_volume(&inst);
    assert_eq!(vol.name, "odoo-conf");
    assert_eq!(vol.config_map.as_ref().unwrap().name, "prod-odoo-conf");
    assert!(vol.secret.is_none());
}

#[test]
fn odoo_conf_moves_to_a_secret_when_the_password_is_secret_sourced() {
    let mut inst = base_instance("prod");
    inst.spec.admin_password = None;
    inst.spec.admin_password_secret_ref = Some(AdminPasswordSecretRef {
        name: "prod-admin-password".to_string(),
        key: "password".to_string(),
    });
    assert!(odoo_conf_in_secret(&inst));
    let vol = odoo_conf_volume(&inst);
    assert_eq!(vol.name, "odoo-conf", "volume name is unchanged");
    assert!(vol.config_map.is_none());
    assert_eq!(
        vol.secret.as_ref().unwrap().secret_name.as_deref(),
        Some("prod-odoo-conf"),
    );
    // The mount path is identical either way, so nothing downstream of
    // /etc/odoo/odoo.conf has to care which kind it is.
    assert_eq!(odoo_conf_mount().mount_path, "/etc/odoo");
}

// ── Namespaced CNPG credential mapping ──────────────────────────────────────

fn app_secret(pairs: &[(&str, &str)]) -> BTreeMap<String, Vec<u8>> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
        .collect()
}

#[test]
fn cnpg_app_secret_follows_the_naming_convention() {
    assert_eq!(cnpg_app_secret_name("prod-db"), "prod-db-app");
}

#[test]
fn cnpg_mapping_falls_back_to_the_service_convention() {
    let data = app_secret(&[("username", "app"), ("password", "s3cret")]);
    let cfg = cnpg_config_from_app_secret("prod-db", "tenant-a", &data).unwrap();
    assert_eq!(cfg.host, "prod-db-rw.tenant-a.svc.cluster.local");
    assert_eq!(cfg.port, 5432);
    assert_eq!(cfg.admin_user, "app");
    assert_eq!(cfg.admin_password, "s3cret");
    assert!(cfg.adopted, "adoption is what skips role/database creation");
    assert!(!cfg.default);
}

#[test]
fn cnpg_mapping_prefers_the_secrets_own_host_and_port() {
    let data = app_secret(&[
        ("username", "app"),
        ("password", "s3cret"),
        ("host", "prod-db-rw.other.svc.cluster.local"),
        ("port", "5433"),
    ]);
    let cfg = cnpg_config_from_app_secret("prod-db", "tenant-a", &data).unwrap();
    assert_eq!(cfg.host, "prod-db-rw.other.svc.cluster.local");
    assert_eq!(cfg.port, 5433);
}

#[test]
fn cnpg_mapping_ignores_blank_and_unparseable_values() {
    let data = app_secret(&[
        ("username", "app"),
        ("password", "s3cret"),
        ("host", ""),
        ("port", "not-a-number"),
    ]);
    let cfg = cnpg_config_from_app_secret("prod-db", "tenant-a", &data).unwrap();
    assert_eq!(cfg.host, "prod-db-rw.tenant-a.svc.cluster.local");
    assert_eq!(cfg.port, 5432);
}

#[test]
fn cnpg_mapping_requires_credentials() {
    let no_user = app_secret(&[("password", "s3cret")]);
    assert!(cnpg_config_from_app_secret("prod-db", "tenant-a", &no_user).is_err());

    let no_pw = app_secret(&[("username", "app")]);
    assert!(cnpg_config_from_app_secret("prod-db", "tenant-a", &no_pw).is_err());
}

#[test]
fn a_clusters_yaml_entry_is_never_adopted() {
    // `adopted` is #[serde(skip)], so no clusters.yaml can set it and thereby
    // talk the operator out of creating the role it owns.
    let yaml = "host: pg.example.com\nport: 5432\nadminUser: postgres\n\
                adminPassword: pw\ndefault: true\nadopted: true\n";
    let cfg: odoo_operator::postgres::PostgresClusterConfig =
        serde_yaml::from_str(yaml).expect("clusters.yaml entry parses");
    assert!(cfg.default);
    assert!(!cfg.adopted, "adopted must not be settable from YAML");
}
