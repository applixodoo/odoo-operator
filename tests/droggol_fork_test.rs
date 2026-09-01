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
    // Shell source, so the path is quoted.
    assert_eq!(
        odoo_probe_command(&with_source_volume(base_instance("prod"))),
        "python3 '/work/instances/prod/odoo/odoo-bin'"
    );
}

#[test]
fn odoo_cmd_env_is_empty_without_a_source_volume() {
    // Empty means the neutralize scripts keep their `${ODOO_CMD:-odoo}`
    // default, i.e. behave exactly as before.
    assert!(odoo_cmd_env(&base_instance("prod")).is_empty());
}

#[test]
fn odoo_cmd_env_keeps_the_config_flag_out_of_the_executable() {
    // The split is the whole point: Odoo's dispatcher only reads a subcommand
    // from the first argument when it does not start with `-`, so `-c` must
    // never precede `neutralize`.
    let envs = odoo_cmd_env(&with_source_volume(base_instance("prod")));
    let get = |k: &str| {
        envs.iter()
            .find(|e| e.name == k)
            .and_then(|e| e.value.clone())
            .unwrap_or_else(|| panic!("{k} not set"))
    };
    assert_eq!(
        get("ODOO_CMD"),
        "python3 /work/instances/prod/odoo/odoo-bin"
    );
    assert!(
        !get("ODOO_CMD").contains("-c"),
        "the config flag must not be baked into ODOO_CMD"
    );
    assert_eq!(get("ODOO_CONF_ARG"), "-c /etc/odoo/odoo.conf");
}

// ── Neutralize invocation grammar (expanded by a real shell) ────────────────

/// The exact invocation line both neutralize scripts use. Asserted to appear
/// verbatim in each script so this test cannot drift away from what ships.
const NEUTRALIZE_INVOCATION: &str = "${ODOO_CMD:-odoo} neutralize ${ODOO_CONF_ARG:-}";

/// Expand `NEUTRALIZE_INVOCATION` through `/bin/sh` with the given environment
/// and return the resulting argv words.
///
/// Word-splitting is the thing under test — asserting on the env strings alone
/// would not catch a quoting or ordering mistake — so this runs the real shell
/// rather than reimplementing its rules.
fn expand_neutralize_argv(env: &[(&str, &str)]) -> Vec<String> {
    let script = format!("printf '%s\\n' {NEUTRALIZE_INVOCATION} --db_host h -d db");
    let mut cmd = std::process::Command::new("/bin/sh");
    cmd.arg("-c").arg(&script).env_clear();
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("sh should run");
    assert!(out.status.success(), "sh failed: {out:?}");
    String::from_utf8(out.stdout)
        .expect("utf8")
        .lines()
        .map(str::to_string)
        .collect()
}

#[test]
fn neutralize_invocation_line_is_the_one_the_scripts_ship() {
    for path in ["scripts/neutralize.sh", "scripts/restore-neutralize.sh"] {
        let body = std::fs::read_to_string(path).expect("script readable");
        assert!(
            body.contains(NEUTRALIZE_INVOCATION),
            "{path} no longer contains the invocation this test pins"
        );
    }
}

#[test]
fn neutralize_argv_without_source_volume_is_exactly_upstream() {
    let argv = expand_neutralize_argv(&[]);
    assert_eq!(
        argv,
        vec!["odoo", "neutralize", "--db_host", "h", "-d", "db"],
        "unset mode must expand to upstream's bare `odoo neutralize …`"
    );
}

#[test]
fn neutralize_argv_puts_the_subcommand_immediately_after_the_executable() {
    let inst = with_source_volume(base_instance("prod"));
    let envs = odoo_cmd_env(&inst);
    let pairs: Vec<(&str, String)> = envs
        .iter()
        .map(|e| (e.name.as_str(), e.value.clone().unwrap_or_default()))
        .collect();
    let borrowed: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let argv = expand_neutralize_argv(&borrowed);

    assert_eq!(
        argv,
        vec![
            "python3",
            "/work/instances/prod/odoo/odoo-bin",
            "neutralize",
            "-c",
            "/etc/odoo/odoo.conf",
            "--db_host",
            "h",
            "-d",
            "db",
        ]
    );
    // The grammar assertion proper: whatever the executable expands to, the
    // subcommand is the very next word, and no flag precedes it.
    let sub = argv
        .iter()
        .position(|w| w == "neutralize")
        .expect("subcommand present");
    assert!(
        !argv[..sub].iter().any(|w| w.starts_with('-')),
        "no flag may precede the subcommand, or Odoo dispatches `server`: {argv:?}"
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
    // Paths are single-quoted: this string is parsed by a shell.
    assert!(
        script.contains("maj=$(python3 '/work/instances/prod/odoo/odoo-bin' --version"),
        "version probe must not go through an entrypoint, and must quote the path: {script}"
    );
    assert!(
        script.contains(
            "exec python3 '/work/instances/prod/odoo/odoo-bin' -c '/etc/odoo/odoo.conf' \"$@\" $flag"
        ),
        "exec must use the full launch command with quoted paths: {script}"
    );
    assert!(!script.contains("/entrypoint.sh"));
}

#[test]
fn demo_script_quoting_survives_a_path_needing_it() {
    // CEL forbids whitespace in odooBin, but quoting still has to be correct
    // for the other characters a shell would otherwise treat specially.
    let mut inst = with_source_volume(base_instance("prod"));
    inst.spec.source_volume.as_mut().unwrap().odoo_bin = "/work/it's/odoo-bin".to_string();
    let (_, args) = init_container_command(&inst, true);
    assert!(
        args[0].contains(r"'/work/it'\''s/odoo-bin'"),
        "single quotes in the path must be escaped: {}",
        args[0]
    );
}

// ── addons_path: the image's own directories ────────────────────────────────

#[test]
fn addons_path_prepends_the_image_defaults_without_a_source_volume() {
    let extra = Some(std::collections::BTreeMap::from([(
        "addons_path".to_string(),
        "/mnt/extra-addons".to_string(),
    )]));
    let conf = odoo_operator::helpers::build_odoo_conf("u", "p", "a", "h", 5432, "d", &extra, true);
    assert!(
        conf.contains("addons_path = /opt/odoo/addons,/opt/odoo/odoo/addons,/mnt/extra-addons\n"),
        "stock images must keep the prepend: {conf}"
    );
}

#[test]
fn addons_path_is_verbatim_with_a_source_volume() {
    // A toolchain image does not ship Odoo, so /opt/odoo/... does not exist
    // and naming it would leave dead entries in addons_path.
    let extra = Some(std::collections::BTreeMap::from([(
        "addons_path".to_string(),
        "/work/instances/prod/odoo/addons,/work/instances/prod/enterprise".to_string(),
    )]));
    let conf =
        odoo_operator::helpers::build_odoo_conf("u", "p", "a", "h", 5432, "d", &extra, false);
    assert!(
        conf.contains(
            "addons_path = /work/instances/prod/odoo/addons,/work/instances/prod/enterprise\n"
        ),
        "source-volume mode must use configOptions.addons_path verbatim: {conf}"
    );
    assert!(!conf.contains("/opt/odoo"), "no image defaults: {conf}");
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

#[test]
fn init_source_entrypoint_inherits_instance_resources() {
    let mut inst = with_source_volume(base_instance("prod"));
    let expected: k8s_openapi::api::core::v1::ResourceRequirements =
        serde_json::from_value(serde_json::json!({
            "requests": {
                "cpu": "731m",
                "memory": "1537Mi",
                "ephemeral-storage": "31Gi",
            },
            "limits": {
                "cpu": "1103m",
                "memory": "2053Mi",
                "ephemeral-storage": "32Gi",
            },
        }))
        .unwrap();
    inst.spec.resources = Some(expected.clone());
    inst.spec.extra_env = vec![k8s_openapi::api::core::v1::EnvVar {
        name: "SOURCE_JOB_SENTINEL".into(),
        value: Some("init".into()),
        ..Default::default()
    }];

    let job = build_init_job(
        "prod-init",
        "tenant",
        "odoo:18.0",
        "odoo_db",
        &["base".to_string()],
        &inst,
        &test_init_job("prod-init", false),
    );
    let container = &job.spec.unwrap().template.spec.unwrap().containers[0];

    assert_eq!(container.name, "init");
    assert_eq!(container.resources, Some(expected));
    assert!(container
        .env
        .as_ref()
        .unwrap()
        .iter()
        .any(|env| env.name == "SOURCE_JOB_SENTINEL"));
    assert!(container
        .volume_mounts
        .as_ref()
        .unwrap()
        .iter()
        .any(|mount| mount.name == "odoo-source"));
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

// ── readOnlySqlAccess on an adopted cluster ─────────────────────────────────

#[test]
fn readonly_sql_provisions_and_tears_down_on_an_owned_cluster() {
    use odoo_operator::controller::odoo_instance::{readonly_sql_action, ReadOnlySqlAction};
    assert_eq!(
        readonly_sql_action(true, false),
        ReadOnlySqlAction::Provision
    );
    assert_eq!(
        readonly_sql_action(false, false),
        ReadOnlySqlAction::Teardown
    );
}

#[test]
fn readonly_sql_is_skipped_not_attempted_on_an_adopted_cluster() {
    use odoo_operator::controller::odoo_instance::{readonly_sql_action, ReadOnlySqlAction};
    // Attempting it would fail 42501 forever, and because this runs before the
    // status patch that would wedge the instance phase-less rather than just
    // dropping one optional feature.
    assert_eq!(
        readonly_sql_action(true, true),
        ReadOnlySqlAction::SkipUnsupported
    );
    // Teardown is skipped too: nothing was ever created there to remove.
    assert_eq!(readonly_sql_action(false, true), ReadOnlySqlAction::Nothing);
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
