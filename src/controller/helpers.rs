//! Shared helpers for controller modules.
//!
//! These functions construct Kubernetes API objects that are reused across
//! multiple controllers (job builders, the instance controller, etc.).
//! Pure utility functions (naming, crypto, config generation) live in
//! `crate::helpers` instead.

use k8s_openapi::api::{
    batch::v1::{Job, JobSpec},
    core::v1::{
        Affinity, ConfigMapKeySelector, Container, EnvVar, EnvVarSource, LocalObjectReference,
        PodSecurityContext, PodSpec, PodTemplateSpec, SecretKeySelector, Volume, VolumeMount,
    },
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::ObjectMeta;
use kube::{Resource, ResourceExt};

use crate::crd::odoo_instance::OdooInstance;

/// Field manager name used for server-side apply patches.
pub const FIELD_MANAGER: &str = "odoo-operator";

/// Standard pod labels applied to every Deployment, Job, and pod template
/// owned by an OdooInstance.  Consumed by downstream systems:
///   - `bemade.org/environment` — Calico network policies key on this to
///     allow or deny egress to real mail servers and other sensitive
///     services (production-only).
///   - `bemade.org/instance` — identifies which OdooInstance this pod
///     belongs to; useful for observability and ad-hoc kubectl queries.
///
/// These are ADDITIVE to the existing `app: <deployment-name>` selector
/// label and must not be added to `spec.selector.matchLabels` on existing
/// Deployments — the selector is immutable after creation.
pub fn instance_labels(instance: &OdooInstance) -> std::collections::BTreeMap<String, String> {
    let mut m = std::collections::BTreeMap::new();
    m.insert(
        "bemade.org/environment".to_string(),
        instance.spec.environment.as_label().to_string(),
    );
    m.insert("bemade.org/instance".to_string(), instance.name_any());
    // Hosting identity is an immutable platform ID, never the mutable instance slug.
    for key in [
        "droggol.sh/server-id",
        "droggol.sh/project-id",
        "droggol.sh/instance-id",
    ] {
        if let Some(value) = instance
            .metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get(key))
        {
            m.insert(key.to_string(), value.clone());
        }
    }
    m
}

/// Env vars injected into post-neutralize Job pods for staging instances
/// when the operator was configured with a staging SMTP sink (Mailpit).
///
/// Returns empty when either:
/// - the instance is `Production` (leave real SMTP config alone), or
/// - the operator has no `staging_smtp_host` configured (keep the
///   neutralize sentinel, i.e. `smtp_host=invalid` — no outbound mail).
///
/// Consumed by `restore-neutralize.sh` and `neutralize.sh` via `MAIL_*` env vars.
pub fn staging_mail_env_vars(
    instance: &OdooInstance,
    defaults: &crate::helpers::OperatorDefaults,
) -> Vec<k8s_openapi::api::core::v1::EnvVar> {
    use crate::crd::odoo_instance::Environment;
    if instance.spec.environment != Environment::Staging || defaults.staging_smtp_host.is_empty() {
        return vec![];
    }
    vec![
        env("MAIL_SMTP_HOST", defaults.staging_smtp_host.clone()),
        env("MAIL_SMTP_PORT", defaults.staging_smtp_port.to_string()),
        env(
            "MAIL_SMTP_ENCRYPTION",
            defaults.staging_smtp_encryption.clone(),
        ),
    ]
}

/// Build a controller OwnerReference for any kube-rs `Resource`.
///
/// This is generic over `K` — the compiler fills in `api_version()` and
/// `kind()` for whichever CRD type you pass in.  The trait bound
/// `K: Resource<DynamicType = ()>` means "any type whose Kubernetes
/// metadata is known at compile time", which is true for every struct
/// that derives `CustomResource`.
pub fn controller_owner_ref<K: Resource<DynamicType = ()>>(obj: &K) -> OwnerReference {
    OwnerReference {
        api_version: K::api_version(&()).to_string(),
        kind: K::kind(&()).to_string(),
        name: obj.name_any(),
        uid: obj.meta().uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Odoo pod security context.
///
/// Defaults to uid 100 / gid 101 — the identity in the official Odoo Docker
/// image, which is what every pod ran as before `spec.runAsUser` /
/// `spec.runAsGroup` existed. `fsGroup` tracks `runAsGroup` so the filestore
/// PVC stays writable for whichever identity the image actually uses.
pub fn odoo_security_context(instance: &OdooInstance) -> PodSecurityContext {
    PodSecurityContext {
        run_as_user: Some(instance.spec.effective_run_as_user()),
        run_as_group: Some(instance.spec.effective_run_as_group()),
        fs_group: Some(instance.spec.effective_run_as_group()),
        ..Default::default()
    }
}

/// Directory the odoo-conf volume is mounted at.
pub const ODOO_CONF_DIR: &str = "/etc/odoo";
/// Absolute path of the rendered odoo.conf inside every Odoo container.
pub const ODOO_CONF_PATH: &str = "/etc/odoo/odoo.conf";
/// Pod volume name for `spec.sourceVolume`'s claim.
pub const SOURCE_VOLUME_NAME: &str = "odoo-source";

/// Name of the object holding the rendered odoo.conf.
///
/// Deliberately the same string for the ConfigMap and the Secret variants:
/// they are distinct resource kinds, so the name can be reused, and every
/// consumer that looks the object up by name keeps working across a switch.
pub fn odoo_conf_name(instance_name: &str) -> String {
    format!("{instance_name}-odoo-conf")
}

/// Whether the rendered odoo.conf lives in a Secret rather than a ConfigMap.
///
/// True exactly when the master password is sourced from a Secret — in that
/// mode `admin_passwd` must not end up in a ConfigMap, so the whole conf moves
/// to a Secret and the pod mounts that instead.
pub fn odoo_conf_in_secret(instance: &OdooInstance) -> bool {
    instance.spec.admin_password_secret_ref.is_some()
}

/// The `odoo-conf` volume: a ConfigMap normally, a Secret when the master
/// password is Secret-sourced.
pub fn odoo_conf_volume(instance: &OdooInstance) -> Volume {
    let name = odoo_conf_name(&instance.name_any());
    if odoo_conf_in_secret(instance) {
        Volume {
            name: "odoo-conf".to_string(),
            secret: Some(k8s_openapi::api::core::v1::SecretVolumeSource {
                secret_name: Some(name),
                ..Default::default()
            }),
            ..Default::default()
        }
    } else {
        Volume {
            name: "odoo-conf".to_string(),
            config_map: Some(k8s_openapi::api::core::v1::ConfigMapVolumeSource {
                name,
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

/// The mount matching [`odoo_conf_volume`].
pub fn odoo_conf_mount() -> VolumeMount {
    VolumeMount {
        name: "odoo-conf".to_string(),
        mount_path: ODOO_CONF_DIR.to_string(),
        ..Default::default()
    }
}

/// Standard volumes shared by the instance Deployment and every job pod:
/// the filestore PVC and the odoo-conf object.
pub fn odoo_volumes(instance: &OdooInstance) -> Vec<Volume> {
    vec![
        Volume {
            name: "filestore".to_string(),
            persistent_volume_claim: Some(
                k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
                    claim_name: format!("{}-filestore-pvc", instance.name_any()),
                    ..Default::default()
                },
            ),
            ..Default::default()
        },
        odoo_conf_volume(instance),
    ]
}

/// Standard volume mounts matching [`odoo_volumes`].
pub fn odoo_volume_mounts() -> Vec<VolumeMount> {
    vec![
        VolumeMount {
            name: "filestore".to_string(),
            mount_path: "/var/lib/odoo".to_string(),
            ..Default::default()
        },
        odoo_conf_mount(),
    ]
}

// ── Source volume + the command that launches Odoo ───────────────────────────

/// Core/dependency and custom-source claims for every Odoo consumer.
pub fn source_volumes(instance: &OdooInstance) -> Vec<Volume> {
    [
        (
            SOURCE_VOLUME_NAME,
            instance.spec.source_volume.as_ref().map(|v| &v.claim_name),
        ),
        (
            "odoo-custom-source",
            instance
                .spec
                .custom_source_volume
                .as_ref()
                .map(|v| &v.claim_name),
        ),
    ]
    .into_iter()
    .filter_map(|(name, claim)| {
        claim.map(|claim| Volume {
            name: name.to_string(),
            persistent_volume_claim: Some(
                k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
                    claim_name: claim.clone(),
                    read_only: (name == "odoo-custom-source").then_some(true),
                },
            ),
            ..Default::default()
        })
    })
    .collect()
}

/// Mount both source claims through the shared serving/job construction path.
pub fn source_volume_mounts(instance: &OdooInstance) -> Vec<VolumeMount> {
    [
        (
            SOURCE_VOLUME_NAME,
            instance.spec.source_volume.as_ref().map(|v| &v.mounts),
        ),
        (
            "odoo-custom-source",
            instance
                .spec
                .custom_source_volume
                .as_ref()
                .map(|v| &v.mounts),
        ),
    ]
    .into_iter()
    .flat_map(|(name, mounts)| {
        mounts.into_iter().flatten().map(move |m| VolumeMount {
            name: name.to_string(),
            mount_path: m.mount_path.clone(),
            sub_path: m.sub_path.clone(),
            read_only: (m.read_only || name == "odoo-custom-source").then_some(true),
            ..Default::default()
        })
    })
    .collect()
}

/// [`odoo_volume_mounts`] plus the source-volume mounts. Use this for every
/// container that actually executes Odoo.
pub fn odoo_volume_mounts_for(instance: &OdooInstance) -> Vec<VolumeMount> {
    let mut mounts = odoo_volume_mounts();
    mounts.extend(source_volume_mounts(instance));
    mounts
}

/// The argv prefix that launches Odoo for this instance.
///
/// Without `spec.sourceVolume` this is the official image's
/// `["/entrypoint.sh", "odoo"]` — byte-identical to what the operator has
/// always emitted. With it, the entrypoint is bypassed in favour of running
/// `odoo-bin` directly under `python3`.
///
/// The `-c` is what replaces the entrypoint's contribution. That script did
/// two things worth keeping: it waited for PostgreSQL, and it appended
/// `--db_host/--db_port/--db_user/--db_password` read *out of the config file
/// at `$ODOO_RC`*. Since those four values come from the same
/// `build_odoo_conf` output either way, naming the config file explicitly
/// carries all of them — plus `addons_path`, `data_dir` and the rest, which
/// the image would otherwise have supplied through its own `ODOO_RC` env var.
/// The wait-for-postgres step is not reproduced: Odoo retries its own
/// connection and the pod restarts on failure.
pub fn odoo_entrypoint(instance: &OdooInstance) -> Vec<String> {
    match instance.spec.source_volume.as_ref() {
        None => vec!["/entrypoint.sh".to_string(), "odoo".to_string()],
        Some(sv) => vec![
            "python3".to_string(),
            sv.odoo_bin.clone(),
            "-c".to_string(),
            ODOO_CONF_PATH.to_string(),
        ],
    }
}

/// [`odoo_entrypoint`] plus trailing arguments, as one argv vector.
pub fn odoo_command(instance: &OdooInstance, args: &[&str]) -> Vec<String> {
    let mut cmd = odoo_entrypoint(instance);
    cmd.extend(args.iter().map(|a| a.to_string()));
    cmd
}

/// Single-quote a word for safe interpolation into a `sh -c` program.
///
/// Only used where the operator builds shell *source*; argv vectors never go
/// through this, since there is no shell to re-parse them.
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

/// The executable part of the Odoo invocation, as shell source — *without* the
/// `-c` flag.
///
/// The `-c <conf>` flag is deliberately NOT part of this, and is carried by a
/// separate `ODOO_CONF_ARG` (see [`odoo_cmd_env`]), because Odoo's CLI
/// dispatcher only treats the first argument as a subcommand when it does not
/// start with `-` (`odoo/cli/command.py`). Folding `-c <conf>` in here would
/// make `$ODOO_CMD neutralize …` expand to
/// `python3 odoo-bin -c … neutralize …`, where the dispatcher sees `-c`,
/// silently falls back to the `server` command, and runs a *server* instead of
/// neutralizing.
pub fn odoo_shell_executable(instance: &OdooInstance) -> String {
    match instance.spec.source_volume.as_ref() {
        None => "odoo".to_string(),
        Some(sv) => format!("python3 {}", shell_quote(&sv.odoo_bin)),
    }
}

/// A shell word-list that runs Odoo *without* the image entrypoint wrapper.
///
/// Used where the goal is to interrogate the binary rather than start a
/// server — the entrypoint would otherwise block on `wait-for-psql.py` first.
/// Matches the bare `odoo` upstream used for exactly that purpose.
pub fn odoo_probe_command(instance: &OdooInstance) -> String {
    odoo_shell_executable(instance)
}

/// [`odoo_entrypoint`] as shell source, for the one `sh -c` step that `exec`s
/// the *server* rather than a subcommand (the demo-data init path).
///
/// No subcommand is involved, so `-c` leading the arguments is correct here —
/// the dispatcher's fallback to `server` is exactly what that step wants.
/// Paths are quoted, unlike the [`odoo_cmd_env`] variables, because this string
/// really is parsed by a shell.
pub fn odoo_entrypoint_shell(instance: &OdooInstance) -> String {
    match instance.spec.source_volume.as_ref() {
        None => "/entrypoint.sh odoo".to_string(),
        Some(sv) => format!(
            "python3 {} -c {}",
            shell_quote(&sv.odoo_bin),
            shell_quote(ODOO_CONF_PATH)
        ),
    }
}

/// `ODOO_CMD` / `ODOO_CONF_ARG` for the shell scripts that shell out to Odoo
/// (`neutralize.sh`, `restore-neutralize.sh`).
///
/// Both are empty when `spec.sourceVolume` is unset, so
/// `${ODOO_CMD:-odoo} neutralize ${ODOO_CONF_ARG:-} …` expands to exactly the
/// bare `odoo neutralize …` those scripts ran before — an unquoted empty
/// expansion contributes no word at all.
///
/// Deliberately *not* shell-quoted: the value is word-split by the shell on
/// expansion and quotes are not re-processed, so an embedded `'` would reach
/// Odoo literally. `spec.sourceVolume.odooBin` is therefore constrained by CEL
/// to contain no whitespace.
pub fn odoo_cmd_env(instance: &OdooInstance) -> Vec<EnvVar> {
    let Some(sv) = instance.spec.source_volume.as_ref() else {
        return vec![];
    };
    vec![
        env("ODOO_CMD", format!("python3 {}", sv.odoo_bin)),
        env("ODOO_CONF_ARG", format!("-c {ODOO_CONF_PATH}")),
    ]
}

/// Build the `imagePullSecrets` list from an OdooInstance spec.
/// Returns `None` when no pull secret is configured (which omits the
/// field from the serialised JSON, matching the K8s convention).
pub fn image_pull_secrets(instance: &OdooInstance) -> Option<Vec<LocalObjectReference>> {
    instance
        .spec
        .image_pull_secret
        .as_ref()
        .map(|name| vec![LocalObjectReference { name: name.clone() }])
}

/// Shorthand for a plain-value `EnvVar`.
pub fn env(name: &str, value: impl Into<String>) -> EnvVar {
    EnvVar {
        name: name.into(),
        value: Some(value.into()),
        ..Default::default()
    }
}

/// Build an `EnvVar` that reads its value from a ConfigMap key.
pub fn cm_env(env_name: &str, cm_name: &str, key: &str) -> EnvVar {
    EnvVar {
        name: env_name.into(),
        value_from: Some(EnvVarSource {
            config_map_key_ref: Some(ConfigMapKeySelector {
                name: cm_name.into(),
                key: key.into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Build an `EnvVar` that reads its value from a Secret key.
pub fn secret_env(env_name: &str, secret_name: &str, key: &str) -> EnvVar {
    EnvVar {
        name: env_name.into(),
        value_from: Some(EnvVarSource {
            secret_key_ref: Some(SecretKeySelector {
                name: secret_name.into(),
                key: key.into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Merge two `EnvVar` slices, last-wins by `name`. `base` order is preserved;
/// an `extra` entry whose `name` matches a base entry overrides it in place,
/// and new names are appended. Used to layer an instance's `spec.extra_env` on
/// top of the operator's own env so users can override by name (not by
/// ordering) without silently clobbering reserved operator vars.
pub fn merge_extra_env(base: &[EnvVar], extra: &[EnvVar]) -> Vec<EnvVar> {
    let mut out: Vec<EnvVar> = base.to_vec();
    for e in extra {
        if let Some(slot) = out.iter_mut().find(|x| x.name == e.name) {
            *slot = e.clone();
        } else {
            out.push(e.clone());
        }
    }
    out
}

/// Layer an instance's `spec.extra_env` / `spec.extra_env_from` onto a single
/// container and return it: `extra_env` is merged last-wins by `name` over the
/// container's own env (base order preserved, via [`merge_extra_env`]), and
/// `extra_env_from` is appended to its `envFrom`. A no-op (container returned
/// untouched) when both are empty, so instances that set neither produce byte-
/// identical pods to before — no spurious server-side-apply diffs.
///
/// Apply this ONLY to containers that run the Odoo image and execute Odoo: the
/// web and cron Deployments and the init / upgrade / neutralize job steps. The
/// operator's own tooling containers — the `mc` backup uploader/downloader, the
/// pg-client `clone-db` / `load-db` steps, the rsync filestore mover — must be
/// left untouched: their env carries operator-managed credentials (e.g. the
/// backup *destination's* `AWS_*` / `S3_*` keys) that a last-wins user override
/// would otherwise clobber, breaking backups when the DR target is a separate
/// account.
pub fn apply_extra_env(mut c: Container, instance: &OdooInstance) -> Container {
    if !instance.spec.extra_env.is_empty() {
        let base = c.env.take().unwrap_or_default();
        c.env = Some(merge_extra_env(&base, &instance.spec.extra_env));
    }
    if !instance.spec.extra_env_from.is_empty() {
        let mut ef = c.env_from.take().unwrap_or_default();
        ef.extend(instance.spec.extra_env_from.iter().cloned());
        c.env_from = Some(ef);
    }
    c
}

/// Get the cron deployment name for an odoo instance
pub fn cron_depl_name(instance: &OdooInstance) -> String {
    instance.name_any().to_string() + "-cron"
}

/// Image carrying pg client tools (`psql`, `pg_dump`, `pg_restore`, `createdb`)
/// that match a server major version. pg_dump/pg_restore must be ≥ the server
/// major on both ends of a pipe, so callers should pass `max(src_major, dst_major)`.
pub fn pg_tools_image(major: u32) -> String {
    format!("postgres:{major}-alpine")
}

// ── OdooJobBuilder ──────────────────────────────────────────────────────────

/// Builder for batch/v1 `Job` resources used by the operator's job controllers.
///
/// Encapsulates the boilerplate that every job shares (metadata, backoff policy,
/// TTL, security context, standard volumes) and lets callers specify only what
/// differs: containers, init containers, extra volumes, deadlines, and affinity.
///
/// # Example
///
/// ```ignore
/// let job = OdooJobBuilder::new("my-init-", ns, &init_job_cr, &instance)
///     .containers(vec![my_container])
///     .build();
/// ```
pub struct OdooJobBuilder {
    generate_name: String,
    namespace: String,
    owner_ref: OwnerReference,
    pull_secrets: Option<Vec<LocalObjectReference>>,
    volumes: Vec<Volume>,
    containers: Vec<Container>,
    init_containers: Option<Vec<Container>>,
    active_deadline: Option<i64>,
    backoff_limit: Option<i32>,
    affinity: Option<Affinity>,
    labels: std::collections::BTreeMap<String, String>,
    security_context: PodSecurityContext,
}

impl OdooJobBuilder {
    /// Create a new builder with the standard Odoo job defaults.
    ///
    /// - `generate_name_prefix`: the `metadata.generateName` value (e.g. `"my-init-"`)
    /// - `ns`: namespace for the Job
    /// - `owner`: the CRD resource that owns this Job (sets the controller owner reference)
    /// - `instance`: the OdooInstance (used for pull secrets and standard volumes)
    pub fn new<K: Resource<DynamicType = ()>>(
        generate_name_prefix: &str,
        ns: &str,
        owner: &K,
        instance: &OdooInstance,
    ) -> Self {
        Self {
            generate_name: generate_name_prefix.to_string(),
            namespace: ns.to_string(),
            owner_ref: controller_owner_ref(owner),
            pull_secrets: image_pull_secrets(instance),
            volumes: odoo_volumes(instance),
            containers: vec![],
            init_containers: None,
            active_deadline: None,
            backoff_limit: None,
            affinity: None,
            labels: instance_labels(instance),
            security_context: odoo_security_context(instance),
        }
    }

    /// Attach the instance's `spec.sourceVolume` claim to the pod. No-op when
    /// the field is unset, so job builders can call it unconditionally.
    ///
    /// Only the jobs whose containers actually execute Odoo need this — the
    /// pg-client, `mc` and rsync tooling steps deliberately do not carry it.
    pub fn with_source_volume(mut self, instance: &OdooInstance) -> Self {
        self.volumes.extend(source_volumes(instance));
        self
    }

    /// Set the main containers for the Job pod.
    pub fn containers(mut self, containers: Vec<Container>) -> Self {
        self.containers = containers;
        self
    }

    /// Set init containers for the Job pod.
    pub fn init_containers(mut self, init_containers: Vec<Container>) -> Self {
        self.init_containers = if init_containers.is_empty() {
            None
        } else {
            Some(init_containers)
        };
        self
    }

    /// Append extra volumes beyond the standard filestore + odoo-conf.
    pub fn extra_volumes(mut self, extra: Vec<Volume>) -> Self {
        self.volumes.extend(extra);
        self
    }

    /// Drop the standard filestore PVC + odoo-conf volumes. Useful for jobs
    /// that don't run odoo-bin and don't touch the filestore — skipping the
    /// PVC mount avoids the fsGroup chown traversal at pod start.
    pub fn without_standard_volumes(mut self) -> Self {
        self.volumes.clear();
        self
    }

    /// Set `spec.activeDeadlineSeconds` on the Job.
    pub fn active_deadline(mut self, seconds: i64) -> Self {
        self.active_deadline = Some(seconds);
        self
    }

    /// Set `spec.backoffLimit` on the Job.  Default (when unset) is 0 —
    /// any pod failure terminates the Job.  Override to allow K8s' built-in
    /// exponential backoff retries for transient failures (network blips,
    /// DB connect, etc.).  Note: ImagePullBackOff is bounded by
    /// `activeDeadlineSeconds`, not `backoffLimit`, because the Pod never
    /// transitions to a terminal exit-code state under that condition.
    pub fn backoff_limit(mut self, n: i32) -> Self {
        self.backoff_limit = Some(n);
        self
    }

    /// Set pod affinity (e.g. co-locate with the instance deployment).
    pub fn affinity(mut self, affinity: Affinity) -> Self {
        self.affinity = Some(affinity);
        self
    }

    /// Consume the builder and produce a `batch/v1 Job`.
    pub fn build(self) -> Job {
        let template_meta = ObjectMeta {
            labels: Some(self.labels.clone()),
            ..Default::default()
        };
        Job {
            metadata: ObjectMeta {
                generate_name: Some(self.generate_name),
                namespace: Some(self.namespace),
                owner_references: Some(vec![self.owner_ref]),
                labels: Some(self.labels),
                ..Default::default()
            },
            spec: Some(JobSpec {
                backoff_limit: Some(self.backoff_limit.unwrap_or(0)),
                ttl_seconds_after_finished: Some(900),
                active_deadline_seconds: self.active_deadline,
                template: PodTemplateSpec {
                    metadata: Some(template_meta),
                    spec: Some(PodSpec {
                        restart_policy: Some("Never".to_string()),
                        image_pull_secrets: self.pull_secrets,
                        security_context: Some(self.security_context),
                        affinity: self.affinity,
                        volumes: Some(self.volumes),
                        init_containers: self.init_containers,
                        containers: self.containers,
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::merge_extra_env;
    use k8s_openapi::api::core::v1::EnvVar;

    fn ev(name: &str, value: &str) -> EnvVar {
        EnvVar {
            name: name.into(),
            value: Some(value.into()),
            ..Default::default()
        }
    }

    #[test]
    fn empty_base_empty_extra() {
        assert!(merge_extra_env(&[], &[]).is_empty());
    }

    #[test]
    fn empty_base_returns_extra() {
        let extra = vec![ev("A", "1"), ev("B", "2")];
        assert_eq!(merge_extra_env(&[], &extra), extra);
    }

    #[test]
    fn empty_extra_returns_base() {
        let base = vec![ev("PGDATABASE", "db")];
        assert_eq!(merge_extra_env(&base, &[]), base);
    }

    #[test]
    fn extra_overrides_base_in_place_and_appends_new() {
        let base = vec![ev("PGDATABASE", "db"), ev("KEEP", "k")];
        let extra = vec![ev("PGDATABASE", "override"), ev("NEW", "n")];
        let out = merge_extra_env(&base, &extra);
        // Order preserved: overridden entry stays in its base slot; new appended.
        assert_eq!(out.len(), 3);
        assert_eq!(out[0], ev("PGDATABASE", "override"));
        assert_eq!(out[1], ev("KEEP", "k"));
        assert_eq!(out[2], ev("NEW", "n"));
    }

    #[test]
    fn multiple_overlaps_last_wins_preserving_order() {
        let base = vec![ev("A", "1"), ev("B", "2"), ev("C", "3")];
        let extra = vec![ev("B", "B2"), ev("A", "A2")];
        let out = merge_extra_env(&base, &extra);
        assert_eq!(out, vec![ev("A", "A2"), ev("B", "B2"), ev("C", "3")]);
    }
}
