use k8s_openapi::api::core::v1::{
    Affinity, EnvFromSource, EnvVar, ResourceRequirements, Toleration,
};
use kube::{CELSchema, CustomResource};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ── Spec sub-types ────────────────────────────────────────────────────────────

/// GatewayRef identifies a Gateway API Gateway resource for HTTPRoute creation.
/// When set on IngressSpec, the operator creates an HTTPRoute instead of an Ingress.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct GatewayRef {
    pub name: String,
    pub namespace: String,
}

/// IngressSpec defines how the OdooInstance should be exposed via an Ingress resource.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IngressSpec {
    pub hosts: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway_ref: Option<GatewayRef>,
}

/// One mount of the [`SourceVolumeSpec`] claim into an Odoo container.
///
/// The same claim is usually mounted more than once — the platform that owns
/// the claim lays out both the Odoo checkouts and a `pip install --target`
/// dependency tree on it, and the runtime has to reproduce the *absolute*
/// paths the build step used (e.g. whole volume at `/work`, `subPath: build`
/// at `/build`).
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceVolumeMount {
    /// Absolute path inside the container.
    pub mount_path: String,
    /// Optional path *within* the volume to mount at `mountPath`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<String>,
    /// Mount read-only. Defaults to `false`.
    #[serde(default)]
    pub read_only: bool,
}

/// SourceVolumeSpec points the instance at an externally managed volume that
/// carries the Odoo source tree, and tells the operator how to launch it.
///
/// The stock operator hardcodes the official Odoo Docker image's launch
/// convention (`/entrypoint.sh odoo …`). An image built by a different
/// toolchain has no `/entrypoint.sh`, and its Odoo source lives on a volume
/// rather than baked into the image — so every container that executes Odoo
/// must instead run `python3 <odooBin> -c /etc/odoo/odoo.conf …` with the
/// claim mounted.
///
/// When this field is absent the operator behaves exactly as upstream: no
/// extra volumes, and the `/entrypoint.sh` convention is used unchanged.
///
/// The claim itself is *never* created by the operator — it is adopted. The
/// owning platform is responsible for populating it before the instance runs.
///
/// Note the `-c /etc/odoo/odoo.conf` is not optional: the official image
/// exports `ODOO_RC` pointing there, and bypassing the entrypoint also
/// bypasses that, so the config file (which carries `addons_path`, `db_host`,
/// `db_user`, `db_password`, …) has to be named explicitly or Odoo starts
/// with nothing but defaults. Where a *subcommand* is involved (`odoo
/// neutralize`) the flag must follow it: Odoo's CLI dispatcher only reads a
/// subcommand from the first argument when that argument does not start with
/// `-`, so a leading `-c` makes it silently run a server instead.
///
/// In this mode `addons_path` is exactly what `configOptions.addons_path`
/// says — the official image's `/opt/odoo/...` defaults are *not* prepended,
/// because a toolchain image does not ship Odoo's source.
///
/// The operator does not put the dependency tree on `PYTHONPATH` for you.
/// Everything that runs Python in this mode — Odoo itself, the cron pod's exec
/// probes (`scripts/cron_*_probe.py`) and any Odoo-adjacent tooling — resolves
/// its imports from the image's own site-packages plus whatever `spec.extraEnv`
/// sets, which is where the platform passes `PYTHONPATH=/build`. An image
/// without the probes' dependencies, or a CR that omits that `extraEnv`, will
/// fail at run time rather than at admission.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceVolumeSpec {
    /// Name of the pre-existing PersistentVolumeClaim holding the source tree.
    pub claim_name: String,
    /// How to mount that claim. Must be non-empty.
    pub mounts: Vec<SourceVolumeMount>,
    /// Absolute path to `odoo-bin` **as seen through `mounts`**.
    pub odoo_bin: String,
}

/// Shared custom addons, read-only to Odoo and managed by the hosting platform.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CustomSourceVolumeSpec {
    pub claim_name: String,
    pub mounts: Vec<SourceVolumeMount>,
}

/// Production HTTP telemetry only. The platform owns the immutable mapping
/// ConfigMap; the operator owns the exporter container and its fixed contract.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MonitoringSpec {
    #[schemars(
        length(max = 256),
        regex(
            pattern = "^(docker.io/)?prom/statsd-exporter:v[0-9]+[.][0-9]+[.][0-9]+@sha256:[a-f0-9]{64}$"
        )
    )]
    pub exporter_image: String,
    /// Content-addressed, immutable ConfigMap with `statsd-mapping.yml`.
    #[schemars(length(max = 63), regex(pattern = "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"))]
    pub config_map_name: String,
    /// Optional exporter resource budget. Omitting this field retains the operator's
    /// default requests (10m CPU, 32Mi memory) and limits (100m CPU, 128Mi memory).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
}

/// AdminPasswordSecretRef sources the Odoo master password from a Secret
/// instead of carrying it in plaintext in the CR.
///
/// Exactly one of `spec.adminPassword` and `spec.adminPasswordSecretRef` must
/// be set; the CRD rejects a CR that sets both or neither.
///
/// When the ref is used the rendered `odoo.conf` is written to a **Secret**
/// rather than the usual ConfigMap, so the master password never lands in a
/// world-readable object — see `child_resources::ensure_config_map`.
///
/// Two edges worth knowing:
///
///   * The CEL XOR tests field *presence*, so an explicit `adminPassword: null`
///     alongside a ref passes admission and then fails at render time with an
///     explicit error. Writing an explicit null is the only way to hit this.
///   * Switching back from a ref to a plaintext `adminPassword` stops the pods
///     mounting the odoo.conf Secret, but does not delete it; its
///     ownerReference reaps it when the instance is deleted.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AdminPasswordSecretRef {
    /// Name of the Secret, in the instance's own namespace.
    pub name: String,
    /// Key within that Secret holding the master password.
    pub key: String,
}

/// FilestoreSpec defines persistent storage for the Odoo filestore.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FilestoreSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
}

/// Policy for what to do when the per-instance database is observed
/// missing (e.g. dropped out-of-band) while `status.dbInitialized == true`.
///
///   * `Ignore` (default) — publish a Warning event and let humans decide
///     whether to restore the DB or trigger a re-init (manually flipping
///     `status.dbInitialized` to false). Safe default: never wipes data
///     during operator-external maintenance windows.
///   * `Recreate` — automatically flip `status.dbInitialized` to false so
///     the state machine drives back to `Uninitialized` and the
///     `init.enabled` auto-init path recreates the DB. Opt in only when
///     the operator is the exclusive owner of DB lifecycle.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub enum DatabaseMissingPolicy {
    #[default]
    Ignore,
    Recreate,
}

/// DatabaseSpec identifies which PostgreSQL cluster to use for this instance.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseSpec {
    /// Which PostgreSQL cluster to use.
    ///
    /// Resolved in two steps, and the order is deliberate: a
    /// `postgresql.cnpg.io/v1` Cluster of this name in the instance's **own
    /// namespace** wins, and only if none exists is the name looked up as a
    /// key in the operator's `clusters.yaml` Secret. A namespaced Cluster
    /// therefore *shadows* a same-named clusters.yaml entry — intentional, so a
    /// tenant's own database is never silently overridden by a global name,
    /// but worth knowing when picking cluster names.
    ///
    /// Only a genuine 404 counts as "no such Cluster". Any other error from
    /// that lookup (notably a 403 from missing RBAC) fails the reconcile rather
    /// than falling through to clusters.yaml, since falling through would point
    /// the instance at an entirely different database.
    ///
    /// A namespaced Cluster is *adopted*: its database and owning role are
    /// assumed to exist already, and the operator neither creates nor drops
    /// them. Migrating an instance INTO an adopted cluster is not supported —
    /// the migration path runs `createdb`, which the app role cannot do.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Reaction when the database is observed missing post-initialization.
    /// See `DatabaseMissingPolicy`. Default `Ignore`.
    #[serde(default)]
    pub missing_policy: DatabaseMissingPolicy,
    /// Whether to ensure the `unaccent` PostgreSQL extension exists in this
    /// instance's database.
    ///
    /// Odoo treats unaccent as opt-in (the `--unaccent` startup flag, which
    /// defaults off) because it changes search semantics database-wide:
    /// with it, `ilike` matching folds accents, so "Montreal" matches
    /// "Montréal". Most deployments want that, so this defaults to `true` —
    /// but it is a behavioural change, not purely an optimisation, and an
    /// operator should be able to decline it per instance.
    ///
    /// Setting this to `false` does not *remove* an already-installed
    /// extension; it only stops the operator from creating one. Dropping it
    /// is deliberately left as a manual action, since other objects may
    /// already depend on it.
    ///
    /// `pg_trgm` is not configurable here: Odoo creates it unconditionally in
    /// `_initialize_db`, so the operator matches that rather than inventing a
    /// difference.
    #[serde(default = "default_true")]
    pub unaccent: bool,
}

/// Environment tags an OdooInstance as production or staging.  Used by:
///   - The `bemade.org/environment` pod label, which Calico network
///     policies key on to allow or deny egress to real mail servers,
///     ERP integrations, etc.
///   - Future: mail-server auto-configuration that points staging
///     instances at Mailpit rather than real SMTP.
///
/// Default is `Staging` — the safer posture.  An accidental omission
/// can't leak production credentials to a real mail server because a
/// Staging-tagged instance is blocked by Calico and auto-reconfigured
/// to Mailpit on neutralize.  Production must be set explicitly.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub enum Environment {
    #[default]
    Staging,
    Production,
}

impl Environment {
    /// Lowercase label value used in `bemade.org/environment`.
    pub fn as_label(&self) -> &'static str {
        match self {
            Environment::Staging => "staging",
            Environment::Production => "production",
        }
    }
}

/// ProductionInstanceRef declares the source-of-truth production
/// `OdooInstance` that a staging instance should be cloned from on first
/// initialization. When set, the operator auto-creates an
/// `OdooStagingRefreshJob` in place of the normal auto-init path so the
/// staging comes up pre-populated from prod in a single manifest apply.
///
/// Only meaningful when `environment == Staging`. Same-namespace only in
/// v1 (matches the same-ns constraint already enforced by
/// `OdooStagingRefreshJob` reconciliation).
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProductionInstanceRef {
    /// Name of the source `OdooInstance`.
    pub name: String,
    /// Reserved for a future cross-namespace phase; must equal the
    /// target namespace (or be unset) in v1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// DeploymentStrategyType specifies the update strategy for the Odoo Deployment.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub enum DeploymentStrategyType {
    #[default]
    Recreate,
    RollingUpdate,
}

/// RollingUpdateSpec configures the RollingUpdate deployment strategy parameters.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RollingUpdateSpec {
    #[serde(default = "default_25_percent")]
    pub max_unavailable: String,
    #[serde(default = "default_25_percent")]
    pub max_surge: String,
}

fn default_25_percent() -> String {
    "25%".to_string()
}

/// StrategySpec defines the Deployment update strategy.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StrategySpec {
    #[serde(default, rename = "type")]
    pub strategy_type: DeploymentStrategyType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rolling_update: Option<RollingUpdateSpec>,
}

/// OdooWebhookConfig defines an optional webhook callback for status change notifications.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct OdooWebhookConfig {
    pub url: String,
}

/// ProbesSpec configures the HTTP health check paths for Kubernetes probes.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProbesSpec {
    #[serde(default = "default_health_path")]
    pub startup_path: String,
    #[serde(default = "default_health_path")]
    pub liveness_path: String,
    #[serde(default = "default_health_path")]
    pub readiness_path: String,
}

fn default_health_path() -> String {
    "/web/health".to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CronSpec {
    #[serde(default = "default_replicas")]
    pub replicas: i32,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
}

impl Default for CronSpec {
    fn default() -> Self {
        CronSpec {
            replicas: default_replicas(),
            resources: None,
        }
    }
}

/// ReadOnlySqlAccessSpec opts a tenant into a read-only Postgres role
/// (`<pg_user>_ro`) that the operator provisions, manages, and tears down
/// declaratively.  Default is absent / disabled — existing instances are
/// unaffected.
///
/// When enabled, the operator:
///   1. Creates a k8s Secret `<instance>-db-ro-password` in the instance
///      namespace with a random password (generated once, never rotated by
///      the operator unless the Secret is deleted).
///   2. Ensures a PostgreSQL role `<pg_user>_ro` with LOGIN, NOSUPERUSER,
///      NOCREATEDB, and the configured `connection_limit`.
///   3. Grants CONNECT on the tenant DB, USAGE on schema public, SELECT on
///      all tables, and ALTER DEFAULT PRIVILEGES … GRANT SELECT for future
///      tables — explicitly no INSERT/UPDATE/DELETE/DDL.
///
/// On disable (field removed or `enabled: false`) or instance deletion, the
/// operator drops the role and deletes the Secret.
///
/// Consumption: the credentials live only in the k8s Secret and are intended
/// for an in-cluster consumer running inside the tenant's own pod (e.g. an
/// in-Odoo read-only SQL console that opens its own connection as this role).
/// The role is not exposed outside the cluster — nothing here provisions a
/// network path to Postgres, and PUBLIC CONNECT on sibling tenant databases is
/// left untouched.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReadOnlySqlAccessSpec {
    /// Enable read-only SQL access for this instance.  Defaults to `false`.
    #[serde(default)]
    pub enabled: bool,

    /// Maximum number of simultaneous connections for the read-only role.
    /// Defaults to 5.
    #[serde(default = "default_ro_connection_limit")]
    pub connection_limit: i32,
}

impl Default for ReadOnlySqlAccessSpec {
    fn default() -> Self {
        ReadOnlySqlAccessSpec {
            enabled: false,
            connection_limit: default_ro_connection_limit(),
        }
    }
}

fn default_ro_connection_limit() -> i32 {
    5
}

/// InitSpec configures automatic database initialization when the instance
/// first reaches the Uninitialized phase. The operator creates an OdooInitJob
/// CR automatically — no external controller needed.
///
/// Defaults to initializing with `["base"]` modules. Set `enabled: false` to
/// skip auto-init (e.g. when restoring from backup or using an external tool).
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InitSpec {
    #[serde(default = "default_true")]
    pub enabled: bool,

    #[serde(default = "default_init_modules")]
    pub modules: Vec<String>,

    /// Install demo data during database initialization.
    /// Defaults to false (Odoo's default `without_demo=all` applies).
    #[serde(default)]
    pub demo: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook: Option<super::shared::WebhookConfig>,
}

impl Default for InitSpec {
    fn default() -> Self {
        InitSpec {
            enabled: true,
            modules: default_init_modules(),
            demo: false,
            webhook: None,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_init_modules() -> Vec<String> {
    vec!["base".to_string()]
}

// ── CRD ───────────────────────────────────────────────────────────────────────

/// OdooInstance is the Schema for the odooinstances API.
#[derive(CustomResource, Clone, Debug, Serialize, Deserialize, CELSchema)]
#[cel_validate(
    rule = Rule::new("self.environment != 'Production' || !has(self.productionInstanceRef)")
        .message(Message::Expression(
            "'spec.productionInstanceRef is forbidden on production instances'".into()
        )),
    // Exactly one of the two master-password sources. `adminPassword` used to
    // be a required field, so "neither" was unrepresentable and "both" did not
    // exist — this rule is what keeps that invariant now that the field is
    // optional. Enforced here (rather than only in the validating webhook)
    // because the webhook is registered for UPDATE only, so CREATE would
    // otherwise slip through.
    rule = Rule::new("has(self.adminPassword) != has(self.adminPasswordSecretRef)")
        .message(Message::Expression(
            "'exactly one of spec.adminPassword and spec.adminPasswordSecretRef must be set'".into()
        )),
    rule = Rule::new("!has(self.sourceVolume) || size(self.sourceVolume.mounts) > 0")
        .message(Message::Expression(
            "'spec.sourceVolume.mounts must not be empty'".into()
        )),
    rule = Rule::new("!has(self.customSourceVolume) || size(self.customSourceVolume.mounts) > 0")
        .message(Message::Expression(
            "'spec.customSourceVolume.mounts must not be empty'".into()
        )),
    rule = Rule::new(
        "!has(self.customSourceVolume) || self.customSourceVolume.mounts.all(m, m.mountPath.startsWith('/') && m.readOnly)"
    )
        .message(Message::Expression(
            "'spec.customSourceVolume mounts must be absolute and read-only'".into()
        )),
    rule = Rule::new("!has(self.sourceVolume) || self.sourceVolume.odooBin.startsWith('/')")
        .message(Message::Expression(
            "'spec.sourceVolume.odooBin must be an absolute path'".into()
        )),
    rule = Rule::new(
        "!has(self.sourceVolume) || self.sourceVolume.mounts.all(m, m.mountPath.startsWith('/'))"
    )
        .message(Message::Expression(
            "'spec.sourceVolume.mounts[].mountPath must be an absolute path'".into()
        )),
    // `odooBin` reaches the neutralize scripts through an environment variable
    // that the shell word-splits on expansion. Quoting it there is impossible
    // (expansion does not re-process quotes), so a path containing whitespace
    // would silently split into two arguments. Reject it at admission instead.
    rule = Rule::new("!has(self.monitoring) || self.environment == 'Production'")
        .message(Message::Expression(
            "'spec.monitoring is supported only for Production'".into()
        )),
    rule = Rule::new("!has(self.sourceVolume) || !self.sourceVolume.odooBin.contains(' ')")
        .message(Message::Expression(
            "'spec.sourceVolume.odooBin must not contain whitespace'".into()
        ))
)]
#[kube(
    group = "bemade.org",
    version = "v1alpha1",
    kind = "OdooInstance",
    shortname = "odoo",
    namespaced,
    status = "OdooInstanceStatus",
    scale = r#"{"specReplicasPath": ".spec.replicas", "statusReplicasPath": ".status.readyReplicas", "labelSelectorPath": ".status.selector"}"#,
    printcolumn = r#"{"name": "Image", "type": "string", "jsonPath": ".spec.image"}"#,
    printcolumn = r#"{"name": "Replicas", "type": "string", "jsonPath": ".status.readyReplicas"}"#,
    printcolumn = r#"{"name": "Phase", "type": "string", "jsonPath": ".status.phase"}"#,
    printcolumn = r#"{"name": "URL", "type": "string", "jsonPath": ".status.url"}"#,
    printcolumn = r#"{"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct OdooInstanceSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_pull_secret: Option<String>,

    /// Odoo master password, in plaintext. Mutually exclusive with
    /// [`OdooInstanceSpec::admin_password_secret_ref`] — exactly one of the two
    /// must be set (enforced by CEL on the CRD and by the validating webhook).
    ///
    /// Still honoured exactly as before for every CR that sets it; it became
    /// `Option` only so the Secret-backed alternative could exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_password: Option<String>,

    /// Source the master password from a Secret instead of `adminPassword`.
    /// In this mode the rendered `odoo.conf` is stored in a Secret rather than
    /// a ConfigMap so the password is not left in a world-readable object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_password_secret_ref: Option<AdminPasswordSecretRef>,

    #[serde(default = "default_replicas")]
    pub replicas: i32,

    #[serde(default)]
    pub cron: CronSpec,

    pub ingress: IngressSpec,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filestore: Option<FilestoreSpec>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_options: Option<std::collections::BTreeMap<String, String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<DatabaseSpec>,

    #[serde(default)]
    pub init: InitSpec,

    /// Environment tag for this instance (`Staging` or `Production`).
    /// Default is `Staging` — the safer posture, since Calico network
    /// policies and future mail-server auto-configuration key on this.
    #[serde(default)]
    pub environment: Environment,

    /// When set on a staging instance, the operator clones the named
    /// source production `OdooInstance` into this one on first
    /// initialization (via an auto-created `OdooStagingRefreshJob`)
    /// instead of running the normal `OdooInitJob` path. Ignored once
    /// `status.dbInitialized == true`. Forbidden on
    /// `environment: Production`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub production_instance_ref: Option<ProductionInstanceRef>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy: Option<StrategySpec>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook: Option<OdooWebhookConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probes: Option<ProbesSpec>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity: Option<Affinity>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tolerations: Vec<Toleration>,

    /// Opt in to a read-only Postgres role for this instance.
    /// When enabled the operator provisions `<pg_user>_ro` with SELECT-only
    /// privileges on the tenant DB, stores the password in a k8s Secret, and
    /// tears everything down on disable or instance deletion.
    /// Default is absent / disabled — existing instances are unaffected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only_sql_access: Option<ReadOnlySqlAccessSpec>,

    /// Extra environment variables injected into the instance's Odoo containers
    /// (web, cron) and the Odoo steps of its jobs (init, upgrade, neutralize).
    /// Use a plain `value`, or `valueFrom.secretKeyRef` / `configMapKeyRef` /
    /// `fieldRef` to source from a Secret/ConfigMap without putting the value in
    /// the DB. Merged after the operator's own env (last-wins by `name`), so a
    /// user entry overrides an operator default of the same name — avoid the
    /// operator's own names (`PGDATABASE`, `ODOO_RC`, …). Operator tooling
    /// containers (the `mc` backup uploader, pg-client clone/restore steps) are
    /// deliberately NOT touched, so this can't clobber a backup destination's
    /// own credentials.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_env: Vec<EnvVar>,

    /// Extra `envFrom` sources (`secretRef` / `configMapRef`) injected into the
    /// same Odoo containers as `extraEnv`. Note this has the *opposite*
    /// override behavior from `extra_env`: Kubernetes always lets explicit
    /// container `env` (the operator's own vars and anything in `extra_env`) win
    /// over `envFrom` on a name collision, regardless of order. So
    /// `extra_env_from` can add new keys or override *other* `envFrom` sources,
    /// but it cannot override an operator env var — use `extra_env` for that.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_env_from: Vec<EnvFromSource>,

    /// Run Odoo from a source tree on an externally managed volume rather than
    /// from the image, using `python3 <odooBin>` in place of the official
    /// image's `/entrypoint.sh` convention. Absent = upstream behaviour.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_volume: Option<SourceVolumeSpec>,

    /// Additional custom source shared with an external editor. The platform
    /// creates and populates the claim; Odoo always mounts it read-only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_source_volume: Option<CustomSourceVolumeSpec>,

    /// Opt-in local StatsD exporter for the production web deployment only.
    /// Cron and job pods never receive instrumentation or exporter containers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub monitoring: Option<MonitoringSpec>,

    /// `runAsUser` for every Odoo pod and job pod. Defaults to 100, the uid in
    /// the official Odoo image. Set this when the image runs Odoo as a
    /// different uid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_as_user: Option<i64>,

    /// `runAsGroup` (and `fsGroup`) for every Odoo pod and job pod. Defaults to
    /// 101, the gid in the official Odoo image. `fsGroup` follows this value so
    /// the filestore PVC stays writable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_as_group: Option<i64>,
}

fn default_replicas() -> i32 {
    1
}

/// uid the official Odoo Docker image runs as; the operator's default.
pub const DEFAULT_RUN_AS_USER: i64 = 100;
/// gid the official Odoo Docker image runs as; the operator's default.
pub const DEFAULT_RUN_AS_GROUP: i64 = 101;

impl OdooInstanceSpec {
    /// Effective `runAsUser` — [`DEFAULT_RUN_AS_USER`] unless overridden.
    pub fn effective_run_as_user(&self) -> i64 {
        self.run_as_user.unwrap_or(DEFAULT_RUN_AS_USER)
    }

    /// Effective `runAsGroup` — [`DEFAULT_RUN_AS_GROUP`] unless overridden.
    /// Also used as `fsGroup`.
    pub fn effective_run_as_group(&self) -> i64 {
        self.run_as_group.unwrap_or(DEFAULT_RUN_AS_GROUP)
    }
}

// ── Status ────────────────────────────────────────────────────────────────────

/// OdooInstancePhase represents the lifecycle state of an OdooInstance.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub enum OdooInstancePhase {
    Provisioning,
    Uninitialized,
    Initializing,
    InitFailed,
    Starting,
    Running,
    Degraded,
    Stopped,
    Upgrading,
    Restoring,
    CloningFromSource,
    BackingUp,
    MigratingFilestore,
    FinalizingFilestoreMigration,
    MigratingDatabase,
    FinalizingDatabaseMigration,
    Error,
}

impl std::fmt::Display for OdooInstancePhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Provisioning => "Provisioning",
            Self::Uninitialized => "Uninitialized",
            Self::Initializing => "Initializing",
            Self::InitFailed => "InitFailed",
            Self::Starting => "Starting",
            Self::Running => "Running",
            Self::Degraded => "Degraded",
            Self::Stopped => "Stopped",
            Self::Upgrading => "Upgrading",
            Self::Restoring => "Restoring",
            Self::CloningFromSource => "CloningFromSource",
            Self::BackingUp => "BackingUp",
            Self::MigratingFilestore => "MigratingFilestore",
            Self::FinalizingFilestoreMigration => "FinalizingFilestoreMigration",
            Self::MigratingDatabase => "MigratingDatabase",
            Self::FinalizingDatabaseMigration => "FinalizingDatabaseMigration",
            Self::Error => "Error",
        };
        write!(f, "{s}")
    }
}

/// OdooInstanceStatus defines the observed state of OdooInstance.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OdooInstanceStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<OdooInstancePhase>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    #[serde(default)]
    pub ready: bool,

    #[serde(default)]
    pub ready_replicas: i32,

    #[serde(default)]
    pub db_initialized: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_backup: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_replicas: Option<i32>,

    /// Label selector for the pods this instance manages, as a serialized
    /// selector string (e.g. `app=<name>`). Surfaced through the `scale`
    /// subresource via `labelSelectorPath`, so a HorizontalPodAutoscaler can
    /// discover the target pods. Populated by the controller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition>,

    // ── Filestore migration ──────────────────────────────────────────────
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration_job_name: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration_pv_name: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration_previous_storage_class: Option<String>,

    // ── Database migration ──────────────────────────────────────────────
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_cluster: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db_migration_job_name: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration_previous_cluster: Option<String>,
}
