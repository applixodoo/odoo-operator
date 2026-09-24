# odoo-operator

A Kubernetes operator for managing Odoo instances. Declaratively deploy, initialize,
upgrade, back up, and restore Odoo databases using custom resources.

Built with [kube-rs](https://kube.rs) in Rust. Deployed via Helm.

## Custom Resources

| Resource | Purpose |
|---|---|
| `OdooInstance` | Declares a running Odoo deployment: image, replicas, ingress, filestore, database |
| `OdooInitJob` | One-shot job to initialize a fresh database |
| `OdooUpgradeJob` | Runs `odoo -u` against an existing database, then rolls the deployment |
| `OdooBackupJob` | Dumps the database and filestore to object storage |
| `OdooRestoreJob` | Restores a backup into an OdooInstance |

## Prerequisites

- Kubernetes 1.26+
- cert-manager (for webhook TLS)
- A PostgreSQL cluster accessible from the operator namespace
- Helm 3

## Installation

### 1. Create the postgres clusters secret

```yaml
# clusters.yaml
main:
  host: postgres.postgres.svc.cluster.local
  port: 5432
  adminUser: postgres
  adminPassword: secret
  default: true
```

```sh
kubectl create namespace odoo-operator
kubectl create secret generic pg-clusters -n odoo-operator \
  --from-file=clusters.yaml=clusters.yaml
```

### 2. Install the chart

```sh
helm upgrade --install odoo-operator oci://ghcr.io/bemade/odoo-operator/charts/odoo-operator \
  --namespace odoo-operator \
  --set defaults.ingressClass=nginx \
  --set defaults.ingressIssuer=letsencrypt-prod
```

**NOTE**: If you have a previously installed version of the chart, you may need to
completely uninstall and reinstall it. This chart is still in early and active
development and breaking changes are still frequent. Uninstalling the chart does not
remove all your running OdooInstances. You may also need to clear the odoo-operator
ServiceAccount resource along with the ClusterRole and ClusterRoleBinding of the same
name. These previously had Helm hook values on them that break installation of later
versions starting at v0.13.3.

### 3. Deploy an Odoo instance

```yaml
apiVersion: bemade.org/v1alpha1
kind: OdooInstance
metadata:
  name: myodoo
  namespace: odoo
spec:
  image: odoo:18.0
  adminPassword: changeme
  replicas: 1
  ingress:
    hosts:
      - myodoo.example.com
```

```sh
kubectl apply -f odoo.yaml
```

By default, the operator automatically initializes the database with the `base`
module. To skip auto-init (e.g. when restoring from a backup), set
`spec.init.enabled: false`.

## Configuration Reference

### OdooInstance spec

| Field | Default | Description |
|---|---|---|
| `image` | operator default | Odoo container image |
| `replicas` | `1` | Number of web pods, or combined web/cron pods. Set to `0` to stop the instance |
| `workloadLayout` | `separate` | `separate` creates web and cron Deployments; `combined` puts web and cron containers in one Deployment |
| `adminPassword` | — | Odoo master password |
| `imagePullSecret` | — | Name of a `kubernetes.io/dockerconfigjson` secret in the operator namespace (auto-copied to instance namespace) |
| `ingress.hosts` | — | Hostnames to expose the instance on |
| `ingress.issuer` | operator default | cert-manager ClusterIssuer for TLS (ignored when `gatewayRef` is set) |
| `ingress.class` | operator default | IngressClass name |
| `ingress.gatewayRef.name` | operator default | Gateway name for HTTPRoute (creates HTTPRoute instead of Ingress) |
| `ingress.gatewayRef.namespace` | operator default | Gateway namespace for HTTPRoute |
| `database.cluster` | secret default | Postgres cluster name from the pg-clusters secret |
| `database.name` | auto-generated | Database name. Defaults to `odoo_<uid>` if unset |
| `init.enabled` | `true` | Automatically initialize the database when the instance is first created (skipped when `productionInstanceRef` is set) |
| `init.modules` | `["base"]` | Odoo modules to install during auto-initialization |
| `init.webhook` | — | Webhook to notify on init job status changes |
| `productionInstanceRef.name` | — | Name of a source production `OdooInstance` to clone from on first init. When set, the operator creates an `OdooStagingRefreshJob` instead of an `OdooInitJob`. Forbidden on `environment: Production`. See [Staging from production](#staging-from-production) |
| `productionInstanceRef.namespace` | same as target | Reserved for a future cross-namespace phase; must equal (or omit) the target's namespace in v1 |
| `filestore.storageSize` | `2Gi` | PVC size. Can only be increased, not decreased |
| `filestore.storageClass` | operator default | StorageClass for the filestore PVC. Immutable after creation |
| `resources` | operator default | CPU/memory requests and limits for web pods |
| `cron.replicas` | `1` | Number of cron pods in `separate` layout. Must remain `1` in `combined` layout |
| `cron.resources` | same as `resources` | CPU/memory requests and limits for cron pods |
| `strategy.type` | `Recreate` | Deployment strategy (`Recreate` or `RollingUpdate`) |
| `strategy.rollingUpdate.maxUnavailable` | `25%` | Max unavailable pods during rolling update |
| `strategy.rollingUpdate.maxSurge` | `25%` | Max extra pods during rolling update |
| `probes.startupPath` | `/web/health` | Startup probe path |
| `probes.livenessPath` | `/web/health` | Liveness probe path |
| `probes.readinessPath` | `/web/health` | Readiness probe path |
| `configOptions` | — | Extra key-value pairs appended to `odoo.conf` |
| `webhook.url` | — | URL to receive status change callbacks |
| `affinity` | operator default | Pod affinity rules |
| `tolerations` | operator default | Pod tolerations |
| `monitoring.exporterImage` | disabled | Official `prom/statsd-exporter` semver image pinned by SHA-256; production web pods only |
| `monitoring.configMapName` | — | Existing immutable, content-addressed ConfigMap containing `statsd-mapping.yml` |

### Staging database sleep

Opt in with `spec.stagingSleep.databaseCluster`, matching `spec.database.cluster`.
The OdooInstance must carry `droggol.sh/instance-kind: staging`; both it and its
same-namespace, single-primary CNPG Cluster must carry matching nonempty
`droggol.sh/server-id` and `droggol.sh/instance-id` labels. This platform staging
identity is independent of Odoo's `spec.environment`; existing mail/neutralization
behavior is preserved.

An external HTTP scaler owns `spec.replicas` and the inactivity timeout. At zero,
the operator scales owned web/cron Deployments down, waits for every serving Pod
(including terminating Pods) to disappear, then sets CNPG's native
`cnpg.io/hibernation: on`. Only initialized normal runtime sleeps; native pending
jobs and migrations keep the database awake. Cron activity does not change the
HTTP scaler's decision. A positive replica target wakes CNPG and waits for its
Ready condition and a Ready primary Pod before PostgreSQL operations or Odoo startup.

Before external database maintenance, pause the HTTP scaler and set
`droggol.sh/staging-maintenance: <job-attempt-id>` on the OdooInstance. Presence of
this annotation wakes/keeps CNPG awake even at zero replicas. Wait for CNPG and its
primary Pod to be Ready before running the operation; clear the annotation only
after it finishes, then resume the scaler. Maintenance belongs to the platform,
not to cron traffic.

To disable/change this opt-in, pause the scaler, set replicas above zero and wait
for the instance to be Running and Ready, then remove/change `stagingSleep`.
Admission rejects removal/change while stopped. Kubernetes cannot atomically patch
the instance and CNPG; the operator rechecks intent around awaited work, uses
UID/resourceVersion preconditions, and immediately requests wake if demand races
hibernation. Instances without `stagingSleep` retain their existing behavior.

Set optional `spec.stagingSleep.mode: warm` to retain prepared web and PostgreSQL
while retiring only cron after five minutes without HTTP activity. Omitted mode
and explicit `hibernate` preserve the zero-Pod policy above. Warm mode requires
`workloadLayout: separate`; container resources and the CR's actual web replica
meaning are unchanged. The platform creates an exact OdooInstance-owned KEDA
ScaledObject named `<instance>-sleep` with min/max replicas both one. The operator
observes its Ready/Active conditions and `lastActiveTime`; before the first request,
its creation timestamp starts the initial cooldown. Unknown/error activity keeps
cron available. Cron startup never gates the retained web Service's readiness.

Warm web and cron use symmetric required same-node Pod affinity, preserving all
existing placement constraints. Native self-affinity permits the first Pod when
both are absent; replacements stay with a surviving peer. A failed peer node can
delay replacement until normal eviction. When migrating an existing combined
instance, the platform must author this affinity while stopped, start the separate
workloads and prove readiness, then change the sleep mode while holding maintenance.
Maintenance, true replicas zero, deletion and disruptive lifecycle states retain
their existing stop/readiness fences; HTTP activity cannot override them.

Install KEDA before starting the operator for warm mode. A missing ScaledObject
API disables only that optional watch at startup, keeping installations without
KEDA unchanged. Restart the operator after installing KEDA later. Other discovery
errors remain visible through the normal watch error path.

### Production HTTP monitoring

The hosting platform supplies the server-wide Odoo instrumentation addon and the
mapping ConfigMap before enabling `spec.monitoring`. The operator adds the local
Unix datagram exporter only to web pods. Cron and job pods remain unchanged.
The state directory `/run/droggol-monitoring` is a bounded 16Mi memory `emptyDir`,
shared only between Odoo and its exporter. The exporter requests 10m CPU / 32Mi RAM,
with limits of 100m / 128Mi and `GOMEMLIMIT=96MiB`; budget this additional pod usage.
Metrics port 9102 is not added to any Service or ingress. The hosting platform must
permit only its collector through network policy. It must also ensure the addon
enforces a hard endpoint cardinality bound; an exporter mapping cache is not a cap.

A shell supervisor restarts the exporter child after two seconds, removes stale
sockets before startup, and forwards termination. Exporter process failure does not
fail Odoo readiness. A whole-container OOM or image/config-volume startup failure
can still affect pod readiness; verify worst-case series memory and restart behavior
before rollout. No exporter readiness/liveness probe weakens Odoo's own probes.
Changing the content-addressed ConfigMap name triggers a pod rollout; removing
`monitoring` removes all exporter resources on reconciliation.

`droggol.sh/server-id`, `droggol.sh/project-id` and `droggol.sh/instance-id` labels
from the instance are copied to owned pods, leaving existing app selectors intact.
Run the real image lifecycle check with
`python3 scripts/tests/test-monitoring-exporter.py` (Docker required).

### Web/Cron Split

By default, each OdooInstance uses `workloadLayout: separate` and creates two
Deployments:

- **Web** (`<name>`) — runs with `--max-cron-threads=0`, serves HTTP traffic on
  ports 8069 and 8072 (websocket). Scaled by `spec.replicas`.
- **Cron** (`<name>-cron`) — runs with `--no-http`, processes scheduled actions
  only. Scaled by `spec.cron.replicas`.

This separation lets you scale web workers independently of cron processing. Cron
pods don't need an HTTP port, so they have no service or ingress routing. During an
upgrade, the cron Deployment is scaled to zero while the web Deployment keeps serving.

With `workloadLayout: combined`, the web and cron containers share the `<name>`
Deployment and Pod. `spec.replicas` scales the whole workload and
`spec.cron.replicas` must be `1`. Upgrades scale the combined Deployment to zero.

To change layouts, first set `spec.replicas: 0` and wait for
`status.phase: Stopped`. Keep replicas at zero while changing `workloadLayout`; the
validating webhook rejects any other transition. When moving to `combined`, the
operator scales the old cron Deployment to zero, deletes it, waits for both the
Deployment and its Pods to disappear, and only then updates the web Pod template.
Set `spec.replicas` to the desired value after the conversion finishes.

You don't need to set `workers` or `max_cron_threads` in `configOptions` — the
operator handles this automatically via the command-line flags on each deployment.

### Staging from production

Set `spec.productionInstanceRef` on a staging `OdooInstance` to declaratively
tie it to a source-of-truth production instance. On first reconcile, the
operator creates an `OdooStagingRefreshJob` (named `<instance>-auto-refresh`)
that clones the prod DB + filestore into the new instance and runs
`odoo neutralize` — in place of the normal `OdooInitJob` path:

```yaml
apiVersion: bemade.org/v1alpha1
kind: OdooInstance
metadata:
  name: client-staging
  namespace: client
spec:
  adminPassword: admin
  image: odoo:18.0
  ingress:
    hosts: [client-staging.example.com]
  filestore:
    storageSize: 50Gi
  environment: Staging
  productionInstanceRef:
    name: client-prod
```

Requirements and limits:

- Forbidden on `environment: Production` (rejected at `kubectl apply` time
  by a CRD CEL rule).
- Same-namespace only in v1. `productionInstanceRef.namespace` is reserved
  for a future cross-namespace phase.
- Auto-refresh fires only on first initialization (when
  `status.dbInitialized` is false). A user-created `OdooStagingRefreshJob`
  pre-empts the auto-create — useful for tuning `filestoreMethod` or
  `skipFilestore`, which the auto-create path leaves at CRD defaults
  (`Auto` / `false`).

### Gateway API Support

By default the operator creates a `networking.v1/Ingress` for each instance. If your
cluster uses Istio or another Gateway API implementation, set `ingress.gatewayRef` to
create an `HTTPRoute` instead:

```yaml
spec:
  ingress:
    hosts:
      - myodoo.example.com
    gatewayRef:
      name: my-gateway
      namespace: istio-system
```

TLS is not managed by the operator in Gateway API mode — configure it on your Gateway
resource (wildcard cert, cert-manager Gateway integration, etc.). The `issuer` field is
ignored when `gatewayRef` is set.

To make all instances use Gateway API by default, set the operator-level defaults:

```sh
helm upgrade odoo-operator ... \
  --set defaults.gatewayRefName=my-gateway \
  --set defaults.gatewayRefNamespace=istio-system
```

### Status conditions

The operator sets standard Kubernetes conditions on each OdooInstance:

| Condition | Description |
|---|---|
| `Ready` | `True` when the instance is in the `Running` phase |
| `Progressing` | `True` during transient phases (Provisioning, Initializing, Starting, Upgrading, Restoring, BackingUp) |

### Webhook validation

The validating webhook rejects:
- `spec.filestore.storageSize` decreases
- `spec.filestore.storageClass` changes after initial set
- `spec.database.cluster` changes after initial set

### Operator flags

| Flag | Default | Description |
|---|---|---|
| `--postgres-clusters-secret` | `postgres-clusters` | Secret name in operator namespace |
| `--default-odoo-image` | `odoo:18.0` | Image used when `spec.image` is unset |
| `--default-storage-class` | `standard` | StorageClass when `spec.filestore.storageClass` is unset |
| `--default-storage-size` | `2Gi` | PVC size when `spec.filestore.storageSize` is unset |
| `--default-ingress-class` | — | IngressClass when `spec.ingress.class` is unset |
| `--default-ingress-issuer` | — | ClusterIssuer when `spec.ingress.issuer` is unset |
| `--default-gateway-ref-name` | — | Gateway name; when both name and namespace are set, creates HTTPRoute instead of Ingress |
| `--default-gateway-ref-namespace` | — | Gateway namespace for the default HTTPRoute parentRef |
| `--default-resources` | — | JSON `ResourceRequirements` when `spec.resources` is unset |
| `--default-affinity` | — | JSON `Affinity` when `spec.affinity` is unset |
| `--default-tolerations` | — | JSON `[]Toleration` when `spec.tolerations` is unset |

## State Machine

The OdooInstance lifecycle is driven by a declarative state machine with transitions
defined in a static table in `src/controller/state_machine.rs`.

See [STATE_MACHINE.md](STATE_MACHINE.md) for the full diagram (auto-generated with `make state-machine`).

## Project Layout

| Directory | Contents |
|---|---|
| `src/crd/` | Custom Resource types (OdooInstance, OdooInitJob, etc.) |
| `src/controller/` | Reconciler, declarative state machine, child resource management |
| `src/controller/states/` | One file per OdooInstancePhase (12 states), each with an idempotent `ensure()` |
| `src/bin/` | CLI tools — CRD YAML generator, Mermaid state diagram generator |
| `scripts/` | Shell scripts embedded into backup/restore/init Jobs |
| `charts/odoo-operator/` | Helm chart |
| `tests/integration/` | envtest-based integration tests (17 tests, parallel via per-test namespaces) |
| `tests/` | Unit tests for helpers, job builder, and admission webhook |
| `testing/` | Local dev/test fixtures (pg-clusters secret, Helm overrides) |
| `.github/workflows/` | CI (lint + test on PRs) and release (image + Helm chart on tag push) |

## Development

```sh
# Run all tests (unit + integration)
cargo test

# Run integration tests only (requires envtest binaries)
cargo test --test integration

# Lint
cargo fmt --check && cargo clippy -- -D warnings

# Generate CRDs and sync to Helm chart
make helm-crds

# Build and deploy to minikube
make install
```

## License

LGPL-3.0-or-later — Copyright 2026 Marc Durepos, Bemade Inc.
