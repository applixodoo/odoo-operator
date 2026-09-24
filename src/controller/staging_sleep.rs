//! Opt-in staging sleep, before the main reconciler makes any PostgreSQL calls.
//!
//! The scaler owns spec.replicas. This gate only drains serving Deployments and
//! toggles CNPG's native hibernation annotation; it never deletes database Pods.
use std::time::Duration;

use chrono::{DateTime, Utc};
use k8s_openapi::api::{apps::v1::Deployment, core::v1::Pod};
use kube::api::{ApiResource, DynamicObject, ListParams, Patch, PatchParams};
use kube::core::GroupVersionKind;
use kube::runtime::controller::Action;
use kube::{Api, Client, ResourceExt};
use serde_json::json;

use super::child_resources::{deployment_controlled_by, scale_deployment_if_current};
use super::helpers::{controller_owner_ref, cron_depl_name};
use super::odoo_instance::phase_to_conditions;
use super::state_machine::{JobStatus, ReconcileSnapshot};
use crate::crd::odoo_instance::{
    OdooInstance, OdooInstancePhase, StagingSleepMode, WorkloadLayout,
};
use crate::error::{Error, Result};

pub const MAINTENANCE_ANNOTATION: &str = "droggol.sh/staging-maintenance";
const HIBERNATION: &str = "cnpg.io/hibernation";
const POLL: Duration = Duration::from_secs(2);
const IDENTITY_LABELS: [&str; 2] = ["droggol.sh/server-id", "droggol.sh/instance-id"];
const IDLE_SECONDS: i64 = 300;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WarmCron {
    pub replicas: i32,
    pub requeue_after: Option<Duration>,
}

pub fn scaler_resource() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind::gvk(
        "keda.sh",
        "v1alpha1",
        "ScaledObject",
    ))
}

pub fn is_warm(instance: &OdooInstance) -> bool {
    instance
        .spec
        .staging_sleep
        .as_ref()
        .is_some_and(|sleep| sleep.mode == Some(StagingSleepMode::Warm))
}

/// Read KEDA's activity without becoming another author of its scale target.
/// Missing/error evidence keeps cron available and is retried; only positively
/// inactive, exact-owned activity can stop it. Maintenance keeps native outputs.
pub async fn warm_cron(client: &Client, instance: &OdooInstance) -> Option<WarmCron> {
    if !is_warm(instance)
        || instance.spec.replicas <= 0
        || instance.annotations().contains_key(MAINTENANCE_ANNOTATION)
    {
        return None;
    }
    let api: Api<DynamicObject> = Api::namespaced_with(
        client.clone(),
        &instance.namespace().unwrap_or_default(),
        &scaler_resource(),
    );
    let scaler = api.get_opt(&format!("{}-sleep", instance.name_any())).await;
    Some(warm_cron_at(
        instance,
        scaler.ok().flatten().as_ref(),
        Utc::now(),
    ))
}

fn warm_cron_at(
    instance: &OdooInstance,
    scaler: Option<&DynamicObject>,
    now: DateTime<Utc>,
) -> WarmCron {
    let awake = WarmCron {
        replicas: instance.spec.cron.replicas,
        requeue_after: Some(Duration::from_secs(30)),
    };
    let Some(scaler) = scaler else { return awake };
    let owned = scaler.namespace() == instance.namespace()
        && scaler.name_any() == format!("{}-sleep", instance.name_any())
        && scaler.uid().is_some()
        && scaler.metadata.deletion_timestamp.is_none()
        && scaler.owner_references().iter().any(|owner| {
            owner.api_version == "bemade.org/v1alpha1"
                && owner.kind == "OdooInstance"
                && owner.name == instance.name_any()
                && Some(&owner.uid) == instance.metadata.uid.as_ref()
                && owner.controller == Some(true)
        })
        && IDENTITY_LABELS.iter().all(|key| {
            instance.labels().get(*key).is_some()
                && scaler.labels().get(*key) == instance.labels().get(*key)
        })
        && scaler
            .data
            .pointer("/spec/scaleTargetRef/apiVersion")
            .and_then(|v| v.as_str())
            == Some("bemade.org/v1alpha1")
        && scaler
            .data
            .pointer("/spec/scaleTargetRef/kind")
            .and_then(|v| v.as_str())
            == Some("OdooInstance")
        && scaler
            .data
            .pointer("/spec/scaleTargetRef/name")
            .and_then(|v| v.as_str())
            == Some(instance.name_any().as_str())
        && scaler
            .data
            .pointer("/spec/minReplicaCount")
            .and_then(|v| v.as_i64())
            == Some(1)
        && scaler
            .data
            .pointer("/spec/maxReplicaCount")
            .and_then(|v| v.as_i64())
            == Some(1)
        && scaler.data.pointer("/spec/fallback").is_none()
        && !scaler
            .annotations()
            .iter()
            .any(|(key, value)| key.starts_with("autoscaling.keda.sh/paused") && value != "false");
    let conditions = scaler
        .data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array());
    let condition = |kind: &str| {
        conditions
            .and_then(|cs| cs.iter().find(|c| c["type"] == kind))
            .and_then(|c| c["status"].as_str())
    };
    if !owned || condition("Ready") != Some("True") || condition("Paused") == Some("True") {
        return awake;
    }
    match condition("Active") {
        Some("True") => {
            return WarmCron {
                requeue_after: None,
                ..awake
            }
        }
        Some("False")
            if conditions.is_some_and(|cs| {
                cs.iter()
                    .any(|c| c["type"] == "Active" && c["reason"] == "ScalerNotActive")
            }) => {}
        _ => return awake,
    }
    let last_active = match scaler.data.pointer("/status/lastActiveTime") {
        Some(value) => value
            .as_str()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|time| time.with_timezone(&Utc)),
        // KEDA uses its creation timestamp for the initial cooldown before any
        // request has been observed. Known-inactive is distinct from unknown.
        None => scaler
            .metadata
            .creation_timestamp
            .as_ref()
            .map(|time| time.0),
    };
    let Some(last_active) = last_active.filter(|time| *time <= now) else {
        return awake;
    };
    let remaining = last_active + chrono::Duration::seconds(IDLE_SECONDS) - now;
    match remaining.to_std() {
        Ok(delay) if !delay.is_zero() => WarmCron {
            requeue_after: Some(delay),
            ..awake
        },
        _ => WarmCron {
            replicas: 0,
            requeue_after: None,
        },
    }
}

/// Warm activity is subordinate to the actual serving and lifecycle state.
pub fn cron_replicas(instance: &OdooInstance, snapshot: &ReconcileSnapshot) -> i32 {
    if !is_warm(instance) {
        return instance.spec.cron.replicas;
    }
    if !cron_can_run(instance, snapshot) {
        return 0;
    }
    snapshot
        .warm_cron
        .map_or(instance.spec.cron.replicas, |decision| decision.replicas)
}

pub fn cron_can_run(instance: &OdooInstance, snapshot: &ReconcileSnapshot) -> bool {
    !(instance.spec.replicas <= 0
        || instance.metadata.deletion_timestamp.is_some()
        || !snapshot.db_initialized
        || snapshot.init_job.is_present()
        || snapshot.restore_job.is_present()
        || snapshot.refresh_job.is_present()
        || snapshot.upgrade_job_ready()
        || snapshot.storage_class_mismatch
        || snapshot.cluster_mismatch
        || snapshot.migration_job.is_present()
        || snapshot.db_migration_job.is_present())
}

/// Warm cron has one writer: the operator. Recheck its parent across awaited
/// reads, then conditionally scale the exact child. Never adopt a replacement.
pub async fn scale_warm_cron(
    client: &Client,
    instance: &OdooInstance,
    replicas: i32,
    idle: bool,
) -> Result<()> {
    let ns = instance.namespace().unwrap_or_default();
    let api: Api<Deployment> = Api::namespaced(client.clone(), &ns);
    let deployment = api.get(&cron_depl_name(instance)).await?;
    if !deployment_controlled_by(&deployment, &controller_owner_ref(instance)) {
        return Err(Error::config(
            "refusing to scale foreign warm cron Deployment",
        ));
    }
    if deployment.spec.as_ref().and_then(|spec| spec.replicas) == Some(replicas) {
        return Ok(());
    }
    let fresh = current(client, instance)
        .await?
        .ok_or_else(|| Error::reconcile("warm cron authority changed"))?;
    if fresh
        .status
        .as_ref()
        .and_then(|status| status.phase.as_ref())
        != instance
            .status
            .as_ref()
            .and_then(|status| status.phase.as_ref())
    {
        return Err(Error::reconcile("warm cron lifecycle phase changed"));
    }
    if idle {
        // Traffic may have arrived while the main snapshot was gathered. A
        // fresh active/unknown observation supersedes that old idle decision.
        if warm_cron(client, &fresh)
            .await
            .is_some_and(|decision| decision.replicas != 0)
        {
            return Err(Error::reconcile("warm cron activity changed"));
        }
        if current(client, instance).await?.is_none() {
            return Err(Error::reconcile("warm cron authority changed"));
        }
    }
    scale_deployment_if_current(client, &ns, &deployment, replicas).await?;
    Ok(())
}

fn sleep_requested(instance: &OdooInstance) -> bool {
    instance.spec.replicas == 0
        && instance.metadata.deletion_timestamp.is_none()
        && !instance.annotations().contains_key(MAINTENANCE_ANNOTATION)
        && instance.status.as_ref().is_some_and(|s| {
            s.db_initialized
                && matches!(
                    s.phase,
                    Some(
                        OdooInstancePhase::Running
                            | OdooInstancePhase::Starting
                            | OdooInstancePhase::Degraded
                            | OdooInstancePhase::Stopped
                    )
                )
        })
}

/// Also checked at runtime: the webhook/CRD may not yet have been upgraded.
pub fn validate_spec(instance: &OdooInstance) -> Result<()> {
    if is_warm(instance) && instance.spec.workload_layout != WorkloadLayout::Separate {
        return Err(Error::config(
            "warm staging requires separate web and cron workloads",
        ));
    }
    if is_warm(instance)
        && IDENTITY_LABELS.iter().any(|key| {
            instance
                .labels()
                .get(*key)
                .is_none_or(|value| value.is_empty())
        })
    {
        return Err(Error::config(
            "warm staging requires server and instance identity labels",
        ));
    }
    if let Some(sleep) = &instance.spec.staging_sleep {
        if instance
            .labels()
            .get("droggol.sh/instance-kind")
            .map(String::as_str)
            != Some("staging")
            || sleep.database_cluster.is_empty()
            || instance
                .spec
                .database
                .as_ref()
                .and_then(|d| d.cluster.as_deref())
                != Some(sleep.database_cluster.as_str())
        {
            return Err(Error::config(
                "stagingSleep requires droggol.sh/instance-kind=staging and the same explicit database.cluster",
            ));
        }
    }
    Ok(())
}

fn validate_cluster(instance: &OdooInstance, cluster: &DynamicObject) -> Result<()> {
    if cluster
        .data
        .pointer("/spec/instances")
        .and_then(|v| v.as_i64())
        != Some(1)
        || cluster
            .data
            .pointer("/spec/replica/enabled")
            .and_then(|v| v.as_bool())
            == Some(true)
    {
        return Err(Error::config(
            "stagingSleep requires a single-primary CNPG cluster",
        ));
    }
    for key in IDENTITY_LABELS {
        if !instance
            .labels()
            .get(key)
            .is_some_and(|id| !id.is_empty() && cluster.labels().get(key) == Some(id))
        {
            return Err(Error::config(format!(
                "stagingSleep CNPG identity mismatch: {key}"
            )));
        }
    }
    if cluster.metadata.deletion_timestamp.is_some() {
        return Err(Error::config("stagingSleep CNPG cluster is being deleted"));
    }
    Ok(())
}

/// Re-read across awaited work. Generation fences spec, labels fence identity,
/// and annotation presence/value fences platform maintenance attempts.
async fn current(client: &Client, original: &OdooInstance) -> Result<Option<OdooInstance>> {
    let api: Api<OdooInstance> =
        Api::namespaced(client.clone(), &original.namespace().unwrap_or_default());
    let fresh = api.get(&original.name_any()).await?;
    Ok((fresh.uid() == original.uid()
        && fresh.metadata.generation == original.metadata.generation
        && fresh.labels() == original.labels()
        && fresh.annotations().get(MAINTENANCE_ANNOTATION)
            == original.annotations().get(MAINTENANCE_ANNOTATION)
        && sleep_requested(&fresh) == sleep_requested(original)
        && fresh.metadata.deletion_timestamp == original.metadata.deletion_timestamp)
        .then_some(fresh))
}

async fn set_hibernation(
    api: &Api<DynamicObject>,
    cluster: &DynamicObject,
    value: &str,
) -> Result<DynamicObject> {
    let uid = cluster
        .uid()
        .ok_or_else(|| Error::config("CNPG cluster has no UID"))?;
    let rv = cluster
        .resource_version()
        .ok_or_else(|| Error::config("CNPG cluster has no resourceVersion"))?;
    Ok(api
        .patch(
            &cluster.name_any(),
            &PatchParams::default(),
            &Patch::Merge(&json!({
                "metadata": {"uid": uid, "resourceVersion": rv, "annotations": {HIBERNATION: value}}
            })),
        )
        .await?)
}

fn cnpg_ready(cluster: &DynamicObject) -> bool {
    let conditions = cluster
        .data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array());
    cluster
        .data
        .pointer("/status/readyInstances")
        .and_then(|v| v.as_i64())
        == Some(1)
        && conditions.is_some_and(|cs| {
            cs.iter()
                .any(|c| c["type"] == "Ready" && c["status"] == "True")
                && !cs
                    .iter()
                    .any(|c| c["type"] == HIBERNATION && c["status"] == "True")
        })
}

/// `Some` holds reconciliation at this gate; `None` permits normal runtime.
/// Instances without the opt-in make no additional API calls.
pub async fn reconcile(client: &Client, instance: &OdooInstance) -> Result<Option<Action>> {
    let Some(sleep) = &instance.spec.staging_sleep else {
        return Ok(None);
    };
    validate_spec(instance)?;
    let ns = instance.namespace().unwrap_or_default();
    let ar = ApiResource::from_gvk(&GroupVersionKind::gvk(
        "postgresql.cnpg.io",
        "v1",
        "Cluster",
    ));
    let clusters: Api<DynamicObject> = Api::namespaced_with(client.clone(), &ns, &ar);
    let cluster = clusters.get(&sleep.database_cluster).await?;
    validate_cluster(instance, &cluster)?;
    if current(client, instance).await?.is_none() {
        return Ok(Some(Action::requeue(POLL)));
    }

    let mut should_sleep = sleep_requested(instance);
    if should_sleep {
        // A pending native job or migration must wake a stopped instance too.
        let snap = ReconcileSnapshot::gather(
            client,
            &ns,
            &instance.name_any(),
            instance,
            &sleep.database_cluster,
        )
        .await?;
        should_sleep = [
            snap.init_job,
            snap.restore_job,
            snap.upgrade_job,
            snap.backup_job,
            snap.refresh_job,
        ]
        .iter()
        .all(|job| *job == JobStatus::Absent)
            && !snap.storage_class_mismatch
            && !snap.cluster_mismatch;
    }
    if !should_sleep {
        if cluster
            .annotations()
            .get(HIBERNATION)
            .is_some_and(|v| v == "on")
        {
            if current(client, instance).await?.is_some() {
                set_hibernation(&clusters, &cluster, "off").await?;
            }
            // Never trust readiness from before the wake patch.
            return Ok(Some(Action::requeue(POLL)));
        }
        if !cnpg_ready(&cluster) {
            return Ok(Some(Action::requeue(POLL)));
        }
        // CNPG conditions can lag annotation changes. Require a live Ready
        // primary Pod belonging to this Cluster before permitting SQL calls.
        let Some(primary) = cluster
            .data
            .pointer("/status/currentPrimary")
            .and_then(|v| v.as_str())
        else {
            return Ok(Some(Action::requeue(POLL)));
        };
        let pods: Api<Pod> = Api::namespaced(client.clone(), &ns);
        let ready = pods.get_opt(primary).await?.is_some_and(|pod| {
            pod.metadata.deletion_timestamp.is_none()
                && pod.owner_references().iter().any(|o| {
                    o.uid == cluster.uid().unwrap_or_default() && o.controller == Some(true)
                })
                && pod
                    .status
                    .as_ref()
                    .and_then(|s| s.conditions.as_ref())
                    .is_some_and(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        });
        return Ok(if ready && current(client, instance).await?.is_some() {
            None
        } else {
            Some(Action::requeue(POLL))
        });
    }

    let deployments: Api<Deployment> = Api::namespaced(client.clone(), &ns);
    let owner = controller_owner_ref(instance);
    let mut draining = false;
    // Include a residual cron Deployment during a separate→combined transition.
    for name in [instance.name_any(), cron_depl_name(instance)] {
        if let Some(deployment) = deployments.get_opt(&name).await? {
            if !deployment_controlled_by(&deployment, &owner) {
                return Err(Error::config(format!(
                    "refusing to sleep foreign Deployment {ns}/{name}"
                )));
            }
            if deployment.spec.as_ref().and_then(|s| s.replicas) != Some(0) {
                if current(client, instance).await?.is_none() {
                    return Ok(Some(Action::requeue(POLL)));
                }
                scale_deployment_if_current(client, &ns, &deployment, 0).await?;
                draining = true;
            } else if !deployment.status.as_ref().is_some_and(|status| {
                status.observed_generation.is_some_and(|observed| {
                    observed >= deployment.metadata.generation.unwrap_or(i64::MAX)
                }) && status.replicas.unwrap_or(0) == 0
                    && status.ready_replicas.unwrap_or(0) == 0
            }) {
                draining = true;
            }
        }
    }
    if draining {
        // Wait until Deployment controllers have observed zero, so an empty
        // Pod list cannot predate application of the scale-down to ReplicaSets.
        return Ok(Some(Action::requeue(POLL)));
    }
    let pods: Api<Pod> = Api::namespaced(client.clone(), &ns);
    // Includes terminating Pods. Broad selector is intentionally conservative:
    // a colliding foreign Pod blocks sleep rather than risking a live DB client.
    if !pods
        .list(&ListParams::default().labels(&format!(
            "app in ({},{})",
            instance.name_any(),
            cron_depl_name(instance)
        )))
        .await?
        .items
        .is_empty()
    {
        return Ok(Some(Action::requeue(POLL)));
    }
    let Some(fresh) = current(client, instance).await? else {
        return Ok(Some(Action::requeue(POLL)));
    };
    let cluster = if cluster
        .annotations()
        .get(HIBERNATION)
        .is_some_and(|v| v == "on")
    {
        cluster
    } else {
        set_hibernation(&clusters, &cluster, "on").await?
    };
    // Kubernetes cannot atomically patch two objects. If a wake/maintenance
    // request raced the conditional Cluster patch, immediately undo hibernation.
    if current(client, instance).await?.is_none() {
        set_hibernation(&clusters, &cluster, "off").await?;
        return Ok(Some(Action::requeue(POLL)));
    }
    if fresh.status.as_ref().is_some_and(|s| {
        s.phase != Some(OdooInstancePhase::Stopped) || s.ready || s.ready_replicas != 0
    }) {
        let api: Api<OdooInstance> = Api::namespaced(client.clone(), &ns);
        api.patch_status(&instance.name_any(), &PatchParams::default(), &Patch::Merge(&json!({
            "metadata": {"uid": fresh.uid(), "resourceVersion": fresh.resource_version()},
            "status": {"phase": "Stopped", "ready": false, "readyReplicas": 0, "targetReplicas": 0,
                "conditions": phase_to_conditions(&OdooInstancePhase::Stopped, fresh.metadata.generation.unwrap_or(0))}
        }))).await?;
    }
    // Poll only until CNPG confirms hibernation. The OdooInstance and job CR
    // watches wake this gate on HTTP demand or maintenance/native work.
    let hibernated = cluster
        .data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .is_some_and(|cs| {
            cs.iter()
                .any(|c| c["type"] == HIBERNATION && c["status"] == "True")
        });
    Ok(Some(if hibernated {
        Action::await_change()
    } else {
        Action::requeue(POLL)
    }))
}

#[cfg(test)]
mod warm_tests {
    use super::*;
    use http::{Method, Request, Response};
    use kube::client::Body;
    use serde_json::Value;
    use std::sync::{Arc, Mutex};
    use tower_test::mock;

    fn instance() -> OdooInstance {
        serde_json::from_value(json!({
            "apiVersion": "bemade.org/v1alpha1", "kind": "OdooInstance",
            "metadata": {"name": "odoo", "namespace": "tenant", "uid": "instance-uid",
                "labels": {"droggol.sh/instance-kind": "staging", "droggol.sh/server-id": "server", "droggol.sh/instance-id": "instance"}},
            "spec": {"adminPassword": "test", "ingress": {"hosts": ["example.test"]}, "replicas": 1,
                "database": {"cluster": "odoo-db"}, "stagingSleep": {"databaseCluster": "odoo-db", "mode": "warm"}},
            "status": {"phase": "Running", "dbInitialized": true, "readyReplicas": 1}
        })).unwrap()
    }

    fn scaler() -> Value {
        json!({
            "apiVersion": "keda.sh/v1alpha1", "kind": "ScaledObject",
            "metadata": {"name": "odoo-sleep", "namespace": "tenant", "uid": "scaler-uid",
                "creationTimestamp": "2026-09-24T10:00:00Z",
                "labels": {"droggol.sh/server-id": "server", "droggol.sh/instance-id": "instance"},
                "ownerReferences": [{"apiVersion": "bemade.org/v1alpha1", "kind": "OdooInstance", "name": "odoo", "uid": "instance-uid", "controller": true}]},
            "spec": {"scaleTargetRef": {"apiVersion": "bemade.org/v1alpha1", "kind": "OdooInstance", "name": "odoo"}, "minReplicaCount": 1, "maxReplicaCount": 1},
            "status": {"lastActiveTime": "2026-09-24T10:00:00Z", "conditions": [
                {"type": "Ready", "status": "True"},
                {"type": "Active", "status": "False", "reason": "ScalerNotActive"}]}
        })
    }

    fn decision(value: Value, seconds: i64) -> WarmCron {
        let now = DateTime::parse_from_rfc3339("2026-09-24T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
            + chrono::Duration::seconds(seconds);
        warm_cron_at(
            &instance(),
            Some(&serde_json::from_value(value).unwrap()),
            now,
        )
    }

    #[test]
    fn warm_inactivity_uses_native_deadline_including_first_request_fallback() {
        assert_eq!(
            decision(scaler(), 299),
            WarmCron {
                replicas: 1,
                requeue_after: Some(Duration::from_secs(1))
            }
        );
        assert_eq!(
            decision(scaler(), 300),
            WarmCron {
                replicas: 0,
                requeue_after: None
            }
        );
        let mut never_active = scaler();
        never_active["status"]
            .as_object_mut()
            .unwrap()
            .remove("lastActiveTime");
        assert_eq!(decision(never_active.clone(), 299).replicas, 1);
        assert_eq!(decision(never_active, 300).replicas, 0);
        let mut active = scaler();
        active["status"]["conditions"][1]["status"] = json!("True");
        assert_eq!(
            decision(active, 600),
            WarmCron {
                replicas: 1,
                requeue_after: None
            }
        );
    }

    #[test]
    fn warm_unknown_error_or_unowned_activity_never_stops_cron() {
        for (pointer, replacement) in [
            ("/metadata/ownerReferences/0/uid", json!("replacement")),
            ("/metadata/labels/droggol.sh~1instance-id", json!("other")),
            ("/metadata/namespace", json!("other")),
            ("/spec/scaleTargetRef/name", json!("other")),
            ("/spec/minReplicaCount", json!(0)),
            ("/status/conditions/0/status", json!("False")),
            ("/status/conditions/0/status", json!("Unknown")),
            ("/status/conditions/1/status", json!("Unknown")),
            ("/status/conditions/1/reason", json!("ScalerError")),
            ("/status/lastActiveTime", json!("invalid")),
            ("/status/lastActiveTime", Value::Null),
            ("/status/lastActiveTime", json!("2026-09-24T10:20:00Z")),
        ] {
            let mut value = scaler();
            *value.pointer_mut(pointer).unwrap() = replacement;
            assert_eq!(decision(value, 600).replicas, 1, "{pointer}");
        }
        for extra in ["paused", "deleted", "fallback", "missing_conditions"] {
            let mut value = scaler();
            match extra {
                "paused" => {
                    value["metadata"]["annotations"] = json!({"autoscaling.keda.sh/paused": "true"})
                }
                "deleted" => value["metadata"]["deletionTimestamp"] = json!("2026-09-24T10:01:00Z"),
                "fallback" => value["spec"]["fallback"] = json!({"replicas": 1}),
                _ => {
                    value["status"]
                        .as_object_mut()
                        .unwrap()
                        .remove("conditions");
                }
            }
            assert_eq!(decision(value, 600).replicas, 1, "{extra}");
        }
        assert_eq!(warm_cron_at(&instance(), None, Utc::now()).replicas, 1);
    }

    #[test]
    fn warm_mode_requires_separate_layout_and_legacy_wire_shape_is_unchanged() {
        let mut cr = instance();
        assert!(validate_spec(&cr).is_ok());
        cr.spec.workload_layout = WorkloadLayout::Combined;
        assert!(validate_spec(&cr).is_err());
        let legacy: crate::crd::odoo_instance::StagingSleepSpec =
            serde_json::from_value(json!({"databaseCluster": "db"})).unwrap();
        assert_eq!(
            serde_json::to_value(legacy).unwrap(),
            json!({"databaseCluster": "db"})
        );
        assert!(
            serde_json::from_value::<crate::crd::odoo_instance::StagingSleepSpec>(
                json!({"databaseCluster": "db", "mode": "paused"})
            )
            .is_err()
        );
    }

    fn snapshot() -> ReconcileSnapshot {
        ReconcileSnapshot {
            ready_replicas: 1,
            deployment_replicas: 1,
            cron_ready_replicas: 0,
            cron_deployment_replicas: 0,
            warm_cron: Some(WarmCron {
                replicas: 0,
                requeue_after: None,
            }),
            db_initialized: true,
            init_job: JobStatus::Absent,
            restore_job: JobStatus::Absent,
            upgrade_job: JobStatus::Absent,
            backup_job: JobStatus::Absent,
            refresh_job: JobStatus::Absent,
            active_init_job: None,
            active_restore_job: None,
            active_upgrade_job: None,
            active_backup_job: None,
            active_refresh_job: None,
            pending_backup_jobs: 0,
            storage_class_mismatch: false,
            actual_storage_class: None,
            migration_job: JobStatus::Absent,
            cluster_mismatch: false,
            db_migration_job: JobStatus::Absent,
            stuck_mount_pods: vec![],
        }
    }

    #[test]
    fn warm_activity_cannot_override_true_stop_or_pending_lifecycle_work() {
        let mut cr = instance();
        let mut snap = snapshot();
        assert_eq!(cron_replicas(&cr, &snap), 0);
        snap.warm_cron = None; // maintenance ignores idle; native readiness still needs cron.
        assert_eq!(cron_replicas(&cr, &snap), 1);
        cr.spec.replicas = 0;
        assert_eq!(cron_replicas(&cr, &snap), 0);
        cr.spec.replicas = 1;
        for job in [
            "init", "restore", "refresh", "upgrade", "storage", "database",
        ] {
            let mut fenced = snapshot();
            fenced.warm_cron = None;
            match job {
                "init" => fenced.init_job = JobStatus::Active,
                "restore" => fenced.restore_job = JobStatus::Active,
                "refresh" => fenced.refresh_job = JobStatus::Active,
                "upgrade" => fenced.upgrade_job = JobStatus::Active,
                "storage" => fenced.storage_class_mismatch = true,
                _ => fenced.cluster_mismatch = true,
            }
            assert_eq!(cron_replicas(&cr, &fenced), 0, "{job}");
        }
        // The existing Running transition depends on the web replica count;
        // a cold/absent separate cron does not alter web readiness.
        let transition = super::super::state_machine::TRANSITIONS
            .iter()
            .find(|t| t.from == OdooInstancePhase::Starting && t.to == OdooInstancePhase::Running)
            .unwrap();
        assert!((transition.guard)(&cr, &snapshot()));
    }

    #[test]
    fn warm_deadline_requeues_without_overriding_faster_lifecycle_timers() {
        let mut snap = snapshot();
        snap.warm_cron.as_mut().unwrap().requeue_after = Some(Duration::from_secs(20));
        assert_eq!(
            super::super::state_machine::requeue_for(&OdooInstancePhase::Running, &snap, true),
            Action::requeue(Duration::from_secs(20))
        );
        assert_eq!(
            super::super::state_machine::requeue_for(&OdooInstancePhase::Starting, &snap, true),
            Action::requeue(Duration::from_secs(10))
        );
        snap.warm_cron.as_mut().unwrap().requeue_after = None;
        assert_eq!(
            super::super::state_machine::requeue_for(&OdooInstancePhase::Running, &snap, false),
            Action::await_change()
        );
    }

    #[tokio::test]
    async fn warm_idle_scale_rechecks_activity_and_maintenance_before_exact_child_patch() {
        for scenario in ["idle", "activity", "maintenance", "phase", "owner"] {
            let original = instance();
            let input = original.clone();
            let patches = Arc::new(Mutex::new(Vec::new()));
            let recorded = patches.clone();
            let (service, mut handle) = mock::pair::<Request<Body>, Response<Body>>();
            let responder = tokio::spawn(async move {
                let mut cr_reads = 0;
                while let Some((request, send)) = handle.next_request().await {
                    let method = request.method().clone();
                    let path = request.uri().path().to_string();
                    let response = if path.ends_with("/deployments/odoo-cron") {
                        if method == Method::PATCH {
                            let body: Value = serde_json::from_slice(
                                &request.into_body().collect_bytes().await.unwrap(),
                            )
                            .unwrap();
                            recorded.lock().unwrap().push(body.clone());
                            assert_eq!(
                                body,
                                json!({"metadata": {"uid": "cron-uid", "resourceVersion": "30"}, "spec": {"replicas": 0}})
                            );
                        }
                        json!({"apiVersion": "apps/v1", "kind": "Deployment",
                            "metadata": {"name": "odoo-cron", "namespace": "tenant", "uid": "cron-uid", "resourceVersion": "30",
                                "ownerReferences": [{"apiVersion": "bemade.org/v1alpha1", "kind": "OdooInstance", "name": "odoo",
                                    "uid": if scenario == "owner" { "other" } else { "instance-uid" }, "controller": true}]},
                            "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "odoo-cron"}}, "template": {"spec": {"containers": []}}}})
                    } else if path.ends_with("/odooinstances/odoo") {
                        assert_eq!(method, Method::GET);
                        cr_reads += 1;
                        let mut current = serde_json::to_value(&input).unwrap();
                        if scenario == "maintenance" && cr_reads == 2 {
                            current["metadata"]["annotations"] =
                                json!({MAINTENANCE_ANNOTATION: "new-job/1"});
                        }
                        if scenario == "phase" {
                            current["status"]["phase"] = json!("Restoring");
                        }
                        current
                    } else {
                        assert!(path.ends_with("/scaledobjects/odoo-sleep"), "{path}");
                        assert_eq!(method, Method::GET);
                        let mut value = scaler();
                        value["status"]["lastActiveTime"] =
                            json!((Utc::now() - chrono::Duration::seconds(600)).to_rfc3339());
                        if scenario == "activity" {
                            value["status"]["conditions"][1]["status"] = json!("True");
                        }
                        value
                    };
                    send.send_response(
                        Response::builder()
                            .body(Body::from(serde_json::to_vec(&response).unwrap()))
                            .unwrap(),
                    );
                }
            });
            let client = Client::new(service, "tenant");
            let result = scale_warm_cron(&client, &original, 0, true).await;
            drop(client);
            responder.await.unwrap();
            assert_eq!(result.is_ok(), scenario == "idle", "{scenario}");
            assert_eq!(
                patches.lock().unwrap().len(),
                usize::from(scenario == "idle"),
                "{scenario}"
            );
        }
    }

    #[tokio::test]
    async fn warm_maintenance_uses_native_cron_without_reading_keda() {
        let mut cr = instance();
        cr.metadata.annotations = Some(std::collections::BTreeMap::from([(
            MAINTENANCE_ANNOTATION.to_string(),
            "job/1".to_string(),
        )]));
        let (service, _handle) = mock::pair::<Request<Body>, Response<Body>>();
        let client = Client::new(service, "tenant");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), warm_cron(&client, &cr))
                .await
                .unwrap()
                .is_none()
        );
    }
}
