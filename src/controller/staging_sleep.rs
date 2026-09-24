//! Opt-in staging sleep, before the main reconciler makes any PostgreSQL calls.
//!
//! The scaler owns spec.replicas. This gate only drains serving Deployments and
//! toggles CNPG's native hibernation annotation; it never deletes database Pods.
use std::time::Duration;

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
use crate::crd::odoo_instance::{OdooInstance, OdooInstancePhase};
use crate::error::{Error, Result};

pub const MAINTENANCE_ANNOTATION: &str = "droggol.sh/staging-maintenance";
const HIBERNATION: &str = "cnpg.io/hibernation";
const POLL: Duration = Duration::from_secs(2);
const IDENTITY_LABELS: [&str; 2] = ["droggol.sh/server-id", "droggol.sh/instance-id"];

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
