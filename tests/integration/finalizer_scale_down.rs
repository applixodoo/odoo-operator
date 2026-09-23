//! Regression test for the running-instance cleanup deadlock (issue #153).
//!
//! Deleting a running OdooInstance used to hang forever: `cleanup_instance`
//! went straight for `DROP DATABASE` while the web/cron pods still held live
//! connections, so the drop failed with "database is being accessed by other
//! users" and the finalizer was retained indefinitely.  The pods only stop
//! once the owner is fully deleted (past the finalizer), but the finalizer
//! can't complete until the pods stop — a genuine deadlock.
//!
//! The fix scales both Deployments to 0 as the first step of cleanup.  This
//! test asserts that scale-down happens when a Running instance is deleted.

use k8s_openapi::api::apps::v1::Deployment;
use kube::api::{Api, DeleteParams, PostParams};
use serde_json::json;

use super::common::*;
use odoo_operator::crd::odoo_instance::OdooInstance;
use odoo_operator::helpers::odoo_username;

const FINALIZER: &str = "bemade.org/postgres-cleanup";

/// Deleting a Running instance must scale the web and cron Deployments to 0
/// during cleanup.  We arm the postgres mock to fail `delete_role` so the
/// finalizer is retained and the (still-present) Deployments can be inspected;
/// the scale-down must have happened regardless, since it precedes the drop.
#[tokio::test]
async fn cleanup_scales_deployments_down_on_delete() -> anyhow::Result<()> {
    let ctx = TestContext::new("test-cleanup-scaledown").await;
    let (c, ns) = (&ctx.client, ctx.ns.as_str());
    let name = "test-cleanup-scaledown";
    let cron = format!("{name}-cron");

    // Bring the instance up to Running with both Deployments scaled up.
    let ready_handle = fast_track_to_running(&ctx, "init-cleanup-scaledown").await;
    check_deployment_scale(c, ns, name, 1).await?;

    // Arm the mock so cleanup's DROP-side (delete_role) keeps failing.  This
    // holds the finalizer open so we can observe the deployment scale after
    // the delete, mirroring the real world where a live connection blocks the
    // drop until the pods actually terminate.
    let username = odoo_username(ns, name);
    mock_pg().fail_delete_role(&username, "database still has sessions (test injection)");

    // The keep-alive task patches Deployment *status* only, not spec.replicas,
    // so it won't fight the scale-down; abort it anyway to keep teardown clean.
    ready_handle.abort();

    let instances: Api<OdooInstance> = Api::namespaced(c.clone(), ns);
    instances
        .delete(name, &DeleteParams::default())
        .await
        .expect("failed to issue delete");

    // Cleanup must scale both Deployments to 0 before (attempting) the drop.
    assert!(
        wait_for(TIMEOUT, POLL, || {
            let (c, ns, name, cron) = (c.clone(), ns.to_string(), name.to_string(), cron.clone());
            async move {
                check_deployment_scale(&c, &ns, &name, 0).await.is_ok()
                    && check_deployment_scale(&c, &ns, &cron, 0).await.is_ok()
            }
        })
        .await,
        "web and cron Deployments should be scaled to 0 during cleanup of a running instance"
    );

    // The finalizer is still held (delete_role is failing), so the instance
    // must still exist — proving the scale-down happened independently of the
    // drop succeeding, not as a side effect of GC after full deletion.
    let inst = instances
        .get_opt(name)
        .await?
        .expect("instance should still exist while delete_role fails");
    let finalizers = inst.metadata.finalizers.unwrap_or_default();
    assert!(
        finalizers.iter().any(|f| f == FINALIZER),
        "postgres-cleanup finalizer should still be held, got: {finalizers:?}"
    );

    // Clear the fault so the instance can finish cleaning up on teardown.
    mock_pg().clear_delete_role_failure(&username);

    Ok(())
}

#[tokio::test]
async fn cleanup_scales_combined_deployment_without_touching_foreign_cron() -> anyhow::Result<()> {
    let ctx = TestContext::new_ns().await;
    let (client, ns) = (&ctx.client, ctx.ns.as_str());
    let name = "test-combined-cleanup";
    let cron_name = format!("{name}-cron");
    let instances: Api<OdooInstance> = Api::namespaced(client.clone(), ns);
    let mut instance = test_instance_json(name, ns, 1);
    instance["spec"]["workloadLayout"] = json!("combined");
    instances
        .create(&PostParams::default(), &serde_json::from_value(instance)?)
        .await?;

    let ready_handle = fast_track_to_running(&ctx, "init-combined-cleanup").await;
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), ns);
    let foreign_cron: Deployment = serde_json::from_value(json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": cron_name, "namespace": ns},
        "spec": {
            "replicas": 3,
            "selector": {"matchLabels": {"app": cron_name}},
            "template": {
                "metadata": {"labels": {"app": cron_name}},
                "spec": {"containers": [{"name": "foreign", "image": "busybox"}]}
            }
        }
    }))?;
    deployments
        .create(&PostParams::default(), &foreign_cron)
        .await?;

    let username = odoo_username(ns, name);
    mock_pg().fail_delete_role(&username, "database still has sessions (test injection)");
    ready_handle.abort();
    instances.delete(name, &DeleteParams::default()).await?;

    assert!(
        wait_for(TIMEOUT, POLL, || async {
            check_deployment_scale(client, ns, name, 0).await.is_ok()
                && check_deployment_scale(client, ns, &cron_name, 3)
                    .await
                    .is_ok()
        })
        .await
    );
    let instance = instances
        .get_opt(name)
        .await?
        .expect("instance should still exist while delete_role fails");
    assert!(instance
        .metadata
        .finalizers
        .unwrap_or_default()
        .iter()
        .any(|finalizer| finalizer == FINALIZER));

    mock_pg().clear_delete_role_failure(&username);
    Ok(())
}
