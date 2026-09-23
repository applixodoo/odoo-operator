use std::collections::HashSet;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Service;
use kube::api::{Api, Patch, PatchParams, PostParams};
use serde_json::json;

use super::common::*;
use odoo_operator::controller::child_resources::retire_owned_cron_deployment;
use odoo_operator::controller::helpers::controller_owner_ref;
use odoo_operator::crd::odoo_instance::{OdooInstance, OdooInstancePhase};
use odoo_operator::crd::odoo_upgrade_job::OdooUpgradeJob;

#[tokio::test]
async fn combined_layout_converts_runs_upgrades_and_rolls_back() -> anyhow::Result<()> {
    let name = "test-combined-layout";
    let cron_name = format!("{name}-cron");
    let ctx = TestContext::new(name).await;
    let (client, ns) = (&ctx.client, ctx.ns.as_str());
    let ready = fast_track_to_running(&ctx, "test-combined-layout-init").await;
    ready.abort();

    patch_instance_spec(client, ns, name, json!({"replicas": 0})).await;
    assert!(wait_for_phase(client, ns, name, OdooInstancePhase::Stopped).await);
    check_deployment_scale(client, ns, name, 0).await?;
    check_deployment_scale(client, ns, &cron_name, 0).await?;

    let web_resources = json!({
        "requests": {"cpu": "300m", "memory": "768Mi"},
        "limits": {"cpu": "1200m", "memory": "1536Mi"},
    });
    let cron_resources = json!({
        "requests": {"cpu": "75m", "memory": "384Mi"},
        "limits": {"cpu": "350m", "memory": "768Mi"},
    });
    patch_instance_spec(
        client,
        ns,
        name,
        json!({
            "workloadLayout": "combined",
            "environment": "Production",
            "resources": web_resources,
            "cron": {"replicas": 1, "resources": cron_resources},
            "sourceVolume": {
                "claimName": "source-artifacts",
                "mounts": [{"mountPath": "/source-artifacts", "readOnly": true}],
                "odooBin": "/work/current/odoo/odoo-bin"
            },
            "customSourceVolume": {
                "claimName": "custom-source",
                "mounts": [{"mountPath": "/custom", "readOnly": true}]
            },
            "monitoring": {
                "exporterImage": format!("prom/statsd-exporter:v0.29.0@sha256:{}", "a".repeat(64)),
                "configMapName": "monitoring-config-abcd"
            },
            "runAsUser": 1000,
            "runAsGroup": 1000
        }),
    )
    .await;

    let deployments: Api<Deployment> = Api::namespaced(client.clone(), ns);
    assert!(
        wait_for(TIMEOUT, POLL, || {
            let deployments = deployments.clone();
            let cron_name = cron_name.clone();
            async move {
                deployments
                    .get(&cron_name)
                    .await
                    .ok()
                    .is_some_and(|deployment| {
                        deployment.metadata.deletion_timestamp.is_some()
                            && deployment.spec.and_then(|spec| spec.replicas) == Some(0)
                    })
            }
        })
        .await
    );
    deployments
        .patch(
            &cron_name,
            &PatchParams::default(),
            &Patch::Merge(&json!({"metadata": {"finalizers": []}})),
        )
        .await?;
    assert!(
        wait_for(TIMEOUT, POLL, || {
            let deployments = deployments.clone();
            let cron_name = cron_name.clone();
            async move {
                deployments.get(&cron_name).await.is_err()
                    && deployments
                        .get(name)
                        .await
                        .ok()
                        .and_then(|deployment| deployment.spec)
                        .and_then(|spec| spec.template.spec)
                        .is_some_and(|pod| {
                            pod.containers
                                .iter()
                                .map(|container| container.name.as_str())
                                .collect::<Vec<_>>()
                                == [
                                    "odoo-test-combined-layout",
                                    "odoo-cron-test-combined-layout",
                                    "odoo-statsd-exporter",
                                ]
                        })
            }
        })
        .await
    );

    let deployment = deployments.get(name).await?.spec.unwrap();
    assert_eq!(
        deployment.selector.match_labels.as_ref().unwrap()["app"],
        name
    );
    let pod = deployment.template.spec.unwrap();
    let volume_names: HashSet<_> = pod
        .volumes
        .as_ref()
        .unwrap()
        .iter()
        .map(|v| &v.name)
        .collect();
    assert_eq!(volume_names.len(), pod.volumes.as_ref().unwrap().len());
    assert_eq!(
        pod.security_context.as_ref().unwrap().run_as_user,
        Some(1000)
    );
    assert_eq!(
        pod.security_context.as_ref().unwrap().run_as_group,
        Some(1000)
    );

    let web = &pod.containers[0];
    let cron = &pod.containers[1];
    assert_eq!(web.name, format!("odoo-{name}"));
    assert_eq!(cron.name, format!("odoo-cron-{name}"));
    assert!(web
        .command
        .as_ref()
        .unwrap()
        .ends_with(&["--max-cron-threads".to_string(), "0".to_string(),]));
    assert!(cron.command.as_ref().unwrap().ends_with(&[
        "--workers".to_string(),
        "0".to_string(),
        "--no-http".to_string(),
    ]));
    assert_eq!(
        serde_json::to_value(web.resources.as_ref().unwrap())?,
        web_resources
    );
    assert_eq!(
        serde_json::to_value(cron.resources.as_ref().unwrap())?,
        cron_resources
    );
    assert!(cron.readiness_probe.is_none());
    assert_eq!(
        cron.startup_probe
            .as_ref()
            .unwrap()
            .exec
            .as_ref()
            .unwrap()
            .command
            .as_ref()
            .unwrap(),
        &[
            "/usr/bin/python3",
            "-I",
            "-S",
            "/usr/local/bin/dsh-cron-probe",
            "startup"
        ]
    );
    assert!(web
        .env
        .as_ref()
        .unwrap()
        .iter()
        .any(|env| env.name == "DSH_MONITORING_ENABLED"));
    assert!(!cron
        .env
        .as_ref()
        .unwrap()
        .iter()
        .any(|env| env.name.starts_with("DSH_MONITORING_")));
    for container in [web, cron] {
        let mounts = container.volume_mounts.as_ref().unwrap();
        assert!(mounts.iter().any(|mount| {
            mount.name == "odoo-source"
                && mount.mount_path == "/source-artifacts"
                && mount.read_only == Some(true)
        }));
        assert!(mounts.iter().any(|mount| {
            mount.name == "odoo-custom-source"
                && mount.mount_path == "/custom"
                && mount.read_only == Some(true)
        }));
        assert!(!mounts.iter().any(|mount| mount.mount_path == "/work"));
    }

    let services: Api<Service> = Api::namespaced(client.clone(), ns);
    assert_eq!(
        services.get(name).await?.spec.unwrap().selector.unwrap()["app"],
        name
    );

    patch_instance_spec(client, ns, name, json!({"replicas": 1})).await;
    assert!(
        wait_for(TIMEOUT, POLL, || async {
            check_deployment_scale(client, ns, name, 1).await.is_ok()
        })
        .await
    );
    fake_deployment_ready(client, ns, name, 1).await;
    assert!(wait_for_phase(client, ns, name, OdooInstancePhase::Running).await);
    fake_deployment_ready(client, ns, name, 0).await;

    let upgrades: Api<OdooUpgradeJob> = Api::namespaced(client.clone(), ns);
    let upgrade: OdooUpgradeJob = serde_json::from_value(json!({
        "apiVersion": "bemade.org/v1alpha1",
        "kind": "OdooUpgradeJob",
        "metadata": {"name": "test-combined-layout-upgrade", "namespace": ns},
        "spec": {"odooInstanceRef": {"name": name}, "modules": ["base"]}
    }))?;
    upgrades.create(&PostParams::default(), &upgrade).await?;
    assert!(wait_for_phase(client, ns, name, OdooInstancePhase::Upgrading).await);
    assert!(
        wait_for(TIMEOUT, POLL, || async {
            check_deployment_scale(client, ns, name, 0).await.is_ok()
        })
        .await
    );
    assert!(deployments.get(&cron_name).await.is_err());

    let job_name =
        wait_for_k8s_job_name::<OdooUpgradeJob>(client, ns, "test-combined-layout-upgrade").await;
    let jobs: Api<Job> = Api::namespaced(client.clone(), ns);
    assert!(jobs.get(&job_name).await.is_ok());
    fake_job_succeeded(client, ns, &job_name).await;
    assert!(
        wait_for(TIMEOUT, POLL, || async {
            check_deployment_scale(client, ns, name, 1).await.is_ok()
        })
        .await
    );

    patch_instance_spec(client, ns, name, json!({"replicas": 0})).await;
    assert!(wait_for_phase(client, ns, name, OdooInstancePhase::Stopped).await);
    patch_instance_spec(client, ns, name, json!({"workloadLayout": "separate"})).await;
    assert!(
        wait_for(TIMEOUT, POLL, || {
            let deployments = deployments.clone();
            let cron_name = cron_name.clone();
            async move {
                deployments
                    .get(&cron_name)
                    .await
                    .ok()
                    .is_some_and(|deployment| {
                        deployment.spec.as_ref().and_then(|spec| spec.replicas) == Some(0)
                    })
                    && deployments
                        .get(name)
                        .await
                        .ok()
                        .and_then(|deployment| deployment.spec)
                        .and_then(|spec| spec.template.spec)
                        .is_some_and(|pod| {
                            !pod.containers
                                .iter()
                                .any(|container| container.name.starts_with("odoo-cron-"))
                        })
            }
        })
        .await
    );

    patch_instance_spec(client, ns, name, json!({"replicas": 1})).await;
    assert!(
        wait_for(TIMEOUT, POLL, || async {
            check_deployment_scale(client, ns, name, 1).await.is_ok()
                && check_deployment_scale(client, ns, &cron_name, 1)
                    .await
                    .is_ok()
        })
        .await
    );
    Ok(())
}

#[tokio::test]
async fn combined_retirement_refuses_a_foreign_cron_deployment() -> anyhow::Result<()> {
    let ctx = TestContext::new_ns().await;
    let name = "foreign-cron-owner";
    let instance: OdooInstance = serde_json::from_value(json!({
        "apiVersion": "bemade.org/v1alpha1",
        "kind": "OdooInstance",
        "metadata": {"name": name, "namespace": &ctx.ns, "uid": "expected-owner-uid"},
        "spec": {"adminPassword": "admin", "ingress": {"hosts": ["test.example.com"]}}
    }))?;
    let cron_name = format!("{name}-cron");
    let deployments: Api<Deployment> = Api::namespaced(ctx.client.clone(), &ctx.ns);
    let foreign: Deployment = serde_json::from_value(json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": cron_name, "namespace": &ctx.ns},
        "spec": {
            "replicas": 3,
            "selector": {"matchLabels": {"app": cron_name}},
            "template": {
                "metadata": {"labels": {"app": cron_name}},
                "spec": {"containers": [{"name": "foreign", "image": "busybox"}]}
            }
        }
    }))?;
    deployments.create(&PostParams::default(), &foreign).await?;

    let result = retire_owned_cron_deployment(
        &ctx.client,
        &ctx.ns,
        &instance,
        &controller_owner_ref(&instance),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        deployments.get(&cron_name).await?.spec.unwrap().replicas,
        Some(3)
    );
    Ok(())
}
