use gateway_api::apis::standard::httproutes::HTTPRoute;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim, Secret, Service};
use k8s_openapi::api::networking::v1::Ingress;
use kube::api::{Api, Patch, PatchParams, PostParams};
use serde_json::json;

use super::common::*;
use odoo_operator::crd::odoo_instance::{OdooInstance, OdooInstancePhase};

#[tokio::test]
async fn monitoring_reconciles_web_only_and_can_be_removed() {
    let name = "test-monitoring";
    let ctx = TestContext::new(name).await;
    assert!(wait_for_phase(&ctx.client, &ctx.ns, name, OdooInstancePhase::Uninitialized).await);
    let api: Api<OdooInstance> = Api::namespaced(ctx.client.clone(), &ctx.ns);
    api.patch(name, &PatchParams::default(), &Patch::Merge(json!({
        "metadata": {"labels": {"droggol.sh/instance-id": "instance-ulid", "droggol.sh/project-id": "project-ulid"}},
        "spec": {"environment": "Production", "monitoring": {
            "exporterImage": format!("prom/statsd-exporter:v0.29.0@sha256:{}", "a".repeat(64)),
            "configMapName": "test-monitoring-config-abcd"
        }}
    }))).await.unwrap();
    let deployments: Api<Deployment> = Api::namespaced(ctx.client.clone(), &ctx.ns);
    assert!(
        wait_for(TIMEOUT, POLL, || {
            let deployments = deployments.clone();
            async move {
                deployments
                    .get(name)
                    .await
                    .ok()
                    .and_then(|dep| dep.spec)
                    .and_then(|spec| spec.template.spec)
                    .is_some_and(|pod| pod.containers.len() == 2)
            }
        })
        .await
    );
    let web = deployments.get(name).await.unwrap().spec.unwrap();
    assert_eq!(web.selector.match_labels.unwrap().len(), 1);
    assert_eq!(
        web.template.metadata.unwrap().labels.unwrap()["droggol.sh/instance-id"],
        "instance-ulid"
    );
    let cron = deployments
        .get(&format!("{name}-cron"))
        .await
        .unwrap()
        .spec
        .unwrap()
        .template
        .spec
        .unwrap();
    assert_eq!(cron.containers.len(), 1);
    assert!(!cron.containers[0]
        .env
        .as_ref()
        .unwrap()
        .iter()
        .any(|env| env.name.starts_with("DSH_MONITORING_")));
    assert!(!cron
        .volumes
        .unwrap()
        .iter()
        .any(|volume| volume.name.starts_with("droggol-monitoring")));
    let services: Api<Service> = Api::namespaced(ctx.client.clone(), &ctx.ns);
    assert!(!services
        .get(name)
        .await
        .unwrap()
        .spec
        .unwrap()
        .ports
        .unwrap()
        .iter()
        .any(|port| port.port == 9102));
    patch_instance_spec(&ctx.client, &ctx.ns, name, json!({"monitoring": null})).await;
    assert!(
        wait_for(TIMEOUT, POLL, || {
            let deployments = deployments.clone();
            async move {
                deployments
                    .get(name)
                    .await
                    .ok()
                    .and_then(|dep| dep.spec)
                    .and_then(|spec| spec.template.spec)
                    .is_some_and(|pod| {
                        pod.containers.len() == 1
                            && !pod
                                .volumes
                                .unwrap()
                                .iter()
                                .any(|volume| volume.name.starts_with("droggol-monitoring"))
                    })
            }
        })
        .await
    );
}

/// When imagePullSecret is set, the operator should copy the registry secret
/// from the operator namespace into the instance namespace.
#[tokio::test]
async fn image_pull_secret_copied_to_namespace() {
    let ctx = TestContext::new("test-pull").await;
    let (c, ns) = (&ctx.client, ctx.ns.as_str());

    // Create a fake registry secret in the operator namespace ("default").
    let op_secrets: Api<Secret> = Api::namespaced(c.clone(), "default");
    let registry_secret: Secret = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "test-registry", "namespace": "default" },
        "type": "kubernetes.io/dockerconfigjson",
        "stringData": {
            ".dockerconfigjson": r#"{"auths":{"registry.example.com":{"auth":"dGVzdDp0ZXN0"}}}"#
        }
    }))
    .unwrap();
    // Ignore AlreadyExists — another parallel test may have created it.
    let _ = op_secrets
        .create(&PostParams::default(), &registry_secret)
        .await;

    // Patch the OdooInstance to reference the pull secret.
    patch_instance_spec(
        c,
        ns,
        "test-pull",
        json!({ "imagePullSecret": "test-registry" }),
    )
    .await;

    // Wait for the operator to copy the secret into the test namespace and
    // verify the type in one shot to avoid a race with envtest shutdown.
    let ns_secrets: Api<Secret> = Api::namespaced(c.clone(), ns);
    assert!(
        wait_for(TIMEOUT, POLL, || {
            let ns_secrets = ns_secrets.clone();
            async move {
                match ns_secrets.get("test-registry").await {
                    Ok(s) => s.type_.as_deref() == Some("kubernetes.io/dockerconfigjson"),
                    Err(_) => false,
                }
            }
        })
        .await,
        "expected registry secret with correct type to be copied into instance namespace"
    );
}

#[tokio::test]
async fn reconcile_creates_child_resources() {
    let ctx = TestContext::new("test-child").await;
    let c = &ctx.client;
    let ns = &ctx.ns;

    assert!(
        wait_for_phase(c, ns, "test-child", OdooInstancePhase::Uninitialized).await,
        "expected Uninitialized"
    );

    let secrets: Api<Secret> = Api::namespaced(c.clone(), ns);
    assert!(
        secrets.get("test-child-odoo-user").await.is_ok(),
        "odoo-user secret missing"
    );

    let cms: Api<ConfigMap> = Api::namespaced(c.clone(), ns);
    assert!(
        cms.get("test-child-odoo-conf").await.is_ok(),
        "odoo-conf configmap missing"
    );

    let svcs: Api<Service> = Api::namespaced(c.clone(), ns);
    assert!(svcs.get("test-child").await.is_ok(), "service missing");

    let ings: Api<Ingress> = Api::namespaced(c.clone(), ns);
    assert!(ings.get("test-child").await.is_ok(), "ingress missing");

    // No gatewayRef set → no HTTPRoute should exist.
    let routes: Api<HTTPRoute> = Api::namespaced(c.clone(), ns);
    assert!(
        routes.get("test-child").await.is_err(),
        "HTTPRoute should not exist when gatewayRef is absent"
    );

    let deps: Api<Deployment> = Api::namespaced(c.clone(), ns);
    assert!(deps.get("test-child").await.is_ok(), "deployment missing");
    let deployment = deps
        .get("test-child-cron")
        .await
        .expect("cron deployment is missing");
    let container = &deployment.spec.unwrap().template.spec.unwrap().containers[0];

    let startup = container.startup_probe.as_ref().unwrap();
    assert_eq!(
        startup.exec.as_ref().unwrap().command.as_ref().unwrap(),
        &vec![
            "/usr/bin/python3".to_string(),
            "-I".to_string(),
            "-S".to_string(),
            "/usr/local/bin/dsh-cron-probe".to_string(),
            "startup".to_string(),
        ]
    );
    assert_eq!(startup.initial_delay_seconds, Some(5));
    assert_eq!(startup.period_seconds, Some(10));
    assert_eq!(startup.timeout_seconds, Some(5));
    assert_eq!(startup.failure_threshold, Some(30));

    let liveness = container.liveness_probe.as_ref().unwrap();
    assert_eq!(
        liveness.exec.as_ref().unwrap().command.as_ref().unwrap(),
        &vec![
            "/usr/bin/python3".to_string(),
            "-I".to_string(),
            "-S".to_string(),
            "/usr/local/bin/dsh-cron-probe".to_string(),
            "liveness".to_string(),
        ]
    );
    assert_eq!(liveness.initial_delay_seconds, Some(300));
    assert_eq!(liveness.period_seconds, Some(30));
    assert_eq!(liveness.timeout_seconds, Some(5));
    assert_eq!(liveness.failure_threshold, Some(3));
    assert!(
        container
            .env
            .as_ref()
            .is_none_or(|env| env.iter().all(|var| var.name != "PYTHONPATH")),
        "cron probes must not depend on a pod-wide PYTHONPATH"
    );
}

/// When gatewayRef is set, the operator should create an HTTPRoute and no Ingress.
#[tokio::test]
async fn reconcile_creates_http_route_when_gateway_ref_set() {
    let ctx = TestContext::new_ns().await;
    let (c, ns) = (&ctx.client, ctx.ns.as_str());

    // Create an OdooInstance with gatewayRef set.
    let api: Api<OdooInstance> = Api::namespaced(c.clone(), ns);
    let inst: OdooInstance = serde_json::from_value(json!({
        "apiVersion": "bemade.org/v1alpha1",
        "kind": "OdooInstance",
        "metadata": { "name": "test-gw", "namespace": ns },
        "spec": {
            "replicas": 1,
            "cron": { "replicas": 1 },
            "adminPassword": "admin",
            "image": "odoo:18.0",
            "ingress": {
                "hosts": ["gw.example.com"],
                "gatewayRef": {
                    "name": "my-gateway",
                    "namespace": "istio-system"
                }
            },
            "filestore": {
                "storageSize": "1Gi",
                "storageClass": "standard"
            },
            "init": { "enabled": false }
        }
    }))
    .unwrap();
    api.create(&PostParams::default(), &inst)
        .await
        .expect("failed to create OdooInstance with gatewayRef");

    assert!(
        wait_for_phase(c, ns, "test-gw", OdooInstancePhase::Uninitialized).await,
        "expected Uninitialized"
    );

    // HTTPRoute should exist.
    let routes: Api<HTTPRoute> = Api::namespaced(c.clone(), ns);
    assert!(
        routes.get("test-gw").await.is_ok(),
        "HTTPRoute missing when gatewayRef is set"
    );

    // Ingress should NOT exist.
    let ings: Api<Ingress> = Api::namespaced(c.clone(), ns);
    assert!(
        ings.get("test-gw").await.is_err(),
        "Ingress should not exist when gatewayRef is set"
    );
}

/// envtest has no PV controller and no StorageClasses, so PVCs never bind and
/// the API server rejects resize attempts. Create an expansion-enabled
/// StorageClass and fake the PVC into a bound state.
async fn fake_pvc_bound(c: &kube::Client, ns: &str, name: &str, capacity: &str) {
    use k8s_openapi::api::storage::v1::StorageClass;
    let scs: Api<StorageClass> = Api::all(c.clone());
    let sc: StorageClass = serde_json::from_value(json!({
        "apiVersion": "storage.k8s.io/v1",
        "kind": "StorageClass",
        "metadata": { "name": "standard" },
        "provisioner": "k8s.io/test-provisioner",
        "allowVolumeExpansion": true,
    }))
    .unwrap();
    let _ = scs.create(&PostParams::default(), &sc).await;

    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(c.clone(), ns);
    let spec_patch = json!({ "spec": { "volumeName": format!("pv-{name}") } });
    pvcs.patch(name, &PatchParams::default(), &Patch::Merge(&spec_patch))
        .await
        .expect("failed to set volumeName");
    let status_patch = json!({
        "status": { "phase": "Bound", "capacity": { "storage": capacity } }
    });
    pvcs.patch_status(name, &PatchParams::default(), &Patch::Merge(&status_patch))
        .await
        .expect("failed to patch PVC status");
}

/// Increasing spec.filestore.storageSize should expand the existing PVC's
/// storage request (PVC expansion in place — no migration phase).
#[tokio::test]
async fn pvc_storage_size_expands_in_place() {
    let ctx = TestContext::new("test-pvc-grow").await;
    let (c, ns) = (&ctx.client, ctx.ns.as_str());

    assert!(
        wait_for_phase(c, ns, "test-pvc-grow", OdooInstancePhase::Uninitialized).await,
        "expected Uninitialized"
    );

    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(c.clone(), ns);
    let pvc_name = "test-pvc-grow-filestore-pvc";

    // Initial size from TestContext default — capture it.
    let initial = pvcs
        .get(pvc_name)
        .await
        .expect("filestore PVC missing")
        .spec
        .and_then(|s| s.resources)
        .and_then(|r| r.requests)
        .and_then(|m| m.get("storage").cloned())
        .expect("PVC has no storage request")
        .0;
    assert_ne!(initial, "10Gi", "test relies on initial size != 10Gi");

    // The API server only allows resizing bound PVCs. envtest has no
    // PV controller, so simulate binding by setting volumeName + status.
    fake_pvc_bound(c, ns, pvc_name, &initial).await;

    // Bump the spec to 10Gi.
    patch_instance_spec(
        c,
        ns,
        "test-pvc-grow",
        json!({ "filestore": { "storageSize": "10Gi" } }),
    )
    .await;

    // Wait for the operator to patch the PVC.
    assert!(
        wait_for(TIMEOUT, POLL, || {
            let pvcs = pvcs.clone();
            async move {
                match pvcs.get(pvc_name).await {
                    Ok(pvc) => pvc
                        .spec
                        .and_then(|s| s.resources)
                        .and_then(|r| r.requests)
                        .and_then(|m| m.get("storage").cloned())
                        .map(|q| q.0 == "10Gi")
                        .unwrap_or(false),
                    Err(_) => false,
                }
            }
        })
        .await,
        "expected filestore PVC storage request to be expanded to 10Gi"
    );
}

/// Decreasing the storage size should be a no-op on the PVC (the webhook would
/// normally reject this, but in-process tests bypass the webhook). Verifies the
/// operator does not attempt to shrink.
#[tokio::test]
async fn pvc_storage_size_shrink_is_noop() {
    let ctx = TestContext::new("test-pvc-shrink").await;
    let (c, ns) = (&ctx.client, ctx.ns.as_str());

    assert!(
        wait_for_phase(c, ns, "test-pvc-shrink", OdooInstancePhase::Uninitialized).await,
        "expected Uninitialized"
    );

    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(c.clone(), ns);
    let pvc_name = "test-pvc-shrink-filestore-pvc";
    let initial = pvcs
        .get(pvc_name)
        .await
        .expect("filestore PVC missing")
        .spec
        .and_then(|s| s.resources)
        .and_then(|r| r.requests)
        .and_then(|m| m.get("storage").cloned())
        .expect("PVC has no storage request")
        .0;

    fake_pvc_bound(c, ns, pvc_name, &initial).await;

    patch_instance_spec(
        c,
        ns,
        "test-pvc-shrink",
        json!({ "filestore": { "storageSize": "1Mi" } }),
    )
    .await;

    // Give the operator several reconciles to (not) act, then assert unchanged.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    let after = pvcs
        .get(pvc_name)
        .await
        .unwrap()
        .spec
        .and_then(|s| s.resources)
        .and_then(|r| r.requests)
        .and_then(|m| m.get("storage").cloned())
        .unwrap()
        .0;
    assert_eq!(after, initial, "PVC should not shrink");
}
