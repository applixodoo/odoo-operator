use std::pin::pin;

use http::{Method, Request, Response, StatusCode};
use kube::client::Body;
use serde_json::{json, Value};
use tower_test::mock;

use odoo_operator::controller::child_resources::retire_owned_cron_deployment;
use odoo_operator::controller::helpers::controller_owner_ref;
use odoo_operator::crd::odoo_instance::OdooInstance;

#[tokio::test]
async fn replacement_during_cron_retirement_returns_conflict() -> anyhow::Result<()> {
    let namespace = "test";
    let name = "odoo";
    let cron_name = format!("{name}-cron");
    let instance: OdooInstance = serde_json::from_value(json!({
        "apiVersion": "bemade.org/v1alpha1",
        "kind": "OdooInstance",
        "metadata": {"name": name, "namespace": namespace, "uid": "owner-uid"},
        "spec": {
            "adminPassword": "admin",
            "ingress": {"hosts": ["test.example.com"]}
        }
    }))?;
    let owned_deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": cron_name,
            "namespace": namespace,
            "uid": "cron-uid",
            "resourceVersion": "10",
            "ownerReferences": [{
                "apiVersion": "bemade.org/v1alpha1",
                "kind": "OdooInstance",
                "name": name,
                "uid": "owner-uid",
                "controller": true
            }]
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": cron_name}},
            "template": {
                "metadata": {"labels": {"app": cron_name}},
                "spec": {"containers": [{"name": "cron", "image": "odoo"}]}
            }
        }
    });

    let (mock_service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let responder = tokio::spawn(async move {
        let mut handle = pin!(handle);

        let (request, send) = handle.next_request().await.expect("GET request");
        assert_eq!(request.method(), Method::GET);
        assert_eq!(
            request.uri().path(),
            "/apis/apps/v1/namespaces/test/deployments/odoo-cron"
        );
        send.send_response(
            Response::builder()
                .body(Body::from(serde_json::to_vec(&owned_deployment).unwrap()))
                .unwrap(),
        );

        let (request, send) = handle.next_request().await.expect("PATCH request");
        assert_eq!(request.method(), Method::PATCH);
        let patch: Value =
            serde_json::from_slice(&request.into_body().collect_bytes().await.unwrap()).unwrap();
        assert_eq!(
            patch,
            json!({
                "metadata": {"uid": "cron-uid", "resourceVersion": "10"},
                "spec": {"replicas": 0}
            })
        );
        let mut patched_deployment = owned_deployment;
        patched_deployment["metadata"]["resourceVersion"] = json!("11");
        patched_deployment["spec"]["replicas"] = json!(0);
        send.send_response(
            Response::builder()
                .body(Body::from(serde_json::to_vec(&patched_deployment).unwrap()))
                .unwrap(),
        );

        let (request, send) = handle.next_request().await.expect("DELETE request");
        assert_eq!(request.method(), Method::DELETE);
        let delete_options: Value =
            serde_json::from_slice(&request.into_body().collect_bytes().await.unwrap()).unwrap();
        assert_eq!(
            delete_options,
            json!({
                "propagationPolicy": "Foreground",
                "preconditions": {"uid": "cron-uid", "resourceVersion": "11"}
            })
        );
        send.send_response(
            Response::builder()
                .status(StatusCode::CONFLICT)
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "apiVersion": "v1",
                        "kind": "Status",
                        "status": "Failure",
                        "message": "Deployment was replaced",
                        "reason": "Conflict",
                        "code": 409
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        );
    });

    let client = kube::Client::new(mock_service, namespace);
    let result = retire_owned_cron_deployment(
        &client,
        namespace,
        &instance,
        &controller_owner_ref(&instance),
    )
    .await;

    assert!(matches!(
        result,
        Err(odoo_operator::error::Error::Kube(kube::Error::Api(response)))
            if response.code == 409
    ));
    responder.await?;
    Ok(())
}

#[tokio::test]
async fn cron_pods_keep_retirement_pending_after_deployment_disappears() -> anyhow::Result<()> {
    let namespace = "test";
    let name = "odoo";
    let instance: OdooInstance = serde_json::from_value(json!({
        "apiVersion": "bemade.org/v1alpha1",
        "kind": "OdooInstance",
        "metadata": {"name": name, "namespace": namespace, "uid": "owner-uid"},
        "spec": {"adminPassword": "admin", "ingress": {"hosts": ["test.example.com"]}}
    }))?;
    let (mock_service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let responder = tokio::spawn(async move {
        let mut handle = pin!(handle);
        let (request, send) = handle.next_request().await.expect("GET request");
        assert_eq!(request.method(), Method::GET);
        send.send_response(
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "apiVersion": "v1", "kind": "Status", "status": "Failure",
                        "reason": "NotFound", "code": 404
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        );
        let (request, send) = handle.next_request().await.expect("Pod list request");
        assert_eq!(request.method(), Method::GET);
        let query = request.uri().query().unwrap_or_default();
        assert_eq!(query, "&labelSelector=app%3Dodoo-cron");
        send.send_response(
            Response::builder()
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "apiVersion": "v1", "kind": "PodList", "items": [{
                            "metadata": {"name": "odoo-cron-pod"}
                        }]
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        );
    });
    let client = kube::Client::new(mock_service, namespace);
    assert!(
        !retire_owned_cron_deployment(
            &client,
            namespace,
            &instance,
            &controller_owner_ref(&instance),
        )
        .await?
    );
    responder.await?;
    Ok(())
}
