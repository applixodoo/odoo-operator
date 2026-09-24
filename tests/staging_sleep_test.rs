use std::sync::{Arc, Mutex};
use std::time::Duration;

use http::{Method, Request, Response};
use kube::client::Body;
use kube::runtime::controller::Action;
use serde_json::{json, Value};
use tower_test::mock;

use odoo_operator::controller::staging_sleep::{reconcile, MAINTENANCE_ANNOTATION};
use odoo_operator::crd::odoo_instance::OdooInstance;
use odoo_operator::error::Result;

fn instance() -> Value {
    json!({
        "apiVersion": "bemade.org/v1alpha1", "kind": "OdooInstance",
        "metadata": {"name": "odoo", "namespace": "tenant", "uid": "instance-uid", "generation": 1, "resourceVersion": "10",
            "labels": {"droggol.sh/instance-kind": "staging", "droggol.sh/server-id": "server", "droggol.sh/instance-id": "instance"}},
        "spec": {"adminPassword": "test", "ingress": {"hosts": ["example.test"]}, "environment": "Production", "replicas": 0,
            "database": {"cluster": "odoo-db"}, "stagingSleep": {"databaseCluster": "odoo-db"}},
        "status": {"phase": "Running", "dbInitialized": true, "ready": true, "readyReplicas": 1}
    })
}

fn cluster() -> Value {
    json!({
        "apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster",
        "metadata": {"name": "odoo-db", "namespace": "tenant", "uid": "cluster-uid", "resourceVersion": "20",
            "labels": {"droggol.sh/server-id": "server", "droggol.sh/instance-id": "instance"}},
        "spec": {"instances": 1},
        "status": {"readyInstances": 1, "currentPrimary": "odoo-db-1", "conditions": [{"type": "Ready", "status": "True"}]}
    })
}

#[derive(Default)]
struct Scenario {
    initial_replicas: i32,
    passes: usize,
    unobserved_scale: bool,
    serving_pod: bool,
    pending_backup: bool,
    primary_unready: bool,
    foreign_deployment: bool,
    conflict_cluster_patch: bool,
    wake_on_read: Option<usize>,
    wake_on_hibernate: bool,
}

#[derive(Debug)]
struct Call {
    method: Method,
    path: String,
    body: Value,
}

async fn run(
    input: Value,
    mut db: Value,
    scenario: Scenario,
) -> (Result<Option<Action>>, Vec<Call>) {
    let passes = scenario.passes.max(1);
    let original: OdooInstance = serde_json::from_value(input.clone()).unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let record = calls.clone();
    let (service, mut handle) = mock::pair::<Request<Body>, Response<Body>>();
    let task = tokio::spawn(async move {
        let mut live = input;
        let mut reads = 0;
        let mut replicas = [scenario.initial_replicas, scenario.initial_replicas];
        while let Some((request, send)) = handle.next_request().await {
            let method = request.method().clone();
            let path = request.uri().path().to_string();
            let body = request.into_body().collect_bytes().await.unwrap();
            let body: Value = if body.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&body).unwrap()
            };
            record.lock().unwrap().push(Call {
                method: method.clone(),
                path: path.clone(),
                body: body.clone(),
            });
            let mut status = 200;
            let response = if path.ends_with("/clusters/odoo-db") {
                if method == Method::PATCH {
                    assert_eq!(body["metadata"]["uid"], "cluster-uid");
                    assert_eq!(
                        body["metadata"]["resourceVersion"],
                        db["metadata"]["resourceVersion"]
                    );
                    db["metadata"]["annotations"] = body["metadata"]["annotations"].clone();
                    let rv = db["metadata"]["resourceVersion"]
                        .as_str()
                        .unwrap()
                        .parse::<usize>()
                        .unwrap()
                        + 1;
                    db["metadata"]["resourceVersion"] = json!(rv.to_string());
                    if scenario.wake_on_hibernate
                        && body["metadata"]["annotations"]["cnpg.io/hibernation"] == "on"
                    {
                        live["spec"]["replicas"] = json!(1);
                        live["metadata"]["generation"] = json!(2);
                    }
                }
                if method == Method::PATCH && scenario.conflict_cluster_patch {
                    status = 409;
                    json!({"apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": "Conflict", "message": "Cluster replaced", "code": 409})
                } else {
                    db.clone()
                }
            } else if path.ends_with("/odooinstances/odoo") {
                assert_eq!(method, Method::GET);
                reads += 1;
                if scenario.wake_on_read == Some(reads) {
                    live["spec"]["replicas"] = json!(1);
                    live["metadata"]["generation"] = json!(2);
                }
                live.clone()
            } else if path.ends_with("/odooinstances/odoo/status") {
                assert_eq!(method, Method::PATCH);
                assert_eq!(body["metadata"]["uid"], "instance-uid");
                assert_eq!(body["metadata"]["resourceVersion"], "10");
                live["status"] = body["status"].clone();
                live.clone()
            } else if path.contains("/deployments/") {
                let name = path.rsplit('/').next().unwrap();
                let index = usize::from(name == "odoo-cron");
                if method == Method::PATCH {
                    assert_eq!(body["metadata"]["uid"], format!("{name}-uid"));
                    assert_eq!(body["metadata"]["resourceVersion"], "30");
                    assert_eq!(body["spec"]["replicas"], 0);
                    replicas[index] = 0;
                }
                json!({"apiVersion": "apps/v1", "kind": "Deployment",
                    "metadata": {"name": name, "namespace": "tenant", "uid": format!("{name}-uid"), "resourceVersion": "30", "generation": 1,
                        "ownerReferences": [{"apiVersion": "bemade.org/v1alpha1", "kind": "OdooInstance", "name": "odoo", "uid": if scenario.foreign_deployment { "foreign" } else { "instance-uid" }, "controller": true}]},
                    "status": {"observedGeneration": if scenario.unobserved_scale { 0 } else { 1 }, "replicas": replicas[index]},
                    "spec": {"replicas": replicas[index], "selector": {"matchLabels": {"app": name}}, "template": {"spec": {"containers": []}}}})
            } else if path.ends_with("/pods/odoo-db-1") {
                json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "odoo-db-1", "ownerReferences": [{"apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster", "name": "odoo-db", "uid": "cluster-uid", "controller": true}]},
                    "status": {"conditions": [{"type": "Ready", "status": if scenario.primary_unready { "False" } else { "True" }}]}})
            } else if path.ends_with("/odoobackupjobs") && scenario.pending_backup {
                json!({"apiVersion": "bemade.org/v1alpha1", "kind": "OdooBackupJobList", "items": [{
                    "metadata": {"name": "queued-backup"},
                    "spec": {"odooInstanceRef": {"name": "odoo"}, "destination": {"bucket": "test", "objectKey": "backup", "endpoint": "https://example.test"}}
                }]})
            } else if path.ends_with("/persistentvolumeclaims/odoo-filestore-pvc") {
                json!({"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {"name": "odoo-filestore-pvc"}})
            } else {
                assert_eq!(method, Method::GET, "unexpected mutation {path}");
                let items = if path.ends_with("/pods") && scenario.serving_pod {
                    vec![
                        json!({"metadata": {"name": "terminating-web", "deletionTimestamp": "2026-09-24T10:00:00Z"}}),
                    ]
                } else {
                    vec![]
                };
                json!({"apiVersion": "v1", "kind": "List", "metadata": {}, "items": items})
            };
            send.send_response(
                Response::builder()
                    .status(status)
                    .body(Body::from(serde_json::to_vec(&response).unwrap()))
                    .unwrap(),
            );
        }
    });
    let client = kube::Client::new(service, "tenant");
    let mut result = Ok(None);
    for _ in 0..passes {
        result = tokio::time::timeout(Duration::from_secs(3), reconcile(&client, &original))
            .await
            .expect("gate must finish");
        if result.is_err() {
            break;
        }
    }
    drop(client);
    task.await.unwrap();
    (
        result,
        Arc::try_unwrap(calls).unwrap().into_inner().unwrap(),
    )
}

fn mutations(calls: &[Call]) -> Vec<&Call> {
    calls.iter().filter(|c| c.method == Method::PATCH).collect()
}

#[tokio::test]
async fn opted_out_instances_do_not_access_cnpg_or_change_behavior() {
    let mut cr = instance();
    cr["spec"].as_object_mut().unwrap().remove("stagingSleep");
    let (result, calls) = run(cr, cluster(), Scenario::default()).await;
    assert!(result.unwrap().is_none());
    assert!(calls.is_empty());
}

#[tokio::test]
async fn sleep_scales_both_owned_deployments_and_proves_pod_absence_before_hibernation() {
    let (result, calls) = run(
        instance(),
        cluster(),
        Scenario {
            initial_replicas: 1,
            passes: 2,
            ..Default::default()
        },
    )
    .await;
    assert!(result.unwrap().is_some());
    let writes = mutations(&calls);
    assert_eq!(writes.len(), 4);
    assert!(writes[0].path.ends_with("/deployments/odoo"));
    assert!(writes[1].path.ends_with("/deployments/odoo-cron"));
    assert_eq!(
        writes[2].body["metadata"]["annotations"]["cnpg.io/hibernation"],
        "on"
    );
    assert_eq!(writes[3].body["status"]["phase"], "Stopped");
    let last_scale = calls
        .iter()
        .rposition(|c| c.method == Method::PATCH && c.path.contains("/deployments/"))
        .unwrap();
    let hibernate = calls
        .iter()
        .position(|c| c.method == Method::PATCH && c.path.contains("/clusters/"))
        .unwrap();
    assert!(calls[last_scale + 1..hibernate]
        .iter()
        .any(|c| c.path.ends_with("/pods")));
}

#[tokio::test]
async fn terminating_serving_pods_keep_database_awake() {
    let (result, calls) = run(
        instance(),
        cluster(),
        Scenario {
            serving_pod: true,
            ..Default::default()
        },
    )
    .await;
    assert!(result.unwrap().is_some());
    assert!(mutations(&calls)
        .iter()
        .all(|c| c.path.contains("/deployments/")));
}

#[tokio::test]
async fn maintenance_at_zero_and_http_demand_wake_without_trusting_old_ready_status() {
    for maintenance in [true, false] {
        let mut cr = instance();
        if maintenance {
            cr["metadata"]["annotations"] = json!({MAINTENANCE_ANNOTATION: "job-attempt"});
        } else {
            cr["spec"]["replicas"] = json!(1);
        }
        let mut db = cluster();
        db["metadata"]["annotations"] = json!({"cnpg.io/hibernation": "on"});
        let (result, calls) = run(cr, db, Scenario::default()).await;
        assert!(result.unwrap().is_some());
        let writes = mutations(&calls);
        assert_eq!(writes.len(), 1);
        assert_eq!(
            writes[0].body["metadata"]["annotations"]["cnpg.io/hibernation"],
            "off"
        );
    }
}

#[tokio::test]
async fn only_ready_cnpg_and_its_ready_primary_allow_postgres_and_odoo_startup() {
    for primary_unready in [true, false] {
        let mut cr = instance();
        cr["spec"]["replicas"] = json!(1);
        let (result, calls) = run(
            cr,
            cluster(),
            Scenario {
                primary_unready,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(result.unwrap().is_none(), !primary_unready);
        assert!(mutations(&calls).is_empty());
    }
}

#[tokio::test]
async fn concurrent_http_wake_before_scale_prevents_sleep() {
    let (result, calls) = run(
        instance(),
        cluster(),
        Scenario {
            wake_on_read: Some(2),
            ..Default::default()
        },
    )
    .await;
    assert!(result.unwrap().is_some());
    assert!(mutations(&calls).is_empty());
}

#[tokio::test]
async fn concurrent_http_wake_after_hibernate_is_immediately_compensated() {
    let (result, calls) = run(
        instance(),
        cluster(),
        Scenario {
            wake_on_hibernate: true,
            ..Default::default()
        },
    )
    .await;
    assert!(result.unwrap().is_some());
    let annotations: Vec<_> = mutations(&calls)
        .iter()
        .filter(|c| c.path.contains("/clusters/"))
        .map(|c| {
            c.body["metadata"]["annotations"]["cnpg.io/hibernation"]
                .as_str()
                .unwrap()
        })
        .collect();
    assert_eq!(annotations, ["on", "off"]);
    assert!(!calls.iter().any(|c| c.path.ends_with("/status")));
}

#[tokio::test]
async fn mismatched_identity_replicas_and_foreign_deployments_fail_closed() {
    for key in ["identity", "replicas", "foreign"] {
        let mut db = cluster();
        if key == "identity" {
            db["metadata"]["labels"]["droggol.sh/instance-id"] = json!("other");
        }
        if key == "replicas" {
            db["spec"]["instances"] = json!(2);
        }
        let (result, calls) = run(
            instance(),
            db,
            Scenario {
                foreign_deployment: key == "foreign",
                ..Default::default()
            },
        )
        .await;
        assert!(result.is_err());
        assert!(mutations(&calls).is_empty());
    }
}

#[tokio::test]
async fn cnpg_replacement_conflict_is_not_retried_without_revalidation() {
    let (result, calls) = run(
        instance(),
        cluster(),
        Scenario {
            conflict_cluster_patch: true,
            ..Default::default()
        },
    )
    .await;
    assert!(
        matches!(result, Err(odoo_operator::error::Error::Kube(kube::Error::Api(e))) if e.code == 409)
    );
    assert_eq!(
        mutations(&calls)
            .iter()
            .filter(|c| c.path.contains("/clusters/"))
            .count(),
        1
    );
}

#[tokio::test]
async fn initialization_and_native_operation_phases_never_hibernate() {
    for phase in [
        "Provisioning",
        "Initializing",
        "Restoring",
        "BackingUp",
        "Upgrading",
        "Error",
    ] {
        let mut cr = instance();
        cr["status"]["phase"] = json!(phase);
        let (result, calls) = run(cr, cluster(), Scenario::default()).await;
        assert!(result.unwrap().is_none(), "{phase}");
        assert!(mutations(&calls).is_empty());
    }
    let mut cr = instance();
    cr["status"]["dbInitialized"] = json!(false);
    let (result, calls) = run(cr, cluster(), Scenario::default()).await;
    assert!(result.unwrap().is_none());
    assert!(mutations(&calls).is_empty());
}

#[tokio::test]
async fn pending_native_backup_wakes_a_stopped_database() {
    let mut db = cluster();
    db["metadata"]["annotations"] = json!({"cnpg.io/hibernation": "on"});
    let (result, calls) = run(
        instance(),
        db,
        Scenario {
            pending_backup: true,
            ..Default::default()
        },
    )
    .await;
    assert!(result.unwrap().is_some());
    let writes = mutations(&calls);
    assert_eq!(writes.len(), 1);
    assert_eq!(
        writes[0].body["metadata"]["annotations"]["cnpg.io/hibernation"],
        "off"
    );
}

#[tokio::test]
async fn empty_pod_list_is_insufficient_until_deployment_observes_zero() {
    let (result, calls) = run(
        instance(),
        cluster(),
        Scenario {
            unobserved_scale: true,
            ..Default::default()
        },
    )
    .await;
    assert!(result.unwrap().is_some());
    assert!(mutations(&calls).is_empty());
}
