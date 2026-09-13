use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Container, PodSpec, ResourceRequirements};
use kube::CustomResourceExt;
use odoo_operator::controller::{helpers::instance_labels, monitoring::apply_web_monitoring};
use odoo_operator::crd::odoo_instance::OdooInstance;
use serde_json::json;

fn instance(environment: &str, monitoring: bool) -> OdooInstance {
    let mut value = json!({
        "metadata": {"name": "prod", "namespace": "tenant"},
        "spec": {
            "adminPassword": "test", "ingress": {"hosts": ["example.test"]},
            "environment": environment,
        },
    });
    if monitoring {
        value["spec"]["monitoring"] = json!({
            "exporterImage": format!("prom/statsd-exporter:v0.29.0@sha256:{}", "a".repeat(64)),
            "configMapName": "prod-monitoring-abcd",
        });
    }
    serde_json::from_value(value).unwrap()
}

fn web_pod() -> PodSpec {
    PodSpec {
        containers: vec![Container {
            name: "odoo-prod".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn absent_monitoring_and_staging_do_not_change_the_pod() {
    for instance in [instance("Production", false), instance("Staging", true)] {
        let mut pod = web_pod();
        let before = pod.clone();
        apply_web_monitoring(&mut pod, &instance);
        assert_eq!(pod, before);
    }
}

#[test]
fn production_uses_fixed_local_bounded_supervised_exporter_contract() {
    let mut pod = web_pod();
    let instance = instance("Production", true);
    apply_web_monitoring(&mut pod, &instance);
    assert_eq!(pod.containers.len(), 2);
    let odoo = &pod.containers[0];
    let exporter = &pod.containers[1];
    let env = odoo.env.as_ref().unwrap();
    assert!(env
        .iter()
        .any(|v| v.name == "DSH_MONITORING_ENABLED" && v.value.as_deref() == Some("1")));
    assert_eq!(odoo.volume_mounts.as_ref().unwrap().len(), 1);
    assert_eq!(exporter.volume_mounts.as_ref().unwrap().len(), 2);
    assert_eq!(
        exporter.image.as_ref().unwrap(),
        &instance.spec.monitoring.as_ref().unwrap().exporter_image
    );
    assert_eq!(exporter.ports.as_ref().unwrap()[0].container_port, 9102);
    assert!(exporter.readiness_probe.is_none() && exporter.liveness_probe.is_none());
    let supervisor = &exporter.command.as_ref().unwrap()[2];
    assert!(supervisor.contains("rm -f /run/droggol-monitoring/statsd.sock && exec"));
    assert!(supervisor.contains("kill -TERM") && supervisor.contains("sleep 2"));
    let limits = exporter
        .resources
        .as_ref()
        .unwrap()
        .limits
        .as_ref()
        .unwrap();
    assert_eq!(limits["memory"].0, "128Mi");
    assert_eq!(limits["cpu"].0, "100m");
    let requests = exporter
        .resources
        .as_ref()
        .unwrap()
        .requests
        .as_ref()
        .unwrap();
    assert_eq!(requests["memory"].0, "32Mi");
    assert_eq!(requests["cpu"].0, "10m");
    let state = pod.volumes.as_ref().unwrap()[0].empty_dir.as_ref().unwrap();
    assert_eq!(state.medium.as_deref(), Some("Memory"));
    assert_eq!(state.size_limit.as_ref().unwrap().0, "16Mi");
    assert!(pod.init_containers.is_none());
}

#[test]
fn custom_monitoring_budget_changes_only_exporter_resources_and_can_be_removed() {
    let mut instance = instance("Production", true);
    let mut default_pod = web_pod();
    apply_web_monitoring(&mut default_pod, &instance);
    let resources: ResourceRequirements = serde_json::from_value(json!({
        "requests": {"cpu": "400m", "memory": "512Mi"},
        "limits": {"cpu": "400m", "memory": "512Mi"},
    }))
    .unwrap();
    instance.spec.monitoring.as_mut().unwrap().resources = Some(resources.clone());
    let mut custom_pod = web_pod();
    apply_web_monitoring(&mut custom_pod, &instance);
    let mut expected = default_pod.clone();
    expected.containers[1].resources = Some(resources);
    assert_eq!(custom_pod, expected);

    instance.spec.monitoring.as_mut().unwrap().resources = None;
    let mut reverted_pod = web_pod();
    apply_web_monitoring(&mut reverted_pod, &instance);
    assert_eq!(reverted_pod, default_pod);
}

#[test]
fn ownership_labels_are_explicitly_allowlisted() {
    let mut instance = instance("Production", false);
    instance.metadata.labels = Some(BTreeMap::from([
        ("droggol.sh/instance-id".into(), "immutable-instance".into()),
        ("droggol.sh/project-id".into(), "immutable-project".into()),
        ("droggol.sh/server-id".into(), "immutable-server".into()),
        ("app".into(), "wrong-selector".into()),
        ("arbitrary".into(), "not-propagated".into()),
    ]));
    let labels = instance_labels(&instance);
    assert_eq!(labels["droggol.sh/instance-id"], "immutable-instance");
    assert_eq!(labels["droggol.sh/project-id"], "immutable-project");
    assert_eq!(labels["droggol.sh/server-id"], "immutable-server");
    assert_eq!(labels["bemade.org/environment"], "production");
    assert!(!labels.contains_key("app") && !labels.contains_key("arbitrary"));
}

#[test]
fn monitoring_schema_requires_pinned_official_image_and_production() {
    let crd = serde_json::to_value(OdooInstance::crd()).unwrap();
    let spec = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"];
    let monitoring = &spec["properties"]["monitoring"]["properties"];
    assert!(monitoring["exporterImage"]["pattern"]
        .as_str()
        .unwrap()
        .contains("@sha256:"));
    assert_eq!(monitoring["exporterImage"]["maxLength"], 256);
    assert_eq!(monitoring["configMapName"]["maxLength"], 63);
    assert_eq!(
        monitoring["resources"]["properties"]["requests"]["type"],
        "object"
    );
    assert_eq!(
        monitoring["resources"]["properties"]["limits"]["type"],
        "object"
    );
    assert!(!spec["properties"]["monitoring"]["required"]
        .as_array()
        .unwrap()
        .iter()
        .any(|field| field == "resources"));
    assert!(spec["x-kubernetes-validations"]
        .as_array()
        .unwrap()
        .iter()
        .any(|rule| rule["rule"] == "!has(self.monitoring) || self.environment == 'Production'"));
}
