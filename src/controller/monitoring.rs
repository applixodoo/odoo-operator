//! Fixed, opt-in production HTTP telemetry. No generic sidecar customization.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{
    Capabilities, ConfigMapVolumeSource, Container, ContainerPort, EmptyDirVolumeSource, KeyToPath,
    PodSpec, ResourceRequirements, SecurityContext, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

use super::helpers::env;
use crate::crd::odoo_instance::{Environment, OdooInstance};

const STATE_VOLUME: &str = "droggol-monitoring-state";
const CONFIG_VOLUME: &str = "droggol-monitoring-config";
const STATE_DIR: &str = "/run/droggol-monitoring";
const CONFIG_DIR: &str = "/etc/droggol-monitoring";

/// Called only when rendering the web Deployment, after applying extraEnv.
/// Reconciliation rebuilds the PodSpec; removing monitoring removes every part.
pub fn apply_web_monitoring(pod: &mut PodSpec, instance: &OdooInstance) {
    let Some(config) = &instance.spec.monitoring else {
        return;
    };
    if instance.spec.environment != Environment::Production {
        return;
    }
    let Some(odoo) = pod.containers.first_mut() else {
        return;
    };
    let state_mount = VolumeMount {
        name: STATE_VOLUME.to_string(),
        mount_path: STATE_DIR.to_string(),
        ..Default::default()
    };
    odoo.volume_mounts
        .get_or_insert_with(Vec::new)
        .push(state_mount.clone());
    let envs = odoo.env.get_or_insert_with(Vec::new);
    envs.retain(|value| {
        !matches!(
            value.name.as_str(),
            "DSH_MONITORING_ENABLED" | "DSH_MONITORING_SOCKET" | "DSH_MONITORING_STATE_DIR"
        )
    });
    envs.extend([
        env("DSH_MONITORING_ENABLED", "1"),
        env("DSH_MONITORING_SOCKET", format!("{STATE_DIR}/statsd.sock")),
        env("DSH_MONITORING_STATE_DIR", STATE_DIR),
    ]);
    pod.volumes.get_or_insert_with(Vec::new).extend([
        Volume {
            name: STATE_VOLUME.to_string(),
            empty_dir: Some(EmptyDirVolumeSource {
                medium: Some("Memory".to_string()),
                size_limit: Some(Quantity("16Mi".to_string())),
            }),
            ..Default::default()
        },
        Volume {
            name: CONFIG_VOLUME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: config.config_map_name.clone(),
                items: Some(vec![KeyToPath {
                    key: "statsd-mapping.yml".to_string(),
                    path: "statsd-mapping.yml".to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        },
    ]);
    pod.containers.push(Container {
        name: "odoo-statsd-exporter".to_string(),
        image: Some(config.exporter_image.clone()),
        image_pull_policy: Some("IfNotPresent".to_string()),
        command: Some(vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            include_str!("../../scripts/monitoring-exporter.sh").to_string(),
            "monitoring-exporter".to_string(),
        ]),
        args: Some(
            [
                "--statsd.listen-udp=",
                "--statsd.listen-tcp=",
                "--statsd.listen-unixgram=/run/droggol-monitoring/statsd.sock",
                "--statsd.unixsocket-mode=660",
                "--statsd.mapping-config=/etc/droggol-monitoring/statsd-mapping.yml",
                "--web.listen-address=:9102",
                "--statsd.event-queue-size=2048",
                "--statsd.udp-packet-queue-size=256",
                "--statsd.cache-size=1000",
                "--no-statsd.parse-influxdb-tags",
                "--no-statsd.parse-librato-tags",
                "--no-statsd.parse-signalfx-tags",
            ]
            .map(str::to_string)
            .to_vec(),
        ),
        env: Some(vec![env("GOMEMLIMIT", "96MiB"), env("GOMAXPROCS", "1")]),
        ports: Some(vec![ContainerPort {
            name: Some("odoo-metrics".to_string()),
            container_port: 9102,
            ..Default::default()
        }]),
        volume_mounts: Some(vec![
            state_mount,
            VolumeMount {
                name: CONFIG_VOLUME.to_string(),
                mount_path: CONFIG_DIR.to_string(),
                read_only: Some(true),
                ..Default::default()
            },
        ]),
        resources: Some(
            config
                .resources
                .clone()
                .unwrap_or_else(|| ResourceRequirements {
                    requests: Some(BTreeMap::from([
                        ("cpu".to_string(), Quantity("10m".to_string())),
                        ("memory".to_string(), Quantity("32Mi".to_string())),
                    ])),
                    limits: Some(BTreeMap::from([
                        ("cpu".to_string(), Quantity("100m".to_string())),
                        ("memory".to_string(), Quantity("128Mi".to_string())),
                    ])),
                    ..Default::default()
                }),
        ),
        security_context: Some(SecurityContext {
            allow_privilege_escalation: Some(false),
            read_only_root_filesystem: Some(true),
            run_as_non_root: Some(true),
            capabilities: Some(Capabilities {
                drop: Some(vec!["ALL".to_string()]),
                ..Default::default()
            }),
            ..Default::default()
        }),
        // The supervisor remains running across exporter failures. An exporter
        // readiness/liveness probe would unnecessarily withdraw healthy Odoo.
        ..Default::default()
    });
}
