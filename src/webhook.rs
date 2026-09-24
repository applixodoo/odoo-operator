//! Validating admission webhook for OdooInstance.
//!
//! Rejects updates that would:
//! - Decrease filestore storage size (PVCs cannot shrink)
//!
//! StorageClass and database cluster changes are allowed — the operator
//! handles migration automatically.  Changes are rejected during unsafe
//! phases (Restoring, Upgrading, BackingUp, migrating phases, Uninitialized).

use std::sync::Arc;

use kube::core::admission::{AdmissionRequest, AdmissionResponse, AdmissionReview};
use rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};
use warp::Filter;

use crate::crd::odoo_instance::{OdooInstance, OdooInstancePhase, WorkloadLayout};
use crate::helpers::parse_quantity;
use crate::tls::spawn_reloading_resolver;

/// Start the validating webhook server on an already-bound TCP listener.
/// Returns a future that runs the HTTPS server forever.
///
/// Taking a pre-bound listener (rather than a `SocketAddr`) lets callers — and
/// tests — observe the actual bound port via [`tokio::net::TcpListener::local_addr`].
///
/// TLS termination is handled here (rather than via warp's built-in
/// `.tls().cert_path()`) so the serving certificate can be **hot-reloaded**
/// when cert-manager rotates it — see [`crate::tls`]. Decrypted connections are
/// fed to the warp filter via [`warp::Server::run_incoming`].
pub async fn run(listener: tokio::net::TcpListener, tls_cert: &str, tls_key: &str) {
    let route = warp::post()
        .and(warp::path("validate-bemade-org-v1alpha1-odooinstance"))
        .and(warp::body::json())
        .map(|review: AdmissionReview<OdooInstance>| {
            let req: AdmissionRequest<OdooInstance> = match review.try_into() {
                Ok(req) => req,
                Err(e) => {
                    warn!(%e, "invalid admission request");
                    let resp = AdmissionResponse::invalid(format!("invalid request: {e}"));
                    return warp::reply::json(&resp.into_review());
                }
            };

            let resp = validate(req);
            warp::reply::json(&resp.into_review())
        });

    // A resolver backed by the cert on disk, kept fresh by a background poller.
    // Failing the initial load is fatal: returning here ends the webhook future
    // in main's `select!`, which exits the process so the pod restarts.
    let resolver = match spawn_reloading_resolver(tls_cert, tls_key) {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "failed to load webhook TLS certificate; webhook not started");
            return;
        }
    };

    // Build with the ring provider explicitly so we don't depend on a
    // process-wide default provider being installed elsewhere.
    let config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("ring provider supports the default protocol versions")
            .with_no_client_auth()
            .with_cert_resolver(resolver);
    let acceptor = TlsAcceptor::from(Arc::new(config));

    let local_addr = listener.local_addr().ok();
    info!(
        ?local_addr,
        "starting validating webhook server (hot-reloading TLS)"
    );

    // Accept TCP connections, perform the TLS handshake off the accept path so a
    // slow/stalled handshake can't block other clients, and stream the decrypted
    // connections into warp. The channel decouples accept from warp's consumer.
    let (tx, rx) = tokio::sync::mpsc::channel(128);
    tokio::spawn(async move {
        loop {
            let (tcp, peer) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "tcp accept failed");
                    continue;
                }
            };
            let acceptor = acceptor.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                match acceptor.accept(tcp).await {
                    Ok(stream) => {
                        // A send error just means warp has shut down; drop the conn.
                        let _ = tx.send(Ok::<_, std::io::Error>(stream)).await;
                    }
                    Err(e) => warn!(%peer, error = %e, "tls handshake failed"),
                }
            });
        }
    });

    let incoming = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|conn| (conn, rx))
    });
    warp::serve(route).run_incoming(incoming).await;
}

/// Exactly one of `spec.adminPassword` / `spec.adminPasswordSecretRef` must be
/// set. Returns the denial message when the object violates that.
///
/// The CRD carries the same rule as a CEL validation, which is what covers
/// CREATE — this webhook is only registered for UPDATE. Duplicating it here
/// keeps the invariant unit-testable and holds even on a cluster whose CRD
/// predates the CEL rule.
fn admin_password_violation(instance: &OdooInstance) -> Option<String> {
    match (
        instance.spec.admin_password.is_some(),
        instance.spec.admin_password_secret_ref.is_some(),
    ) {
        (true, false) | (false, true) => None,
        (true, true) => Some(
            "spec.adminPassword and spec.adminPasswordSecretRef are mutually exclusive — set exactly one"
                .to_string(),
        ),
        (false, false) => Some(
            "one of spec.adminPassword or spec.adminPasswordSecretRef must be set".to_string(),
        ),
    }
}

fn workload_layout_violation(instance: &OdooInstance) -> Option<String> {
    (instance.spec.workload_layout == WorkloadLayout::Combined && instance.spec.cron.replicas != 1)
        .then(|| "spec.cron.replicas must be 1 when spec.workloadLayout is combined".to_string())
}

/// Validate an OdooInstance admission request.
fn validate(req: AdmissionRequest<OdooInstance>) -> AdmissionResponse {
    // The master-password XOR is checked for any object the request carries,
    // including CREATE — the rest of the rules below are diffs and need an
    // old object, but this one is a property of the new object alone.
    if let Some(ref new) = req.object {
        if let Some(msg) = admin_password_violation(new) {
            return AdmissionResponse::from(&req).deny(msg);
        }
        if let Err(error) = crate::controller::staging_sleep::validate_spec(new) {
            return AdmissionResponse::from(&req).deny(error.to_string());
        }
        if let Some(msg) = workload_layout_violation(new) {
            return AdmissionResponse::from(&req).deny(msg);
        }
    }

    // CREATE and DELETE are always allowed.
    if req.old_object.is_none() {
        return AdmissionResponse::from(&req);
    }

    let old = match req.old_object {
        Some(ref obj) => obj,
        None => return AdmissionResponse::from(&req),
    };
    let new = match req.object {
        Some(ref obj) => obj,
        None => return AdmissionResponse::from(&req),
    };

    if old.spec.staging_sleep.is_some()
        && old.spec.staging_sleep != new.spec.staging_sleep
        && (old.spec.replicas <= 0
            || new.spec.replicas <= 0
            || !old
                .status
                .as_ref()
                .is_some_and(|s| s.ready && s.phase == Some(OdooInstancePhase::Running)))
    {
        return AdmissionResponse::from(&req).deny(
            "wake staging to Running with replicas > 0 before changing or removing stagingSleep",
        );
    }

    if old.spec.workload_layout != new.spec.workload_layout
        && (old.spec.replicas != 0
            || new.spec.replicas != 0
            || old.status.as_ref().and_then(|status| status.phase.as_ref())
                != Some(&OdooInstancePhase::Stopped))
    {
        return AdmissionResponse::from(&req).deny(
            "spec.workloadLayout can change only from a Stopped instance while old and new spec.replicas are 0",
        );
    }

    // 1. Reject storage size decreases — PVCs cannot shrink.
    if let (Some(old_fs), Some(new_fs)) = (&old.spec.filestore, &new.spec.filestore) {
        if let (Some(old_size), Some(new_size)) = (&old_fs.storage_size, &new_fs.storage_size) {
            if !old_size.is_empty() && !new_size.is_empty() {
                if let Err(msg) = compare_quantities(old_size, new_size) {
                    return AdmissionResponse::from(&req).deny(msg);
                }
            }
        }
    }

    // 2. Reject database cluster changes during unsafe phases.
    //    Allow rollback: changing back to the previous cluster stored in status.
    let old_cluster = old
        .spec
        .database
        .as_ref()
        .and_then(|d| d.cluster.as_deref())
        .unwrap_or("");
    let new_cluster = new
        .spec
        .database
        .as_ref()
        .and_then(|d| d.cluster.as_deref())
        .unwrap_or("");
    if !old_cluster.is_empty() && !new_cluster.is_empty() && new_cluster != old_cluster {
        use crate::crd::odoo_instance::OdooInstancePhase::*;
        let phase = old.status.as_ref().and_then(|s| s.phase.as_ref());
        let prev_cluster = old
            .status
            .as_ref()
            .and_then(|s| s.migration_previous_cluster.as_deref());
        let is_rollback = prev_cluster.is_some_and(|c| c == new_cluster);
        let blocked = !is_rollback
            && matches!(
                phase,
                Some(
                    Restoring
                        | Upgrading
                        | BackingUp
                        | MigratingFilestore
                        | FinalizingFilestoreMigration
                        | MigratingDatabase
                        | FinalizingDatabaseMigration
                        | Uninitialized,
                )
            );
        if blocked {
            return AdmissionResponse::from(&req).deny(format!(
                "spec.database.cluster: cannot change cluster while instance is in {} phase",
                phase.unwrap()
            ));
        }
    }

    // 3. Reject storageClass changes when the instance is in an unsafe phase.
    //    Allow rollback: changing back to the previous SC stored in status.
    let old_class = old
        .spec
        .filestore
        .as_ref()
        .and_then(|f| f.storage_class.as_deref())
        .unwrap_or("");
    let new_class = new
        .spec
        .filestore
        .as_ref()
        .and_then(|f| f.storage_class.as_deref())
        .unwrap_or("");
    if !old_class.is_empty() && !new_class.is_empty() && old_class != new_class {
        use crate::crd::odoo_instance::OdooInstancePhase::*;
        let phase = old.status.as_ref().and_then(|s| s.phase.as_ref());
        let prev_sc = old
            .status
            .as_ref()
            .and_then(|s| s.migration_previous_storage_class.as_deref());
        let is_rollback = prev_sc.is_some_and(|sc| sc == new_class);
        let blocked = !is_rollback
            && matches!(
                phase,
                Some(
                    Restoring
                        | Upgrading
                        | BackingUp
                        | MigratingFilestore
                        | FinalizingFilestoreMigration
                        | Uninitialized,
                )
            );
        if blocked {
            return AdmissionResponse::from(&req).deny(format!(
                "spec.filestore.storageClass: cannot change storage class while instance is in {} phase",
                phase.unwrap()
            ));
        }
    }

    AdmissionResponse::from(&req)
}

/// Compare two Kubernetes quantity strings and reject if new < old.
/// Uses a simplified parser that handles common suffixes (Ki, Mi, Gi, Ti).
fn compare_quantities(old: &str, new: &str) -> Result<(), String> {
    let old_bytes =
        parse_quantity(old).map_err(|e| format!("invalid old quantity {old:?}: {e}"))?;
    let new_bytes =
        parse_quantity(new).map_err(|e| format!("invalid new quantity {new:?}: {e}"))?;

    if new_bytes < old_bytes {
        return Err(format!(
            "spec.filestore.storageSize: cannot decrease storage size from {old} to {new}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::core::admission::AdmissionRequest;

    fn make_instance_json_full(
        db_name: Option<&str>,
        cluster: Option<&str>,
        storage_class: Option<&str>,
    ) -> serde_json::Value {
        let mut db = serde_json::Map::new();
        if let Some(n) = db_name {
            db.insert("name".into(), serde_json::json!(n));
        }
        if let Some(c) = cluster {
            db.insert("cluster".into(), serde_json::json!(c));
        }
        let mut spec = serde_json::json!({
            "adminPassword": "admin",
            "ingress": { "hosts": ["test.example.com"] }
        });
        if !db.is_empty() {
            spec["database"] = serde_json::Value::Object(db);
        }
        if let Some(sc) = storage_class {
            spec["filestore"] = serde_json::json!({ "storageClass": sc });
        }
        serde_json::json!({
            "apiVersion": "bemade.org/v1alpha1",
            "kind": "OdooInstance",
            "metadata": { "name": "test", "namespace": "default", "uid": "test-uid" },
            "spec": spec
        })
    }

    fn make_instance_json(db_name: Option<&str>, cluster: Option<&str>) -> serde_json::Value {
        make_instance_json_full(db_name, cluster, None)
    }

    fn make_update_request(
        old_db_name: Option<&str>,
        old_cluster: Option<&str>,
        new_db_name: Option<&str>,
        new_cluster: Option<&str>,
    ) -> AdmissionRequest<OdooInstance> {
        let review: serde_json::Value = serde_json::json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": {
                "uid": "req-1",
                "kind": { "group": "bemade.org", "version": "v1alpha1", "kind": "OdooInstance" },
                "resource": { "group": "bemade.org", "version": "v1alpha1", "resource": "odooinstances" },
                "name": "test",
                "namespace": "default",
                "operation": "UPDATE",
                "userInfo": { "username": "test" },
                "object": make_instance_json(new_db_name, new_cluster),
                "oldObject": make_instance_json(old_db_name, old_cluster),
                "dryRun": false,
            }
        });
        let ar: kube::core::admission::AdmissionReview<OdooInstance> =
            serde_json::from_value(review).expect("valid AdmissionReview");
        ar.try_into().expect("valid AdmissionRequest")
    }

    fn make_sc_change_request(
        old_class: &str,
        new_class: &str,
        old_phase: Option<&str>,
    ) -> AdmissionRequest<OdooInstance> {
        make_sc_change_request_with_prev(old_class, new_class, old_phase, None)
    }

    fn make_sc_change_request_with_prev(
        old_class: &str,
        new_class: &str,
        old_phase: Option<&str>,
        prev_sc: Option<&str>,
    ) -> AdmissionRequest<OdooInstance> {
        let mut old_obj = make_instance_json_full(None, None, Some(old_class));
        if let Some(phase) = old_phase {
            let mut status = serde_json::json!({"phase": phase});
            if let Some(sc) = prev_sc {
                status["migrationPreviousStorageClass"] = serde_json::json!(sc);
            }
            old_obj["status"] = status;
        }
        let review: serde_json::Value = serde_json::json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": {
                "uid": "req-sc",
                "kind": { "group": "bemade.org", "version": "v1alpha1", "kind": "OdooInstance" },
                "resource": { "group": "bemade.org", "version": "v1alpha1", "resource": "odooinstances" },
                "name": "test",
                "namespace": "default",
                "operation": "UPDATE",
                "userInfo": { "username": "test" },
                "object": make_instance_json_full(None, None, Some(new_class)),
                "oldObject": old_obj,
                "dryRun": false,
            }
        });
        let ar: kube::core::admission::AdmissionReview<OdooInstance> =
            serde_json::from_value(review).expect("valid AdmissionReview");
        ar.try_into().expect("valid AdmissionRequest")
    }

    fn make_cluster_change_request(
        old_cluster: &str,
        new_cluster: &str,
        old_phase: Option<&str>,
    ) -> AdmissionRequest<OdooInstance> {
        let mut old_obj = make_instance_json(None, Some(old_cluster));
        if let Some(phase) = old_phase {
            old_obj["status"] = serde_json::json!({"phase": phase});
        }
        let review: serde_json::Value = serde_json::json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": {
                "uid": "req-cluster",
                "kind": { "group": "bemade.org", "version": "v1alpha1", "kind": "OdooInstance" },
                "resource": { "group": "bemade.org", "version": "v1alpha1", "resource": "odooinstances" },
                "name": "test",
                "namespace": "default",
                "operation": "UPDATE",
                "userInfo": { "username": "test" },
                "object": make_instance_json(None, Some(new_cluster)),
                "oldObject": old_obj,
                "dryRun": false,
            }
        });
        let ar: kube::core::admission::AdmissionReview<OdooInstance> =
            serde_json::from_value(review).expect("valid AdmissionReview");
        ar.try_into().expect("valid AdmissionRequest")
    }

    fn make_layout_change_request(
        old_replicas: i32,
        new_replicas: i32,
        old_phase: &str,
        cron_replicas: i32,
    ) -> AdmissionRequest<OdooInstance> {
        let mut old_obj = make_instance_json(None, None);
        old_obj["spec"]["replicas"] = serde_json::json!(old_replicas);
        old_obj["spec"]["workloadLayout"] = serde_json::json!("separate");
        old_obj["status"] = serde_json::json!({"phase": old_phase});
        let mut new_obj = make_instance_json(None, None);
        new_obj["spec"]["replicas"] = serde_json::json!(new_replicas);
        new_obj["spec"]["workloadLayout"] = serde_json::json!("combined");
        new_obj["spec"]["cron"] = serde_json::json!({"replicas": cron_replicas});
        let review = serde_json::json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": {
                "uid": "req-layout",
                "kind": { "group": "bemade.org", "version": "v1alpha1", "kind": "OdooInstance" },
                "resource": { "group": "bemade.org", "version": "v1alpha1", "resource": "odooinstances" },
                "name": "test",
                "namespace": "default",
                "operation": "UPDATE",
                "userInfo": { "username": "test" },
                "object": new_obj,
                "oldObject": old_obj,
                "dryRun": false,
            }
        });
        let review: kube::core::admission::AdmissionReview<OdooInstance> =
            serde_json::from_value(review).expect("valid AdmissionReview");
        review.try_into().expect("valid AdmissionRequest")
    }

    #[test]
    fn test_parse_quantity() {
        assert_eq!(parse_quantity("2Gi").unwrap(), 2 * 1024 * 1024 * 1024);
        assert_eq!(parse_quantity("10Gi").unwrap(), 10 * 1024 * 1024 * 1024);
        assert_eq!(parse_quantity("500Mi").unwrap(), 500 * 1024 * 1024);
        assert_eq!(parse_quantity("1Ti").unwrap(), 1024 * 1024 * 1024 * 1024);
        assert_eq!(parse_quantity("100").unwrap(), 100);
    }

    #[test]
    fn test_compare_quantities_allows_increase() {
        assert!(compare_quantities("2Gi", "10Gi").is_ok());
        assert!(compare_quantities("2Gi", "2Gi").is_ok());
    }

    #[test]
    fn test_compare_quantities_rejects_decrease() {
        assert!(compare_quantities("10Gi", "2Gi").is_err());
        assert!(compare_quantities("1Gi", "500Mi").is_err());
    }

    #[test]
    fn test_validate_allows_normal_update() {
        let req = make_update_request(Some("mydb"), None, Some("mydb"), None);
        let resp = validate(req);
        assert!(resp.allowed);
    }

    #[test]
    fn test_validate_rejects_combined_with_nonstandard_cron_replicas() {
        let response = validate(make_layout_change_request(0, 0, "Stopped", 2));
        assert!(!response.allowed);
    }

    #[test]
    fn test_validate_rejects_layout_change_before_stopped_boundary() {
        for request in [
            make_layout_change_request(1, 0, "Running", 1),
            make_layout_change_request(0, 0, "Running", 1),
            make_layout_change_request(0, 1, "Stopped", 1),
        ] {
            assert!(!validate(request).allowed);
        }
    }

    #[test]
    fn test_validate_allows_layout_change_while_stopped_at_zero() {
        assert!(validate(make_layout_change_request(0, 0, "Stopped", 1)).allowed);
    }

    #[test]
    fn test_validate_allows_cluster_change_when_running() {
        let req = make_update_request(None, Some("pg-cluster-a"), None, Some("pg-cluster-b"));
        let resp = validate(req);
        assert!(
            resp.allowed,
            "cluster change should be allowed (no phase = safe state)"
        );
    }

    #[test]
    fn test_validate_rejects_cluster_change_when_restoring() {
        let req = make_cluster_change_request("pg-a", "pg-b", Some("Restoring"));
        let resp = validate(req);
        assert!(
            !resp.allowed,
            "cluster change should be rejected when Restoring"
        );
    }

    #[test]
    fn test_validate_rejects_cluster_change_when_migrating_db() {
        let req = make_cluster_change_request("pg-a", "pg-b", Some("MigratingDatabase"));
        let resp = validate(req);
        assert!(
            !resp.allowed,
            "cluster change should be rejected when already migrating"
        );
    }

    #[test]
    fn test_validate_allows_storage_class_change_when_running() {
        let req = make_sc_change_request("cephfs", "juicefs", Some("Running"));
        let resp = validate(req);
        assert!(
            resp.allowed,
            "storageClass change should be allowed when Running"
        );
    }

    #[test]
    fn test_validate_allows_storage_class_change_when_stopped() {
        let req = make_sc_change_request("cephfs", "juicefs", Some("Stopped"));
        let resp = validate(req);
        assert!(
            resp.allowed,
            "storageClass change should be allowed when Stopped"
        );
    }

    #[test]
    fn test_validate_rejects_storage_class_change_when_restoring() {
        let req = make_sc_change_request("cephfs", "juicefs", Some("Restoring"));
        let resp = validate(req);
        assert!(
            !resp.allowed,
            "storageClass change should be rejected when Restoring"
        );
    }

    #[test]
    fn test_validate_rejects_storage_class_change_when_upgrading() {
        let req = make_sc_change_request("cephfs", "juicefs", Some("Upgrading"));
        let resp = validate(req);
        assert!(
            !resp.allowed,
            "storageClass change should be rejected when Upgrading"
        );
    }

    #[test]
    fn test_validate_rejects_storage_class_change_when_backing_up() {
        let req = make_sc_change_request("cephfs", "juicefs", Some("BackingUp"));
        let resp = validate(req);
        assert!(
            !resp.allowed,
            "storageClass change should be rejected when BackingUp"
        );
    }

    #[test]
    fn test_validate_rejects_storage_class_change_when_migrating() {
        let req = make_sc_change_request("cephfs", "juicefs", Some("MigratingFilestore"));
        let resp = validate(req);
        assert!(
            !resp.allowed,
            "storageClass change should be rejected when already migrating"
        );
    }

    #[test]
    fn test_validate_rejects_storage_class_change_when_uninitialized() {
        let req = make_sc_change_request("cephfs", "juicefs", Some("Uninitialized"));
        let resp = validate(req);
        assert!(
            !resp.allowed,
            "storageClass change should be rejected when Uninitialized"
        );
    }

    #[test]
    fn test_validate_allows_rollback_during_migration() {
        // Reverting to the previous SC (stored in status) should be allowed
        // even during MigratingFilestore.
        let req = make_sc_change_request_with_prev(
            "juicefs",
            "cephfs",
            Some("MigratingFilestore"),
            Some("cephfs"),
        );
        let resp = validate(req);
        assert!(
            resp.allowed,
            "rollback to previous storageClass should be allowed during migration"
        );
    }

    // ── master-password XOR ─────────────────────────────────────────────

    /// Build an UPDATE request whose new object carries the given
    /// master-password shape. The old object is always the plaintext form, so
    /// only the new object's shape is under test.
    fn make_admin_password_request(
        plaintext: bool,
        secret_ref: bool,
    ) -> AdmissionRequest<OdooInstance> {
        let mut spec = serde_json::json!({
            "ingress": { "hosts": ["test.example.com"] }
        });
        if plaintext {
            spec["adminPassword"] = serde_json::json!("admin");
        }
        if secret_ref {
            spec["adminPasswordSecretRef"] = serde_json::json!({ "name": "pw", "key": "password" });
        }
        let new_obj = serde_json::json!({
            "apiVersion": "bemade.org/v1alpha1",
            "kind": "OdooInstance",
            "metadata": { "name": "test", "namespace": "default", "uid": "test-uid" },
            "spec": spec
        });
        let review: serde_json::Value = serde_json::json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": {
                "uid": "req-pw",
                "kind": { "group": "bemade.org", "version": "v1alpha1", "kind": "OdooInstance" },
                "resource": { "group": "bemade.org", "version": "v1alpha1", "resource": "odooinstances" },
                "name": "test",
                "namespace": "default",
                "operation": "UPDATE",
                "userInfo": { "username": "test" },
                "object": new_obj,
                "oldObject": make_instance_json(None, None),
                "dryRun": false,
            }
        });
        let ar: kube::core::admission::AdmissionReview<OdooInstance> =
            serde_json::from_value(review).expect("valid AdmissionReview");
        ar.try_into().expect("valid AdmissionRequest")
    }

    #[test]
    fn test_admin_password_plaintext_only_is_allowed() {
        let resp = validate(make_admin_password_request(true, false));
        assert!(
            resp.allowed,
            "plaintext adminPassword alone must be allowed"
        );
    }

    #[test]
    fn test_admin_password_secret_ref_only_is_allowed() {
        let resp = validate(make_admin_password_request(false, true));
        assert!(resp.allowed, "adminPasswordSecretRef alone must be allowed");
    }

    #[test]
    fn test_admin_password_both_is_rejected() {
        let resp = validate(make_admin_password_request(true, true));
        assert!(
            !resp.allowed,
            "setting both adminPassword and adminPasswordSecretRef must be rejected"
        );
    }

    #[test]
    fn test_admin_password_neither_is_rejected() {
        let resp = validate(make_admin_password_request(false, false));
        assert!(
            !resp.allowed,
            "setting neither adminPassword nor adminPasswordSecretRef must be rejected"
        );
    }

    #[test]
    fn test_admin_password_xor_precedes_other_rules() {
        // A request that ALSO shrinks storage must still be denied; the point
        // is that the XOR check runs before the early return for CREATE, so it
        // cannot be bypassed by omitting an old object.
        let violation = make_admin_password_request(true, true);
        assert!(!validate(violation).allowed);
    }

    #[test]
    fn test_validate_rejects_non_rollback_during_migration() {
        // Changing to a DIFFERENT SC (not the previous one) during migration
        // should still be rejected.
        let req = make_sc_change_request_with_prev(
            "juicefs",
            "longhorn",
            Some("MigratingFilestore"),
            Some("cephfs"),
        );
        let resp = validate(req);
        assert!(
            !resp.allowed,
            "changing to a third storageClass during migration should be rejected"
        );
    }
    #[test]
    fn staging_sleep_disable_requires_awake_running_instance() {
        for (replicas, ready, allowed) in [(0, false, false), (1, false, false), (1, true, true)] {
            let mut req = make_layout_change_request(replicas, replicas, "Running", 1);
            let old = req.old_object.as_mut().unwrap();
            old.spec.staging_sleep = Some(crate::crd::odoo_instance::StagingSleepSpec {
                database_cluster: "db".into(),
                mode: Default::default(),
            });
            old.status.as_mut().unwrap().ready = ready;
            req.object.as_mut().unwrap().spec.workload_layout = old.spec.workload_layout;
            assert_eq!(validate(req).allowed, allowed);
        }
    }

    #[test]
    fn staging_sleep_requires_platform_staging_identity_not_odoo_environment() {
        let mut req = make_update_request(None, None, None, None);
        let new = req.object.as_mut().unwrap();
        new.spec.environment = crate::crd::odoo_instance::Environment::Production;
        new.spec.staging_sleep = Some(crate::crd::odoo_instance::StagingSleepSpec {
            database_cluster: "db".into(),
            mode: Default::default(),
        });
        new.spec.database =
            Some(serde_json::from_value(serde_json::json!({"cluster": "db"})).unwrap());
        assert!(!validate(req.clone()).allowed);
        req.object.as_mut().unwrap().metadata.labels = Some(std::collections::BTreeMap::from([(
            "droggol.sh/instance-kind".into(),
            "staging".into(),
        )]));
        assert!(validate(req).allowed);
    }
}
