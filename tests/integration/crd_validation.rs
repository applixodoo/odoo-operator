//! Apiserver-level validation of the fork's CEL rules.
//!
//! These assert against a *real* API server rather than the operator, which is
//! the only place the CEL rules actually run. Two things are proven here that
//! no unit test can:
//!   1. the rules are structurally valid and within the apiserver's CEL cost
//!      budget — an over-budget rule makes the whole CRD unappliable;
//!   2. they reject on CREATE, which matters because the validating webhook is
//!      registered for UPDATE only.

use kube::api::{Api, DeleteParams, PostParams};
use serde_json::json;

use super::common::*;
use odoo_operator::crd::odoo_instance::OdooInstance;

/// Build an OdooInstance body, then apply `mutate` to its `spec`.
fn instance_with_spec(
    name: &str,
    ns: &str,
    mutate: impl FnOnce(&mut serde_json::Value),
) -> serde_json::Value {
    let mut obj = test_instance_json(name, ns, 1);
    mutate(&mut obj["spec"]);
    obj
}

async fn try_create(
    client: &kube::Client,
    ns: &str,
    body: serde_json::Value,
) -> Result<(), String> {
    let api: Api<OdooInstance> = Api::namespaced(client.clone(), ns);
    let inst: OdooInstance = serde_json::from_value(body).expect("valid OdooInstance json");
    match api.create(&PostParams::default(), &inst).await {
        Ok(created) => {
            // Clean up so the operator does not start reconciling it.
            let name = created.metadata.name.clone().unwrap_or_default();
            let _ = api.delete(&name, &DeleteParams::default()).await;
            Ok(())
        }
        Err(e) => Err(e.to_string()),
    }
}

#[tokio::test]
async fn admin_password_plaintext_alone_is_accepted() {
    let ctx = TestContext::new_ns().await;
    let body = instance_with_spec("pw-plain", &ctx.ns, |_| {});
    try_create(&ctx.client, &ctx.ns, body)
        .await
        .expect("plaintext adminPassword alone must be accepted");
}

#[tokio::test]
async fn admin_password_secret_ref_alone_is_accepted() {
    let ctx = TestContext::new_ns().await;
    let body = instance_with_spec("pw-ref", &ctx.ns, |spec| {
        spec.as_object_mut().unwrap().remove("adminPassword");
        spec["adminPasswordSecretRef"] = json!({ "name": "pw", "key": "password" });
    });
    try_create(&ctx.client, &ctx.ns, body)
        .await
        .expect("adminPasswordSecretRef alone must be accepted");
}

#[tokio::test]
async fn admin_password_both_is_rejected_on_create() {
    let ctx = TestContext::new_ns().await;
    let body = instance_with_spec("pw-both", &ctx.ns, |spec| {
        spec["adminPasswordSecretRef"] = json!({ "name": "pw", "key": "password" });
    });
    let err = try_create(&ctx.client, &ctx.ns, body)
        .await
        .expect_err("setting both password sources must be rejected");
    assert!(
        err.contains("exactly one of spec.adminPassword"),
        "unexpected rejection message: {err}"
    );
}

#[tokio::test]
async fn admin_password_neither_is_rejected_on_create() {
    let ctx = TestContext::new_ns().await;
    let body = instance_with_spec("pw-none", &ctx.ns, |spec| {
        spec.as_object_mut().unwrap().remove("adminPassword");
    });
    let err = try_create(&ctx.client, &ctx.ns, body)
        .await
        .expect_err("omitting both password sources must be rejected");
    assert!(
        err.contains("exactly one of spec.adminPassword"),
        "unexpected rejection message: {err}"
    );
}

#[tokio::test]
async fn valid_source_volume_is_accepted() {
    let ctx = TestContext::new_ns().await;
    let body = instance_with_spec("sv-ok", &ctx.ns, |spec| {
        spec["sourceVolume"] = json!({
            "claimName": "prod-src",
            "mounts": [
                { "mountPath": "/work" },
                { "mountPath": "/build", "subPath": "build" },
            ],
            "odooBin": "/work/instances/prod/odoo/odoo-bin",
        });
    });
    try_create(&ctx.client, &ctx.ns, body)
        .await
        .expect("a well-formed sourceVolume must be accepted");
}

#[tokio::test]
async fn custom_source_requires_nonempty_absolute_read_only_mounts() {
    let ctx = TestContext::new_ns().await;
    for (name, mounts, accepted) in [
        (
            "custom-ok",
            json!([{ "mountPath": "/custom", "readOnly": true }]),
            true,
        ),
        ("custom-empty", json!([]), false),
        (
            "custom-relative",
            json!([{ "mountPath": "custom", "readOnly": true }]),
            false,
        ),
        (
            "custom-writable",
            json!([{ "mountPath": "/custom", "readOnly": false }]),
            false,
        ),
    ] {
        let body = instance_with_spec(name, &ctx.ns, |spec| {
            spec["customSourceVolume"] =
                json!({ "claimName": "prod-custom-source", "mounts": mounts });
        });
        let result = try_create(&ctx.client, &ctx.ns, body).await;
        assert_eq!(result.is_ok(), accepted, "{name}: {result:?}");
        if let Err(error) = result {
            assert!(error.contains("spec.customSourceVolume"), "{error}");
        }
    }
}

#[tokio::test]
async fn source_volume_with_no_mounts_is_rejected() {
    let ctx = TestContext::new_ns().await;
    let body = instance_with_spec("sv-empty", &ctx.ns, |spec| {
        spec["sourceVolume"] = json!({
            "claimName": "prod-src",
            "mounts": [],
            "odooBin": "/work/instances/prod/odoo/odoo-bin",
        });
    });
    let err = try_create(&ctx.client, &ctx.ns, body)
        .await
        .expect_err("an empty mounts list must be rejected");
    assert!(
        err.contains("mounts must not be empty"),
        "unexpected rejection message: {err}"
    );
}

#[tokio::test]
async fn source_volume_with_a_relative_odoo_bin_is_rejected() {
    let ctx = TestContext::new_ns().await;
    let body = instance_with_spec("sv-rel", &ctx.ns, |spec| {
        spec["sourceVolume"] = json!({
            "claimName": "prod-src",
            "mounts": [{ "mountPath": "/work" }],
            "odooBin": "instances/prod/odoo/odoo-bin",
        });
    });
    let err = try_create(&ctx.client, &ctx.ns, body)
        .await
        .expect_err("a relative odooBin must be rejected");
    assert!(
        err.contains("odooBin must be an absolute path"),
        "unexpected rejection message: {err}"
    );
}

#[tokio::test]
async fn source_volume_with_whitespace_in_odoo_bin_is_rejected() {
    // odooBin reaches the neutralize scripts through an environment variable
    // the shell word-splits; quoting it there is impossible, so whitespace has
    // to be rejected at admission rather than silently splitting into two args.
    let ctx = TestContext::new_ns().await;
    let body = instance_with_spec("sv-space", &ctx.ns, |spec| {
        spec["sourceVolume"] = json!({
            "claimName": "prod-src",
            "mounts": [{ "mountPath": "/work" }],
            "odooBin": "/work/my instances/odoo-bin",
        });
    });
    let err = try_create(&ctx.client, &ctx.ns, body)
        .await
        .expect_err("whitespace in odooBin must be rejected");
    assert!(
        err.contains("odooBin must not contain whitespace"),
        "unexpected rejection message: {err}"
    );
}

#[tokio::test]
async fn source_volume_with_a_relative_mount_path_is_rejected() {
    let ctx = TestContext::new_ns().await;
    let body = instance_with_spec("sv-relmount", &ctx.ns, |spec| {
        spec["sourceVolume"] = json!({
            "claimName": "prod-src",
            "mounts": [{ "mountPath": "work" }],
            "odooBin": "/work/instances/prod/odoo/odoo-bin",
        });
    });
    let err = try_create(&ctx.client, &ctx.ns, body)
        .await
        .expect_err("a relative mountPath must be rejected");
    assert!(
        err.contains("mountPath must be an absolute path"),
        "unexpected rejection message: {err}"
    );
}
