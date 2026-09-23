use async_trait::async_trait;
use kube::api::ResourceExt;

use crate::crd::odoo_instance::OdooInstance;
use crate::error::Result;

use super::cloning_from_source::maybe_retry_failed_neutralize;
use super::{Context, ReconcileSnapshot, State};
use crate::controller::state_machine::scale_serving_deployments;

/// InitFailed: init job failed.  Deployment stays down.
pub struct InitFailed;

#[async_trait]
impl State for InitFailed {
    async fn ensure(
        &self,
        instance: &OdooInstance,
        ctx: &Context,
        _snap: &ReconcileSnapshot,
    ) -> Result<()> {
        let ns = instance.namespace().unwrap_or_default();
        scale_serving_deployments(&ctx.client, instance, &ns, 0, 0).await?;
        // Spec-drift retry: if the user corrected the OdooInstance image
        // after a neutralize failure, clear the failed refresh CR's
        // neutralize status so the InitFailed → CloningFromSource recovery
        // transition fires on the next snapshot pass.  Idempotent: a no-op
        // when the recorded hash already matches the live image.
        let _ = maybe_retry_failed_neutralize(instance, ctx).await?;
        Ok(())
    }
}
