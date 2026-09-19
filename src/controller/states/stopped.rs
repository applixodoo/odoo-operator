use async_trait::async_trait;
use kube::api::ResourceExt;

use crate::crd::odoo_instance::OdooInstance;
use crate::error::Result;

use super::{Context, ReconcileSnapshot, State};
use crate::controller::state_machine::scale_serving_deployments;

/// Stopped: user set replicas to 0.
pub struct Stopped;

#[async_trait]
impl State for Stopped {
    async fn ensure(
        &self,
        instance: &OdooInstance,
        ctx: &Context,
        _snap: &ReconcileSnapshot,
    ) -> Result<()> {
        let ns = instance.namespace().unwrap_or_default();
        scale_serving_deployments(&ctx.client, instance, &ns, 0, 0).await
    }
}
