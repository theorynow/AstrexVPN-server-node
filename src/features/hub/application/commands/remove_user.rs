use crate::features::hub::domain::ports::{
    hysteria_commander::HysteriaCommander, xray_commander::XrayCommander,
};
use std::sync::Arc;

pub struct RemoveUserCommand {
    xray_client: Arc<dyn XrayCommander>,
    hysteria_client: Arc<dyn HysteriaCommander>,
}

impl RemoveUserCommand {
    pub fn new(
        xray_client: Arc<dyn XrayCommander>,
        hysteria_client: Arc<dyn HysteriaCommander>,
    ) -> Self {
        Self {
            xray_client,
            hysteria_client,
        }
    }

    pub async fn execute(&self, uuid: &str, inbound_tags: &[String]) -> Result<(), String> {
        let mut errors = Vec::new();

        // 1. Remove user from all requested Xray inbounds idempotently
        for tag in inbound_tags {
            if let Err(e) = self.xray_client.remove_user(tag, uuid).await {
                errors.push(format!("Xray tag {}: {}", tag, e));
            }
        }

        // 2. Remove user from Hysteria2 idempotently
        if let Err(e) = self.hysteria_client.remove_user(uuid).await {
            errors.push(format!("Hysteria2: {}", e));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(format!("Errors during user removal: {}", errors.join("; ")))
        }
    }
}
