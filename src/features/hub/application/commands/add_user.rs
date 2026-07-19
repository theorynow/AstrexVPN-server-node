use crate::features::hub::domain::ports::{
    hysteria_commander::HysteriaCommander, xray_commander::XrayCommander,
};
use std::sync::Arc;

pub struct AddUserCommand {
    xray_client: Arc<dyn XrayCommander>,
    hysteria_client: Arc<dyn HysteriaCommander>,
}

impl AddUserCommand {
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
        let mut added_xray_tags = Vec::new();

        // 1. Add user to all requested Xray inbounds
        for tag in inbound_tags {
            match self.xray_client.add_user(tag, uuid, uuid).await {
                Ok(_) => {
                    added_xray_tags.push(tag.clone());
                }
                Err(e) => {
                    // Rollback Xray additions
                    for added_tag in added_xray_tags {
                        let _ = self.xray_client.remove_user(&added_tag, uuid).await;
                    }
                    return Err(format!("Xray failed to add user on tag {}: {}", tag, e));
                }
            }
        }

        // 2. Add user to Hysteria2
        if let Err(e) = self.hysteria_client.add_user(uuid).await {
            // Rollback Xray additions
            for added_tag in added_xray_tags {
                let _ = self.xray_client.remove_user(&added_tag, uuid).await;
            }
            return Err(format!("Hysteria2 failed to add user: {}", e));
        }

        Ok(())
    }
}
