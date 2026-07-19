use crate::features::hub::domain::ports::{
    hysteria_commander::HysteriaCommander, xray_commander::XrayCommander,
};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeHealth {
    Online,
    Degraded(String),
}

pub struct HealthCheckQuery {
    xray_client: Arc<dyn XrayCommander>,
    hysteria_client: Arc<dyn HysteriaCommander>,
}

impl HealthCheckQuery {
    pub fn new(
        xray_client: Arc<dyn XrayCommander>,
        hysteria_client: Arc<dyn HysteriaCommander>,
    ) -> Self {
        Self {
            xray_client,
            hysteria_client,
        }
    }

    pub async fn execute(&self) -> NodeHealth {
        let xray_ok = self.xray_client.ping().await;
        let hysteria_ok = self.hysteria_client.ping().await;

        match (xray_ok, hysteria_ok) {
            (true, true) => NodeHealth::Online,
            (false, true) => NodeHealth::Degraded("Xray API is not responding".to_string()),
            (true, false) => NodeHealth::Degraded("Hysteria API is not responding".to_string()),
            (false, false) => {
                NodeHealth::Degraded("Both Xray and Hysteria APIs are not responding".to_string())
            }
        }
    }
}
