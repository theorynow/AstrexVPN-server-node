use crate::features::hub::domain::ports::{
    hysteria_commander::HysteriaCommander, xray_commander::XrayCommander,
};
use std::collections::HashMap;
use std::sync::Arc;

pub struct ReportTrafficCommand {
    xray_client: Arc<dyn XrayCommander>,
    hysteria_client: Arc<dyn HysteriaCommander>,
}

impl ReportTrafficCommand {
    pub fn new(
        xray_client: Arc<dyn XrayCommander>,
        hysteria_client: Arc<dyn HysteriaCommander>,
    ) -> Self {
        Self {
            xray_client,
            hysteria_client,
        }
    }

    pub async fn execute(&self) -> Result<HashMap<String, u64>, String> {
        let mut aggregated = HashMap::new();

        // 1. Fetch and aggregate Xray statistics
        match self.xray_client.query_user_stats().await {
            Ok(xray_stats) => {
                for (user, bytes) in xray_stats {
                    *aggregated.entry(user).or_insert(0) += bytes;
                }
            }
            Err(e) => {
                return Err(format!("ReportTraffic failed to query Xray stats: {}", e));
            }
        }

        // 2. Fetch and aggregate Hysteria2 statistics
        match self.hysteria_client.get_traffic_stats().await {
            Ok(hysteria_stats) => {
                for (user, bytes) in hysteria_stats {
                    *aggregated.entry(user).or_insert(0) += bytes;
                }
            }
            Err(e) => {
                return Err(format!(
                    "ReportTraffic failed to query Hysteria2 stats: {}",
                    e
                ));
            }
        }

        Ok(aggregated)
    }
}
