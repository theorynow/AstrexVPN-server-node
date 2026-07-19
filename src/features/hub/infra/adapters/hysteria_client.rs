use crate::features::hub::domain::ports::hysteria_commander::HysteriaCommander;
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub struct HysteriaClient {
    api_url: String,
    http_client: Client,
}

#[derive(Serialize)]
struct HysteriaUserPayload {
    auth: String,
}

#[derive(Deserialize, Clone)]
pub struct TrafficStats {
    pub tx: u64,
    pub rx: u64,
}

impl HysteriaClient {
    pub fn new(api_url: String) -> Self {
        Self {
            api_url,
            http_client: Client::new(),
        }
    }
}

#[async_trait]
impl HysteriaCommander for HysteriaClient {
    async fn add_user(&self, uuid: &str) -> Result<(), String> {
        let url = format!("{}/v1/users", self.api_url);
        let payload = HysteriaUserPayload {
            auth: uuid.to_string(),
        };

        let resp = self
            .http_client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("Hysteria API post error: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = resp.text().await.unwrap_or_default();
            return Err(format!(
                "Hysteria rejected add: status {}, body: {}",
                status, err_body
            ));
        }

        Ok(())
    }

    async fn remove_user(&self, uuid: &str) -> Result<(), String> {
        let url = format!("{}/v1/users", self.api_url);
        let payload = HysteriaUserPayload {
            auth: uuid.to_string(),
        };

        let resp = self
            .http_client
            .delete(&url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("Hysteria API delete error: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err_body = resp.text().await.unwrap_or_default();
            if status == reqwest::StatusCode::NOT_FOUND
                || err_body.contains("not found")
                || err_body.contains("does not exist")
            {
                return Ok(());
            }
            return Err(format!(
                "Hysteria rejected delete: status {}, body: {}",
                status, err_body
            ));
        }

        Ok(())
    }

    async fn get_traffic_stats(&self) -> Result<HashMap<String, u64>, String> {
        let url = format!("{}/v1/stats", self.api_url);
        let resp = self
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Hysteria API stats error: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("Hysteria rejected stats: status {}", resp.status()));
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("Hysteria stats JSON decode error: {}", e))?;

        let mut user_bytes = HashMap::new();

        if let Some(traffic) = body.get("traffic").and_then(|t| t.as_object()) {
            for (auth, stats_val) in traffic {
                let tx = stats_val.get("tx").and_then(|v| v.as_u64()).unwrap_or(0);
                let rx = stats_val.get("rx").and_then(|v| v.as_u64()).unwrap_or(0);
                user_bytes.insert(auth.clone(), tx + rx);
            }
        } else if let Some(obj) = body.as_object() {
            for (auth, stats_val) in obj {
                if auth == "traffic" {
                    continue;
                }
                if let Some(tx) = stats_val.get("tx").and_then(|v| v.as_u64()) {
                    let rx = stats_val.get("rx").and_then(|v| v.as_u64()).unwrap_or(0);
                    user_bytes.insert(auth.clone(), tx + rx);
                }
            }
        }

        Ok(user_bytes)
    }

    async fn ping(&self) -> bool {
        let url = format!("{}/v1/stats", self.api_url);
        match self.http_client.get(&url).send().await {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        }
    }
}

pub struct NoopHysteriaClient;

#[async_trait]
impl HysteriaCommander for NoopHysteriaClient {
    async fn add_user(&self, _uuid: &str) -> Result<(), String> {
        Ok(())
    }
    async fn remove_user(&self, _uuid: &str) -> Result<(), String> {
        Ok(())
    }
    async fn get_traffic_stats(&self) -> Result<HashMap<String, u64>, String> {
        Ok(HashMap::new())
    }
    async fn ping(&self) -> bool {
        true
    }
}
