use crate::features::hub::domain::ports::hysteria_commander::HysteriaCommander;
use async_trait::async_trait;
use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use parking_lot::RwLock;
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

pub type UserMap = Arc<RwLock<HashMap<String, ()>>>;

#[derive(Deserialize, Debug)]
pub struct HysteriaAuthRequest {
    pub auth: String,
    #[serde(default)]
    pub addr: String,
}

pub async fn auth_handler(
    State(users): State<UserMap>,
    Json(payload): Json<HysteriaAuthRequest>,
) -> StatusCode {
    let is_valid = users.read().contains_key(&payload.auth);
    if is_valid {
        StatusCode::OK
    } else {
        StatusCode::UNAUTHORIZED
    }
}

pub struct HysteriaClient {
    api_url: String,
    http_client: Client,
    users: UserMap,
    last_stats: std::sync::Mutex<HashMap<String, u64>>,
}

impl HysteriaClient {
    pub fn new(api_url: String, auth_addr: String) -> Self {
        let users: UserMap = Arc::new(RwLock::new(HashMap::new()));
        let users_clone = users.clone();

        tokio::spawn(async move {
            let app = Router::new()
                .route("/auth", post(auth_handler))
                .with_state(users_clone);

            match tokio::net::TcpListener::bind(&auth_addr).await {
                Ok(listener) => {
                    info!("🚀 Hysteria2 Auth Server listening on {}", auth_addr);
                    if let Err(e) = axum::serve(listener, app).await {
                        tracing::error!("Hysteria2 Auth Server error: {}", e);
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to bind Hysteria2 Auth Server on {}: {}",
                        auth_addr,
                        e
                    );
                }
            }
        });

        Self {
            api_url,
            http_client: Client::new(),
            users,
            last_stats: std::sync::Mutex::new(HashMap::new()),
        }
    }

    pub fn users(&self) -> &UserMap {
        &self.users
    }
}

#[async_trait]
impl HysteriaCommander for HysteriaClient {
    async fn add_user(&self, uuid: &str) -> Result<(), String> {
        // 1. Add to In-Memory UserMap
        {
            let mut map = self.users.write();
            map.insert(uuid.to_string(), ());
        }
        info!("➕ User {} added to Hysteria2 memory database", uuid);

        Ok(())
    }

    async fn remove_user(&self, uuid: &str) -> Result<(), String> {
        // 1. Remove from In-Memory UserMap
        {
            let mut map = self.users.write();
            map.remove(uuid);
        }
        info!("➖ User {} removed from Hysteria2 memory database", uuid);

        // 2. Call Hysteria2 Kick API to terminate any active connections immediately
        let url = format!("{}/kick", self.api_url);
        let payload = vec![uuid];

        match self.http_client.post(&url).json(&payload).send().await {
            Ok(res) if res.status().is_success() => {
                info!("✅ User {} successfully kicked from Hysteria2", uuid);
            }
            Ok(res) => {
                let status = res.status();
                let err_body = res.text().await.unwrap_or_default();
                if status == reqwest::StatusCode::NOT_FOUND
                    || err_body.contains("not found")
                    || err_body.contains("does not exist")
                {
                    return Ok(());
                }
                warn!(
                    "⚠️ Hysteria2 returned status {} on kick for {}: {}",
                    status, uuid, err_body
                );
            }
            Err(e) => {
                warn!(
                    "❌ Failed to connect to Hysteria2 Kick API: {}. Skipping Hysteria2 kick.",
                    e
                );
            }
        }

        Ok(())
    }

    async fn get_traffic_stats(&self) -> Result<HashMap<String, u64>, String> {
        let url = format!("{}/traffic", self.api_url.trim_end_matches('/'));
        let resp = match self.http_client.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(
                    "Hysteria2 API is offline or unreachable on {}: {}. Skipping Hysteria traffic stats collection.",
                    url, e
                );
                return Ok(HashMap::new());
            }
        };

        if !resp.status().is_success() {
            warn!(
                "Hysteria2 /traffic returned status {}, skipping Hysteria traffic stats collection.",
                resp.status()
            );
            return Ok(HashMap::new());
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

        let mut last = self.last_stats.lock().unwrap();
        let mut delta_bytes = HashMap::new();

        for (auth, total) in user_bytes {
            let prev = last.get(&auth).copied().unwrap_or(0);
            if total >= prev {
                let delta = total - prev;
                if delta > 0 {
                    delta_bytes.insert(auth.clone(), delta);
                }
            } else {
                // Hysteria restarted or counters reset
                if total > 0 {
                    delta_bytes.insert(auth.clone(), total);
                }
            }
            last.insert(auth, total);
        }

        Ok(delta_bytes)
    }

    async fn ping(&self) -> bool {
        let url = format!("{}/traffic", self.api_url.trim_end_matches('/'));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_hysteria_auth_handler() {
        let users: UserMap = Arc::new(RwLock::new(HashMap::new()));
        users.write().insert("valid-uuid".to_string(), ());

        let valid_req = HysteriaAuthRequest {
            auth: "valid-uuid".to_string(),
            addr: "1.2.3.4:5678".to_string(),
        };
        let status = auth_handler(State(users.clone()), Json(valid_req)).await;
        assert_eq!(status, StatusCode::OK);

        let invalid_req = HysteriaAuthRequest {
            auth: "invalid-uuid".to_string(),
            addr: "1.2.3.4:5678".to_string(),
        };
        let status_invalid = auth_handler(State(users), Json(invalid_req)).await;
        assert_eq!(status_invalid, StatusCode::UNAUTHORIZED);
    }
}
