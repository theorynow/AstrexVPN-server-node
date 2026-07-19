use async_trait::async_trait;
use std::collections::HashMap;

#[async_trait]
pub trait HysteriaCommander: Send + Sync {
    async fn add_user(&self, uuid: &str) -> Result<(), String>;
    async fn remove_user(&self, uuid: &str) -> Result<(), String>;
    async fn get_traffic_stats(&self) -> Result<HashMap<String, u64>, String>;
    async fn ping(&self) -> bool;
}
