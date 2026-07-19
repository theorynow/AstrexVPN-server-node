use async_trait::async_trait;
use std::collections::HashMap;

#[async_trait]
pub trait XrayCommander: Send + Sync {
    async fn add_user(&self, tag: &str, email: &str, uuid: &str) -> Result<(), String>;
    async fn remove_user(&self, tag: &str, email: &str) -> Result<(), String>;
    async fn query_user_stats(&self) -> Result<HashMap<String, u64>, String>;
    async fn ping(&self) -> bool;
}
