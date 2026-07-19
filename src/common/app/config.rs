use std::env;
use thiserror::Error;
use tracing::info;

#[derive(Clone, Debug)]
pub struct Config {
    pub hub_url: String,
    pub node_id: String,
    pub auth_secret: String,
    pub public_ip: String,
    pub xray_grpc_url: String,
    pub hysteria_api_url: String,
    pub inbound_tags: Vec<String>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Required environment variable {name} is not set")]
    MissingEnv { name: &'static str },
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        dotenvy::dotenv().ok();

        let inbound_tags_str =
            env::var("INBOUND_TAGS").unwrap_or_else(|_| "vless-reality-in".to_string());
        let inbound_tags = inbound_tags_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let config = Self {
            hub_url: required_env("HUB_URL")?,
            node_id: required_env("NODE_ID")?,
            auth_secret: required_env("AUTH_SECRET")?,
            public_ip: required_env("PUBLIC_IP")?,
            xray_grpc_url: required_env("XRAY_GRPC_URL")?,
            hysteria_api_url: env::var("HYSTERIA_API_URL")
                .unwrap_or_else(|_| "disabled".to_string()),
            inbound_tags,
        };

        info!(
            hub_url = %config.hub_url,
            node_id = %config.node_id,
            public_ip = %config.public_ip,
            xray_grpc_url = %config.xray_grpc_url,
            hysteria_api_url = %config.hysteria_api_url,
            inbound_tags = ?config.inbound_tags,
            "Node configuration loaded"
        );

        Ok(config)
    }
}

fn required_env(name: &'static str) -> Result<String, ConfigError> {
    let value = env::var(name).map_err(|_| ConfigError::MissingEnv { name })?;

    if value.trim().is_empty() {
        return Err(ConfigError::MissingEnv { name });
    }

    Ok(value)
}
