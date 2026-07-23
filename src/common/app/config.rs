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
    pub hysteria_auth_addr: String,
    pub inbound_tags: Vec<String>,
    pub name_en: String,
    pub name_ru: String,
    pub country_flag: String,
    pub xray_port: u16,
    pub xray_sni: String,
    pub xray_public_key: String,
    pub xray_short_id: String,
    pub hysteria_port: u16,
    pub hysteria_sni: String,
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

        let hysteria_auth_addr =
            env::var("HYSTERIA_AUTH_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
        let name_en = env::var("NAME_EN").unwrap_or_else(|_| "Germany".to_string());
        let name_ru = env::var("NAME_RU").unwrap_or_else(|_| "Германия".to_string());
        let country_flag = env::var("COUNTRY_FLAG").unwrap_or_else(|_| "🇩🇪".to_string());

        let xray_port = env::var("XRAY_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(443);
        let xray_sni = env::var("XRAY_SNI").unwrap_or_else(|_| "www.yahoo.com".to_string());
        let xray_public_key = env::var("XRAY_PUBLIC_KEY").unwrap_or_default();
        let xray_short_id = env::var("XRAY_SHORT_ID").unwrap_or_default();

        let hysteria_port = env::var("HYSTERIA_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(443);
        let hysteria_sni = env::var("HYSTERIA_SNI").unwrap_or_else(|_| "fuckbook.pro".to_string());

        let config = Self {
            hub_url: required_env("HUB_URL")?,
            node_id: required_env("NODE_ID")?,
            auth_secret: required_env("AUTH_SECRET")?,
            public_ip: required_env("PUBLIC_IP")?,
            xray_grpc_url: required_env("XRAY_GRPC_URL")?,
            hysteria_api_url: env::var("HYSTERIA_API_URL")
                .unwrap_or_else(|_| "disabled".to_string()),
            hysteria_auth_addr,
            inbound_tags,
            name_en,
            name_ru,
            country_flag,
            xray_port,
            xray_sni,
            xray_public_key,
            xray_short_id,
            hysteria_port,
            hysteria_sni,
        };

        info!(
            hub_url = %config.hub_url,
            node_id = %config.node_id,
            name_en = %config.name_en,
            name_ru = %config.name_ru,
            country_flag = %config.country_flag,
            public_ip = %config.public_ip,
            xray_grpc_url = %config.xray_grpc_url,
            hysteria_api_url = %config.hysteria_api_url,
            hysteria_auth_addr = %config.hysteria_auth_addr,
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
