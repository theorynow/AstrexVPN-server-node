pub mod common;
pub mod features;

use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::common::app::config::Config;
use crate::features::hub::{
    application::commands::{
        add_user::AddUserCommand, remove_user::RemoveUserCommand,
        report_traffic::ReportTrafficCommand,
    },
    application::queries::healthcheck::HealthCheckQuery,
    domain::ports::{hysteria_commander::HysteriaCommander, xray_commander::XrayCommander},
    infra::adapters::{
        hub_client::HubClient, hysteria_client::HysteriaClient, xray_client::XrayClient,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize simple string logging
    tracing_subscriber::registry()
        .with(fmt::layer().compact()) // String output to stdout
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    info!("Starting AstrexVPN Node Agent...");

    // 2. Load settings from config
    let config = Config::from_env()?;

    // 3. Instantiate adapters as domain port trait objects
    let xray_client: Arc<dyn XrayCommander> =
        Arc::new(XrayClient::new(config.xray_grpc_url.clone()));
    let hysteria_client: Arc<dyn HysteriaCommander> =
        if config.hysteria_api_url == "disabled" || config.hysteria_api_url.trim().is_empty() {
            info!("Hysteria2 integration is disabled. Running in Xray-only mode.");
            Arc::new(crate::features::hub::infra::adapters::hysteria_client::NoopHysteriaClient)
        } else {
            Arc::new(HysteriaClient::new(
                config.hysteria_api_url.clone(),
                config.hysteria_auth_addr.clone(),
            ))
        };

    // 4. Instantiate application logic commands and queries
    let add_user_cmd = Arc::new(AddUserCommand::new(
        xray_client.clone(),
        hysteria_client.clone(),
    ));
    let remove_user_cmd = Arc::new(RemoveUserCommand::new(
        xray_client.clone(),
        hysteria_client.clone(),
    ));
    let report_traffic_cmd = Arc::new(ReportTrafficCommand::new(
        xray_client.clone(),
        hysteria_client.clone(),
    ));
    let healthcheck_query = Arc::new(HealthCheckQuery::new(
        xray_client.clone(),
        hysteria_client.clone(),
    ));

    let xray_config = Some(crate::features::hub::api::dto::XrayConfig {
        port: config.xray_port,
        sni: config.xray_sni,
        public_key: config.xray_public_key,
        short_id: config.xray_short_id,
    });

    let hysteria_config = if config.hysteria_api_url != "disabled" && !config.hysteria_api_url.is_empty() {
        Some(crate::features::hub::api::dto::HysteriaConfig {
            port: config.hysteria_port,
            sni: config.hysteria_sni,
        })
    } else {
        None
    };

    // 5. Instantiate and start Hub WebSocket Client daemon
    let hub_client = HubClient::new(
        config.hub_url,
        config.node_id,
        config.auth_secret,
        config.public_ip,
        config.inbound_tags,
        config.name_en,
        config.country_code,
        config.country_flag,
        xray_config,
        hysteria_config,
        add_user_cmd,
        remove_user_cmd,
        report_traffic_cmd,
        healthcheck_query,
    );

    hub_client.start().await;

    Ok(())
}
