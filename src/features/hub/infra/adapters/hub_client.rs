use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use crate::features::hub::api::dto::{HubMessage, HysteriaConfig, NodeMessage, XrayConfig};
use crate::features::hub::application::commands::{
    add_user::AddUserCommand, remove_user::RemoveUserCommand, report_traffic::ReportTrafficCommand,
};
use crate::features::hub::application::queries::healthcheck::{HealthCheckQuery, NodeHealth};

pub struct HubClient {
    hub_url: String,
    node_id: String,
    auth_secret: String,
    public_ip: String,
    inbound_tags: Vec<String>,
    pub name_en: String,
    pub country_code: String,
    pub country_flag: String,
    pub xray: Option<XrayConfig>,
    pub hysteria: Option<HysteriaConfig>,
    pub add_user_cmd: Arc<AddUserCommand>,
    pub remove_user_cmd: Arc<RemoveUserCommand>,
    pub report_traffic_cmd: Arc<ReportTrafficCommand>,
    pub healthcheck_query: Arc<HealthCheckQuery>,
}

impl HubClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        hub_url: String,
        node_id: String,
        auth_secret: String,
        public_ip: String,
        inbound_tags: Vec<String>,
        name_en: String,
        country_code: String,
        country_flag: String,
        xray: Option<XrayConfig>,
        hysteria: Option<HysteriaConfig>,
        add_user_cmd: Arc<AddUserCommand>,
        remove_user_cmd: Arc<RemoveUserCommand>,
        report_traffic_cmd: Arc<ReportTrafficCommand>,
        healthcheck_query: Arc<HealthCheckQuery>,
    ) -> Self {
        Self {
            hub_url,
            node_id,
            auth_secret,
            public_ip,
            inbound_tags,
            name_en,
            country_code,
            country_flag,
            xray,
            hysteria,
            add_user_cmd,
            remove_user_cmd,
            report_traffic_cmd,
            healthcheck_query,
        }
    }

    pub async fn start(&self) {
        let mut reconnect_delay = Duration::from_secs(5);
        loop {
            info!("Connecting to Hub at {}...", self.hub_url);
            match connect_async(&self.hub_url).await {
                Ok((ws_stream, _)) => {
                    info!("WebSocket channel established. Performing authentication...");
                    match self.run_ws_loop(ws_stream).await {
                        Ok(()) => {
                            info!("WebSocket loop completed cleanly. Reconnecting in 5s...");
                            sleep(Duration::from_secs(5)).await;
                            reconnect_delay = Duration::from_secs(5);
                        }
                        Err(err) => {
                            if err.contains("Authentication failed") {
                                error!("Hub auth failed: {}. Retrying in 30s...", err);
                                sleep(Duration::from_secs(30)).await;
                                reconnect_delay = Duration::from_secs(5);
                            } else {
                                error!("WebSocket loop terminated with error: {}. Reconnecting in {:?}...", err, reconnect_delay);
                                sleep(reconnect_delay).await;
                                reconnect_delay =
                                    std::cmp::min(reconnect_delay * 2, Duration::from_secs(60));
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to connect to Hub: {}. Retrying in {:?}...",
                        e, reconnect_delay
                    );
                    sleep(reconnect_delay).await;
                    reconnect_delay = std::cmp::min(reconnect_delay * 2, Duration::from_secs(60));
                }
            }
        }
    }

    async fn run_ws_loop(
        &self,
        ws_stream: tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Result<(), String> {
        let (mut ws_sender, mut ws_receiver) = ws_stream.split();

        // 1. Send registration message
        let reg_msg = NodeMessage::Register {
            node_id: self.node_id.clone(),
            auth_secret: self.auth_secret.clone(),
            public_ip: self.public_ip.clone(),
            inbound_tags: self.inbound_tags.clone(),
            name_en: Some(self.name_en.clone()),
            country_code: Some(self.country_code.clone()),
            country_flag: Some(self.country_flag.clone()),
            xray: self.xray.clone(),
            hysteria: self.hysteria.clone(),
        };

        let reg_str = serde_json::to_string(&reg_msg)
            .map_err(|e| format!("Failed to serialize Register message: {}", e))?;

        ws_sender
            .send(Message::Text(reg_str))
            .await
            .map_err(|e| format!("Failed to send Register message: {}", e))?;

        // 2. Wait for HubMessage::AuthOk or AuthFailed
        let auth_response = loop {
            match ws_receiver.next().await {
                Some(Ok(Message::Text(text))) => match serde_json::from_str::<HubMessage>(&text) {
                    Ok(resp) => break resp,
                    Err(e) => return Err(format!("Failed to parse auth response: {}", e)),
                },
                Some(Ok(Message::Ping(ping))) => {
                    let _ = ws_sender.send(Message::Pong(ping)).await;
                }
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Close(reason))) => {
                    return Err(format!("Connection closed during auth: {:?}", reason));
                }
                Some(Ok(_)) => {
                    // Ignore binary or other frames, keep waiting for the Auth response
                }
                Some(Err(e)) => return Err(format!("Error reading auth response: {}", e)),
                None => return Err("Connection closed during auth".to_string()),
            }
        };

        match auth_response {
            HubMessage::AuthOk => {
                info!("Authentication successful!");
            }
            HubMessage::AuthFailed { reason } => {
                return Err(format!("Authentication failed: {}", reason));
            }
            _ => {
                return Err("Received unexpected message during auth".to_string());
            }
        }

        // 3. Main select loop
        let last_pong = Arc::new(Mutex::new(Instant::now()));
        let last_pong_clone = last_pong.clone();

        let (tx_outbound, mut rx_outbound) = mpsc::channel::<NodeMessage>(100);

        // Periodic loop: runs every 10 seconds.
        // Send traffic reports, pings, health checks.
        let tx_outbound_periodic = tx_outbound.clone();
        let report_traffic_cmd = self.report_traffic_cmd.clone();
        let healthcheck_query = self.healthcheck_query.clone();

        let periodic_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));

            loop {
                interval.tick().await;

                // Health check
                match healthcheck_query.execute().await {
                    NodeHealth::Online => {
                        info!("Health check: Node is ONLINE");
                    }
                    NodeHealth::Degraded(reason) => {
                        warn!("Health check: Node is DEGRADED. Reason: {}", reason);
                    }
                }

                // Traffic report
                match report_traffic_cmd.execute().await {
                    Ok(user_bytes) => {
                        if !user_bytes.is_empty() {
                            info!("Reporting traffic for {} users...", user_bytes.len());
                            let msg = NodeMessage::TrafficReport { user_bytes };
                            if tx_outbound_periodic.send(msg).await.is_err() {
                                error!("Outbound channel closed, stopping periodic tasks");
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to collect traffic statistics: {}", e);
                    }
                }

                // Send Ping
                if tx_outbound_periodic.send(NodeMessage::Ping).await.is_err() {
                    error!("Outbound channel closed, stopping periodic tasks");
                    break;
                }
            }
        });

        let add_user_cmd = self.add_user_cmd.clone();
        let remove_user_cmd = self.remove_user_cmd.clone();
        let tx_outbound_inbound = tx_outbound.clone();

        let mut check_interval = tokio::time::interval(Duration::from_secs(5));

        let res = loop {
            tokio::select! {
                // Outbound messages to WebSocket
                out_msg = rx_outbound.recv() => {
                    match out_msg {
                        Some(msg) => {
                            match serde_json::to_string(&msg) {
                                Ok(json_str) => {
                                    if ws_sender.send(Message::Text(json_str)).await.is_err() {
                                        break Err("Failed to send outbound message over WS".to_string());
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to serialize NodeMessage: {}", e);
                                }
                            }
                        }
                        None => {
                            break Err("Outbound queue closed".to_string());
                        }
                    }
                }
                // Inbound messages from WebSocket
                in_msg = ws_receiver.next() => {
                    match in_msg {
                        Some(Ok(Message::Text(text))) => {
                            match serde_json::from_str::<HubMessage>(&text) {
                                Ok(hub_msg) => {
                                    match hub_msg {
                                        HubMessage::AddUser { command_id, uuid, inbound_tags } => {
                                            let add_user_inner = add_user_cmd.clone();
                                            let tx_inner = tx_outbound_inbound.clone();
                                            tokio::spawn(async move {
                                                info!("Executing AddUser for uuid={}", uuid);
                                                let result = match add_user_inner.execute(&uuid, &inbound_tags).await {
                                                    Ok(_) => NodeMessage::CommandResult {
                                                        command_id,
                                                        success: true,
                                                        error_message: String::new(),
                                                    },
                                                    Err(e) => NodeMessage::CommandResult {
                                                        command_id,
                                                        success: false,
                                                        error_message: e,
                                                    },
                                                };
                                                let _ = tx_inner.send(result).await;
                                            });
                                        }
                                        HubMessage::RemoveUser { command_id, email, inbound_tags } => {
                                            let remove_user_inner = remove_user_cmd.clone();
                                            let tx_inner = tx_outbound_inbound.clone();
                                            tokio::spawn(async move {
                                                info!("Executing RemoveUser for email={}", email);
                                                let result = match remove_user_inner.execute(&email, &inbound_tags).await {
                                                    Ok(_) => NodeMessage::CommandResult {
                                                        command_id,
                                                        success: true,
                                                        error_message: String::new(),
                                                    },
                                                    Err(e) => NodeMessage::CommandResult {
                                                        command_id,
                                                        success: false,
                                                        error_message: e,
                                                    },
                                                };
                                                let _ = tx_inner.send(result).await;
                                            });
                                        }
                                        HubMessage::Pong => {
                                            let mut last = last_pong_clone.lock().await;
                                            *last = Instant::now();
                                        }
                                        _ => {}
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to parse HubMessage: {}", e);
                                }
                            }
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            break Err(format!("WS receiver error: {}", e));
                        }
                        None => {
                            break Err("WS connection closed by Hub".to_string());
                        }
                    }
                }
                // Liveness check: if no Pong received from Hub in 60 seconds -> close WS and reconnect
                _ = check_interval.tick() => {
                    let last = last_pong.lock().await;
                    if last.elapsed() > Duration::from_secs(60) {
                        break Err("No Pong received in 60s, connection dead".to_string());
                    }
                }
            }
        };

        periodic_handle.abort();
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::features::hub::domain::ports::{
        hysteria_commander::HysteriaCommander, xray_commander::XrayCommander,
    };
    use axum::{
        extract::ws::{Message as AxumMessage, WebSocketUpgrade},
        routing::get,
        Router,
    };
    use std::collections::HashMap;
    use tokio::net::TcpListener;

    struct MockXray;
    #[async_trait::async_trait]
    impl XrayCommander for MockXray {
        async fn add_user(&self, _tag: &str, _email: &str, _uuid: &str) -> Result<(), String> {
            Ok(())
        }
        async fn remove_user(&self, _tag: &str, _email: &str) -> Result<(), String> {
            Ok(())
        }
        async fn query_user_stats(&self) -> Result<HashMap<String, u64>, String> {
            Ok(HashMap::new())
        }
        async fn ping(&self) -> bool {
            true
        }
    }

    struct MockHysteria;
    #[async_trait::async_trait]
    impl HysteriaCommander for MockHysteria {
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

    #[tokio::test]
    async fn test_hub_client_lifecycle() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        let (tx_ws_established, mut rx_ws_established) =
            mpsc::channel::<(mpsc::Sender<String>, mpsc::Receiver<String>)>(1);

        // Simple mock server handler
        let app = Router::new().route(
            "/ws/node",
            get(move |ws: WebSocketUpgrade| async move {
                ws.on_upgrade(move |socket| async move {
                    let (mut ws_sender, mut ws_receiver) = socket.split();

                    // Expect Register
                    let msg = ws_receiver.next().await.unwrap().unwrap();
                    let reg_text = match msg {
                        AxumMessage::Text(t) => t.to_string(),
                        _ => panic!("Expected text message"),
                    };
                    assert!(reg_text.contains("register"));

                    // Send AuthOk
                    let auth_ok = HubMessage::AuthOk;
                    let auth_ok_str = serde_json::to_string(&auth_ok).unwrap();
                    ws_sender
                        .send(AxumMessage::Text(auth_ok_str.into()))
                        .await
                        .unwrap();

                    // Setup channels to communicate with the test body
                    let (tx_to_client, mut rx_to_client) = mpsc::channel::<String>(10);
                    let (tx_from_client, rx_from_client) = mpsc::channel::<String>(10);
                    let tx_from_client_clone = tx_from_client.clone();

                    let _ = tx_ws_established
                        .send((tx_to_client, rx_from_client))
                        .await;

                    let mut ws_sender = ws_sender;
                    let mut ws_receiver = ws_receiver;

                    // Concurrent task to relay messages to/from WebSocket inside the mock server
                    tokio::spawn(async move {
                        loop {
                            tokio::select! {
                                msg_to_send = rx_to_client.recv() => {
                                    match msg_to_send {
                                        Some(json_str) => {
                                            if ws_sender.send(AxumMessage::Text(json_str.into())).await.is_err() {
                                                break;
                                            }
                                        }
                                        None => break,
                                    }
                                }
                                msg_recv = ws_receiver.next() => {
                                    match msg_recv {
                                        Some(Ok(AxumMessage::Text(text))) => {
                                            if tx_from_client_clone.send(text.to_string()).await.is_err() {
                                                break;
                                            }
                                        }
                                        _ => break,
                                    }
                                }
                            }
                        }
                    });
                })
            }),
        );

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // Instantiate HubClient
        let hub_url = format!("ws://{}/ws/node", local_addr);
        let add_user_cmd = Arc::new(AddUserCommand::new(
            Arc::new(MockXray),
            Arc::new(MockHysteria),
        ));
        let remove_user_cmd = Arc::new(RemoveUserCommand::new(
            Arc::new(MockXray),
            Arc::new(MockHysteria),
        ));
        let report_traffic_cmd = Arc::new(ReportTrafficCommand::new(
            Arc::new(MockXray),
            Arc::new(MockHysteria),
        ));
        let healthcheck_query = Arc::new(HealthCheckQuery::new(
            Arc::new(MockXray),
            Arc::new(MockHysteria),
        ));

        let client = HubClient::new(
            hub_url,
            "test-node".to_string(),
            "secret123".to_string(),
            "127.0.0.1".to_string(),
            vec!["vless-inbound".to_string()],
            "Germany".to_string(),
            "DE".to_string(),
            "🇩🇪".to_string(),
            None,
            None,
            add_user_cmd,
            remove_user_cmd,
            report_traffic_cmd,
            healthcheck_query,
        );

        // Connect client
        let ws_stream = match connect_async(&client.hub_url).await {
            Ok((stream, _)) => stream,
            Err(e) => panic!("Failed to connect: {}", e),
        };

        // Run HubClient loop in background
        let client_handle = tokio::spawn(async move {
            let _ = client.run_ws_loop(ws_stream).await;
        });

        // Wait for connection to be established in mock server
        let (tx_to_client, mut rx_from_client) = rx_ws_established.recv().await.unwrap();

        // 1. Send AddUser command to Node client
        let add_user_msg = HubMessage::AddUser {
            command_id: "cmd-uuid-1".to_string(),
            uuid: "user-uuid-1".to_string(),
            inbound_tags: vec!["vless-inbound".to_string()],
        };
        tx_to_client
            .send(serde_json::to_string(&add_user_msg).unwrap())
            .await
            .unwrap();

        // Expect CommandResult from Node client
        let result_json = rx_from_client.recv().await.unwrap();
        let result_msg: NodeMessage = serde_json::from_str(&result_json).unwrap();
        match result_msg {
            NodeMessage::CommandResult {
                command_id,
                success,
                error_message,
            } => {
                assert_eq!(command_id, "cmd-uuid-1");
                assert!(success);
                assert!(error_message.is_empty());
            }
            other => panic!("Expected CommandResult, got {:?}", other),
        }

        // 2. Expect Ping from Node client within reasonable time (since periodic loop triggers Ping immediately)
        let ping_json = rx_from_client.recv().await.unwrap();
        let ping_msg: NodeMessage = serde_json::from_str(&ping_json).unwrap();
        assert!(matches!(ping_msg, NodeMessage::Ping));

        // Cleanup
        drop(tx_to_client);
        client_handle.abort();
    }

    #[tokio::test]
    async fn test_hub_client_xray_only() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        let (tx_ws_established, mut rx_ws_established) =
            mpsc::channel::<(mpsc::Sender<String>, mpsc::Receiver<String>)>(1);

        let app = Router::new().route(
            "/ws/node",
            get(move |ws: WebSocketUpgrade| async move {
                ws.on_upgrade(move |socket| async move {
                    let (mut ws_sender, mut ws_receiver) = socket.split();

                    // Expect Register
                    let msg = ws_receiver.next().await.unwrap().unwrap();
                    let reg_text = match msg {
                        AxumMessage::Text(t) => t.to_string(),
                        _ => panic!("Expected text message"),
                    };
                    assert!(reg_text.contains("register"));

                    // Send AuthOk
                    let auth_ok = HubMessage::AuthOk;
                    let auth_ok_str = serde_json::to_string(&auth_ok).unwrap();
                    ws_sender
                        .send(AxumMessage::Text(auth_ok_str.into()))
                        .await
                        .unwrap();

                    let (tx_to_client, mut rx_to_client) = mpsc::channel::<String>(10);
                    let (tx_from_client, rx_from_client) = mpsc::channel::<String>(10);
                    let tx_from_client_clone = tx_from_client.clone();

                    let _ = tx_ws_established
                        .send((tx_to_client, rx_from_client))
                        .await;

                    let mut ws_sender = ws_sender;
                    let mut ws_receiver = ws_receiver;

                    tokio::spawn(async move {
                        loop {
                            tokio::select! {
                                msg_to_send = rx_to_client.recv() => {
                                    match msg_to_send {
                                        Some(json_str) => {
                                            if ws_sender.send(AxumMessage::Text(json_str.into())).await.is_err() {
                                                break;
                                            }
                                        }
                                        None => break,
                                    }
                                }
                                msg_recv = ws_receiver.next() => {
                                    match msg_recv {
                                        Some(Ok(AxumMessage::Text(text))) => {
                                            if tx_from_client_clone.send(text.to_string()).await.is_err() {
                                                break;
                                            }
                                        }
                                        _ => break,
                                    }
                                }
                            }
                        }
                    });
                })
            }),
        );

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // Use NoopHysteriaClient for disabled Hysteria mode
        let hysteria_client =
            Arc::new(crate::features::hub::infra::adapters::hysteria_client::NoopHysteriaClient);
        let xray_client = Arc::new(MockXray);

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

        let client = HubClient::new(
            format!("ws://{}/ws/node", local_addr),
            "test-node".to_string(),
            "secret123".to_string(),
            "127.0.0.1".to_string(),
            vec!["vless-inbound".to_string()],
            "Germany".to_string(),
            "DE".to_string(),
            "🇩🇪".to_string(),
            None,
            None,
            add_user_cmd,
            remove_user_cmd,
            report_traffic_cmd,
            healthcheck_query,
        );

        let ws_stream = connect_async(&client.hub_url).await.unwrap().0;
        let client_handle = tokio::spawn(async move {
            let _ = client.run_ws_loop(ws_stream).await;
        });

        let (tx_to_client, mut rx_from_client) = rx_ws_established.recv().await.unwrap();

        // Send AddUser command to Node client
        let add_user_msg = HubMessage::AddUser {
            command_id: "cmd-uuid-2".to_string(),
            uuid: "user-uuid-2".to_string(),
            inbound_tags: vec!["vless-inbound".to_string()],
        };
        tx_to_client
            .send(serde_json::to_string(&add_user_msg).unwrap())
            .await
            .unwrap();

        // Expect CommandResult from Node client (should succeed because Hysteria is nooped)
        let result_json = rx_from_client.recv().await.unwrap();
        let result_msg: NodeMessage = serde_json::from_str(&result_json).unwrap();
        match result_msg {
            NodeMessage::CommandResult {
                command_id,
                success,
                error_message,
            } => {
                assert_eq!(command_id, "cmd-uuid-2");
                assert!(success);
                assert!(error_message.is_empty());
            }
            other => panic!("Expected CommandResult, got {:?}", other),
        }

        // Cleanup
        drop(tx_to_client);
        client_handle.abort();
    }
}
