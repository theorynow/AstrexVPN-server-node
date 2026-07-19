use crate::features::hub::{
    api::grpc_codegen::xray::{
        app::proxyman::command::{
            handler_service_client::HandlerServiceClient, AddUserOperation, AlterInboundRequest,
            RemoveUserOperation,
        },
        app::stats::command::{stats_service_client::StatsServiceClient, QueryStatsRequest},
        common::protocol::User,
        common::serial::TypedMessage,
        proxy::vless::Account,
    },
    domain::ports::xray_commander::XrayCommander,
};
use async_trait::async_trait;
use prost::Message;
use tonic::transport::Channel;

pub struct XrayClient {
    grpc_url: String,
}

impl XrayClient {
    pub fn new(grpc_url: String) -> Self {
        Self { grpc_url }
    }

    async fn get_handler_client(
        &self,
    ) -> Result<HandlerServiceClient<Channel>, tonic::transport::Error> {
        HandlerServiceClient::connect(self.grpc_url.clone()).await
    }

    async fn get_stats_client(
        &self,
    ) -> Result<StatsServiceClient<Channel>, tonic::transport::Error> {
        StatsServiceClient::connect(self.grpc_url.clone()).await
    }
}

#[async_trait]
impl XrayCommander for XrayClient {
    async fn add_user(&self, tag: &str, email: &str, uuid: &str) -> Result<(), String> {
        let mut client = self
            .get_handler_client()
            .await
            .map_err(|e| format!("Failed to connect to Xray gRPC: {}", e))?;

        // 1. Construct VLESS Account
        let account = Account {
            id: uuid.to_string(),
            flow: "xtls-rprx-vision".to_string(),
            encryption: String::new(),
        };
        let mut account_bytes = Vec::new();
        account
            .encode(&mut account_bytes)
            .map_err(|e| format!("Failed to encode VLESS account: {}", e))?;

        let account_msg = TypedMessage {
            r#type: "xray.proxy.vless.Account".to_string(),
            value: account_bytes,
        };

        // 2. Construct User
        let user = User {
            level: 0,
            email: email.to_string(),
            account: Some(account_msg),
        };

        // 3. Construct AddUserOperation
        let add_user_op = AddUserOperation { user: Some(user) };
        let mut add_user_bytes = Vec::new();
        add_user_op
            .encode(&mut add_user_bytes)
            .map_err(|e| format!("Failed to encode AddUserOperation: {}", e))?;

        let operation_msg = TypedMessage {
            r#type: "xray.app.proxyman.command.AddUserOperation".to_string(),
            value: add_user_bytes,
        };

        // 4. Construct AlterInboundRequest
        let req = AlterInboundRequest {
            tag: tag.to_string(),
            operation: Some(operation_msg),
        };

        client
            .alter_inbound(req)
            .await
            .map_err(|e| format!("Xray AlterInbound failed: {}", e.message()))?;

        Ok(())
    }

    async fn remove_user(&self, tag: &str, email: &str) -> Result<(), String> {
        let mut client = self
            .get_handler_client()
            .await
            .map_err(|e| format!("Failed to connect to Xray gRPC: {}", e))?;

        // 1. Construct RemoveUserOperation
        let remove_user_op = RemoveUserOperation {
            email: email.to_string(),
        };
        let mut remove_user_bytes = Vec::new();
        remove_user_op
            .encode(&mut remove_user_bytes)
            .map_err(|e| format!("Failed to encode RemoveUserOperation: {}", e))?;

        let operation_msg = TypedMessage {
            r#type: "xray.app.proxyman.command.RemoveUserOperation".to_string(),
            value: remove_user_bytes,
        };

        // 2. Construct AlterInboundRequest
        let req = AlterInboundRequest {
            tag: tag.to_string(),
            operation: Some(operation_msg),
        };

        match client.alter_inbound(req).await {
            Ok(_) => Ok(()),
            Err(e) => {
                let msg = e.message();
                if msg.contains("not found")
                    || msg.contains("does not exist")
                    || msg.contains("UserNotFound")
                {
                    Ok(())
                } else {
                    Err(format!("Xray AlterInbound failed: {}", msg))
                }
            }
        }
    }

    async fn query_user_stats(&self) -> Result<std::collections::HashMap<String, u64>, String> {
        let mut client = self
            .get_stats_client()
            .await
            .map_err(|e| format!("Failed to connect to Xray gRPC: {}", e))?;

        let req = QueryStatsRequest {
            pattern: "user>>>".to_string(),
            reset: true,
        };

        let resp = client
            .query_stats(req)
            .await
            .map_err(|e| format!("Xray QueryStats failed: {}", e.message()))?;

        let mut user_bytes = std::collections::HashMap::new();

        for stat in resp.into_inner().stat {
            let parts: Vec<&str> = stat.name.split(">>>").collect();
            if parts.len() >= 4 && parts[0] == "user" {
                let email = parts[1].to_string();
                let val = stat.value as u64;
                *user_bytes.entry(email).or_insert(0) += val;
            }
        }

        Ok(user_bytes)
    }

    async fn ping(&self) -> bool {
        match self.get_stats_client().await {
            Ok(mut client) => {
                let req = QueryStatsRequest {
                    pattern: "user>>>".to_string(),
                    reset: false,
                };
                client.query_stats(req).await.is_ok()
            }
            Err(_) => false,
        }
    }
}
