use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeMessage {
    Register {
        node_id: String,
        auth_secret: String,
        public_ip: String,
        inbound_tags: Vec<String>,
    },
    TrafficReport {
        user_bytes: HashMap<String, u64>,
    },
    CommandResult {
        command_id: String,
        success: bool,
        error_message: String,
    },
    Ping,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HubMessage {
    AuthOk,
    AuthFailed {
        reason: String,
    },
    AddUser {
        command_id: String,
        uuid: String,
        inbound_tags: Vec<String>,
    },
    RemoveUser {
        command_id: String,
        email: String,
        inbound_tags: Vec<String>,
    },
    Pong,
}
