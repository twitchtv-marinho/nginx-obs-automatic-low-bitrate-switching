use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::{error, info, trace, warn};
use twitch_irc::{
    ClientConfig, SecureTCPTransport, TwitchIRCClient,
    login::StaticLoginCredentials,
    message,
    transport::tcp::{TCPTransport, TLS},
};

use crate::{
    ChatSender,
    chat::{self, ChatPlatform, HandleMessage},
    twitch_pubsub::PubsubManager,
};

const ANNOUNCE_COLOR_ENV: &str = "TWITCH_ANNOUNCE_COLOR";
const ANNOUNCE_COLORS: [&str; 5] = ["blue", "green", "orange", "purple", "primary"];
const ANNOUNCE_SCOPE: &str = "moderator:manage:announcements";

#[derive(Clone)]
pub struct Twitch {
    client: TwitchIRCClient<TCPTransport<TLS>, StaticLoginCredentials>,
    pub event_loop: Arc<tokio::task::JoinHandle<()>>,
    announcer: Option<Arc<Announcer>>,
}

impl Twitch {
    pub fn new(bot_username: String, mut oauth: String, chat_handler_tx: ChatSender) -> Self {
        if let Some(oauth_without_prefix) = oauth.strip_prefix("oauth:") {
            oauth = oauth_without_prefix.to_string();
        }

        let config = ClientConfig::new_simple(StaticLoginCredentials::new(
            bot_username.to_lowercase(),
            Some(oauth.to_lowercase()),
        ));

        let (incoming_messages, client) =
            TwitchIRCClient::<SecureTCPTransport, StaticLoginCredentials>::new(config);

        let channel_ids = Arc::new(Mutex::new(HashMap::new()));
        let announcer = Announcer::from_env(oauth, channel_ids.clone()).map(Arc::new);

        let pubsub = PubsubManager::new(chat_handler_tx.clone());
        let event_loop_handle = tokio::spawn(Self::chat_loop(
            incoming_messages,
            chat_handler_tx,
            pubsub,
            channel_ids,
        ));

        Self {
            client,
            event_loop: Arc::new(event_loop_handle),
            announcer,
        }
    }

    pub fn get_client(&self) -> TwitchIRCClient<TCPTransport<TLS>, StaticLoginCredentials> {
        self.client.clone()
    }

    async fn chat_loop(
        mut incoming_messages: mpsc::UnboundedReceiver<message::ServerMessage>,
        chat_handler_tx: tokio::sync::mpsc::Sender<super::HandleMessage>,
        pubsub: PubsubManager,
        channel_ids: Arc<Mutex<HashMap<String, String>>>,
    ) {
        while let Some(message) = incoming_messages.recv().await {
            // println!("Received message: {:?}", message);

            match message {
                message::ServerMessage::RoomState(state) => {
                    trace!(
                        "user_id: {}, user_name: {}",
                        state.channel_id, state.channel_login
                    );
                    channel_ids
                        .lock()
                        .unwrap()
                        .insert(state.channel_login.clone(), state.channel_id.clone());
                    pubsub.add_raid(state.channel_id, state.channel_login).await;
                }
                message::ServerMessage::Notice(msg) => {
                    if msg.message_text == "Login authentication failed" {
                        error!("Twitch authentication failed");

                        // TODO: Handle panic
                        // panic!("Twitch authentication failed");
                    }
                }
                message::ServerMessage::Privmsg(msg) => {
                    // Ignore messages not originating from the current channel
                    let source_room_id = msg.source.tags.0.get("source-room-id");
                    if let Some(source_room_id) = source_room_id
                        && &msg.channel_id != source_room_id
                    {
                        continue;
                    }

                    let permission =
                        msg.badges
                            .iter()
                            .fold(chat::Permission::Public, |acc, badge| {
                                match badge.name.as_str() {
                                    "vip" => chat::Permission::Vip,
                                    "moderator" => chat::Permission::Mod,
                                    "lead_moderator" => chat::Permission::Mod,
                                    "broadcaster" => chat::Permission::Admin,
                                    _ => acc,
                                }
                            });

                    chat_handler_tx
                        .send(HandleMessage::ChatMessage(chat::ChatMessage {
                            platform: ChatPlatform::Twitch,
                            permission,
                            channel: msg.channel_login,
                            sender: msg.sender.login,
                            message: msg.message_text.to_owned(),
                        }))
                        .await
                        .unwrap();
                }
                _ => {}
            }
        }
    }

    pub fn join_channel(&self, channel: String) {
        info!("Joining channel: {}", channel);
        if let Err(e) = self.client.join(channel) {
            error!("Error joining channel: {}", e);
        }
    }
}

#[async_trait]
impl super::ChatLogic for Twitch {
    async fn send_message(&self, channel: String, message: String) {
        if let Some(announcer) = &self.announcer {
            match announcer.announce(&channel, &message).await {
                Ok(()) => return,
                Err(err) => warn!("Announcement failed, sending as a normal message: {}", err),
            }
        }

        if let Err(err) = self.client.say(channel, message).await {
            error!("Error sending message to twitch: {}", err);
        }
    }
}

impl Drop for Twitch {
    // Abort the spawned tasks
    fn drop(&mut self) {
        self.event_loop.abort();
    }
}

/// Sends the bot replies as chat announcements (Helix) when TWITCH_ANNOUNCE_COLOR is set.
/// The bot must be a moderator and the token needs the moderator:manage:announcements scope.
struct Announcer {
    http: reqwest::Client,
    token: String,
    color: String,
    // (client_id, bot user_id), read from the token validation
    ids: tokio::sync::OnceCell<(String, String)>,
    // channel login -> channel id, filled from ROOMSTATE
    channel_ids: Arc<Mutex<HashMap<String, String>>>,
}

impl Announcer {
    fn from_env(token: String, channel_ids: Arc<Mutex<HashMap<String, String>>>) -> Option<Self> {
        let color = std::env::var(ANNOUNCE_COLOR_ENV).ok()?.trim().to_lowercase();
        if color.is_empty() {
            return None;
        }

        if !ANNOUNCE_COLORS.contains(&color.as_str()) {
            error!(
                "{} must be one of {:?}, announcements disabled",
                ANNOUNCE_COLOR_ENV, ANNOUNCE_COLORS
            );
            return None;
        }

        info!("Twitch replies will be sent as {} announcements", color);

        Some(Self {
            // Bounded Helix calls: on timeout announce() errors and the reply falls back
            // to IRC, so a stuck Helix never holds the chat handler (or a state lock).
            http: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(3))
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .unwrap_or_default(),
            token,
            color,
            ids: tokio::sync::OnceCell::new(),
            channel_ids,
        })
    }

    async fn validate(&self) -> Result<(String, String), String> {
        let res = self
            .http
            .get("https://id.twitch.tv/oauth2/validate")
            .header("Authorization", format!("OAuth {}", self.token))
            .send()
            .await
            .map_err(|e| format!("token validation request failed: {}", e))?;

        if !res.status().is_success() {
            return Err(format!("token validation returned {}", res.status()));
        }

        let body: serde_json::Value = res
            .json()
            .await
            .map_err(|e| format!("token validation response: {}", e))?;

        let has_scope = body["scopes"]
            .as_array()
            .is_some_and(|scopes| scopes.iter().any(|s| s.as_str() == Some(ANNOUNCE_SCOPE)));
        if !has_scope {
            return Err(format!("token is missing the {} scope", ANNOUNCE_SCOPE));
        }

        let client_id = body["client_id"].as_str().unwrap_or_default().to_string();
        let user_id = body["user_id"].as_str().unwrap_or_default().to_string();
        if client_id.is_empty() || user_id.is_empty() {
            return Err("token validation without client_id or user_id".to_string());
        }

        Ok((client_id, user_id))
    }

    async fn announce(&self, channel: &str, message: &str) -> Result<(), String> {
        let broadcaster_id = self
            .channel_ids
            .lock()
            .unwrap()
            .get(&channel.to_lowercase())
            .cloned()
            .ok_or_else(|| format!("channel id of {} not known yet", channel))?;

        let (client_id, moderator_id) = self.ids.get_or_try_init(|| self.validate()).await?;

        let res = self
            .http
            .post("https://api.twitch.tv/helix/chat/announcements")
            .query(&[
                ("broadcaster_id", broadcaster_id.as_str()),
                ("moderator_id", moderator_id.as_str()),
            ])
            .header("Client-Id", client_id.as_str())
            .bearer_auth(&self.token)
            .json(&serde_json::json!({ "message": message, "color": self.color }))
            .send()
            .await
            .map_err(|e| format!("announcement request failed: {}", e))?;

        if res.status().is_success() {
            Ok(())
        } else {
            let status = res.status();
            let body = res.text().await.unwrap_or_default();
            Err(format!("announcement returned {}: {}", status, body))
        }
    }
}
