use async_trait::async_trait;
use tokio::sync;

use crate::{error::Error, state};

pub mod obs_v5;

#[async_trait]
pub trait BroadcastingSoftwareLogic: Send + Sync {
    async fn switch_scene(&self, scene: &str) -> Result<String, Error>;

    async fn wait_for_scene_transition(&self) -> Result<(), Error>;

    async fn start_streaming(&self) -> Result<(), Error>;

    async fn stop_streaming(&self) -> Result<(), Error>;

    /// MARINHO: aperta um atalho do OBS pelo nome (os do saidas-extras.lua)
    async fn trigger_hotkey(&self, name: &str) -> Result<(), Error>;

    /// MARINHO: a saída principal (Twitch) está no ar?
    async fn streaming_active(&self) -> Result<bool, Error>;

    /// MARINHO: uma saída pelo nome (marinho_saida_yt, marinho_saida_kick) está no ar?
    async fn output_active(&self, name: &str) -> Result<bool, Error>;

    async fn toggle_recording(&self) -> Result<(), Error>;

    async fn is_recording(&self) -> Result<bool, Error>;

    async fn fix(&self) -> Result<(), Error>;

    async fn current_scene(&self) -> Result<String, Error>;

    async fn toggle_source(&self, source: &str) -> Result<(String, bool), Error>;

    async fn set_collection_and_profile(
        &self,
        source: &crate::config::CollectionPair,
    ) -> Result<(), Error>;

    async fn info(
        &self,
        state: &sync::RwLockReadGuard<state::State>,
    ) -> Result<state::StreamStatus, Error>;

    // async fn get_media_source_status(
    //     &self,
    //     source_name: &str,
    // ) -> Result<(obws::responses::MediaState, i64), Error>;

    // async fn create_special_media_source(
    //     &self,
    //     source_name: &str,
    //     scene: &str,
    // ) -> Result<String, Error>;

    // async fn remove_special_media_source(
    //     &self,
    //     source_name: &str,
    //     scene: &str,
    // ) -> Result<(), Error>;
}
