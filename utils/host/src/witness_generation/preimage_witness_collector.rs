use std::{env, sync::{Arc, Mutex, OnceLock}};

use async_trait::async_trait;
use kona_preimage::{
    errors::PreimageOracleResult, CommsClient, HintWriterClient, PreimageKey, PreimageOracleClient,
};
use kona_proof::FlushableCache;
use op_succinct_client_utils::witness::preimage_store::PreimageStore;

/// Get the hint delay in milliseconds from HINT_DELAY_MS env var (default: 0)
fn get_hint_delay_ms() -> u64 {
    static HINT_DELAY: OnceLock<u64> = OnceLock::new();
    *HINT_DELAY.get_or_init(|| {
        env::var("HINT_DELAY_MS")
            .unwrap_or_else(|_| "0".to_string())
            .parse()
            .unwrap_or(0)
    })
}

#[derive(Clone, Debug)]
pub struct PreimageWitnessCollector<P: CommsClient + FlushableCache + Send + Sync + Clone> {
    pub preimage_oracle: Arc<P>,
    pub preimage_witness_store: Arc<Mutex<PreimageStore>>,
}

#[async_trait]
impl<P> PreimageOracleClient for PreimageWitnessCollector<P>
where
    P: CommsClient + FlushableCache + Send + Sync + Clone,
{
    async fn get(&self, key: PreimageKey) -> PreimageOracleResult<Vec<u8>> {
        let value = self.preimage_oracle.get(key).await?;
        self.save(key, &value);
        Ok(value)
    }

    async fn get_exact(&self, key: PreimageKey, buf: &mut [u8]) -> PreimageOracleResult<()> {
        self.preimage_oracle.get_exact(key, buf).await?;
        self.save(key, buf);
        Ok(())
    }
}

#[async_trait]
impl<P> HintWriterClient for PreimageWitnessCollector<P>
where
    P: CommsClient + FlushableCache + Send + Sync + Clone,
{
    async fn write(&self, hint: &str) -> PreimageOracleResult<()> {
        // Rate limit L1 hints to avoid 429 errors on L1 RPC
        let delay = get_hint_delay_ms();
        if delay > 0 && hint.starts_with("l1-") {
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
        self.preimage_oracle.write(hint).await
    }
}

impl<P> FlushableCache for PreimageWitnessCollector<P>
where
    P: CommsClient + FlushableCache + Send + Sync + Clone,
{
    fn flush(&self) {
        self.preimage_oracle.flush();
    }
}

impl<P> PreimageWitnessCollector<P>
where
    P: CommsClient + FlushableCache + Send + Sync + Clone,
{
    pub fn save(&self, key: PreimageKey, value: &[u8]) {
        self.preimage_witness_store.lock().unwrap().save_preimage(key, value.to_vec());
    }
}
