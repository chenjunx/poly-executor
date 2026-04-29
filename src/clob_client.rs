use std::str::FromStr;

use polymarket_client_sdk_v2::POLYGON;
use polymarket_client_sdk_v2::auth::{LocalSigner, Normal, Signer as _, state::Authenticated};
use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config as ClobConfig};

use crate::config::AuthConfig;

const CLOB_HOST: &str = "https://clob.polymarket.com";

pub type AuthenticatedClobClient = ClobClient<Authenticated<Normal>>;

pub async fn build_authenticated_clob_client(
    auth: &AuthConfig,
) -> anyhow::Result<AuthenticatedClobClient> {
    let signer = LocalSigner::from_str(&auth.private_key)?.with_chain_id(Some(POLYGON));
    Ok(ClobClient::new(
        CLOB_HOST,
        ClobConfig::builder().use_server_time(true).build(),
    )?
    .authentication_builder(&signer)
    .funder(auth.funder.parse()?)
    .signature_type(polymarket_client_sdk_v2::clob::types::SignatureType::Proxy)
    .authenticate()
    .await?)
}
