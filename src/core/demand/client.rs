use crate::core::demand::encoding::{RequestEncoder, ResponseDecoder};
use crate::core::models::bidder::{Bidder, Endpoint, HttpProto};
use anyhow::anyhow;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, StatusCode, redirect, retry};
use rtb::{BidRequest, BidResponse};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tracing::debug;

pub struct DemandResponse {
    pub status_code: u32,
    pub status_message: String,
    pub response: Option<BidResponse>,
}

pub struct DemandClient {
    h1_client: OnceLock<Client>,
    h2c_client: OnceLock<Client>,
    h2_client: OnceLock<Client>,
}

impl DemandClient {
    fn init_client(proto: HttpProto) -> Result<Client, anyhow::Error> {
        let mut client_builder = reqwest::ClientBuilder::new()
            .danger_accept_invalid_certs(true)
            .user_agent("ad-client")
            .connect_timeout(Duration::from_secs(1))
            .pool_max_idle_per_host(128)
            .pool_idle_timeout(Some(Duration::from_secs(30)))
            .tcp_keepalive(Some(Duration::from_secs(20)))
            .retry(retry::never())
            .referer(false)
            .redirect(redirect::Policy::none())
            .timeout(Duration::from_secs(1))
            .tcp_nodelay(true)
            .deflate(true)
            .gzip(true)
            .hickory_dns(true);

        client_builder = match proto {
            HttpProto::Http1 => client_builder
                .http1_only()
                .http1_ignore_invalid_headers_in_responses(true),
            HttpProto::H2c => client_builder
                .http2_prior_knowledge()
                .http2_adaptive_window(true),
            HttpProto::Http2 => client_builder.http2_adaptive_window(true),
        };

        client_builder.build().map_err(anyhow::Error::from)
    }

    /// Create a new demand cliet which will eagerly create underlying
    /// http clients to afford graceful failure on startup
    pub fn new() -> Result<Self, anyhow::Error> {
        Ok(DemandClient {
            h1_client: OnceLock::from(Self::init_client(HttpProto::Http1)?),
            h2c_client: OnceLock::from(Self::init_client(HttpProto::H2c)?),
            h2_client: OnceLock::from(Self::init_client(HttpProto::Http2)?),
        })
    }

    /// Send a demand bid request. If a non 200 status code is
    /// returned, the client will immediately return and skip
    /// reading the body (if any present)
    ///
    /// # Behavior
    /// Returns an error if encoding or request sending fails,
    /// but does not return an error on completed http requests
    /// regardless of status code. E.g. a server response of
    /// http 400 will return Ok because the request succeeded
    pub async fn send_request(
        &self,
        bidder: Arc<Bidder>,
        endpoint: Arc<Endpoint>,
        req: &BidRequest,
    ) -> Result<DemandResponse, anyhow::Error> {
        let client = match endpoint.protocol {
            HttpProto::Http1 => self.h1_client.get(),
            HttpProto::H2c => self.h2c_client.get(),
            HttpProto::Http2 => self.h2_client.get(),
        }
        .expect("Client should never be missing");

        if tracing::event_enabled!(tracing::Level::TRACE) {
            tracing::trace!("{}", serde_json::to_string(&req)?);
        }

        let request_encoding = RequestEncoder::encode(req, &endpoint.encoding, bidder.gzip)?;

        let mut headers = HeaderMap::new();
        for header in request_encoding.headers {
            let key = HeaderName::from_static(header.key);
            let value = HeaderValue::from_str(&header.value)
                .map_err(|e| anyhow!("Invalid header value: {}", e))?;

            headers.insert(key, value);
        }

        let req = client
            .post(&endpoint.url)
            .headers(headers)
            .body(request_encoding.data)
            .build()
            .map_err(|e| anyhow!("Failed to build http request for {}: {}", endpoint.name, e))?;

        let res = client.execute(req).await.map_err(|e| {
            anyhow!(
                "Failed to execute http request for {}: {}",
                endpoint.name,
                e
            )
        })?;

        let status = res.status();
        let status_code = res.status().as_u16() as u32;
        let status_message = res
            .status()
            .canonical_reason()
            .unwrap_or("no status message")
            .to_string();

        if status > StatusCode::OK {
            debug!("Non 200 status, early exit");
            return Ok(DemandResponse {
                status_code,
                status_message,
                response: None,
            });
        }

        debug!("Http 200 - awaiting body");

        let bytes = res
            .bytes()
            .await
            .map_err(|e| anyhow!("Failed to read http response for {}: {}", endpoint.name, e))?;

        let bid_response = ResponseDecoder::decode(&endpoint.encoding, &bytes)?;

        Ok(DemandResponse {
            status_code,
            status_message,
            response: Some(bid_response),
        })
    }
}
