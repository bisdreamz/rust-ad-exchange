use crate::core::models::bidder::Encoding;
use anyhow::anyhow;
use bytes::Bytes;
use rtb::{BidRequest, BidResponse};
use std::cell::RefCell;

thread_local! {
    static COMPRESSOR: RefCell<libdeflater::Compressor> =
        RefCell::new(libdeflater::Compressor::new(libdeflater::CompressionLvl::fastest()));
}

pub struct Header {
    pub key: &'static str,
    pub value: String,
}

impl Header {
    pub fn new(key: &'static str, value: String) -> Self {
        Self { key, value }
    }
}

pub struct RequestEncoder {
    pub headers: Vec<Header>,
    pub data: Vec<u8>,
}

impl RequestEncoder {
    fn encode_json(req: &BidRequest) -> Result<Vec<u8>, anyhow::Error> {
        serde_json::to_vec(req).map_err(|e| anyhow::Error::from(e))
    }

    fn encode_protobuf(req: &BidRequest) -> Result<Vec<u8>, anyhow::Error> {
        use prost::Message;
        Ok(req.encode_to_vec())
    }

    fn compress(data: Vec<u8>) -> Result<Vec<u8>, anyhow::Error> {
        COMPRESSOR.with(|c| {
            let mut compressor = c.borrow_mut();
            let max_size = compressor.gzip_compress_bound(data.len());
            let mut compressed = vec![0u8; max_size];

            let actual_size = compressor
                .gzip_compress(&data, &mut compressed)
                .map_err(|e| anyhow!("Compression failed: {:?}", e))?;

            compressed.truncate(actual_size);
            Ok(compressed)
        })
    }

    /// Encodes the given request to a byte array and associated any required headers
    /// such as content type
    pub fn encode(
        req: &BidRequest,
        encoding: &Encoding,
        gzip: bool,
    ) -> Result<Self, anyhow::Error> {
        let mut headers = Vec::new();

        let mut data = match encoding {
            Encoding::Json => {
                headers.push(Header::new(
                    "content-type".into(),
                    "application/json".into(),
                ));

                Self::encode_json(req)
            }
            Encoding::Protobuf => {
                headers.push(Header::new(
                    "content-type".into(),
                    "application/x-protobuf".into(),
                ));

                Self::encode_protobuf(req)
            }
        }?;

        if gzip {
            headers.push(Header::new("content-encoding".into(), "gzip".into()));
            data = Self::compress(data)?;
        }

        Ok(Self { headers, data })
    }
}

pub struct ResponseDecoder;

impl ResponseDecoder {
    fn decode_protobuf(bytes: &Bytes) -> Result<BidResponse, anyhow::Error> {
        use prost::Message;
        BidResponse::decode(bytes.as_ref())
            .map_err(|e| anyhow!("Failed to decode protobuf response: {}", e))
    }

    fn decode_json(bytes: &Bytes) -> Result<BidResponse, anyhow::Error> {
        serde_json::from_slice(bytes.as_ref())
            .map_err(|e| anyhow!("Failed decoding json response: {}", e))
    }

    pub fn decode(encoding: &Encoding, data: &Bytes) -> Result<BidResponse, anyhow::Error> {
        match encoding {
            Encoding::Json => Self::decode_json(data),
            Encoding::Protobuf => Self::decode_protobuf(data),
        }
    }
}
