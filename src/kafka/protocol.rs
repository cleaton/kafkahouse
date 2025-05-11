use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::buf::ByteBuf;
use kafka_protocol::protocol::{Decodable, Encodable, VersionRange};
use log::debug;

// Represents a parsed Kafka request with its header and payload
#[derive(Debug)]
pub struct KafkaRequestMessage {
    pub header: RequestHeader,
    pub api_key: ApiKey,
    pub request: RequestKind,
}

#[inline]
fn is_valid_version(version: i16, range: &VersionRange) -> bool {
    version >= range.min && version <= range.max
}

impl KafkaRequestMessage {
    pub fn decode(data: &mut Bytes) -> Result<Self, anyhow::Error> {
        debug!("Decoding request with data length: {:?}", data.len());

        let api_key = ApiKey::try_from(data.peek_bytes(0..2).get_i16())
        .map_err(|_| anyhow::anyhow!("Invalid API key"))?;
        let api_version = data.peek_bytes(2..4).get_i16();
        if !is_valid_version(api_version, &api_key.valid_versions()) {
            return Err(anyhow::anyhow!("Invalid API version"));
        }
        let header_version = api_key.request_header_version(api_version);
        let header = RequestHeader::decode(data, header_version).unwrap();
        debug!("Decoded request header with correlation_id: {}", header.correlation_id);
        debug!("Decoding request with API key: {:?}", api_key);
        debug!("Data length: {:?}", data.len());
        debug!("Data: {:?}", data);
        let request = RequestKind::decode(api_key, data, header.request_api_version)?;
        Ok(KafkaRequestMessage { header, api_key, request })
    }
}


// Represents a processed response ready to be sent
#[derive(Debug)]
pub struct KafkaResponseMessage {
    pub request_header: RequestHeader,
    pub api_key: ApiKey,
    pub response: ResponseKind,
    pub response_size: i32,
}

impl KafkaResponseMessage {
    pub fn encode(&self, buffer: &mut BytesMut) -> Result<(), anyhow::Error> {
        let api_version = self.request_header.request_api_version;
        debug!("Encoding response for correlation_id: {}", self.request_header.correlation_id);
        
        // Encode response header first
        let response_header = ResponseHeader::default()
            .with_correlation_id(self.request_header.correlation_id);
        debug!("Created response header with correlation_id: {}", response_header.correlation_id);

        let response_header_version = self.api_key.response_header_version(api_version);
        let header_size = response_header.compute_size(response_header_version)? as i32;

        // Write size at start of buffer (excluding size field itself)
        buffer.put_i32(header_size + self.response_size);
        response_header.encode(buffer, response_header_version)?;
        self.response.encode(buffer, api_version)?;

        Ok(())
    }
}