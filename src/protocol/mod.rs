use anyhow::{Result, anyhow};
use bytes::{BytesMut, Buf, BufMut};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use kafka_protocol::messages::{ApiKey, ResponseHeader, ApiVersionsResponse};
use kafka_protocol::protocol::Encodable;
use kafka_protocol::protocol::StrBytes;
use std::convert::TryFrom;
use std::any::{Any, TypeId};
use tracing::{debug, trace};

#[derive(Debug)]
pub struct KafkaRequest {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<StrBytes>,
    pub payload: Vec<u8>,
}

pub async fn read_request(stream: &mut TcpStream) -> Result<KafkaRequest> {
    // First read the size
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let size = i32::from_be_bytes(size_buf);
    
    debug!("Reading request of size {}", size);
    
    // Read the full message
    let mut buf = BytesMut::with_capacity(size as usize);
    buf.resize(size as usize, 0);
    stream.read_exact(&mut buf).await?;

    trace!("Raw request bytes: {:?}", buf);

    // Get API key and version
    let mut tmp = buf.clone();
    let raw_api_key = tmp.get_i16();
    let api_key = ApiKey::try_from(raw_api_key)
        .map_err(|_| anyhow!("Invalid API key: {}", raw_api_key))?;
    let api_version = tmp.get_i16();
    let correlation_id = tmp.get_i32();
    
    // Read client ID length
    let client_id_len = tmp.get_i16();
    let client_id = if client_id_len >= 0 {
        let mut client_id = vec![0; client_id_len as usize];
        tmp.copy_to_slice(&mut client_id);
        let client_id_str = String::from_utf8(client_id)?;
        Some(StrBytes::from_string(client_id_str))
    } else {
        None
    };

    debug!("Request header: api_key={:?}, version={}, correlation_id={}, client_id={:?}", 
           api_key, api_version, correlation_id, client_id);

    // Construct payload from the remaining bytes in tmp
    let mut payload = tmp.to_vec();
    if api_version == 0 {
        payload.push(0);
    }
    debug!("Request payload (len={}): {:x?}", payload.len(), payload);

    Ok(KafkaRequest {
        api_key,
        api_version,
        correlation_id,
        client_id,
        payload,
    })
}

pub async fn write_api_versions_response(
    stream: &mut TcpStream,
    response: &ApiVersionsResponse,
    version: i16,
    correlation_id: i32
) -> Result<()> {
    let mut buf = BytesMut::new();
    
    // Write response header (correlation_id: int32)
    buf.put_i32(correlation_id);
    
    if version == 0 {
        // error_code: int16
        buf.put_i16(response.error_code);
        
        // api_versions_count: int32
        buf.put_i32(response.api_keys.len() as i32);
        
        // Write each API version entry (each: api_key, min_version, max_version as int16s)
        for api_version in &response.api_keys {
            buf.put_i16(api_version.api_key);      // api_key: int16
            buf.put_i16(api_version.min_version);    // min_version: int16
            buf.put_i16(api_version.max_version);    // max_version: int16
        }
    } else {
        response.encode(&mut buf, version)?;
    }
    
    // Write size prefix (excluding the size field itself)
    let size = buf.len() as i32;
    let size_bytes = size.to_be_bytes();

    debug!("Writing ApiVersions response: size={}, correlation_id={}", size, correlation_id);
    trace!("Response header bytes: {:?}", &buf[..4]);  // First 4 bytes are correlation_id
    trace!("Response body bytes: {:?}", &buf[4..]);

    // Write the response
    stream.write_all(&size_bytes).await?;
    stream.write_all(&buf).await?;
    stream.flush().await?;
    
    Ok(())
}

pub async fn write_response<T: Encodable>(
    stream: &mut TcpStream,
    response: &T,
    version: i16,
    correlation_id: i32
) -> Result<()> {
    let mut buf = BytesMut::new();
    
    // Write response header (correlation_id: int32)
    buf.put_i32(correlation_id);
    trace!("After writing correlation_id={}, buf len={}", correlation_id, buf.len());
    
    // Write response body
    trace!("About to encode response with version={}", version);
    response.encode(&mut buf, version)?;
    trace!("After encoding response, buf len={}, raw bytes={:?}", buf.len(), buf);
    
    // Write size prefix (excluding the size field itself)
    let size = buf.len() as i32;
    let size_bytes = size.to_be_bytes();

    debug!("Writing response: type={}, size={}, correlation_id={}, version={}", 
           std::any::type_name::<T>(), size, correlation_id, version);
    trace!("Size bytes: {:?}", size_bytes);
    trace!("Response header bytes: {:?}", &buf[..4]);  // First 4 bytes are correlation_id
    trace!("Response body bytes: {:?}", &buf[4..]);

    // Write the response
    stream.write_all(&size_bytes).await?;
    trace!("Wrote size bytes");
    stream.write_all(&buf).await?;
    trace!("Wrote response bytes");
    stream.flush().await?;
    trace!("Flushed stream");
    
    Ok(())
} 