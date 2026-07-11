use std::io;

use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::request_response;

/// ブロック交換プロトコルの識別子。
#[derive(Debug, Clone)]
pub struct BlockExchangeProtocol;

impl AsRef<str> for BlockExchangeProtocol {
    fn as_ref(&self) -> &str {
        "/deilephila/block-exchange/1.0.0"
    }
}

/// ブロック取得リクエスト: CID の raw bytes。
#[derive(Debug, Clone)]
pub struct WantBlock {
    pub cid_bytes: Vec<u8>,
}

/// ブロック取得レスポンス。
#[derive(Debug, Clone)]
pub enum BlockResponse {
    Found { data: Vec<u8> },
    NotFound,
}

/// リクエスト(CID bytes)の長さ上限。CID は実際には数十バイト。
const MAX_REQUEST_LEN: u32 = 256;
/// レスポンス(ブロック本体)の長さ上限。イベント 1 ブロックは高々数 KB。
const MAX_RESPONSE_LEN: u32 = 1024 * 1024;

/// request_response 用 codec。長さプレフィックス付きバイト列交換。
#[derive(Debug, Clone, Default)]
pub struct BlockExchangeCodec;

#[async_trait]
impl request_response::Codec for BlockExchangeCodec {
    type Protocol = BlockExchangeProtocol;
    type Request = WantBlock;
    type Response = BlockResponse;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let buf = read_length_prefixed(io, MAX_REQUEST_LEN).await?;
        Ok(WantBlock { cid_bytes: buf })
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let tag = read_u8(io).await?;
        match tag {
            0 => Ok(BlockResponse::NotFound),
            1 => {
                let buf = read_length_prefixed(io, MAX_RESPONSE_LEN).await?;
                Ok(BlockResponse::Found { data: buf })
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "unknown tag")),
        }
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_u32(io, req.cid_bytes.len() as u32).await?;
        io.write_all(&req.cid_bytes).await?;
        io.flush().await
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        resp: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        match resp {
            BlockResponse::NotFound => {
                write_u8(io, 0).await?;
            }
            BlockResponse::Found { data } => {
                write_u8(io, 1).await?;
                write_u32(io, data.len() as u32).await?;
                io.write_all(&data).await?;
            }
        }
        io.flush().await
    }
}

// --- フレーミングヘルパー ---

async fn read_u8<T: AsyncRead + Unpin>(io: &mut T) -> io::Result<u8> {
    let mut buf = [0u8; 1];
    io.read_exact(&mut buf).await?;
    Ok(buf[0])
}

async fn read_u32<T: AsyncRead + Unpin>(io: &mut T) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    io.read_exact(&mut buf).await?;
    Ok(u32::from_be_bytes(buf))
}

/// u32 長プレフィックス付きバイト列を読む。申告長が `max` を超える場合は
/// バッファを確保せず `InvalidData` で即拒否する(メモリ DoS 対策)。
async fn read_length_prefixed<T: AsyncRead + Unpin>(io: &mut T, max: u32) -> io::Result<Vec<u8>> {
    let len = read_u32(io).await?;
    if len > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("length prefix {len} exceeds limit {max}"),
        ));
    }
    let mut buf = vec![0u8; len as usize];
    io.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn write_u8<T: AsyncWrite + Unpin>(io: &mut T, v: u8) -> io::Result<()> {
    io.write_all(&[v]).await
}

async fn write_u32<T: AsyncWrite + Unpin>(io: &mut T, v: u32) -> io::Result<()> {
    io.write_all(&v.to_be_bytes()).await
}

// --- テスト ---

#[cfg(test)]
mod tests {
    use super::*;
    use futures::io::Cursor;
    use request_response::Codec;

    #[tokio::test]
    async fn read_request_rejects_oversized_length() {
        let mut codec = BlockExchangeCodec;
        let mut io = Cursor::new((MAX_REQUEST_LEN + 1).to_be_bytes().to_vec());
        let err = codec
            .read_request(&BlockExchangeProtocol, &mut io)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn read_response_rejects_oversized_length() {
        let mut codec = BlockExchangeCodec;
        let mut bytes = vec![1u8]; // Found タグ
        bytes.extend_from_slice(&(MAX_RESPONSE_LEN + 1).to_be_bytes());
        let mut io = Cursor::new(bytes);
        let err = codec
            .read_response(&BlockExchangeProtocol, &mut io)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn request_round_trip_within_limit() {
        let mut codec = BlockExchangeCodec;
        let req = WantBlock {
            cid_bytes: vec![0xAB; 36],
        };
        let mut buf = Cursor::new(Vec::new());
        codec
            .write_request(&BlockExchangeProtocol, &mut buf, req.clone())
            .await
            .unwrap();
        let mut io = Cursor::new(buf.into_inner());
        let read = codec
            .read_request(&BlockExchangeProtocol, &mut io)
            .await
            .unwrap();
        assert_eq!(read.cid_bytes, req.cid_bytes);
    }

    #[tokio::test]
    async fn response_round_trip_within_limit() {
        let mut codec = BlockExchangeCodec;
        let data = vec![0xCD; 4096];
        let mut buf = Cursor::new(Vec::new());
        codec
            .write_response(
                &BlockExchangeProtocol,
                &mut buf,
                BlockResponse::Found { data: data.clone() },
            )
            .await
            .unwrap();
        let mut io = Cursor::new(buf.into_inner());
        match codec
            .read_response(&BlockExchangeProtocol, &mut io)
            .await
            .unwrap()
        {
            BlockResponse::Found { data: read } => assert_eq!(read, data),
            BlockResponse::NotFound => panic!("expected Found"),
        }
    }
}
