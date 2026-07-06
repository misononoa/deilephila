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
        let len = read_u32(io).await?;
        let mut buf = vec![0u8; len as usize];
        io.read_exact(&mut buf).await?;
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
                let len = read_u32(io).await?;
                let mut buf = vec![0u8; len as usize];
                io.read_exact(&mut buf).await?;
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

async fn write_u8<T: AsyncWrite + Unpin>(io: &mut T, v: u8) -> io::Result<()> {
    io.write_all(&[v]).await
}

async fn write_u32<T: AsyncWrite + Unpin>(io: &mut T, v: u32) -> io::Result<()> {
    io.write_all(&v.to_be_bytes()).await
}
