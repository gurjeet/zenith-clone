//! Server-side synchronous Postgres connection, as limited as we need.
//! To use, create PostgresBackend and run() it, passing the Handler
//! implementation determining how to process the queries. Currently its API
//! is rather narrow, but we can extend it once required.

use crate::pq_proto::{BeMessage, FeMessage, FeStartupMessage, StartupRequestCode};
use crate::sock_split::{BidiStream, ReadStream, WriteStream};
use anyhow::{anyhow, bail, ensure, Result};
use bytes::{Bytes, BytesMut};
use log::*;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::io::{self, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::str::FromStr;
use std::sync::Arc;

pub trait Handler {
    /// Handle single query.
    /// postgres_backend will issue ReadyForQuery after calling this (this
    /// might be not what we want after CopyData streaming, but currently we don't
    /// care).
    fn process_query(&mut self, pgb: &mut PostgresBackend, query_string: Bytes) -> Result<()>;

    /// Called on startup packet receival, allows to process params.
    ///
    /// If Ok(false) is returned postgres_backend will skip auth -- that is needed for new users
    /// creation is the proxy code. That is quite hacky and ad-hoc solution, may be we could allow
    /// to override whole init logic in implementations.
    fn startup(&mut self, _pgb: &mut PostgresBackend, _sm: &FeStartupMessage) -> Result<()> {
        Ok(())
    }

    /// Check auth md5
    fn check_auth_md5(&mut self, _pgb: &mut PostgresBackend, _md5_response: &[u8]) -> Result<()> {
        bail!("MD5 auth failed")
    }

    /// Check auth jwt
    fn check_auth_jwt(&mut self, _pgb: &mut PostgresBackend, _jwt_response: &[u8]) -> Result<()> {
        bail!("JWT auth failed")
    }
}

/// PostgresBackend protocol state.
/// XXX: The order of the constructors matters.
#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub enum ProtoState {
    Initialization,
    Encrypted,
    Authentication,
    Established,
}

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum AuthType {
    Trust,
    MD5,
    // This mimics postgres's AuthenticationCleartextPassword but instead of password expects JWT
    ZenithJWT,
}

impl FromStr for AuthType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Trust" => Ok(Self::Trust),
            "MD5" => Ok(Self::MD5),
            "ZenithJWT" => Ok(Self::ZenithJWT),
            _ => bail!("invalid value \"{}\" for auth type", s),
        }
    }
}

#[derive(Clone, Copy)]
pub enum ProcessMsgResult {
    Continue,
    Break,
}

/// Always-writeable sock_split stream.
/// May not be readable. See [`PostgresBackend::take_stream_in`]
pub enum Stream {
    Bidirectional(BidiStream),
    WriteOnly(WriteStream),
}

impl Stream {
    fn shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        match self {
            Self::Bidirectional(bidi_stream) => bidi_stream.shutdown(how),
            Self::WriteOnly(write_stream) => write_stream.shutdown(how),
        }
    }
}

impl io::Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Bidirectional(bidi_stream) => bidi_stream.write(buf),
            Self::WriteOnly(write_stream) => write_stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Bidirectional(bidi_stream) => bidi_stream.flush(),
            Self::WriteOnly(write_stream) => write_stream.flush(),
        }
    }
}

pub struct PostgresBackend {
    stream: Option<Stream>,
    // Output buffer. c.f. BeMessage::write why we are using BytesMut here.
    buf_out: BytesMut,

    pub state: ProtoState,

    md5_salt: [u8; 4],
    auth_type: AuthType,

    peer_addr: SocketAddr,
    pub tls_config: Option<Arc<rustls::ServerConfig>>,
}

pub fn query_from_cstring(query_string: Bytes) -> Vec<u8> {
    let mut query_string = query_string.to_vec();
    if let Some(ch) = query_string.last() {
        if *ch == 0 {
            query_string.pop();
        }
    }
    query_string
}

impl PostgresBackend {
    pub fn new(
        socket: TcpStream,
        auth_type: AuthType,
        tls_config: Option<Arc<rustls::ServerConfig>>,
    ) -> io::Result<Self> {
        let peer_addr = socket.peer_addr()?;
        Ok(Self {
            stream: Some(Stream::Bidirectional(BidiStream::from_tcp(socket))),
            buf_out: BytesMut::with_capacity(10 * 1024),
            state: ProtoState::Initialization,
            md5_salt: [0u8; 4],
            auth_type,
            tls_config,
            peer_addr,
        })
    }

    pub fn into_stream(self) -> Stream {
        self.stream.unwrap()
    }

    /// Get direct reference (into the Option) to the read stream.
    fn get_stream_in(&mut self) -> Result<&mut BidiStream> {
        match &mut self.stream {
            Some(Stream::Bidirectional(stream)) => Ok(stream),
            _ => Err(anyhow!("reader taken")),
        }
    }

    pub fn get_peer_addr(&self) -> &SocketAddr {
        &self.peer_addr
    }

    pub fn take_stream_in(&mut self) -> Option<ReadStream> {
        let stream = self.stream.take();
        match stream {
            Some(Stream::Bidirectional(bidi_stream)) => {
                let (read, write) = bidi_stream.split();
                self.stream = Some(Stream::WriteOnly(write));
                Some(read)
            }
            stream => {
                self.stream = stream;
                None
            }
        }
    }

    /// Read full message or return None if connection is closed.
    pub fn read_message(&mut self) -> Result<Option<FeMessage>> {
        let (state, stream) = (self.state, self.get_stream_in()?);

        use ProtoState::*;
        match state {
            Initialization | Encrypted => FeStartupMessage::read(stream),
            Authentication | Established => FeMessage::read(stream),
        }
    }

    /// Write message into internal output buffer.
    pub fn write_message_noflush(&mut self, message: &BeMessage) -> io::Result<&mut Self> {
        BeMessage::write(&mut self.buf_out, message)?;
        Ok(self)
    }

    /// Flush output buffer into the socket.
    pub fn flush(&mut self) -> io::Result<&mut Self> {
        let stream = self.stream.as_mut().unwrap();
        stream.write_all(&self.buf_out)?;
        self.buf_out.clear();
        Ok(self)
    }

    /// Write message into internal buffer and flush it.
    pub fn write_message(&mut self, message: &BeMessage) -> io::Result<&mut Self> {
        self.write_message_noflush(message)?;
        self.flush()
    }

    // Wrapper for run_message_loop() that shuts down socket when we are done
    pub fn run(mut self, handler: &mut impl Handler) -> Result<()> {
        let ret = self.run_message_loop(handler);
        if let Some(stream) = self.stream.as_mut() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        ret
    }

    fn run_message_loop(&mut self, handler: &mut impl Handler) -> Result<()> {
        trace!("postgres backend to {:?} started", self.peer_addr);

        let mut unnamed_query_string = Bytes::new();

        while let Some(msg) = self.read_message()? {
            trace!("got message {:?}", msg);

            match self.process_message(handler, msg, &mut unnamed_query_string)? {
                ProcessMsgResult::Continue => continue,
                ProcessMsgResult::Break => break,
            }
        }

        trace!("postgres backend to {:?} exited", self.peer_addr);
        Ok(())
    }

    pub fn start_tls(&mut self) -> anyhow::Result<()> {
        match self.stream.take() {
            Some(Stream::Bidirectional(bidi_stream)) => {
                let session = rustls::ServerSession::new(&self.tls_config.clone().unwrap());
                self.stream = Some(Stream::Bidirectional(bidi_stream.start_tls(session)?));
                Ok(())
            }
            stream => {
                self.stream = stream;
                bail!("can't start TLs without bidi stream");
            }
        }
    }

    fn process_message(
        &mut self,
        handler: &mut impl Handler,
        msg: FeMessage,
        unnamed_query_string: &mut Bytes,
    ) -> Result<ProcessMsgResult> {
        // Allow only startup and password messages during auth. Otherwise client would be able to bypass auth
        // TODO: change that to proper top-level match of protocol state with separate message handling for each state
        if self.state < ProtoState::Established {
            ensure!(
                matches!(
                    msg,
                    FeMessage::PasswordMessage(_) | FeMessage::StartupMessage(_)
                ),
                "protocol violation"
            );
        }

        match msg {
            FeMessage::StartupMessage(m) => {
                trace!("got startup message {:?}", m);

                match m.kind {
                    StartupRequestCode::NegotiateSsl => {
                        info!("SSL requested");

                        if self.tls_config.is_some() {
                            self.write_message(&BeMessage::EncryptionResponse(true))?;
                            self.start_tls()?;
                            self.state = ProtoState::Encrypted;
                        } else {
                            self.write_message(&BeMessage::EncryptionResponse(false))?;
                        }
                    }
                    StartupRequestCode::NegotiateGss => {
                        info!("GSS requested");
                        self.write_message(&BeMessage::EncryptionResponse(false))?;
                    }
                    StartupRequestCode::Normal => {
                        if self.tls_config.is_some() && !matches!(self.state, ProtoState::Encrypted)
                        {
                            self.write_message(&BeMessage::ErrorResponse(
                                "must connect with TLS".to_string(),
                            ))?;
                            bail!("client did not connect with TLS");
                        }

                        // NB: startup() may change self.auth_type -- we are using that in proxy code
                        // to bypass auth for new users.
                        handler.startup(self, &m)?;

                        match self.auth_type {
                            AuthType::Trust => {
                                self.write_message_noflush(&BeMessage::AuthenticationOk)?;
                                // psycopg2 will not connect if client_encoding is not
                                // specified by the server
                                self.write_message_noflush(&BeMessage::ParameterStatus)?;
                                self.write_message(&BeMessage::ReadyForQuery)?;
                                self.state = ProtoState::Established;
                            }
                            AuthType::MD5 => {
                                rand::thread_rng().fill(&mut self.md5_salt);
                                let md5_salt = self.md5_salt;
                                self.write_message(&BeMessage::AuthenticationMD5Password(
                                    &md5_salt,
                                ))?;
                                self.state = ProtoState::Authentication;
                            }
                            AuthType::ZenithJWT => {
                                self.write_message(&BeMessage::AuthenticationCleartextPassword)?;
                                self.state = ProtoState::Authentication;
                            }
                        }
                    }
                    StartupRequestCode::Cancel => {
                        return Ok(ProcessMsgResult::Break);
                    }
                }
            }

            FeMessage::PasswordMessage(m) => {
                trace!("got password message '{:?}'", m);

                assert!(self.state == ProtoState::Authentication);

                match self.auth_type {
                    AuthType::Trust => unreachable!(),
                    AuthType::MD5 => {
                        let (_, md5_response) = m
                            .split_last()
                            .ok_or_else(|| anyhow::Error::msg("protocol violation"))?;

                        if let Err(e) = handler.check_auth_md5(self, md5_response) {
                            self.write_message(&BeMessage::ErrorResponse(format!("{}", e)))?;
                            bail!("auth failed: {}", e);
                        }
                    }
                    AuthType::ZenithJWT => {
                        let (_, jwt_response) = m
                            .split_last()
                            .ok_or_else(|| anyhow::Error::msg("protocol violation"))?;

                        if let Err(e) = handler.check_auth_jwt(self, jwt_response) {
                            self.write_message(&BeMessage::ErrorResponse(format!("{}", e)))?;
                            bail!("auth failed: {}", e);
                        }
                    }
                }
                self.write_message_noflush(&BeMessage::AuthenticationOk)?;
                // psycopg2 will not connect if client_encoding is not
                // specified by the server
                self.write_message_noflush(&BeMessage::ParameterStatus)?;
                self.write_message(&BeMessage::ReadyForQuery)?;
                self.state = ProtoState::Established;
            }

            FeMessage::Query(m) => {
                trace!("got query {:?}", m.body);
                // xxx distinguish fatal and recoverable errors?
                if let Err(e) = handler.process_query(self, m.body.clone()) {
                    let errmsg = format!("{}", e);
                    // ":#" uses the alternate formatting style, which makes anyhow display the
                    // full cause of the error, not just the top-level context. We don't want to
                    // send that in the ErrorResponse though, because it's not relevant to the
                    // compute node logs.
                    warn!("query handler for {:?} failed: {:#}", m.body, e);
                    self.write_message_noflush(&BeMessage::ErrorResponse(errmsg))?;
                }
                self.write_message(&BeMessage::ReadyForQuery)?;
            }

            FeMessage::Parse(m) => {
                *unnamed_query_string = m.query_string;
                self.write_message(&BeMessage::ParseComplete)?;
            }

            FeMessage::Describe(_) => {
                self.write_message_noflush(&BeMessage::ParameterDescription)?
                    .write_message(&BeMessage::NoData)?;
            }

            FeMessage::Bind(_) => {
                self.write_message(&BeMessage::BindComplete)?;
            }

            FeMessage::Close(_) => {
                self.write_message(&BeMessage::CloseComplete)?;
            }

            FeMessage::Execute(_) => {
                handler.process_query(self, unnamed_query_string.clone())?;
            }

            FeMessage::Sync => {
                self.write_message(&BeMessage::ReadyForQuery)?;
            }

            FeMessage::Terminate => {
                return Ok(ProcessMsgResult::Break);
            }

            // We prefer explicit pattern matching to wildcards, because
            // this helps us spot the places where new variants are missing
            FeMessage::CopyData(_) | FeMessage::CopyDone | FeMessage::CopyFail => {
                bail!("unexpected message type: {:?}", msg);
            }
        }

        Ok(ProcessMsgResult::Continue)
    }
}
