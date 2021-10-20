//! Initial connection functions.

use bytes::Bytes;
use futures::{future::BoxFuture, Future, FutureExt, Sink, Stream, TryStreamExt};
use std::{
    convert::TryInto,
    error::Error,
    fmt, io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};
use tokio_util::codec::LengthDelimitedCodec;

use crate::{
    chmux::{ChMux, ChMuxError},
    codec,
    rch::base,
    RemoteSend,
};

/// Error occurred during establishing a connection over a physical transport.
#[cfg_attr(docsrs, doc(cfg(feature = "rch")))]
#[derive(Debug, Clone)]
pub enum ConnectError<TransportSinkError, TransportStreamError> {
    /// Establishing [chmux](crate::chmux) connection failed.
    ChMux(ChMuxError<TransportSinkError, TransportStreamError>),
    /// Opening initial [remote](crate::rch::base) channel failed.
    RemoteConnect(base::ConnectError),
}

impl<TransportSinkError, TransportStreamError> fmt::Display
    for ConnectError<TransportSinkError, TransportStreamError>
where
    TransportSinkError: fmt::Display,
    TransportStreamError: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::ChMux(err) => write!(f, "chmux error: {}", err),
            Self::RemoteConnect(err) => write!(f, "channel connect failed: {}", err),
        }
    }
}

impl<TransportSinkError, TransportStreamError> Error for ConnectError<TransportSinkError, TransportStreamError>
where
    TransportSinkError: Error,
    TransportStreamError: Error,
{
}

impl<TransportSinkError, TransportStreamError> From<ChMuxError<TransportSinkError, TransportStreamError>>
    for ConnectError<TransportSinkError, TransportStreamError>
{
    fn from(err: ChMuxError<TransportSinkError, TransportStreamError>) -> Self {
        Self::ChMux(err)
    }
}

impl<TransportSinkError, TransportStreamError> From<base::ConnectError>
    for ConnectError<TransportSinkError, TransportStreamError>
{
    fn from(err: base::ConnectError) -> Self {
        Self::RemoteConnect(err)
    }
}

/// Methods for establishing a connection over a physical transport.
///
/// You must poll the returned [Connect] future or spawn it onto a task for the connection to work.
///
/// # Example
///
/// In the following example the server listens on TCP port 9875 and the client connects to it.
/// Then both ends establish a ReMOC connection using [Connect::io] over the TCP connection.
/// The connection dispatchers are spawned onto new tasks and the `client` and `server` functions
/// are called with the established [base channel](crate::rch::base).
///
/// ```
/// use std::net::Ipv4Addr;
/// use tokio::net::{TcpStream, TcpListener};
/// use remoc::prelude::*;
///
/// #[tokio::main]
/// async fn main() {
///     // For demonstration we run both client and server in
///     // the same process. In real life connect_client() and
///     // connect_server() would run on different machines.
///     futures::join!(connect_client(), connect_server());
/// }
///
/// // This would be run on the client.
/// async fn connect_client() {
///     // Wait for server to be ready.
///     tokio::time::sleep(std::time::Duration::from_secs(1)).await;
///
///     // Establish TCP connection.
///     let socket = TcpStream::connect((Ipv4Addr::LOCALHOST, 9875)).await.unwrap();
///     let (socket_rx, socket_tx) = socket.into_split();
///
///     // Establish ReMOC connection over TCP.
///     let (conn, tx, rx) =
///         remoc::Connect::io(Default::default(), socket_rx, socket_tx).await.unwrap();
///     tokio::spawn(conn);
///
///     // Run client.
///     client(tx, rx).await;
/// }
///
/// // This would be run on the server.
/// async fn connect_server() {
///     // Listen for incoming TCP connection.
///     let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 9875)).await.unwrap();
///     let (socket, _) = listener.accept().await.unwrap();
///     let (socket_rx, socket_tx) = socket.into_split();
///
///     // Establish ReMOC connection over TCP.
///     let (conn, tx, rx) =
///         remoc::Connect::io(Default::default(), socket_rx, socket_tx).await.unwrap();
///     tokio::spawn(conn);
///
///     // Run server.
///     server(tx, rx).await;
/// }
///
/// // This would be run on the client.
/// async fn client(mut tx: rch::base::Sender<u16>, mut rx: rch::base::Receiver<String>) {
///     tx.send(1).await.unwrap();
///     assert_eq!(rx.recv().await.unwrap(), Some("1".to_string()));
/// }
///
/// // This would be run on the server.
/// async fn server(mut tx: rch::base::Sender<String>, mut rx: rch::base::Receiver<u16>) {
///     while let Some(number) = rx.recv().await.unwrap() {
///         tx.send(number.to_string()).await.unwrap();
///     }
/// }
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "rch")))]
#[must_use = "You must poll or spawn the Connect future for the connection to work."]
pub struct Connect<'transport, TransportSinkError, TransportStreamError>(
    BoxFuture<'transport, Result<(), ChMuxError<TransportSinkError, TransportStreamError>>>,
);

impl<'transport, TransportSinkError, TransportStreamError>
    Connect<'transport, TransportSinkError, TransportStreamError>
{
    /// Establishes a connection over a framed transport (a [sink](Sink) and a [stream](Stream) of binary data) and
    /// returns a remote [sender](base::Sender) and [receiver](base::Receiver).
    ///
    /// This establishes a [chmux](crate::chmux) connection over the transport and opens a remote channel.
    ///
    /// You must poll the returned [Connect] future or spawn it for the connection to work.
    ///
    /// # Panics
    /// Panics if the chmux configuration is invalid.
    pub async fn framed<TransportSink, TransportStream, Tx, Rx, Codec>(
        cfg: crate::Cfg, transport_sink: TransportSink, transport_stream: TransportStream,
    ) -> Result<
        (
            Connect<'transport, TransportSinkError, TransportStreamError>,
            base::Sender<Tx, Codec>,
            base::Receiver<Rx, Codec>,
        ),
        ConnectError<TransportSinkError, TransportStreamError>,
    >
    where
        TransportSink: Sink<Bytes, Error = TransportSinkError> + Send + Sync + Unpin + 'transport,
        TransportSinkError: Error + Send + Sync + 'static,
        TransportStream: Stream<Item = Result<Bytes, TransportStreamError>> + Send + Sync + Unpin + 'transport,
        TransportStreamError: Error + Send + Sync + 'static,
        Tx: RemoteSend,
        Rx: RemoteSend,
        Codec: codec::Codec,
    {
        let (mux, client, mut listener) = ChMux::new(cfg, transport_sink, transport_stream).await?;
        let mut connection = Self(mux.run().boxed());

        tokio::select! {
            biased;
            Err(err) = &mut connection => Err(err.into()),
            result = base::connect(&client, &mut listener) => {
                match result {
                    Ok((tx, rx)) => Ok((connection, tx, rx)),
                    Err(err) => Err(err.into()),
                }
            }
        }
    }
}

impl<'transport> Connect<'transport, io::Error, io::Error> {
    /// Establishes a connection over an IO transport (an [AsyncRead] and [AsyncWrite]) and
    /// returns a remote [sender](base::Sender) and [receiver](base::Receiver).
    ///
    /// A [chmux](crate::chmux) connection is established over the transport and a remote channel is opened.
    /// This prepends a length header to each chmux packet for transportation over the unframed connection.
    ///
    /// This method performs no buffering of read and writes and thus may exhibit suboptimal
    /// performance if the underlying reader and writer are unbuffered.
    /// In this case use [io_buffered](Self::io_buffered) instead.
    ///
    /// You must poll the returned [Connect] future or spawn it for the connection to work.
    ///
    /// # Panics
    /// Panics if the chmux configuration is invalid.
    pub async fn io<Read, Write, Tx, Rx, Codec>(
        cfg: crate::Cfg, input: Read, output: Write,
    ) -> Result<
        (Connect<'transport, io::Error, io::Error>, base::Sender<Tx, Codec>, base::Receiver<Rx, Codec>),
        ConnectError<io::Error, io::Error>,
    >
    where
        Read: AsyncRead + Send + Sync + Unpin + 'transport,
        Write: AsyncWrite + Send + Sync + Unpin + 'transport,
        Tx: RemoteSend,
        Rx: RemoteSend,
        Codec: codec::Codec,
    {
        let max_recv_frame_length: usize = cfg.max_frame_length().try_into().unwrap();
        let transport_sink = LengthDelimitedCodec::builder()
            .little_endian()
            .length_field_length(4)
            .max_frame_length(u32::MAX as _)
            .new_write(output);
        let transport_stream = LengthDelimitedCodec::builder()
            .little_endian()
            .length_field_length(4)
            .max_frame_length(max_recv_frame_length)
            .new_read(input)
            .map_ok(|item| item.freeze());
        Self::framed(cfg, transport_sink, transport_stream).await
    }

    /// Establishes a buffered connection over an IO transport (an [AsyncRead] and [AsyncWrite]) and
    /// returns a remote [sender](base::Sender) and [receiver](base::Receiver).
    ///
    /// A [chmux](crate::chmux) connection is established over the transport and a remote channel is opened.
    /// This prepends a length header to each chmux packet for transportation over the unframed connection.
    ///
    /// This method performs internal buffering of reads and writes.
    ///
    /// You must poll the returned [Connect] future or spawn it for the connection to work.
    ///
    /// # Panics
    /// Panics if the chmux configuration is invalid.
    pub async fn io_buffered<Read, Write, Tx, Rx, Codec>(
        cfg: crate::Cfg, input: Read, output: Write, buffer: usize,
    ) -> Result<
        (Connect<'transport, io::Error, io::Error>, base::Sender<Tx, Codec>, base::Receiver<Rx, Codec>),
        ConnectError<io::Error, io::Error>,
    >
    where
        Read: AsyncRead + Send + Sync + Unpin + 'transport,
        Write: AsyncWrite + Send + Sync + Unpin + 'transport,
        Tx: RemoteSend,
        Rx: RemoteSend,
        Codec: codec::Codec,
    {
        let buf_input = BufReader::with_capacity(buffer, input);
        let buf_output = BufWriter::with_capacity(buffer, output);
        Self::io(cfg, buf_input, buf_output).await
    }
}

impl<'transport, TransportSinkError, TransportStreamError> Future
    for Connect<'transport, TransportSinkError, TransportStreamError>
{
    /// Result of connection after it has been terminated.
    type Output = Result<(), ChMuxError<TransportSinkError, TransportStreamError>>;

    /// This future runs the dispatcher for this connection.
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        Pin::into_inner(self).0.poll_unpin(cx)
    }
}
