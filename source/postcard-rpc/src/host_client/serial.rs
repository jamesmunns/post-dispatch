use std::{collections::VecDeque, future::Future};

use cobs::encode_vec;
use postcard_schema::Schema;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio_serial::{SerialPortBuilderExt, SerialStream};

use crate::{
    accumulator::raw::{CobsAccumulator, FeedResult},
    header::VarSeqKind,
    host_client::{HostClient, WireRx, WireSpawn, WireTx},
};

/// # Serial Constructor Methods
///
/// These methods are used to create a new [HostClient] instance for use with tokio serial and cobs encoding.
///
/// **Requires feature**: `cobs-serial`
impl<WireErr> HostClient<WireErr>
where
    WireErr: DeserializeOwned + Schema,
{
    /// Create a new [HostClient]
    ///
    /// `serial_path` is the path to the serial port used. `err_uri_path` is
    /// the path associated with the `WireErr` message type.
    ///
    /// This constructor is available when the `cobs-serial` feature is enabled.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// use postcard_rpc::host_client::HostClient;
    /// use postcard_rpc::header::VarSeqKind;
    /// use serde::{Serialize, Deserialize};
    /// use postcard_schema::Schema;
    ///
    /// /// A "wire error" type your server can use to respond to any
    /// /// kind of request, for example if deserializing a request fails
    /// #[derive(Debug, PartialEq, Schema, Serialize, Deserialize)]
    /// pub enum Error {
    ///    SomethingBad
    /// }
    ///
    /// let client = HostClient::<Error>::new_serial_cobs(
    ///     // the serial port path
    ///     "/dev/ttyACM0",
    ///     // the URI/path for `Error` messages
    ///     "error",
    ///     // Outgoing queue depth in messages
    ///     8,
    ///     // Baud rate of serial (does not generally matter for
    ///     //  USB UART/CDC-ACM serial connections)
    ///     115_200,
    ///     // Use one-byte sequence numbers
    ///     VarSeqKind::Seq1,
    /// );
    /// ```
    pub fn try_new_serial_cobs(
        serial_path: &str,
        err_uri_path: &str,
        outgoing_depth: usize,
        baud: u32,
        seq_no_kind: VarSeqKind,
    ) -> Result<Self, String> {
        let port = tokio_serial::new(serial_path, baud)
            .open_native_async()
            .map_err(|e| format!("Open Error: {e:?}"))?;

        let (rx, tx) = tokio::io::split(port);

        Ok(HostClient::new_with_wire(
            SerialWireTx { tx },
            SerialWireRx {
                rx,
                buf: Box::new([0u8; 1024]),
                acc: Box::new(CobsAccumulator::new()),
                pending: VecDeque::new(),
            },
            SerialSpawn,
            seq_no_kind,
            err_uri_path,
            outgoing_depth,
        ))
    }

    /// Create a new [HostClient]
    ///
    /// Panics if we couldn't open the serial port.
    ///
    /// See [`HostClient::try_new_serial_cobs`] for more details
    pub fn new_serial_cobs(
        serial_path: &str,
        err_uri_path: &str,
        outgoing_depth: usize,
        baud: u32,
        seq_no_kind: VarSeqKind,
    ) -> Self {
        Self::try_new_serial_cobs(serial_path, err_uri_path, outgoing_depth, baud, seq_no_kind)
            .unwrap()
    }
}

//////////////////////////////////////////////////////////////////////////////
// Wire Interface Implementation
//////////////////////////////////////////////////////////////////////////////

/// Tokio Serial Wire Interface Implementor
///
/// Uses Tokio for spawning tasks
struct SerialSpawn;

impl WireSpawn for SerialSpawn {
    fn spawn(&mut self, fut: impl Future<Output = ()> + Send + 'static) {
        // Explicitly drop the joinhandle as it impls Future and this makes
        // clippy mad if you just let it drop implicitly
        core::mem::drop(tokio::task::spawn(fut));
    }
}

/// Tokio Serial Wire Transmit Interface Implementor
struct SerialWireTx {
    // boq: Queue<Vec<u8>>,
    tx: WriteHalf<SerialStream>,
}

#[derive(thiserror::Error, Debug)]
enum SerialWireTxError {
    #[error("Transfer Error on Send")]
    Transfer(#[from] std::io::Error),
}

impl WireTx for SerialWireTx {
    type Error = SerialWireTxError;

    #[inline]
    fn send(&mut self, data: Vec<u8>) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.send_inner(data)
    }
}

impl SerialWireTx {
    async fn send_inner(&mut self, data: Vec<u8>) -> Result<(), SerialWireTxError> {
        // Turn the serialized message into a COBS encoded message
        //
        // TODO: this is a little wasteful, data is already a vec,
        // then we encode that to a second cobs-encoded vec. Oh well.
        let mut msg = encode_vec(&data);
        msg.push(0);

        // And send it!
        self.tx.write_all(&msg).await?;
        Ok(())
    }
}

/// NUSB Wire Receive Interface Implementor
struct SerialWireRx {
    rx: ReadHalf<SerialStream>,
    buf: Box<[u8; 1024]>,
    acc: Box<CobsAccumulator<1024>>,
    pending: VecDeque<Vec<u8>>,
}

#[derive(thiserror::Error, Debug)]
enum SerialWireRxError {
    #[error("Transfer Error on Recv")]
    Transfer(#[from] std::io::Error),
}

impl WireRx for SerialWireRx {
    type Error = SerialWireRxError;

    #[inline]
    fn receive(&mut self) -> impl Future<Output = Result<Vec<u8>, Self::Error>> + Send {
        self.recv_inner()
    }
}

impl SerialWireRx {
    async fn recv_inner(&mut self) -> Result<Vec<u8>, SerialWireRxError> {
        // Receive until we've gotten AT LEAST one message, though we will continue
        // consuming and buffering any read (partial) messages, to ensure they are not lost.
        loop {
            // Do we have any messages already prepared?
            if let Some(p) = self.pending.pop_front() {
                return Ok(p);
            }

            // Nothing in the pending queue, do a read to see if we can pull more
            // data from the serial port
            let used = self.rx.read(self.buf.as_mut_slice()).await?;

            let mut window = &self.buf[..used];

            // This buffering loop is necessary as a single `read()` might include
            // more than one message
            'cobs: while !window.is_empty() {
                window = match self.acc.feed(window) {
                    // Consumed the whole USB frame
                    FeedResult::Consumed => break 'cobs,
                    // Ignore line errors
                    FeedResult::OverFull(new_wind) => {
                        tracing::warn!("Overflowed COBS accumulator");
                        new_wind
                    }
                    FeedResult::DeserError(new_wind) => {
                        tracing::warn!("COBS formatting error");
                        new_wind
                    }
                    // We got a message! Attempt to dispatch it
                    FeedResult::Success { data, remaining } => {
                        self.pending.push_back(data.to_vec());
                        remaining
                    }
                };
            }
        }
    }
}
