//! A postcard-rpc host client
//!
//! This library is meant to be used with the `Dispatch` type and the
//! postcard-rpc wire protocol.

use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

#[cfg(feature = "cobs-serial")]
mod serial;

use crate::{Endpoint, Key, Topic, WireHeader};
use maitake_sync::{
    wait_map::{WaitError, WakeOutcome},
    WaitMap,
};
use postcard::experimental::schema::Schema;
use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    select,
    sync::mpsc::{Receiver, Sender},
};

/// Host Error Kind
#[derive(Debug, PartialEq)]
pub enum HostErr<WireErr> {
    /// An error of the user-specified wire error type
    Wire(WireErr),
    /// We got a response that didn't match the expected value or the
    /// user specified wire error type
    BadResponse,
    /// Exhausted number of retries.
    Retries,
    /// Deserialization of the message failed
    Postcard(postcard::Error),
    /// The interface has been closed, and no further messages are possible
    Closed,
}

impl<T> From<postcard::Error> for HostErr<T> {
    fn from(value: postcard::Error) -> Self {
        Self::Postcard(value)
    }
}

impl<T> From<WaitError> for HostErr<T> {
    fn from(_: WaitError) -> Self {
        Self::Closed
    }
}

/// The [HostClient] is the primary PC-side interface.
///
/// It is generic over a single type, `WireErr`, which can be used by the
/// embedded system when a request was not understood, or some other error
/// has occurred.
///
/// [HostClient]s can be cloned, and used across multiple tasks/threads.
pub struct HostClient<WireErr, const RETRY_BITS: usize = 0> {
    ctx: Arc<HostContext>,
    out: Sender<RpcFrame>,
    subber: Sender<SubInfo>,
    err_key: Key,
    _pd: PhantomData<fn() -> WireErr>,
}

/// # Constructor Methods
impl<WireErr, const RETRY_BITS: usize> HostClient<WireErr, RETRY_BITS>
where
    WireErr: DeserializeOwned + Schema,
{
    const _CHECK_NUM_BITS: () = assert!(RETRY_BITS <= 8);

    /// Create a new manually implemented [HostClient].
    ///
    /// This allows you to implement your own "Wire" abstraction, if you
    /// aren't using a COBS-encoded serial port.
    ///
    /// This is temporary solution until Rust 1.76 when async traits are
    /// stable, and we can have users provide a `Wire` trait that acts as
    /// a bidirectional [RpcFrame] sink/source.
    pub fn new_manual(err_uri_path: &str, outgoing_depth: usize) -> (Self, WireContext) {
        let _ = Self::_CHECK_NUM_BITS;

        let (tx_pc, rx_pc) = tokio::sync::mpsc::channel(outgoing_depth);
        let (tx_si, rx_si) = tokio::sync::mpsc::channel(outgoing_depth);

        let ctx = Arc::new(HostContext {
            map: WaitMap::new(),
            seq: AtomicU32::new(0),
        });

        let err_key = Key::for_path::<WireErr>(err_uri_path);

        let me = HostClient {
            ctx: ctx.clone(),
            out: tx_pc,
            err_key,
            _pd: PhantomData,
            subber: tx_si.clone(),
        };

        let wire = WireContext {
            outgoing: rx_pc,
            incoming: ctx,
            new_subs: rx_si,
        };

        (me, wire)
    }

    /// Create a new retry tracker.
    pub fn retry_tracker(&self) -> RetryTracker<RETRY_BITS> {
        RetryTracker {
            seq_no: self.ctx.seq.fetch_add(1 << RETRY_BITS, Ordering::Relaxed),
            retries: 0,
        }
    }
}

/// Tracks retries.
pub struct RetryTracker<const RETRY_BITS: usize> {
    seq_no: u32,
    retries: u8,
}

impl<const RETRY_BITS: usize> RetryTracker<RETRY_BITS> {
    /// Try to generate another sequence number.
    fn try_next_sequence_number<WireErr>(&mut self) -> Result<u32, HostErr<WireErr>>
    where
        WireErr: DeserializeOwned + Schema,
    {
        if RETRY_BITS == 0 {
            return Err(HostErr::Retries);
        }

        if self.retries >= (1 << RETRY_BITS) - 1 {
            return Err(HostErr::Retries);
        }

        let retries = self.retries;
        self.retries += 1;

        Ok(self.seq_no | retries as u32)
    }
}

/// # Interface Methods
impl<WireErr, const RETRY_BITS: usize> HostClient<WireErr, RETRY_BITS>
where
    WireErr: DeserializeOwned + Schema,
{
    /// Send a message of type [Endpoint::Request][Endpoint] to `path`, and await
    /// a response of type [Endpoint::Response][Endpoint] (or WireErr) to `path`.
    ///
    /// This function will wait potentially forever. Consider using with a timeout.
    pub async fn send_resp<E: Endpoint>(
        &self,
        t: &E::Request,
    ) -> Result<E::Response, HostErr<WireErr>>
    where
        E::Request: Serialize + Schema,
        E::Response: DeserializeOwned + Schema,
    {
        let seq_no = self.ctx.seq.fetch_add(1 << RETRY_BITS, Ordering::Relaxed);
        self.send_resp_inner::<E>(t, seq_no).await
    }

    /// Send a message of type [Endpoint::Request][Endpoint] to `path`, and await
    /// a response of type [Endpoint::Response][Endpoint] (or WireErr) to `path`.
    ///
    /// This function will wait potentially forever. Consider using with a timeout.
    /// If used with timeout, this allows for retrying the same command again for up to
    /// `(1 << RETRY_BITS) - 1` times.
    pub async fn send_resp_with_retries<E: Endpoint>(
        &self,
        retry_state: &mut RetryTracker<RETRY_BITS>,
        t: &E::Request,
    ) -> Result<E::Response, HostErr<WireErr>>
    where
        E::Request: Serialize + Schema,
        E::Response: DeserializeOwned + Schema,
    {
        let seq_no = retry_state.try_next_sequence_number()?;
        self.send_resp_inner::<E>(t, seq_no).await
    }

    async fn send_resp_inner<E: Endpoint>(
        &self,
        t: &E::Request,
        seq_no: u32,
    ) -> Result<E::Response, HostErr<WireErr>>
    where
        E::Request: Serialize + Schema,
        E::Response: DeserializeOwned + Schema,
    {
        let msg = postcard::to_stdvec(&t).expect("Allocations should not ever fail");
        let frame = RpcFrame {
            header: WireHeader {
                key: E::REQ_KEY,
                seq_no,
            },
            body: msg,
        };
        self.out.send(frame).await.map_err(|_| HostErr::Closed)?;
        let ok_resp = self.ctx.map.wait(WireHeader {
            seq_no,
            key: E::RESP_KEY,
        });
        let err_resp = self.ctx.map.wait(WireHeader {
            seq_no,
            key: self.err_key,
        });

        select! {
            o = ok_resp => {
                let resp = o?;
                let r = postcard::from_bytes::<E::Response>(&resp)?;
                Ok(r)
            },
            e = err_resp => {
                let resp = e?;
                let r = postcard::from_bytes::<WireErr>(&resp)?;
                Err(HostErr::Wire(r))
            },
        }
    }

    /// Publish a [Topic] [Message][Topic::Message].
    ///
    /// There is no feedback if the server received our message. If the I/O worker is
    /// closed, an error is returned.
    pub async fn publish<T: Topic>(&self, seq_no: u32, msg: &T::Message) -> Result<(), IoClosed>
    where
        T::Message: Serialize,
    {
        let smsg = postcard::to_stdvec(msg).expect("alloc should never fail");
        self.out
            .send(RpcFrame {
                header: WireHeader {
                    key: T::TOPIC_KEY,
                    seq_no,
                },
                body: smsg,
            })
            .await
            .map_err(|_| IoClosed)
    }

    /// Begin listening to a [Topic], receiving a [Subscription] that will give a
    /// stream of [Message][Topic::Message]s.
    ///
    /// If you subscribe to the same topic multiple times, the previous subscription
    /// will be closed (there can be only one).
    ///
    /// Returns an Error if the I/O worker is closed.
    pub async fn subscribe<T: Topic>(
        &self,
        depth: usize,
    ) -> Result<Subscription<T::Message>, IoClosed>
    where
        T::Message: DeserializeOwned,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(depth);
        self.subber
            .send(SubInfo {
                key: T::TOPIC_KEY,
                tx,
            })
            .await
            .map_err(|_| IoClosed)?;
        Ok(Subscription {
            rx,
            _pd: PhantomData,
        })
    }
}

/// A structure that represents a subscription to the given topic
pub struct Subscription<M> {
    rx: Receiver<RpcFrame>,
    _pd: PhantomData<M>,
}

impl<M> Subscription<M>
where
    M: DeserializeOwned,
{
    /// Await a message for the given subscription.
    ///
    /// Returns [None]` if the subscription was closed
    pub async fn recv(&mut self) -> Option<M> {
        loop {
            let frame = self.rx.recv().await?;
            if let Ok(m) = postcard::from_bytes(&frame.body) {
                return Some(m);
            }
        }
    }
}

// Manual Clone impl because WireErr may not impl Clone
impl<WireErr, const RETRY_BITS: usize> Clone for HostClient<WireErr, RETRY_BITS> {
    fn clone(&self) -> Self {
        Self {
            ctx: self.ctx.clone(),
            out: self.out.clone(),
            err_key: self.err_key,
            _pd: PhantomData,
            subber: self.subber.clone(),
        }
    }
}

/// A new subscription that should be accounted for
pub struct SubInfo {
    pub key: Key,
    pub tx: Sender<RpcFrame>,
}

/// Items necessary for implementing a custom I/O Task
pub struct WireContext {
    /// This is a stream of frames that should be placed on the
    /// wire towards the server.
    pub outgoing: Receiver<RpcFrame>,
    /// This shared information contains the WaitMap used for replying to
    /// open requests.
    pub incoming: Arc<HostContext>,
    /// This is a stream of new subscriptions that should be tracked
    pub new_subs: Receiver<SubInfo>,
}

/// A single postcard-rpc frame
pub struct RpcFrame {
    /// The wire header
    pub header: WireHeader,
    /// The serialized message payload
    pub body: Vec<u8>,
}

impl RpcFrame {
    /// Serialize the `RpcFrame` into a Vec of bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = postcard::to_stdvec(&self.header).expect("Alloc should never fail");
        out.extend_from_slice(&self.body);
        out
    }
}

/// Shared context between [HostClient] and the I/O worker task
pub struct HostContext {
    map: WaitMap<WireHeader, Vec<u8>>,
    seq: AtomicU32,
}

/// The I/O worker has closed.
#[derive(Debug)]
pub struct IoClosed;

/// Error for [HostContext::process].
#[derive(Debug, PartialEq)]
pub enum ProcessError {
    /// All [HostClient]s have been dropped, no further requests
    /// will be made and no responses will be processed.
    Closed,
}

impl HostContext {
    pub fn process(&self, frame: RpcFrame) -> Result<(), ProcessError> {
        if let WakeOutcome::Closed(_) = self.map.wake(&frame.header, frame.body) {
            Err(ProcessError::Closed)
        } else {
            Ok(())
        }
    }
}
