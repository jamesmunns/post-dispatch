//! Implementation using `embassy-usb` and bulk interfaces

use crate::{
    header::{VarHeader, VarKey, VarKeyKind, VarSeq},
    server::{WireRx, WireRxErrorKind, WireTx, WireTxErrorKind},
    standard_icd::LoggingTopic,
    Topic,
};
use core::fmt::Arguments;
use core::sync::atomic::{AtomicU8, Ordering};
use embassy_futures::select::{select, Either};
use embassy_sync::{blocking_mutex::raw::RawMutex, mutex::Mutex};
use embassy_time::Timer;
use embassy_usb_driver::{Driver, EndpointError, EndpointIn, EndpointOut};
use serde::Serialize;
use static_cell::ConstStaticCell;

struct PoststationHandler {}

static STINDX: AtomicU8 = AtomicU8::new(0xFF);
static HDLR: ConstStaticCell<PoststationHandler> = ConstStaticCell::new(PoststationHandler {});

impl embassy_usb_0_3::Handler for PoststationHandler {
    fn get_string(
        &mut self,
        index: embassy_usb_0_3::types::StringIndex,
        lang_id: u16,
    ) -> Option<&str> {
        use embassy_usb_0_3::descriptor::lang_id;

        let stindx = STINDX.load(Ordering::Relaxed);
        if stindx == 0xFF {
            return None;
        }
        if lang_id == lang_id::ENGLISH_US && index.0 == stindx {
            Some("Poststation")
        } else {
            None
        }
    }
}

/// A collection of types and aliases useful for importing the correct types
pub mod dispatch_impl {
    pub use super::embassy_spawn as spawn_fn;
    use super::{EUsbWireRx, EUsbWireTx, EUsbWireTxInner, UsbDeviceBuffers};

    /// Used for defining the USB interface
    pub const DEVICE_INTERFACE_GUIDS: &[&str] = &["{AFB9A6FB-30BA-44BC-9232-806CFC875321}"];

    use embassy_sync::{blocking_mutex::raw::RawMutex, mutex::Mutex};
    use embassy_usb_0_3::{
        msos::{self, windows_version},
        Builder, Config, UsbDevice,
    };
    use embassy_usb_driver::Driver;
    use static_cell::{ConstStaticCell, StaticCell};

    /// Type alias for `WireTx` impl
    pub type WireTxImpl<M, D> = super::EUsbWireTx<M, D>;
    /// Type alias for `WireRx` impl
    pub type WireRxImpl<D> = super::EUsbWireRx<D>;
    /// Type alias for `WireSpawn` impl
    pub type WireSpawnImpl = super::EUsbWireSpawn;
    /// Type alias for the receive buffer
    pub type WireRxBuf = &'static mut [u8];

    /// A helper type for `static` storage of buffers and driver components
    pub struct WireStorage<
        M: RawMutex + 'static,
        D: Driver<'static> + 'static,
        const CONFIG: usize = 256,
        const BOS: usize = 256,
        const CONTROL: usize = 64,
        const MSOS: usize = 256,
    > {
        /// Usb buffer storage
        pub bufs_usb: ConstStaticCell<UsbDeviceBuffers<CONFIG, BOS, CONTROL, MSOS>>,
        /// WireTx/Sender static storage
        pub cell: StaticCell<Mutex<M, EUsbWireTxInner<D>>>,
    }

    impl<
            M: RawMutex + 'static,
            D: Driver<'static> + 'static,
            const CONFIG: usize,
            const BOS: usize,
            const CONTROL: usize,
            const MSOS: usize,
        > WireStorage<M, D, CONFIG, BOS, CONTROL, MSOS>
    {
        /// Create a new, uninitialized static set of buffers
        pub const fn new() -> Self {
            Self {
                bufs_usb: ConstStaticCell::new(UsbDeviceBuffers::new()),
                cell: StaticCell::new(),
            }
        }

        /// Initialize the static storage, reporting as poststation compatible
        ///
        /// This must only be called once.
        pub fn init_poststation(
            &'static self,
            driver: D,
            config: Config<'static>,
            tx_buf: &'static mut [u8],
        ) -> (UsbDevice<'static, D>, WireTxImpl<M, D>, WireRxImpl<D>) {
            let bufs = self.bufs_usb.take();

            let mut builder = Builder::new(
                driver,
                config,
                &mut bufs.config_descriptor,
                &mut bufs.bos_descriptor,
                &mut bufs.msos_descriptor,
                &mut bufs.control_buf,
            );

            // Register a poststation-compatible string handler
            let hdlr = super::HDLR.take();
            builder.handler(hdlr);

            // Add the Microsoft OS Descriptor (MSOS/MOD) descriptor.
            // We tell Windows that this entire device is compatible with the "WINUSB" feature,
            // which causes it to use the built-in WinUSB driver automatically, which in turn
            // can be used by libusb/rusb software without needing a custom driver or INF file.
            // In principle you might want to call msos_feature() just on a specific function,
            // if your device also has other functions that still use standard class drivers.
            builder.msos_descriptor(windows_version::WIN8_1, 0);
            builder.msos_feature(msos::CompatibleIdFeatureDescriptor::new("WINUSB", ""));
            builder.msos_feature(msos::RegistryPropertyFeatureDescriptor::new(
                "DeviceInterfaceGUIDs",
                msos::PropertyData::RegMultiSz(DEVICE_INTERFACE_GUIDS),
            ));

            // Add a vendor-specific function (class 0xFF), and corresponding interface,
            // that uses our custom handler.
            let mut function = builder.function(0xFF, 0, 0);
            let mut interface = function.interface();
            let stindx = interface.string();
            super::STINDX.store(stindx.0, core::sync::atomic::Ordering::Relaxed);
            let mut alt = interface.alt_setting(0xFF, 0xCA, 0x7D, Some(stindx));
            let ep_out = alt.endpoint_bulk_out(64);
            let ep_in = alt.endpoint_bulk_in(64);
            drop(function);

            let wtx = self.cell.init(Mutex::new(EUsbWireTxInner {
                ep_in,
                log_seq: 0,
                tx_buf,
                pending_frame: false,
            }));

            // Build the builder.
            let usb = builder.build();

            (usb, EUsbWireTx { inner: wtx }, EUsbWireRx { ep_out })
        }

        /// Initialize the static storage.
        ///
        /// This must only be called once.
        pub fn init(
            &'static self,
            driver: D,
            config: Config<'static>,
            tx_buf: &'static mut [u8],
        ) -> (UsbDevice<'static, D>, WireTxImpl<M, D>, WireRxImpl<D>) {
            let (builder, wtx, wrx) = self.init_without_build(driver, config, tx_buf);
            let usb = builder.build();
            (usb, wtx, wrx)
        }
        /// Initialize the static storage, without building `Builder`
        ///
        /// This must only be called once.
        pub fn init_without_build(
            &'static self,
            driver: D,
            config: Config<'static>,
            tx_buf: &'static mut [u8],
        ) -> (Builder<'static, D>, WireTxImpl<M, D>, WireRxImpl<D>) {
            let bufs = self.bufs_usb.take();

            let mut builder = Builder::new(
                driver,
                config,
                &mut bufs.config_descriptor,
                &mut bufs.bos_descriptor,
                &mut bufs.msos_descriptor,
                &mut bufs.control_buf,
            );

            // Add the Microsoft OS Descriptor (MSOS/MOD) descriptor.
            // We tell Windows that this entire device is compatible with the "WINUSB" feature,
            // which causes it to use the built-in WinUSB driver automatically, which in turn
            // can be used by libusb/rusb software without needing a custom driver or INF file.
            // In principle you might want to call msos_feature() just on a specific function,
            // if your device also has other functions that still use standard class drivers.
            builder.msos_descriptor(windows_version::WIN8_1, 0);
            builder.msos_feature(msos::CompatibleIdFeatureDescriptor::new("WINUSB", ""));
            builder.msos_feature(msos::RegistryPropertyFeatureDescriptor::new(
                "DeviceInterfaceGUIDs",
                msos::PropertyData::RegMultiSz(DEVICE_INTERFACE_GUIDS),
            ));

            // Add a vendor-specific function (class 0xFF), and corresponding interface,
            // that uses our custom handler.
            let mut function = builder.function(0xFF, 0, 0);
            let mut interface = function.interface();
            let mut alt = interface.alt_setting(0xFF, 0, 0, None);
            let ep_out = alt.endpoint_bulk_out(64);
            let ep_in = alt.endpoint_bulk_in(64);
            drop(function);

            let wtx = self.cell.init(Mutex::new(EUsbWireTxInner {
                ep_in,
                log_seq: 0,
                tx_buf,
                pending_frame: false,
            }));

            (builder, EUsbWireTx { inner: wtx }, EUsbWireRx { ep_out })
        }
    }
}

//////////////////////////////////////////////////////////////////////////////
// TX
//////////////////////////////////////////////////////////////////////////////

/// Implementation detail, holding the endpoint and scratch buffer used for sending
pub struct EUsbWireTxInner<D: Driver<'static>> {
    ep_in: D::EndpointIn,
    log_seq: u16,
    tx_buf: &'static mut [u8],
    pending_frame: bool,
}

/// A [`WireTx`] implementation for embassy-usb 0.3.
#[derive(Copy)]
pub struct EUsbWireTx<M: RawMutex + 'static, D: Driver<'static> + 'static> {
    inner: &'static Mutex<M, EUsbWireTxInner<D>>,
}

impl<M: RawMutex + 'static, D: Driver<'static> + 'static> Clone for EUsbWireTx<M, D> {
    fn clone(&self) -> Self {
        EUsbWireTx { inner: self.inner }
    }
}

impl<M: RawMutex + 'static, D: Driver<'static> + 'static> WireTx for EUsbWireTx<M, D> {
    type Error = WireTxErrorKind;

    async fn send<T: Serialize + ?Sized>(
        &self,
        hdr: VarHeader,
        msg: &T,
    ) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock().await;

        let EUsbWireTxInner {
            ep_in,
            log_seq: _,
            tx_buf,
            pending_frame,
        }: &mut EUsbWireTxInner<D> = &mut inner;

        let (hdr_used, remain) = hdr.write_to_slice(tx_buf).ok_or(WireTxErrorKind::Other)?;
        let bdy_used = postcard::to_slice(msg, remain).map_err(|_| WireTxErrorKind::Other)?;
        let used_ttl = hdr_used.len() + bdy_used.len();

        if let Some(used) = tx_buf.get(..used_ttl) {
            send_all::<D>(ep_in, used, pending_frame).await
        } else {
            Err(WireTxErrorKind::Other)
        }
    }

    async fn send_raw(&self, buf: &[u8]) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock().await;
        let EUsbWireTxInner {
            ep_in,
            pending_frame,
            ..
        }: &mut EUsbWireTxInner<D> = &mut inner;
        send_all::<D>(ep_in, buf, pending_frame).await
    }

    async fn send_log_str(&self, kkind: VarKeyKind, s: &str) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock().await;

        let EUsbWireTxInner {
            ep_in,
            log_seq,
            tx_buf,
            pending_frame,
        }: &mut EUsbWireTxInner<D> = &mut inner;

        let key = match kkind {
            VarKeyKind::Key1 => VarKey::Key1(LoggingTopic::TOPIC_KEY1),
            VarKeyKind::Key2 => VarKey::Key2(LoggingTopic::TOPIC_KEY2),
            VarKeyKind::Key4 => VarKey::Key4(LoggingTopic::TOPIC_KEY4),
            VarKeyKind::Key8 => VarKey::Key8(LoggingTopic::TOPIC_KEY),
        };
        let ctr = *log_seq;
        *log_seq = log_seq.wrapping_add(1);
        let wh = VarHeader {
            key,
            seq_no: VarSeq::Seq2(ctr),
        };

        let (hdr_used, remain) = wh.write_to_slice(tx_buf).ok_or(WireTxErrorKind::Other)?;
        let bdy_used = postcard::to_slice::<str>(s, remain).map_err(|_| WireTxErrorKind::Other)?;
        let used_ttl = hdr_used.len() + bdy_used.len();

        if let Some(used) = tx_buf.get(..used_ttl) {
            send_all::<D>(ep_in, used, pending_frame).await
        } else {
            Err(WireTxErrorKind::Other)
        }
    }

    async fn send_log_fmt<'a>(
        &self,
        kkind: VarKeyKind,
        args: Arguments<'a>,
    ) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock().await;

        let EUsbWireTxInner {
            ep_in,
            log_seq,
            tx_buf,
            pending_frame,
        }: &mut EUsbWireTxInner<D> = &mut inner;
        let ttl_len = tx_buf.len();

        let key = match kkind {
            VarKeyKind::Key1 => VarKey::Key1(LoggingTopic::TOPIC_KEY1),
            VarKeyKind::Key2 => VarKey::Key2(LoggingTopic::TOPIC_KEY2),
            VarKeyKind::Key4 => VarKey::Key4(LoggingTopic::TOPIC_KEY4),
            VarKeyKind::Key8 => VarKey::Key8(LoggingTopic::TOPIC_KEY),
        };
        let ctr = *log_seq;
        *log_seq = log_seq.wrapping_add(1);
        let wh = VarHeader {
            key,
            seq_no: VarSeq::Seq2(ctr),
        };
        let Some((_hdr, remaining)) = wh.write_to_slice(tx_buf) else {
            return Err(WireTxErrorKind::Other);
        };
        let max_log_len = actual_varint_max_len(remaining.len());

        // Then, reserve space for non-canonical length fields
        // We also set all but the last bytes to be "continuation"
        // bytes
        if remaining.len() < max_log_len {
            return Err(WireTxErrorKind::Other);
        }

        let (len_field, body) = remaining.split_at_mut(max_log_len);
        for b in len_field.iter_mut() {
            *b = 0x80;
        }
        if let Some(b) = len_field.last_mut() {
            *b = 0x00;
        }

        // Then, do the formatting
        let body_len = body.len();
        let mut sw = SliceWriter(body);
        let res = core::fmt::write(&mut sw, args);

        // Calculate the number of bytes used *for formatting*.
        let remain = sw.0.len();
        let used = body_len - remain;

        // If we had an error, that's probably because we ran out
        // of room. If we had an error, AND there is at least three
        // bytes, then replace those with '.'s like ...
        if res.is_err() && (body.len() >= 3) {
            let start = body.len() - 3;
            body[start..].iter_mut().for_each(|b| *b = b'.');
        }

        // then go back and fill in the len - we write the len
        // directly to the reserved bytes, and if we DIDN'T use
        // the full space, we mark the end of the real length as
        // a continuation field. This will result in a non-canonical
        // "extended" length in postcard, and will "spill into" the
        // bytes we wrote previously above
        let mut len_bytes = [0u8; varint_max::<usize>()];
        let len_used = varint_usize(used, &mut len_bytes);
        if len_used.len() != len_field.len() {
            if let Some(b) = len_used.last_mut() {
                *b |= 0x80;
            }
        }
        len_field[..len_used.len()].copy_from_slice(len_used);

        // Calculate the TOTAL amount
        let act_used = ttl_len - remain;

        send_all::<D>(ep_in, &tx_buf[..act_used], pending_frame).await
    }
}

#[inline]
async fn send_all<D>(
    ep_in: &mut D::EndpointIn,
    out: &[u8],
    pending_frame: &mut bool,
) -> Result<(), WireTxErrorKind>
where
    D: Driver<'static>,
{
    if out.is_empty() {
        return Ok(());
    }

    // Calculate an estimated timeout based on the number of frames we need to send
    // For now, we use 2ms/frame, rounded UP
    let frames = (out.len() + 63) / 64;
    let timeout_ms = frames * 2;

    let send_fut = async {
        // If we left off a pending frame, send one now so we don't leave an unterminated
        // message
        if *pending_frame && ep_in.write(&[]).await.is_err() {
            return Err(WireTxErrorKind::ConnectionClosed);
        }
        *pending_frame = true;

        // write in segments of 64. The last chunk may
        // be 0 < len <= 64.
        for ch in out.chunks(64) {
            if ep_in.write(ch).await.is_err() {
                return Err(WireTxErrorKind::ConnectionClosed);
            }
        }
        // If the total we sent was a multiple of 64, send an
        // empty message to "flush" the transaction. We already checked
        // above that the len != 0.
        if (out.len() & (64 - 1)) == 0 && ep_in.write(&[]).await.is_err() {
            return Err(WireTxErrorKind::ConnectionClosed);
        }

        *pending_frame = false;
        Ok(())
    };

    match select(send_fut, Timer::after_millis(timeout_ms as u64)).await {
        Either::First(res) => res,
        Either::Second(()) => Err(WireTxErrorKind::Timeout),
    }
}

struct SliceWriter<'a>(&'a mut [u8]);

impl<'a> core::fmt::Write for SliceWriter<'a> {
    fn write_str(&mut self, s: &str) -> Result<(), core::fmt::Error> {
        let sli = core::mem::take(&mut self.0);

        // If this write would overflow us, note that, but still take
        // as much as we possibly can here
        let bad = s.len() > sli.len();
        let to_write = s.len().min(sli.len());
        let (now, later) = sli.split_at_mut(to_write);
        now.copy_from_slice(s.as_bytes());
        self.0 = later;

        // Now, report whether we overflowed or not
        if bad {
            Err(core::fmt::Error)
        } else {
            Ok(())
        }
    }
}

/// Returns the maximum number of bytes required to encode T.
const fn varint_max<T: Sized>() -> usize {
    const BITS_PER_BYTE: usize = 8;
    const BITS_PER_VARINT_BYTE: usize = 7;

    // How many data bits do we need for this type?
    let bits = core::mem::size_of::<T>() * BITS_PER_BYTE;

    // We add (BITS_PER_VARINT_BYTE - 1), to ensure any integer divisions
    // with a remainder will always add exactly one full byte, but
    // an evenly divided number of bits will be the same
    let roundup_bits = bits + (BITS_PER_VARINT_BYTE - 1);

    // Apply division, using normal "round down" integer division
    roundup_bits / BITS_PER_VARINT_BYTE
}

#[inline]
fn varint_usize(n: usize, out: &mut [u8; varint_max::<usize>()]) -> &mut [u8] {
    let mut value = n;
    for i in 0..varint_max::<usize>() {
        out[i] = value.to_le_bytes()[0];
        if value < 128 {
            return &mut out[..=i];
        }

        out[i] |= 0x80;
        value >>= 7;
    }
    debug_assert_eq!(value, 0);
    &mut out[..]
}

fn actual_varint_max_len(largest: usize) -> usize {
    if largest < (2 << 7) {
        1
    } else if largest < (2 << 14) {
        2
    } else if largest < (2 << 21) {
        3
    } else if largest < (2 << 28) {
        4
    } else {
        varint_max::<usize>()
    }
}

//////////////////////////////////////////////////////////////////////////////
// RX
//////////////////////////////////////////////////////////////////////////////

/// A [`WireRx`] implementation for embassy-usb 0.3.
pub struct EUsbWireRx<D: Driver<'static>> {
    ep_out: D::EndpointOut,
}

impl<D: Driver<'static>> WireRx for EUsbWireRx<D> {
    type Error = WireRxErrorKind;

    async fn receive<'a>(&mut self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Self::Error> {
        let buflen = buf.len();
        let mut window = &mut buf[..];
        while !window.is_empty() {
            let n = match self.ep_out.read(window).await {
                Ok(n) => n,
                Err(EndpointError::BufferOverflow) => {
                    return Err(WireRxErrorKind::ReceivedMessageTooLarge)
                }
                Err(EndpointError::Disabled) => return Err(WireRxErrorKind::ConnectionClosed),
            };

            let (_now, later) = window.split_at_mut(n);
            window = later;
            if n != 64 {
                // We now have a full frame! Great!
                let wlen = window.len();
                let len = buflen - wlen;
                let frame = &mut buf[..len];

                return Ok(frame);
            }
        }

        // If we got here, we've run out of space. That's disappointing. Accumulate to the
        // end of this packet
        loop {
            match self.ep_out.read(buf).await {
                Ok(64) => {}
                Ok(_) => return Err(WireRxErrorKind::ReceivedMessageTooLarge),
                Err(EndpointError::BufferOverflow) => {
                    return Err(WireRxErrorKind::ReceivedMessageTooLarge)
                }
                Err(EndpointError::Disabled) => return Err(WireRxErrorKind::ConnectionClosed),
            };
        }
    }
}

//////////////////////////////////////////////////////////////////////////////
// SPAWN
//////////////////////////////////////////////////////////////////////////////
pub use super::embassy_shared::embassy_spawn;
pub use super::embassy_shared::EmbassyWireSpawn as EUsbWireSpawn;

//////////////////////////////////////////////////////////////////////////////
// OTHER
//////////////////////////////////////////////////////////////////////////////

/// A generically sized storage type for buffers
pub struct UsbDeviceBuffers<
    const CONFIG: usize = 256,
    const BOS: usize = 256,
    const CONTROL: usize = 64,
    const MSOS: usize = 256,
> {
    /// Config descriptor storage
    pub config_descriptor: [u8; CONFIG],
    /// BOS descriptor storage
    pub bos_descriptor: [u8; BOS],
    /// CONTROL endpoint buffer storage
    pub control_buf: [u8; CONTROL],
    /// MSOS descriptor buffer storage
    pub msos_descriptor: [u8; MSOS],
}

impl<const CONFIG: usize, const BOS: usize, const CONTROL: usize, const MSOS: usize>
    UsbDeviceBuffers<CONFIG, BOS, CONTROL, MSOS>
{
    /// Create a new, empty set of buffers
    pub const fn new() -> Self {
        Self {
            config_descriptor: [0u8; CONFIG],
            bos_descriptor: [0u8; BOS],
            msos_descriptor: [0u8; MSOS],
            control_buf: [0u8; CONTROL],
        }
    }
}

/// Static storage for generically sized input and output packet buffers
pub struct PacketBuffers<const TX: usize = 1024, const RX: usize = 1024> {
    /// the transmit buffer
    pub tx_buf: [u8; TX],
    /// thereceive buffer
    pub rx_buf: [u8; RX],
}

impl<const TX: usize, const RX: usize> PacketBuffers<TX, RX> {
    /// Create new empty buffers
    pub const fn new() -> Self {
        Self {
            tx_buf: [0u8; TX],
            rx_buf: [0u8; RX],
        }
    }
}

/// This is a basic example that everything compiles. It is intended to exercise the macro above,
/// as well as provide impls for docs. Don't rely on any of this!
#[doc(hidden)]
#[allow(dead_code)]
#[cfg(feature = "test-utils")]
pub mod fake {
    use crate::{
        define_dispatch, endpoints,
        server::{Sender, SpawnContext},
        topics,
    };
    use crate::{header::VarHeader, Schema};
    use embassy_usb_driver::{Bus, ControlPipe, EndpointIn, EndpointOut};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Schema)]
    pub struct AReq(pub u8);
    #[derive(Serialize, Deserialize, Schema)]
    pub struct AResp(pub u8);
    #[derive(Serialize, Deserialize, Schema)]
    pub struct BReq(pub u16);
    #[derive(Serialize, Deserialize, Schema)]
    pub struct BResp(pub u32);
    #[derive(Serialize, Deserialize, Schema)]
    pub struct GReq;
    #[derive(Serialize, Deserialize, Schema)]
    pub struct GResp;
    #[derive(Serialize, Deserialize, Schema)]
    pub struct DReq;
    #[derive(Serialize, Deserialize, Schema)]
    pub struct DResp;
    #[derive(Serialize, Deserialize, Schema)]
    pub struct EReq;
    #[derive(Serialize, Deserialize, Schema)]
    pub struct EResp;
    #[derive(Serialize, Deserialize, Schema)]
    pub struct ZMsg(pub i16);

    endpoints! {
        list = ENDPOINT_LIST;
        | EndpointTy        | RequestTy     | ResponseTy    | Path              |
        | ----------        | ---------     | ----------    | ----              |
        | AlphaEndpoint     | AReq          | AResp         | "alpha"           |
        | BetaEndpoint      | BReq          | BResp         | "beta"            |
        | GammaEndpoint     | GReq          | GResp         | "gamma"           |
        | DeltaEndpoint     | DReq          | DResp         | "delta"           |
        | EpsilonEndpoint   | EReq          | EResp         | "epsilon"         |
    }

    topics! {
        list = TOPICS_IN_LIST;
        direction = crate::TopicDirection::ToServer;
        | TopicTy           | MessageTy     | Path              |
        | ----------        | ---------     | ----              |
        | ZetaTopic1        | ZMsg          | "zeta1"           |
        | ZetaTopic2        | ZMsg          | "zeta2"           |
        | ZetaTopic3        | ZMsg          | "zeta3"           |
    }

    topics! {
        list = TOPICS_OUT_LIST;
        direction = crate::TopicDirection::ToClient;
        | TopicTy           | MessageTy     | Path              |
        | ----------        | ---------     | ----              |
        | ZetaTopic10       | ZMsg          | "zeta10"          |
    }

    pub struct FakeMutex;
    pub struct FakeDriver;
    pub struct FakeEpOut;
    pub struct FakeEpIn;
    pub struct FakeCtlPipe;
    pub struct FakeBus;

    impl embassy_usb_driver::Endpoint for FakeEpOut {
        fn info(&self) -> &embassy_usb_driver::EndpointInfo {
            todo!()
        }

        async fn wait_enabled(&mut self) {
            todo!()
        }
    }

    impl EndpointOut for FakeEpOut {
        async fn read(
            &mut self,
            _buf: &mut [u8],
        ) -> Result<usize, embassy_usb_driver::EndpointError> {
            todo!()
        }
    }

    impl embassy_usb_driver::Endpoint for FakeEpIn {
        fn info(&self) -> &embassy_usb_driver::EndpointInfo {
            todo!()
        }

        async fn wait_enabled(&mut self) {
            todo!()
        }
    }

    impl EndpointIn for FakeEpIn {
        async fn write(&mut self, _buf: &[u8]) -> Result<(), embassy_usb_driver::EndpointError> {
            todo!()
        }
    }

    impl ControlPipe for FakeCtlPipe {
        fn max_packet_size(&self) -> usize {
            todo!()
        }

        async fn setup(&mut self) -> [u8; 8] {
            todo!()
        }

        async fn data_out(
            &mut self,
            _buf: &mut [u8],
            _first: bool,
            _last: bool,
        ) -> Result<usize, embassy_usb_driver::EndpointError> {
            todo!()
        }

        async fn data_in(
            &mut self,
            _data: &[u8],
            _first: bool,
            _last: bool,
        ) -> Result<(), embassy_usb_driver::EndpointError> {
            todo!()
        }

        async fn accept(&mut self) {
            todo!()
        }

        async fn reject(&mut self) {
            todo!()
        }

        async fn accept_set_address(&mut self, _addr: u8) {
            todo!()
        }
    }

    impl Bus for FakeBus {
        async fn enable(&mut self) {
            todo!()
        }

        async fn disable(&mut self) {
            todo!()
        }

        async fn poll(&mut self) -> embassy_usb_driver::Event {
            todo!()
        }

        fn endpoint_set_enabled(
            &mut self,
            _ep_addr: embassy_usb_driver::EndpointAddress,
            _enabled: bool,
        ) {
            todo!()
        }

        fn endpoint_set_stalled(
            &mut self,
            _ep_addr: embassy_usb_driver::EndpointAddress,
            _stalled: bool,
        ) {
            todo!()
        }

        fn endpoint_is_stalled(&mut self, _ep_addr: embassy_usb_driver::EndpointAddress) -> bool {
            todo!()
        }

        async fn remote_wakeup(&mut self) -> Result<(), embassy_usb_driver::Unsupported> {
            todo!()
        }
    }

    impl embassy_usb_driver::Driver<'static> for FakeDriver {
        type EndpointOut = FakeEpOut;

        type EndpointIn = FakeEpIn;

        type ControlPipe = FakeCtlPipe;

        type Bus = FakeBus;

        fn alloc_endpoint_out(
            &mut self,
            _ep_type: embassy_usb_driver::EndpointType,
            _max_packet_size: u16,
            _interval_ms: u8,
        ) -> Result<Self::EndpointOut, embassy_usb_driver::EndpointAllocError> {
            todo!()
        }

        fn alloc_endpoint_in(
            &mut self,
            _ep_type: embassy_usb_driver::EndpointType,
            _max_packet_size: u16,
            _interval_ms: u8,
        ) -> Result<Self::EndpointIn, embassy_usb_driver::EndpointAllocError> {
            todo!()
        }

        fn start(self, _control_max_packet_size: u16) -> (Self::Bus, Self::ControlPipe) {
            todo!()
        }
    }

    unsafe impl embassy_sync::blocking_mutex::raw::RawMutex for FakeMutex {
        const INIT: Self = Self;

        fn lock<R>(&self, _f: impl FnOnce() -> R) -> R {
            todo!()
        }
    }

    pub struct TestContext {
        pub a: u32,
        pub b: u32,
    }

    impl SpawnContext for TestContext {
        type SpawnCtxt = TestSpawnContext;

        fn spawn_ctxt(&mut self) -> Self::SpawnCtxt {
            TestSpawnContext { b: self.b }
        }
    }

    pub struct TestSpawnContext {
        b: u32,
    }

    // TODO: How to do module path concat?
    use crate::server::impls::embassy_usb_v0_3::dispatch_impl::{
        spawn_fn, WireSpawnImpl, WireTxImpl,
    };

    define_dispatch! {
        app: SingleDispatcher;
        spawn_fn: spawn_fn;
        tx_impl: WireTxImpl<FakeMutex, FakeDriver>;
        spawn_impl: WireSpawnImpl;
        context: TestContext;

        endpoints: {
            list: ENDPOINT_LIST;

            | EndpointTy        | kind      | handler                   |
            | ----------        | ----      | -------                   |
            | AlphaEndpoint     | async     | test_alpha_handler        |
            | EpsilonEndpoint   | spawn     | test_epsilon_handler_task |
        };
        topics_in: {
            list: TOPICS_IN_LIST;

            | TopicTy           | kind      | handler               |
            | ----------        | ----      | -------               |
            // | ZetaTopic1        | blocking  | test_zeta_blocking    |
            // | ZetaTopic2        | async     | test_zeta_async       |
            // | ZetaTopic3        | spawn     | test_zeta_spawn       |
        };
        topics_out: {
            list: TOPICS_OUT_LIST;
        };
    }

    async fn test_alpha_handler(
        _context: &mut TestContext,
        _header: VarHeader,
        _body: AReq,
    ) -> AResp {
        todo!()
    }

    async fn test_beta_handler(
        _context: &mut TestContext,
        _header: VarHeader,
        _body: BReq,
    ) -> BResp {
        todo!()
    }

    async fn test_gamma_handler(
        _context: &mut TestContext,
        _header: VarHeader,
        _body: GReq,
    ) -> GResp {
        todo!()
    }

    fn test_delta_handler(_context: &mut TestContext, _header: VarHeader, _body: DReq) -> DResp {
        todo!()
    }

    #[embassy_executor::task]
    async fn test_epsilon_handler_task(
        _context: TestSpawnContext,
        _header: VarHeader,
        _body: EReq,
        _sender: Sender<WireTxImpl<FakeMutex, FakeDriver>>,
    ) {
        todo!()
    }
}
