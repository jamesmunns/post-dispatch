//! Implementation using `embedded-io-async`
use core::{fmt::Arguments, ops::DerefMut};

use crate::{
    header::{VarHeader, VarKey, VarKeyKind, VarSeq},
    server::{WireRx, WireRxErrorKind, WireTx, WireTxErrorKind},
    standard_icd::LoggingTopic,
    Topic,
};
use cobs::decode;
use embassy_sync::{blocking_mutex::raw::RawMutex, mutex::Mutex};
use embedded_io_async_0_6::{Read, Write};
use postcard::{
    ser_flavors::{Cobs, Flavor, Slice},
    Serializer,
};
use serde::Serialize;

/// A collection of types and aliases useful for importing the correct types
pub mod dispatch_impl {
    pub use crate::server::impls::embassy_shared::embassy_spawn as spawn_fn;

    // use embassy_sync::{blocking_mutex::raw::RawMutex, mutex::Mutex};
    // use embassy_usb_0_4::{
    //     msos::{self, windows_version},
    //     Builder, Config, UsbDevice,
    // };
    // use embassy_usb_driver::Driver;
    // use static_cell::{ConstStaticCell, StaticCell};

    /// Type alias for `WireTx` impl
    pub type WireTxImpl<M, D> = super::EioWireTx<M, D>;
    /// Type alias for `WireRx` impl
    pub type WireRxImpl<D> = super::EioWireRx<D>;
    /// Type alias for `WireSpawn` impl
    pub type WireSpawnImpl = crate::server::impls::embassy_shared::EmbassyWireSpawn;
    /// Type alias for the receive buffer
    pub type WireRxBuf = &'static mut [u8];
}

pub use super::embassy_shared::embassy_spawn;
pub use super::embassy_shared::EmbassyWireSpawn as EioWireSpawn;

/// ...
pub struct EioWireTxInner<Tx: Write> {
    /// ...
    pub t: Tx,
    /// ...
    pub tx_buf: &'static mut [u8],
    /// ...
    pub log_seq: u16,
}

/// ...
pub struct EioWireTx<R, Tx>
where
    R: RawMutex + 'static,
    Tx: Write + 'static,
{
    /// ...
    pub t: &'static Mutex<R, EioWireTxInner<Tx>>,
}

impl<R, Tx> Clone for EioWireTx<R, Tx>
where
    R: RawMutex + 'static,
    Tx: Write + 'static,
{
    fn clone(&self) -> Self {
        Self { t: self.t }
    }
}

fn flava_flav(buf: &'_ mut [u8]) -> Result<Cobs<Slice<'_>>, WireTxErrorKind> {
    Cobs::try_new(Slice::new(buf)).map_err(|_| WireTxErrorKind::ConnectionClosed)
}

fn header_to_flavor(hdr: &VarHeader, flava: &mut Cobs<Slice<'_>>) -> Result<(), WireTxErrorKind> {
    // Serialize the header to a side buffer, since it doesn't use Serde
    let mut hdr_buf = [0u8; 1 + 4 + 8];
    let (used, _unused) = hdr
        .write_to_slice(&mut hdr_buf)
        .ok_or(WireTxErrorKind::Other)?;

    // Put the header into the buffer, which will cobs encode it
    flava.try_extend(used).map_err(|_| WireTxErrorKind::Other)?;

    Ok(())
}

fn body_to_flavor<'a, T: Serialize + ?Sized>(
    msg: &T,
    flava: Cobs<Slice<'a>>,
) -> Result<&'a [u8], WireTxErrorKind> {
    let mut serializer = Serializer { output: flava };
    msg.serialize(&mut serializer)
        .map_err(|_| WireTxErrorKind::Other)?;
    let used = serializer
        .output
        .finalize()
        .map_err(|_| WireTxErrorKind::Other)?;
    Ok(used)
}

impl<R, Tx> WireTx for EioWireTx<R, Tx>
where
    R: RawMutex + 'static,
    Tx: Write + 'static,
{
    type Error = WireTxErrorKind;

    async fn send<T: Serialize + ?Sized>(
        &self,
        hdr: VarHeader,
        msg: &T,
    ) -> Result<(), Self::Error> {
        let mut guard = self.t.lock().await;
        let EioWireTxInner { t, tx_buf, .. } = guard.deref_mut();

        // Create a cobs-encoding flavor using our temp buffer
        let mut flavor = flava_flav(tx_buf)?;

        // Put the header into the buffer, which will cobs encode it
        header_to_flavor(&hdr, &mut flavor)?;

        // Now do normal serialization (and cobs encoding)
        let used = body_to_flavor(msg, flavor)?;

        // Write it all to the serial port now
        t.write_all(used)
            .await
            .map_err(|_| WireTxErrorKind::ConnectionClosed)?;

        // defmt::println!("SENT NORMAL {=usize} {=[u8]}", used.len(), used);
        // We did it! yaaaay!
        Ok(())
    }

    async fn send_raw(&self, buf: &[u8]) -> Result<(), Self::Error> {
        let mut guard = self.t.lock().await;
        let EioWireTxInner { t, tx_buf, .. } = guard.deref_mut();

        // Create a cobs-encoding flavor using our temp buffer
        let mut flavor = flava_flav(tx_buf)?;

        flavor.try_extend(buf).map_err(|_| WireTxErrorKind::Other)?;
        let used = flavor.finalize().map_err(|_| WireTxErrorKind::Other)?;

        // Write it all to the serial port now
        t.write_all(used)
            .await
            .map_err(|_| WireTxErrorKind::ConnectionClosed)?;

        // defmt::println!("SENT RAW");
        // We did it! yaaaay!
        Ok(())
    }

    async fn send_log_str(&self, kkind: VarKeyKind, s: &str) -> Result<(), Self::Error> {
        let mut guard = self.t.lock().await;
        let EioWireTxInner { t, tx_buf, log_seq } = guard.deref_mut();

        // Create a cobs-encoding flavor using our temp buffer
        let mut flavor = flava_flav(tx_buf)?;

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

        header_to_flavor(&wh, &mut flavor)?;
        let used = body_to_flavor(s, flavor)?;

        // Write it all to the serial port now
        t.write_all(used)
            .await
            .map_err(|_| WireTxErrorKind::ConnectionClosed)?;

        // We did it! yaaaay!
        Ok(())
    }

    async fn send_log_fmt<'a>(
        &self,
        _kkind: VarKeyKind,
        _a: Arguments<'a>,
    ) -> Result<(), Self::Error> {
        todo!()
    }
}

/// ...
pub struct EioWireRx<R: Read> {
    /// ...
    pub remain: &'static mut [u8],
    /// ...
    pub offset: usize,
    /// ...
    pub rx: R,
}

fn copy_backwards(buf: &mut [u8], start: usize) {
    if buf.len() < start {
        return;
    }
    let count = buf.len() - start;
    let base = buf.as_mut_ptr();
    unsafe {
        core::ptr::copy(base.add(start).cast_const(), base, count);
    }
}

impl<R: Read> WireRx for EioWireRx<R> {
    type Error = WireRxErrorKind;

    async fn receive<'a>(&mut self, buf: &'a mut [u8]) -> Result<&'a mut [u8], Self::Error> {
        if self.offset != 0 {
            if let Some(r) = self.remain.get_mut(..self.offset) {
                if let Some(pos) = r.iter().position(|b| *b == 0) {
                    let (now, later) = r.split_at(pos + 1);
                    let res = decode(now, buf);

                    let after_len = later.len();
                    copy_backwards(r, pos + 1);
                    self.offset = after_len;

                    return match res {
                        Ok(used) => Ok(&mut buf[..used]),
                        Err(_e) => Err(WireRxErrorKind::ReceivedMessageTooLarge),
                    };
                }
                // else: We have data but no zero
            } else {
                self.offset = 0;
            }
        }
        let Self { remain, offset, rx } = self;
        loop {
            if *offset >= remain.len() {
                *offset = 0;
                // todo: just wait for zero and discard?
                return Err(WireRxErrorKind::ConnectionClosed);
            }
            let (old, new) = remain.split_at_mut(*offset);
            let got = rx.read(new).await;
            let got = match got {
                Ok(0) => {
                    *offset = 0;
                    return Err(WireRxErrorKind::ConnectionClosed);
                }
                Ok(n) => n,
                Err(_) => {
                    *offset = 0;
                    return Err(WireRxErrorKind::Other);
                }
            };
            // defmt::println!("Got {=usize} bytes: {=[u8]}", got, &new[..got]);

            //           v----- offset
            // | old     | new                      |
            //           |---got--------|
            // | old     | left         | right     |
            // | old     | left  0| lr  | right     |
            //                   ^----- pos
            // |    DECODE        | lr  | right     |
            // | lr  |                              |

            let (left, _right) = new.split_at(got);
            if let Some(pos) = left.iter().position(|b| *b == 0) {
                // defmt::println!("FOUND ZERO");
                let old_left_len = old.len() + left.len();
                let lrstart = old.len() + (pos + 1);
                let lrlen = left.len() - (pos + 1);

                let res = decode(&remain[..lrstart], buf);
                copy_backwards(&mut remain[..old_left_len], lrstart);
                self.offset = lrlen;

                return match res {
                    Ok(used) => {
                        // defmt::println!("GOT MSG");
                        Ok(&mut buf[..used])
                    }
                    Err(_e) => {
                        // defmt::println!("GOT ERR");
                        Err(WireRxErrorKind::ReceivedMessageTooLarge)
                    }
                };
            } else {
                *offset += got;
            }
        }
    }
}
