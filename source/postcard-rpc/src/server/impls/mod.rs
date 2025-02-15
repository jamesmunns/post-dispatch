//! Implementations of various Server traits
//!
//! The implementations in this module typically require feature flags to be set.

#[cfg(feature = "embassy-usb-0_3-server")]
pub mod embassy_usb_v0_3;

#[cfg(feature = "embassy-usb-0_4-server")]
pub mod embassy_usb_v0_4;

#[cfg(feature = "embedded-io-async-0_6-server")]
pub mod embedded_io_async_v0_6;

#[cfg(feature = "test-utils")]
pub mod test_channels;

#[cfg(any(
    feature = "embassy-usb-0_3-server",
    feature = "embassy-usb-0_4-server",
    feature = "embedded-io-async-0_6-server",
))]
pub(crate) mod embassy_shared {
    use crate::server::WireSpawn;
    use embassy_executor::{SpawnError, SpawnToken, Spawner};

    //////////////////////////////////////////////////////////////////////////////
    // SPAWN
    //////////////////////////////////////////////////////////////////////////////

    /// A [`WireSpawn`] impl using the embassy executor
    #[derive(Clone)]
    pub struct EmbassyWireSpawn {
        /// The embassy-executor spawner
        pub spawner: Spawner,
    }

    impl From<Spawner> for EmbassyWireSpawn {
        fn from(value: Spawner) -> Self {
            Self { spawner: value }
        }
    }

    impl WireSpawn for EmbassyWireSpawn {
        type Error = SpawnError;

        type Info = Spawner;

        fn info(&self) -> &Self::Info {
            &self.spawner
        }
    }

    /// Attempt to spawn the given token
    pub fn embassy_spawn<Sp, S: Sized>(sp: &Sp, tok: SpawnToken<S>) -> Result<(), Sp::Error>
    where
        Sp: WireSpawn<Error = SpawnError, Info = Spawner>,
    {
        let info = sp.info();
        info.spawn(tok)
    }
}
