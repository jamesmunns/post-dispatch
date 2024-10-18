/// # Define Dispatch Macro
///
// ```rust,skip
// # use postcard_rpc::target_server::dispatch_macro::fake::*;
// # use postcard_rpc::{endpoint, target_server::{sender::Sender, SpawnContext}, WireHeader, define_dispatch};
// # use postcard_schema::Schema;
// # use embassy_usb_driver::{Bus, ControlPipe, EndpointIn, EndpointOut};
// # use serde::{Deserialize, Serialize};
//
// pub struct DispatchCtx;
// pub struct SpawnCtx;
//
// // This trait impl is necessary if you want to use the `spawn` variant,
// // as spawned tasks must take ownership of any context they need.
// impl SpawnContext for DispatchCtx {
//     type SpawnCtxt = SpawnCtx;
//     fn spawn_ctxt(&mut self) -> Self::SpawnCtxt {
//         SpawnCtx
//     }
// }
//
// define_dispatch2! {
//     dispatcher: Dispatcher<
//         Mutex = FakeMutex,
//         Driver = FakeDriver,
//         Context = DispatchCtx,
//     >;
//     AlphaEndpoint => async alpha_handler,
//     BetaEndpoint => async beta_handler,
//     GammaEndpoint => async gamma_handler,
//     DeltaEndpoint => blocking delta_handler,
//     EpsilonEndpoint => spawn epsilon_handler_task,
// }
//
// async fn alpha_handler(_c: &mut DispatchCtx, _h: WireHeader, _b: AReq) -> AResp {
//     todo!()
// }
//
// async fn beta_handler(_c: &mut DispatchCtx, _h: WireHeader, _b: BReq) -> BResp {
//     todo!()
// }
//
// async fn gamma_handler(_c: &mut DispatchCtx, _h: WireHeader, _b: GReq) -> GResp {
//     todo!()
// }
//
// fn delta_handler(_c: &mut DispatchCtx, _h: WireHeader, _b: DReq) -> DResp {
//     todo!()
// }
//
// #[embassy_executor::task]
// async fn epsilon_handler_task(_c: SpawnCtx, _h: WireHeader, _b: EReq, _sender: Sender<FakeMutex, FakeDriver>) {
//     todo!()
// }
// ```

#[macro_export]
macro_rules! define_dispatch2 {
    // This is the "blocking execution" arm for defining an endpoint
    (@arm blocking ($endpoint:ty) $handler:ident $context:ident $header:ident $req:ident $outputter:ident $spawn_fn:ident $spawner:ident) => {
        {
            let reply = $handler($context, $header.clone(), $req);
            if $outputter.reply::<$endpoint>($header.seq_no, &reply).await.is_err() {
                let err = $crate::standard_icd::WireError::SerFailed;
                $outputter.error($header.seq_no, err).await
            } else {
                Ok(())
            }
        }
    };
    // This is the "async execution" arm for defining an endpoint
    (@arm async ($endpoint:ty) $handler:ident $context:ident $header:ident $req:ident $outputter:ident $spawn_fn:ident $spawner:ident) => {
        {
            let reply = $handler($context, $header.clone(), $req).await;
            if $outputter.reply::<$endpoint>($header.seq_no, &reply).await.is_err() {
                let err = $crate::standard_icd::WireError::SerFailed;
                $outputter.error($header.seq_no, err).await
            } else {
                Ok(())
            }
        }
    };
    // This is the "spawn an embassy task" arm for defining an endpoint
    (@arm spawn ($endpoint:ty) $handler:ident $context:ident $header:ident $req:ident $outputter:ident $spawn_fn:ident $spawner:ident) => {
        {
            let context = $crate::target_server::SpawnContext::spawn_ctxt($context);
            if $spawn_fn($spawner, $handler(context, $header.clone(), $req, $outputter.clone())).is_err() {
                let err = $crate::standard_icd::WireError::FailedToSpawn;
                $outputter.error($header.seq_no, err).await
            } else {
                Ok(())
            }
        }
    };
    // Optional trailing comma lol
    (
        dispatcher: $name:ident<Mutex = $mutex:ty, Driver = $driver:ty, Context = $context:ty,>;
        spawn_fn: $spawner:ident;
        $($endpoint:ty => $flavor:tt $handler:ident,)*
    ) => {
        define_dispatch2! {
            dispatcher: $name<Mutex = $mutex, Driver = $driver, Context = $context>;
            spawn_fn: $spawner;
            $(
                $endpoint => $flavor $handler,
            )*
        }
    };
    // This is the main entrypoint
    (
        dispatcher: $name:ident<WireTx = $wire_tx:ty, WireSpawn = $wire_spawn:ty, Context = $context:ty>;
        spawn_fn: $spawner:ident;
        $($endpoint:ty => $flavor:tt $handler:ident,)*
    ) => {
        /// This is a structure that handles dispatching, generated by the
        /// `postcard-rpc::define_dispatch2!()` macro.
        pub struct $name {
            pub context: $context,
            pub spawn: $wire_spawn,
        }

        impl $name {
            /// Create a new instance of the dispatcher
            pub fn new(
                context: $context,
                spawn: $wire_spawn,
            ) -> Self {
                $name {
                    context,
                    spawn,
                }
            }
        }

        impl $crate::server2::Dispatch2 for $name {
            type Tx = $wire_tx;

            /// Handle dispatching of a single frame
            async fn handle(
                &mut self,
                tx: &$crate::server2::Outputter<Self::Tx>,
                hdr: &$crate::WireHeader,
                body: &[u8],
            ) -> Result<(), <Self::Tx as WireTx>::Error> {
                const _REQ_KEYS_MUST_BE_UNIQUE: () = {
                    let keys = [$(<$endpoint as $crate::Endpoint>::REQ_KEY),*];

                    let mut i = 0;

                    while i < keys.len() {
                        let mut j = i + 1;
                        while j < keys.len() {
                            if keys[i].const_cmp(&keys[j]) {
                                panic!("Keys are not unique, there is a collision!");
                            }
                            j += 1;
                        }

                        i += 1;
                    }
                };

                let _ = _REQ_KEYS_MUST_BE_UNIQUE;

                match hdr.key {
                    $(
                        <$endpoint as $crate::Endpoint>::REQ_KEY => {
                            // Can we deserialize the request?
                            let Ok(req) = postcard::from_bytes::<<$endpoint as $crate::Endpoint>::Request>(body) else {
                                let err = $crate::standard_icd::WireError::DeserFailed;
                                return tx.error(hdr.seq_no, err).await;
                            };

                            // Store some items as named bindings, so we can use `ident` in the
                            // recursive macro expansion. Load bearing order: we borrow `context`
                            // from `dispatch` because we need `dispatch` AFTER `context`, so NLL
                            // allows this to still borrowck
                            let dispatch = self;
                            let context = &mut dispatch.context;
                            #[allow(unused)]
                            let spawninfo = &dispatch.spawn;

                            // This will expand to the right "flavor" of handler
                            define_dispatch2!(@arm $flavor ($endpoint) $handler context hdr req tx $spawner spawninfo)
                        }
                    )*
                    other => {
                        // huh! We have no idea what this key is supposed to be!
                        let err = $crate::standard_icd::WireError::UnknownKey(other.to_bytes());
                        tx.error(hdr.seq_no, err).await
                    },
                }
            }
        }

    }
}
