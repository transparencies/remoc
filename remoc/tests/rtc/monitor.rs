#[cfg(feature = "js")]
use wasm_bindgen_test::wasm_bindgen_test;

use std::{
    error::Error,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures::{FutureExt, future::BoxFuture};

use remoc::rtc::{DispatchDecision, MonitorableServer, Req, ServeError, Server, ServerMonitor};

use crate::loop_channel;

#[remoc::rtc::remote]
pub trait Counter {
    async fn value(self) -> Result<u32, remoc::rtc::CallError>;
    async fn value_ref(&self) -> Result<u32, remoc::rtc::CallError>;
    async fn increase(&mut self, by: u32) -> Result<(), remoc::rtc::CallError>;
}

pub struct CounterObj {
    value: u32,
}

impl Counter for CounterObj {
    async fn value(self) -> Result<u32, remoc::rtc::CallError> {
        Ok(self.value)
    }

    async fn value_ref(&self) -> Result<u32, remoc::rtc::CallError> {
        Ok(self.value)
    }

    async fn increase(&mut self, by: u32) -> Result<(), remoc::rtc::CallError> {
        self.value += by;
        Ok(())
    }
}

/// Monitor that counts handled requests.
///
/// It keeps the request borrowed across an await point inside the returned
/// future, which exercises the borrowing-future lifetime `'a` of
/// [`ServerMonitor::pre_dispatch`]. Because the returned future is `Send` and
/// holds a shared reference `&req`, the request must be `Sync` (a `&T` is
/// `Send` only when `T: Sync`). The generated request types are `Send + Sync`,
/// so this is always satisfied.
struct CountingMonitor {
    count: Arc<AtomicUsize>,
}

impl<V, R, M> ServerMonitor<V, R, M> for CountingMonitor
where
    Req<V, R, M>: Sync,
{
    fn pre_dispatch<'a>(
        &mut self, req: &'a Result<Option<Req<V, R, M>>, remoc::rch::mpsc::RecvError>,
    ) -> BoxFuture<'a, DispatchDecision> {
        let count = self.count.clone();
        async move {
            // Do some async work while keeping the request borrowed.
            futures::future::ready(()).await;
            if matches!(req, Ok(Some(_))) {
                count.fetch_add(1, Ordering::SeqCst);
            }
            DispatchDecision::Handle
        }
        .boxed()
    }
}

/// Error returned by [`RateLimitMonitor`] once the request budget is exhausted.
#[derive(Debug)]
struct RateLimited;

impl fmt::Display for RateLimited {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "too many requests")
    }
}

impl Error for RateLimited {}

/// Monitor that allows a fixed number of requests and then fails the server.
struct RateLimitMonitor {
    remaining: usize,
}

impl<V, R, M> ServerMonitor<V, R, M> for RateLimitMonitor {
    fn pre_dispatch<'a>(
        &mut self, req: &'a Result<Option<Req<V, R, M>>, remoc::rch::mpsc::RecvError>,
    ) -> BoxFuture<'a, DispatchDecision> {
        let allow = if matches!(req, Ok(Some(_))) {
            if self.remaining == 0 {
                false
            } else {
                self.remaining -= 1;
                true
            }
        } else {
            true
        };

        async move {
            if allow {
                DispatchDecision::Handle
            } else {
                DispatchDecision::Error(Box::new(RateLimited))
            }
        }
        .boxed()
    }
}

#[cfg_attr(not(feature = "js"), tokio::test)]
#[cfg_attr(feature = "js", wasm_bindgen_test)]
async fn count() {
    crate::init();
    let ((mut a_tx, _), (_, mut b_rx)) = loop_channel::<CounterClient>().await;

    let count = Arc::new(AtomicUsize::new(0));

    println!("Creating counter server with counting monitor");
    let (mut server, client) = CounterServer::new(CounterObj { value: 0 }, 1);
    server.set_monitor(CountingMonitor { count: count.clone() });

    a_tx.send(client).await.unwrap();

    let client_task = async move {
        let mut client = b_rx.recv().await.unwrap().unwrap();

        client.increase(20).await.unwrap();
        assert_eq!(client.value_ref().await.unwrap(), 20);
        client.increase(45).await.unwrap();
        assert_eq!(client.value().await.unwrap(), 65);
    };

    let (_, (obj, res)) = tokio::join!(client_task, server.serve());
    res.unwrap();
    assert!(obj.is_none());

    // increase + value_ref + increase + value == 4 requests.
    assert_eq!(count.load(Ordering::SeqCst), 4);
}

#[cfg_attr(not(feature = "js"), tokio::test)]
#[cfg_attr(feature = "js", wasm_bindgen_test)]
async fn rate_limit() {
    crate::init();
    let ((mut a_tx, _), (_, mut b_rx)) = loop_channel::<CounterClient>().await;

    println!("Creating counter server with rate-limiting monitor");
    let (mut server, client) = CounterServer::new(CounterObj { value: 0 }, 1);
    server.set_monitor(RateLimitMonitor { remaining: 1 });

    a_tx.send(client).await.unwrap();

    let client_task = async move {
        let mut client = b_rx.recv().await.unwrap().unwrap();

        // First request is allowed.
        client.increase(20).await.unwrap();

        // Second request is rejected, which fails the server, so the call errors out.
        assert!(client.increase(45).await.is_err());
    };

    let (_, (obj, res)) = tokio::join!(client_task, server.serve());

    // The server failed with the monitor error but still returns the target.
    assert!(matches!(res, Err(ServeError::Monitor(_))));
    assert!(obj.is_some());
}
