use std::{str::FromStr, time::Duration};

use iroh::{
    Endpoint, EndpointAddr, PublicKey, RelayUrl, SecretKey,
    endpoint::presets,
};
use tokio::{sync::mpsc, task::AbortHandle};

use crate::CopUri;

const ALPN: &[u8] = b"cassis-order";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// More than this arriving in one flow is a server gone wrong, not a page.
const MAX_FLOW_BYTES: usize = 32 * 1024 * 1024;

/// What happened on the stream to the server.
#[derive(Clone, Debug)]
pub enum LinkEvent {
    /// Bytes of hypereffect lines from the server.
    Data(Vec<u8>),
    /// The flow is over; the reason is there if it was not a clean close.
    Closed(Option<String>),
}

/// The stream of one flow. Dropping it ends the flow.
pub struct Link {
    pub incoming: async_channel::Receiver<LinkEvent>,
    outgoing: mpsc::UnboundedSender<String>,
    task: AbortHandle,
}

impl Link {
    /// Queue a line (without its line feed) for the server.
    pub fn send(&self, line: String) {
        let _ = self.outgoing.send(line);
    }

    /// A cheap handle to [`Link::send`] for closures.
    pub fn sender(&self) -> impl Fn(String) + Send + Sync + 'static {
        let outgoing = self.outgoing.clone();
        move |line| {
            let _ = outgoing.send(line);
        }
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Dial the server of `uri` on `runtime` and keep the stream: lines queued with [`Link::send`]
/// go out (the first must be the `hello`), bytes that come in arrive as [`LinkEvent`]s.
///
/// `key` is the iroh identity the server sees: `None` for a fresh one every flow, which cannot
/// be linked to any other.
pub fn connect(runtime: &tokio::runtime::Handle, uri: &CopUri, key: Option<[u8; 32]>) -> Link {
    let (event_tx, incoming) = async_channel::unbounded();
    let (outgoing, lines) = mpsc::unbounded_channel();
    let uri = uri.clone();
    let task = runtime.spawn(async move {
        let reason = run(&uri, key, lines, &event_tx).await.err();
        let _ = event_tx.send(LinkEvent::Closed(reason)).await;
    });
    Link {
        incoming,
        outgoing,
        task: task.abort_handle(),
    }
}

async fn run(
    uri: &CopUri,
    key: Option<[u8; 32]>,
    mut lines: mpsc::UnboundedReceiver<String>,
    events: &async_channel::Sender<LinkEvent>,
) -> Result<(), String> {
    let mut addr = EndpointAddr::new(
        PublicKey::from_str(&uri.endpoint).map_err(|e| format!("endpoint id: {e}"))?,
    );
    if let Some(relay) = &uri.relay {
        if relay_host_is_onion(relay) {
            return Err("this server is only reachable over Tor, which is not supported".into());
        }
        addr = addr.with_relay_url(
            RelayUrl::from_str(relay).map_err(|e| format!("relay {relay}: {e}"))?,
        );
    }
    let mut builder = Endpoint::builder(presets::N0);
    if let Some(key) = key {
        builder = builder.secret_key(SecretKey::from(key));
    }
    let endpoint = builder
        .bind()
        .await
        .map_err(|e| format!("bind iroh endpoint: {e}"))?;
    let connection = tokio::time::timeout(CONNECT_TIMEOUT, endpoint.connect(addr, ALPN))
        .await
        .map_err(|_| "the server did not answer in time".to_string())?
        .map_err(|e| format!("connect: {e:#}"))?;
    let (mut send, mut receive) = connection
        .open_bi()
        .await
        .map_err(|e| format!("open stream: {e}"))?;

    let mut total = 0usize;
    let mut buffer = vec![0u8; 16 * 1024];
    loop {
        tokio::select! {
            line = lines.recv() => {
                let Some(line) = line else { break };
                send.write_all(line.trim_end().as_bytes()).await.map_err(|e| format!("send: {e}"))?;
                send.write_all(b"\n").await.map_err(|e| format!("send: {e}"))?;
            }
            read = receive.read(&mut buffer) => {
                match read.map_err(|e| format!("receive: {e}"))? {
                    Some(n) => {
                        total += n;
                        if total > MAX_FLOW_BYTES {
                            return Err("the server sent too much".into());
                        }
                        if events.send(LinkEvent::Data(buffer[..n].to_vec())).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }
    let _ = send.finish();
    connection.close(0u32.into(), b"done");
    endpoint.close().await;
    Ok(())
}

fn relay_host_is_onion(relay: &str) -> bool {
    let rest = relay.split_once("://").map_or(relay, |(_, rest)| rest);
    let host = rest.split(['/', ':']).next().unwrap_or("");
    host.ends_with(".onion")
}
