use std::sync::{Arc, Mutex};

use freya_hypereffect::{ClientHost, HyperHost};
use hypereffect::{Map, Value, client::ResolvedTree, message::encode_line};

use crate::{CopUri, Link, LinkEvent, connect, new_session_id};

/// An `invoice` the server asks the client to pay.
#[derive(Clone, Debug, PartialEq)]
pub struct InvoiceOffer {
    /// The Cassis invoice, as the server sent it. COP never looks inside.
    pub invoice: Value,
    /// A short note from the server. Amount and payee come from the invoice, not from this.
    pub memo: Option<String>,
}

/// An `invoicerequest`: the server offers money and asks for an invoice to pay it with.
#[derive(Clone, Debug, PartialEq)]
pub struct InvoiceRequest {
    pub amount_sat: u64,
    /// The Cassis network ids the server can pay from.
    pub networks: Vec<String>,
    pub memo: Option<String>,
    pub expires_in: Option<u64>,
}

/// What the server did that is not about the page.
#[derive(Clone, Debug, PartialEq)]
pub enum CopEvent {
    Invoice(InvoiceOffer),
    InvoiceRequest(InvoiceRequest),
    /// The flow is over; the reason is there if it was not a clean close.
    Closed(Option<String>),
}

type Events = Arc<Mutex<Vec<CopEvent>>>;

/// One flow with a server: its page, and what else it sends. Lives on the UI thread; the
/// network is on the other side of a [`Link`].
pub struct CopSession {
    uri: CopUri,
    session: String,
    host: ClientHost,
    link: Link,
    events: Events,
    /// Counts what the server changes outside the view, so the view redraws for it.
    revision: u64,
}

impl CopSession {
    /// Open a flow to `uri`. `session` reopens an earlier flow, `None` starts a new one. `key`
    /// is the iroh identity to present, `None` for a fresh one.
    pub fn open(
        runtime: &tokio::runtime::Handle,
        uri: CopUri,
        session: Option<String>,
        key: Option<[u8; 32]>,
    ) -> Self {
        let session = session.unwrap_or_else(new_session_id);
        let link = connect(runtime, &uri, key);
        let mut params = uri.params.clone();
        params.insert("session".into(), session.clone());
        let mut host = ClientHost::with_hello(link.sender(), &params);

        let events = Events::default();
        let sink = events.clone();
        host.client().on_message("invoice", move |body| {
            if let Some(offer) = parse_offer(&body) {
                push(&sink, CopEvent::Invoice(offer));
            }
        });
        let sink = events.clone();
        host.client().on_message("invoicerequest", move |body| {
            if let Some(request) = parse_request(&body) {
                push(&sink, CopEvent::InvoiceRequest(request));
            }
        });
        Self {
            uri,
            session,
            host,
            link,
            events,
            revision: 0,
        }
    }

    pub fn uri(&self) -> &CopUri {
        &self.uri
    }

    /// The `session` string of this flow, to keep so it can be reopened.
    pub fn session(&self) -> &str {
        &self.session
    }

    /// What to show as the server's name: what it calls itself, else what the URI says.
    pub fn name(&mut self) -> Option<String> {
        let name = self.host.client().metadata().name.clone();
        name.or_else(|| self.uri.name().map(str::to_owned))
    }

    /// Where the events of the stream arrive. Await them, and give each to
    /// [`CopSession::handle`].
    pub fn incoming(&self) -> async_channel::Receiver<LinkEvent> {
        self.link.incoming.clone()
    }

    pub fn handle(&mut self, event: LinkEvent) {
        match event {
            LinkEvent::Data(bytes) => {
                self.host.receive(&bytes);
                self.revision += 1;
            }
            LinkEvent::Closed(reason) => {
                self.host.close(reason.clone());
                self.revision += 1;
                push(&self.events, CopEvent::Closed(reason));
            }
        }
    }

    /// Everything handled so far that is not about the page, oldest first.
    pub fn take_events(&mut self) -> Vec<CopEvent> {
        std::mem::take(&mut *self.events.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// Answer an `invoicerequest` with the invoice made for it.
    pub fn send_invoice(&mut self, invoice: Value) {
        let mut body = Map::new();
        body.insert("invoice".into(), invoice);
        self.link.send(encode_line("invoice", &body));
    }
}

fn push(events: &Events, event: CopEvent) {
    events.lock().unwrap_or_else(|e| e.into_inner()).push(event);
}

fn parse_offer(body: &Map<String, Value>) -> Option<InvoiceOffer> {
    let invoice = body.get("invoice").filter(|i| i.is_object())?.clone();
    Some(InvoiceOffer {
        invoice,
        memo: body.get("memo").and_then(Value::as_str).map(str::to_owned),
    })
}

fn parse_request(body: &Map<String, Value>) -> Option<InvoiceRequest> {
    let amount_sat = body.get("amount")?.as_u64()?;
    let networks: Vec<String> = body
        .get("networks")?
        .as_array()?
        .iter()
        .filter_map(|n| n.as_str().map(str::to_owned))
        .collect();
    if networks.is_empty() {
        return None;
    }
    Some(InvoiceRequest {
        amount_sat,
        networks,
        memo: body.get("memo").and_then(Value::as_str).map(str::to_owned),
        expires_in: body.get("expiresIn").and_then(Value::as_u64),
    })
}

impl HyperHost for CopSession {
    fn tree(&self) -> Vec<ResolvedTree> {
        self.host.tree()
    }

    fn value(&self, path: &str) -> Option<Value> {
        self.host.value(path)
    }

    fn write(&mut self, path: &str, value: Value) {
        self.host.write(path, value);
    }

    fn act(&mut self, id: &str, event: &str) {
        self.host.act(id, event);
    }

    fn revision(&self) -> u64 {
        self.revision
    }

    fn error(&self) -> Option<String> {
        self.host.error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_messages() {
        let body = |json: &str| match hypereffect::serde_json::from_str::<Value>(json).unwrap() {
            Value::Object(m) => m,
            _ => unreachable!(),
        };
        let offer = parse_offer(&body(r#"{"invoice":{"a":1},"memo":"x"}"#)).unwrap();
        assert_eq!(offer.memo.as_deref(), Some("x"));
        assert!(parse_offer(&body(r#"{"memo":"x"}"#)).is_none());

        let request = parse_request(&body(
            r#"{"amount":24000,"networks":["liquid","cashu::m.example"],"expiresIn":900}"#,
        ))
        .unwrap();
        assert_eq!(request.amount_sat, 24000);
        assert_eq!(request.expires_in, Some(900));
        assert!(parse_request(&body(r#"{"amount":1,"networks":[]}"#)).is_none());
    }
}
