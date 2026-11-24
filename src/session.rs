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

/// How the connection URI that opened a flow may be used: the `mode` of the server's metadata.
/// With no `mode`, it is reusable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Mode {
    /// It may start any number of flows, for anyone.
    #[default]
    Reusable,
    /// It is meant to start one flow: keep its session, but don't start a new flow from it again.
    SingleUse,
}

impl Mode {
    /// The mode a `mode` value names, `None` for one COP does not define: it is ignored.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "single_use" => Some(Self::SingleUse),
            _ => None,
        }
    }
}

/// What the server did that is not about the page.
#[derive(Clone, Debug, PartialEq)]
pub enum CopEvent {
    Invoice(InvoiceOffer),
    InvoiceRequest(InvoiceRequest),
    /// The flow is over. The reason is the server's `close` reason (such as `session_not_found`)
    /// when it sent one, else the transport's reason if it was not a clean close.
    Closed(Option<String>),
}

impl CopEvent {
    /// The reason a [`CopEvent::Closed`] gave, if any.
    pub fn close_reason(&self) -> Option<&str> {
        match self {
            CopEvent::Closed(reason) => reason.as_deref(),
            _ => None,
        }
    }
}

/// The reason a server gives when it cannot resume a session the client asked it to.
pub const SESSION_NOT_FOUND: &str = "session_not_found";

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
    /// Whether [`CopEvent::Closed`] was already sent.
    closed: bool,
}

impl CopSession {
    /// Open a flow to `uri`. `session` names an earlier flow to reopen, with `force_resume`, so
    /// the server resumes it or closes with [`SESSION_NOT_FOUND`]; `None` starts a new one. `key`
    /// is the iroh identity to present, `None` for a fresh one.
    pub fn open(
        runtime: &tokio::runtime::Handle,
        uri: CopUri,
        session: Option<String>,
        key: Option<[u8; 32]>,
    ) -> Self {
        let resume = session.as_deref().is_some_and(|s| !s.is_empty());
        let session = match session {
            Some(session) if !session.is_empty() => session,
            _ => new_session_id(),
        };
        let link = connect(runtime, &uri, key);
        let hello = hello(&uri, &session, resume);
        let mut host = ClientHost::with_hello(link.sender(), &hello);

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
            closed: false,
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

    /// How the URI that opened this flow may be used, as the server's metadata says.
    pub fn mode(&mut self) -> Mode {
        let mode = self.host.client().metadata().extra.get("mode");
        mode.and_then(|m| Mode::parse(m)).unwrap_or_default()
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
                self.report_close(false, None);
            }
            LinkEvent::Closed(reason) => {
                // A `close` message already ended the session and set its reason on the host:
                // the transport closing afterwards must not erase it.
                if self.host.client().closed().is_none() {
                    self.host.close(reason.clone());
                }
                self.revision += 1;
                self.report_close(true, reason);
            }
        }
    }

    /// Send [`CopEvent::Closed`] once: with the server's `close` reason if it sent one, else with
    /// `fallback`, the transport's reason. `ended` says the transport is over, so a clean close
    /// with no `close` message is still the end of the flow.
    fn report_close(&mut self, ended: bool, fallback: Option<String>) {
        if self.closed {
            return;
        }
        match self.host.client().closed() {
            Some(close) => {
                self.closed = true;
                push(&self.events, CopEvent::Closed(close.reason.clone()));
            }
            None if ended => {
                self.closed = true;
                push(&self.events, CopEvent::Closed(fallback));
            }
            None => {}
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

/// The `hello` object: the fragment parameters as `params`, the client's `session`, and
/// `force_resume` when reopening a saved session.
fn hello(uri: &CopUri, session: &str, force_resume: bool) -> Map<String, Value> {
    let mut hello = Map::new();
    if !uri.params.is_empty() {
        let params = uri
            .params
            .iter()
            .map(|(key, value)| (key.clone(), Value::String(value.clone())))
            .collect();
        hello.insert("params".into(), Value::Object(params));
    }
    hello.insert("session".into(), Value::String(session.to_owned()));
    if force_resume {
        hello.insert("force_resume".into(), Value::Bool(true));
    }
    hello
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

    fn is_closed(&self) -> bool {
        self.host.is_closed()
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

    #[test]
    fn hello_carries_the_session_and_params() {
        let uri = CopUri::parse(&format!(
            "cop://{}#table=12&name=Luigi%27s%20Pizza",
            "5f1c9d0a27e4b6c38d2f71a09e4b5c6d8a3f2e1b0c9d8e7f6a5b4c3d2e1f0a9b"
        ))
        .unwrap();

        // A new flow: the session is a top-level member and there is no `force_resume`.
        let new = hello(&uri, "abc", false);
        assert_eq!(new["session"], Value::String("abc".into()));
        assert_eq!(
            new["params"],
            hypereffect::json!({"name": "Luigi's Pizza", "table": "12"})
        );
        assert!(!new.contains_key("force_resume"));

        // Reopening one asks the server to resume it or say it cannot.
        let resume = hello(&uri, "abc", true);
        assert_eq!(resume["force_resume"], Value::Bool(true));

        // What actually goes on the wire: `hello` with the members above.
        let sent = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let log = sent.clone();
        let _host = ClientHost::with_hello(move |line| log.borrow_mut().push(line), &resume);
        let sent = sent.borrow();
        let Ok(hypereffect::RendererMessage::Hello(body)) =
            hypereffect::RendererMessage::parse(sent[0].trim_end())
        else {
            panic!("not a hello: {}", sent[0]);
        };
        assert_eq!(body, resume);

        // Without fragment parameters the `params` member is omitted.
        let bare = CopUri::parse(&format!(
            "cop://{}",
            "5f1c9d0a27e4b6c38d2f71a09e4b5c6d8a3f2e1b0c9d8e7f6a5b4c3d2e1f0a9b"
        ))
        .unwrap();
        assert!(!hello(&bare, "abc", false).contains_key("params"));
    }

    #[test]
    fn close_reason_is_visible() {
        let event = CopEvent::Closed(Some(SESSION_NOT_FOUND.into()));
        assert_eq!(event.close_reason(), Some("session_not_found"));
        assert_eq!(CopEvent::Closed(None).close_reason(), None);
    }

    #[test]
    fn close_message_ends_the_flow_with_its_reason() {
        // A current-thread runtime never polls the link task here, so no connection is made:
        // the test feeds the session the lines a server would send.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let uri = CopUri::parse(&format!("cop://{}", "0".repeat(64))).unwrap();
        let mut session = CopSession::open(runtime.handle(), uri, None, None);

        session.handle(LinkEvent::Data(
            b"close {\"reason\":\"session_not_found\"}\n".to_vec(),
        ));
        assert_eq!(
            session.take_events(),
            vec![CopEvent::Closed(Some(SESSION_NOT_FOUND.into()))]
        );

        // The transport closing after the message does not report the end twice.
        session.handle(LinkEvent::Closed(None));
        assert!(session.take_events().is_empty());
    }
}
