# freya-cop

A client of the [Cassis Order Protocol](../cassis_order_protocol/spec.md) (COP) for Freya apps. COP is hypereffect over an iroh connection plus two messages for money (`invoice`, `invoicerequest`); this crate does the COP half and leaves the money to the app.

- `CopUri`, `resolve`: `cop://` URIs, and web addresses that publish one (`<link rel="cassis-order">`, HTTPS only, bounded, never rendered).
- `connect`, `Link`: dials the server (ALPN `cassis-order`) and shuttles bytes. Presents a fresh iroh key per flow unless given one.
- `CopSession`: the page as a `freya_hypereffect::HyperHost`, the `session` string, and the `CopEvent`s: `Invoice`, `InvoiceRequest`, `Closed`. The two COP messages reach it through the handlers hypereffect lets a protocol register (`Client::on_message`).
- `CopView`: draws the page and hands the events to the app, which shows **its own** payment screen: only the client's screen is trustworthy, so none is drawn here.

Invoices stay `serde_json::Value`: the app hands them to its payment subsystem, and answers an `invoicerequest` with `CopSession::send_invoice`.

Not supported: servers reached through a `.onion` relay (it needs Tor).
