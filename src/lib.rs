//! A client of the [Cassis Order Protocol](https://github.com/cassiscash/cassis_order_protocol)
//! (COP) for [Freya](https://freyaui.dev) apps.
//!
//! COP is hypereffect over an iroh connection, plus two messages for money. This crate does the
//! COP half and leaves the money to the app:
//!
//! - [`CopUri`] parses `cop://` URIs and [`resolve`] turns a web address into one.
//! - [`connect`] dials the server and shuttles bytes; [`CopSession`] is the page (a
//!   [`freya_hypereffect::HyperHost`]) and the stream of [`CopEvent`]s: an `invoice` the server
//!   asks to be paid, an `invoicerequest` offering money, and the end of the flow.
//! - [`CopView`] draws the page and hands the events to the app, which shows **its own** payment
//!   screen for them: only the client's screen is trustworthy, so none is drawn here.
//!
//! An invoice is a [`serde_json::Value`] here: COP never looks inside it. The app hands it to
//! its payment subsystem, and answers an `invoicerequest` with
//! [`CopSession::send_invoice`].

mod link;
mod session;
mod uri;
mod view;
#[cfg(feature = "web")]
mod web;

pub use hypereffect::Value;
pub use link::{Link, LinkEvent, connect};
pub use session::{CopEvent, CopSession, InvoiceOffer, InvoiceRequest, Mode};
pub use uri::{CopUri, UriError, new_session_id};
pub use view::CopView;
#[cfg(feature = "web")]
pub use web::{ResolveError, resolve};
