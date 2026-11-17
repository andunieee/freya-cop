use std::{cell::RefCell, rc::Rc};

use freya::prelude::*;
use freya_hypereffect::{HyperView, SharedHost};

use crate::{CopEvent, CopSession};

/// The page of a [`CopSession`]. Keeps the session fed as the server writes, and passes what it
/// sends besides the page (invoices, the end of the flow) to `on_event`.
#[derive(Clone)]
pub struct CopView {
    session: Rc<RefCell<CopSession>>,
    on_event: EventHandler<CopEvent>,
}

impl CopView {
    pub fn new(
        session: Rc<RefCell<CopSession>>,
        on_event: impl FnMut(CopEvent) + 'static,
    ) -> Self {
        Self {
            session,
            on_event: EventHandler::new(on_event),
        }
    }
}

impl PartialEq for CopView {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.session, &other.session)
    }
}

impl Component for CopView {
    fn render(&self) -> impl IntoElement {
        let mut rev = use_state(|| 0u64);
        let session = self.session.clone();
        let on_event = self.on_event.clone();

        use_future(move || {
            let session = session.clone();
            let on_event = on_event.clone();
            async move {
                let incoming = session.borrow().incoming();
                while let Ok(event) = incoming.recv().await {
                    // Never held across an await: the view borrows the session to draw.
                    let events = {
                        let mut session = session.borrow_mut();
                        session.handle(event);
                        session.take_events()
                    };
                    let next = *rev.read() + 1;
                    rev.set(next);
                    for event in events {
                        on_event.call(event);
                    }
                }
            }
        });

        let host: SharedHost = self.session.clone();
        HyperView::new(host, rev)
    }
}
