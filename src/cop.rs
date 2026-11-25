//! What every service shares: the iroh endpoint of a service, the COP flow
//! on one connection (`hello`, then hypereffect lines both ways), and
//! asking the payer for an invoice this node issues and claims.

use crate::history::{History, Log};
use async_trait::async_trait;
use cassis_core::unix_now;
use cassis_wallet::db::{InvoiceHash, InvoiceState};
use cassis_wallet::Node;
use hypereffect::message::{encode_line, parse_line};
use hypereffect::vtree;
use hypereffect::{json, Action, Close, Map, Metadata, RendererMessage, ServerMessage, Value};
use iroh::endpoint::{presets, SendStream};
use iroh::{Endpoint, SecretKey};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::AbortHandle;
use tracing::{info, warn};

pub const ALPN: &[u8] = b"cassis-order";
/// The alpn of the admin interface, `hyperb://<id>/admin` (see the hyperb
/// spec, section 3).
pub const ADMIN_ALPN: &[u8] = b"hyperb/0/admin";
pub const SESSION_NOT_FOUND: &str = "session_not_found";
const INVOICE_TTL_SECS: u64 = 15 * 60;

/// The sessions of a service, kept by the client's `session` token: what
/// a flow is in the middle of, in memory, and what was done in it, in the
/// [`History`], which outlives a restart.
pub struct Sessions<T> {
    service: &'static str,
    history: History,
    open: Mutex<HashMap<String, Arc<Mutex<T>>>>,
}

impl<T: Default> Sessions<T> {
    pub fn new(history: History, service: &'static str) -> Self {
        Self {
            service,
            history,
            open: Mutex::new(HashMap::new()),
        }
    }

    /// Find `session`, or start a new one unless `force_resume` asked not
    /// to. A session the history knows resumes with what was done in it.
    pub fn open(&self, session: String, force_resume: bool) -> Option<(Arc<Mutex<T>>, Log)> {
        let mut open = self.open.lock().unwrap_or_else(|e| e.into_inner());
        if force_resume && !open.contains_key(&session) && !self.history.knows(self.service, &session) {
            return None;
        }
        let log = self.history.log(self.service, &session);
        Some((open.entry(session).or_default().clone(), log))
    }
}

/// A service: what a flow on its endpoint shows and does.
#[async_trait]
pub trait Service: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    /// What the service's web page shows of what it holds, as HTML.
    fn preview(&self) -> String {
        String::new()
    }
    /// A flow was opened: draw its first page, or return `None` when
    /// `force_resume` asked for a `session` the service does not have.
    async fn open(
        self: Arc<Self>,
        session: String,
        force_resume: bool,
        out: Out,
    ) -> Result<Option<Arc<dyn Flow>>, String>;
}

/// One open flow of a service.
#[async_trait]
pub trait Flow: Send + Sync {
    async fn action(&self, action: Action) -> Result<(), String>;
    /// The client answered an `invoicerequest` with `invoice`. Nothing is
    /// asked of a client unless a flow sends one, so by default an
    /// `invoice` is unsolicited and ignored.
    async fn invoice(&self, _invoice: Value) -> Result<(), String> {
        warn!("ignoring an unsolicited invoice");
        Ok(())
    }
}

struct Stream {
    send: SendStream,
    /// Sent before the first message of an opened flow, and dropped if the
    /// flow closes right away with `session_not_found`.
    pending: String,
    /// The page the client was last sent.
    page: Option<vtree::Node>,
}

/// The stream to a client. Cheap to clone.
#[derive(Clone)]
pub struct Out {
    stream: Arc<AsyncMutex<Stream>>,
    tasks: Arc<Mutex<Vec<AbortHandle>>>,
}

impl Out {
    pub fn new(send: SendStream) -> Self {
        Self::with_pending(send, String::new())
    }

    pub fn with_metadata(send: SendStream, metadata: Metadata) -> Self {
        Self::with_pending(send, ServerMessage::from(metadata).to_line())
    }

    fn with_pending(send: SendStream, pending: String) -> Self {
        Self {
            stream: Arc::new(AsyncMutex::new(Stream {
                send,
                pending,
                page: None,
            })),
            tasks: Default::default(),
        }
    }

    pub async fn send(&self, lines: String) -> Result<(), String> {
        let mut stream = self.stream.lock().await;
        let mut out = std::mem::take(&mut stream.pending);
        out.push_str(&lines);
        stream
            .send
            .write_all(out.as_bytes())
            .await
            .map_err(|e| format!("send: {e}"))
    }

    /// Send what changed on the page since it was last sent.
    pub async fn redraw(&self, new: vtree::Node) -> Result<(), String> {
        let mut stream = self.stream.lock().await;
        let messages = match &stream.page {
            Some(old) => vtree::diff(old, &new),
            None => vtree::render(&new),
        };
        let mut lines = std::mem::take(&mut stream.pending);
        lines.extend(messages.iter().map(ServerMessage::to_line));
        stream
            .send
            .write_all(lines.as_bytes())
            .await
            .map_err(|e| format!("send: {e}"))?;
        stream.page = Some(new);
        Ok(())
    }

    pub async fn signal(&self, patch: Value) -> Result<(), String> {
        self.send(ServerMessage::signal(patch).to_line()).await
    }

    /// End the flow with a hypereffect `close` message and finish the stream.
    pub async fn close(&self, close: Close) -> Result<(), String> {
        let mut stream = self.stream.lock().await;
        stream.pending.clear();
        stream
            .send
            .write_all(close.to_line().as_bytes())
            .await
            .map_err(|e| format!("send: {e}"))?;
        let _ = stream.send.finish();
        let _ = tokio::time::timeout(Duration::from_secs(2), stream.send.stopped()).await;
        Ok(())
    }

    /// Run `task` until the flow ends.
    pub fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        self.tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(tokio::spawn(task).abort_handle());
    }

    pub fn stop(&self) {
        for task in self.tasks.lock().unwrap_or_else(|e| e.into_inner()).drain(..) {
            task.abort();
        }
    }
}

/// An endpoint whose id is the public key of `secret`.
pub async fn bind(secret: [u8; 32], alpn: &[u8]) -> Result<Endpoint, String> {
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(SecretKey::from(secret))
        .alpns(vec![alpn.to_vec()])
        .bind()
        .await
        .map_err(|e| format!("bind iroh endpoint: {e}"))?;
    let _ = tokio::time::timeout(Duration::from_secs(10), endpoint.online()).await;
    Ok(endpoint)
}

/// The `cop://` URI of `endpoint`.
pub fn uri(endpoint: &Endpoint, name: &str) -> String {
    let mut uri = format!("cop://{}", endpoint.id());
    if let Some(relay) = endpoint.addr().relay_urls().next() {
        uri.push_str(&format!("?_relay={}", encode(&relay.to_string())));
    }
    uri.push_str(&format!("#name={}", encode(name)));
    uri
}

pub fn encode(text: &str) -> String {
    text.bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

/// Serve `service` on `endpoint` until the endpoint closes.
pub async fn serve(endpoint: Endpoint, service: Arc<dyn Service>) {
    while let Some(incoming) = endpoint.accept().await {
        let service = service.clone();
        tokio::spawn(async move {
            let connection = match incoming.await {
                Ok(connection) => connection,
                Err(error) => return warn!("order connection failed: {error}"),
            };
            let Ok((send, recv)) = connection.accept_bi().await else {
                return;
            };
            let metadata = Metadata::new()
                .name(service.name())
                .description(service.description());
            let out = Out::with_metadata(send, metadata);
            if let Err(error) = flow(service.clone(), out.clone(), recv).await {
                warn!("{} flow ended: {error}", service.name());
            }
            out.stop();
            connection.close(0u32.into(), b"done");
        });
    }
}

fn parse_hello(hello: &Map<String, Value>) -> Result<(String, bool), String> {
    let session = hello
        .get("session")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "the hello has no session".to_string())?
        .to_string();
    let force_resume = hello
        .get("force_resume")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Ok((session, force_resume))
}

async fn flow(
    service: Arc<dyn Service>,
    out: Out,
    recv: iroh::endpoint::RecvStream,
) -> Result<(), String> {
    let mut lines = BufReader::new(recv).lines();
    let (session, force_resume) = match lines.next_line().await.map_err(|e| e.to_string())? {
        Some(line) => match RendererMessage::parse(&line) {
            Ok(RendererMessage::Hello(hello)) => parse_hello(&hello)?,
            _ => return Err("the flow does not start with a hello".into()),
        },
        None => return Ok(()),
    };
    let Some(flow) = service.clone().open(session, force_resume, out.clone()).await? else {
        info!("{}: session not found", service.name());
        return out.close(Close::new(SESSION_NOT_FOUND)).await;
    };
    info!("{} flow opened", service.name());
    while let Some(line) = lines.next_line().await.map_err(|e| e.to_string())? {
        if line.is_empty() {
            continue;
        }
        // `invoice` is COP's, not hypereffect's: look at it first.
        if let Ok(("invoice", body)) = parse_line(&line) {
            flow.invoice(body.get("invoice").cloned().unwrap_or_default())
                .await?;
            continue;
        }
        match RendererMessage::parse(&line) {
            Ok(RendererMessage::Action(action)) => flow.action(action).await?,
            Ok(RendererMessage::Hello(_)) => warn!("ignoring a second hello"),
            Err(error) => warn!("ignoring message: {error}"),
        }
    }
    Ok(())
}

/// Ask the client for an invoice of `sats` on any network of this node,
/// to pay it: the answer comes to [`Flow::invoice`].
pub async fn request_invoice(node: &Node, out: &Out, sats: u64, memo: &str) -> Result<(), String> {
    let networks: Vec<String> = node.network_ids().iter().map(|network| network.to_string()).collect();
    out.send(encode_line(
        "invoicerequest",
        &json!({
            "amount": sats,
            "networks": networks,
            "memo": memo,
            "expiresIn": INVOICE_TTL_SECS,
        }),
    ))
    .await
}

/// Ask the payer for `sats` with an invoice of this node, and run
/// `on_paid` once the node claims it. That is tied to the node, not to the
/// flow: a payer who pays after disconnecting still gets what they paid for.
pub async fn request_payment(
    node: &Node,
    out: &Out,
    sats: u64,
    memo: String,
    on_paid: impl Future<Output = ()> + Send + 'static,
) -> Result<(), String> {
    let expires_at = unix_now() + INVOICE_TTL_SECS;
    let invoice = node
        .create_invoice(
            &node.network_ids(),
            sats * 1000,
            expires_at,
            Some(memo.clone()),
            None,
        )
        .await?;
    out.send(encode_line("invoice", &json!({"invoice": invoice, "memo": memo})))
        .await?;
    let key = InvoiceHash(invoice.body.payment_hash.0);
    let watcher = node.clone();
    node.spawn(async move {
        while unix_now() < expires_at {
            let record = watcher.db().invoices().get(key.clone()).ok().flatten();
            if record.is_some_and(|record| record.state == InvoiceState::Settled) {
                return on_paid.await;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_session_and_force_resume() {
        let parse = |line: &str| match RendererMessage::parse(line).unwrap() {
            RendererMessage::Hello(hello) => parse_hello(&hello),
            _ => panic!("not a hello"),
        };

        assert_eq!(
            parse(r#"hello {"session":"f1K9xQ2v8Tn4LmZp7Rb3wq"}"#),
            Ok(("f1K9xQ2v8Tn4LmZp7Rb3wq".into(), false))
        );
        assert_eq!(
            parse(
                r#"hello {"params":{"name":"Wall"},"session":"f1K9xQ2v8Tn4LmZp7Rb3wq","force_resume":true}"#
            ),
            Ok(("f1K9xQ2v8Tn4LmZp7Rb3wq".into(), true))
        );
        assert_eq!(
            parse(r#"hello {"session":"f1K9xQ2v8Tn4LmZp7Rb3wq","force_resume":"yes"}"#),
            Ok(("f1K9xQ2v8Tn4LmZp7Rb3wq".into(), false))
        );
        assert!(parse(r#"hello {}"#).is_err());
        assert!(parse(r#"hello {"session":""}"#).is_err());
        assert!(parse(r#"hello {"params":{"session":"f1K9xQ2v8Tn4LmZp7Rb3wq"}}"#).is_err());
    }

    #[test]
    fn sessions_resume_or_fail_when_forced() {
        let history = History::in_memory();
        let sessions = Sessions::<u32>::new(history.clone(), "wall");
        assert!(sessions.open("unknown".into(), true).is_none());

        let (first, _) = sessions.open("abc".into(), false).unwrap();
        *first.lock().unwrap() = 42;

        let (resumed, _) = sessions.open("abc".into(), true).unwrap();
        assert_eq!(*resumed.lock().unwrap(), 42);

        let (reopened, _) = sessions.open("abc".into(), false).unwrap();
        assert_eq!(*reopened.lock().unwrap(), 42);

        // After a restart, what the flow was in the middle of is gone, but
        // the session resumes with what was done in it.
        let (_, log) = sessions.open("abc".into(), false).unwrap();
        log.record(10, crate::history::Deed::Received);
        let restarted = Sessions::<u32>::new(history, "wall");
        let (state, log) = restarted.open("abc".into(), true).unwrap();
        assert_eq!(*state.lock().unwrap(), 0);
        assert_eq!(log.entries().len(), 1);
        assert!(restarted.open("unknown".into(), true).is_none());

        assert_eq!(
            Close::new(SESSION_NOT_FOUND).to_line(),
            "close {\"reason\":\"session_not_found\"}\n"
        );
    }
}
