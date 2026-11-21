//! What every service shares: the iroh endpoint of a service, the COP flow
//! on one connection (`hello`, then hypereffect lines both ways), and
//! asking the payer for an invoice this node issues and claims.

use async_trait::async_trait;
use cassis_core::unix_now;
use cassis_wallet::db::{InvoiceHash, InvoiceState};
use cassis_wallet::Node;
use hypereffect::message::{encode_line, parse_line};
use hypereffect::vtree;
use hypereffect::{json, Action, Metadata, RendererMessage, ServerMessage};
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
const INVOICE_TTL_SECS: u64 = 15 * 60;

/// A service: what a flow on its endpoint shows and does.
#[async_trait]
pub trait Service: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    /// What the service's web page shows of what it holds, as HTML.
    fn preview(&self) -> String {
        String::new()
    }
    /// A flow was opened: draw its first page. `session` is the one the
    /// client sent in its `hello`.
    async fn open(self: Arc<Self>, session: String, out: Out) -> Result<Arc<dyn Flow>, String>;
}

/// One open flow of a service.
#[async_trait]
pub trait Flow: Send + Sync {
    async fn action(&self, action: Action) -> Result<(), String>;
    /// The client answered an `invoicerequest` with `invoice`. Nothing is
    /// asked of a client unless a flow sends one, so by default an
    /// `invoice` is unsolicited and ignored.
    async fn invoice(&self, _invoice: hypereffect::Value) -> Result<(), String> {
        warn!("ignoring an unsolicited invoice");
        Ok(())
    }
}

struct Stream {
    send: SendStream,
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
        Self {
            stream: Arc::new(AsyncMutex::new(Stream { send, page: None })),
            tasks: Default::default(),
        }
    }

    pub async fn send(&self, lines: String) -> Result<(), String> {
        let mut stream = self.stream.lock().await;
        stream
            .send
            .write_all(lines.as_bytes())
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
        let lines: String = messages.iter().map(ServerMessage::to_line).collect();
        stream
            .send
            .write_all(lines.as_bytes())
            .await
            .map_err(|e| format!("send: {e}"))?;
        stream.page = Some(new);
        Ok(())
    }

    pub async fn signal(&self, patch: hypereffect::Value) -> Result<(), String> {
        self.send(ServerMessage::signal(patch).to_line()).await
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
            let out = Out::new(send);
            if let Err(error) = flow(service.clone(), out.clone(), recv).await {
                warn!("{} flow ended: {error}", service.name());
            }
            out.stop();
            connection.close(0u32.into(), b"done");
        });
    }
}

async fn flow(
    service: Arc<dyn Service>,
    out: Out,
    recv: iroh::endpoint::RecvStream,
) -> Result<(), String> {
    let mut lines = BufReader::new(recv).lines();
    let params: HashMap<String, String> = match lines.next_line().await.map_err(|e| e.to_string())? {
        Some(line) => match RendererMessage::parse(&line) {
            Ok(RendererMessage::Hello { params }) => params.into_iter().collect(),
            _ => return Err("the flow does not start with a hello".into()),
        },
        None => return Ok(()),
    };
    let session = params.get("session").cloned().unwrap_or_default();
    info!("{} flow opened", service.name());
    let metadata = Metadata::new()
        .name(service.name())
        .description(service.description());
    out.send(ServerMessage::from(metadata).to_line()).await?;
    let flow = service.clone().open(session, out.clone()).await?;
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
            Ok(RendererMessage::Hello { .. }) => warn!("ignoring a second hello"),
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
