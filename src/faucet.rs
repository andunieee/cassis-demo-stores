//! The faucet: anyone asks it for sats, it asks their wallet for an
//! invoice with an `invoicerequest` and pays it from this node. Wallets
//! are told apart by the key of the payee of the invoices they send, and
//! each may take a limited sum in all.

use crate::cop::{request_invoice, Flow, Icon, Out, Service, Sessions};
use crate::history::{Deed, History, Log};
use async_trait::async_trait;
use cassis_core::{unix_now, Invoice};
use hypereffect::vtree::{self, button, card, column, input, notice, row, text};
use hypereffect::{json, Action, Handler};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tracing::{info, warn};

/// What the faucet gives.
#[derive(Clone, Copy)]
pub struct Limits {
    /// The most a single request may ask for.
    pub request_sats: u64,
    /// The most a wallet may take in all.
    pub wallet_sats: u64,
}

pub struct Faucet {
    node: cassis_wallet::Node,
    path: PathBuf,
    limits: Limits,
    /// Sats taken so far, by the key of the wallet.
    taken: Mutex<BTreeMap<String, u64>>,
    changed: broadcast::Sender<()>,
    sessions: Sessions<State>,
}

impl Faucet {
    pub fn new(
        node: cassis_wallet::Node,
        path: PathBuf,
        limits: Limits,
        history: History,
    ) -> Result<Arc<Self>, String> {
        let taken = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| format!("{}: {e}", path.display()))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(error) => return Err(format!("read {}: {error}", path.display())),
        };
        Ok(Arc::new(Self {
            node,
            path,
            limits,
            taken: Mutex::new(taken),
            changed: broadcast::channel(16).0,
            sessions: Sessions::new(history, "faucet"),
        }))
    }

    /// Fires when what a wallet has taken changes.
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.changed.subscribe()
    }

    /// Every wallet that took anything, and how much, the biggest first.
    pub fn wallets(&self) -> Vec<(String, u64)> {
        let mut wallets: Vec<_> = self.taken().iter().map(|(k, v)| (k.clone(), *v)).collect();
        wallets.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        wallets
    }

    /// Let `wallet` take from the start again. Whether it had taken any.
    pub fn forget(&self, wallet: &str) -> bool {
        let mut forgotten = false;
        self.change(|taken| forgotten = taken.remove(wallet).is_some());
        forgotten
    }

    /// Let every wallet take from the start again.
    pub fn reset(&self) {
        self.change(BTreeMap::clear);
    }

    fn taken(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, u64>> {
        self.taken.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Count `sats` as taken by `wallet`, if that stays within its limit.
    /// Otherwise how much it may still take.
    fn reserve(&self, wallet: &str, sats: u64) -> Result<(), u64> {
        let mut result = Ok(());
        self.change(|taken| {
            let so_far = taken.get(wallet).copied().unwrap_or(0);
            match so_far + sats <= self.limits.wallet_sats {
                true => {
                    taken.insert(wallet.to_string(), so_far + sats);
                }
                false => result = Err(self.limits.wallet_sats.saturating_sub(so_far)),
            }
        });
        result
    }

    /// Take back what [`Faucet::reserve`] counted, as it was not sent.
    fn release(&self, wallet: &str, sats: u64) {
        self.change(|taken| {
            if let Some(so_far) = taken.get_mut(wallet) {
                *so_far = so_far.saturating_sub(sats);
                if *so_far == 0 {
                    taken.remove(wallet);
                }
            }
        });
    }

    fn change(&self, change: impl FnOnce(&mut BTreeMap<String, u64>)) {
        let json = {
            let mut taken = self.taken();
            change(&mut taken);
            serde_json::to_vec(&*taken).expect("serialize what was taken")
        };
        let tmp = self.path.with_extension("tmp");
        if let Err(error) = std::fs::write(&tmp, json).and_then(|_| std::fs::rename(&tmp, &self.path)) {
            tracing::error!("save {}: {error}", self.path.display());
        }
        let _ = self.changed.send(());
    }
}

/// A wallet's key, shortened to be read.
pub fn short(wallet: &str) -> String {
    match wallet.len() > 16 {
        true => format!("{}…{}", &wallet[..8], &wallet[wallet.len() - 8..]),
        false => wallet.to_string(),
    }
}

#[async_trait]
impl Service for Faucet {
    fn name(&self) -> &'static str {
        "Faucet"
    }

    fn description(&self) -> &'static str {
        "Free test sats"
    }

    fn icon(&self) -> Icon {
        Icon {
            top: "#4dabf7",
            bottom: "#1864ab",
            shapes: "\
                <rect x=\"35\" y=\"16\" width=\"30\" height=\"8\" rx=\"4\" fill=\"#fff\"/>\
                <rect x=\"46\" y=\"22\" width=\"8\" height=\"12\" fill=\"#dee2e6\"/>\
                <rect x=\"12\" y=\"32\" width=\"10\" height=\"22\" rx=\"3\" fill=\"#ced4da\"/>\
                <path d=\"M20 35H60a16 16 0 0 1 16 16v7H62v-5a3 3 0 0 0-3-3H20z\" fill=\"#fff\"/>\
                <path d=\"M69 64c-5 7-8 11.5-8 15a8 8 0 0 0 16 0c0-3.5-3-8-8-15z\" fill=\"#99e9f2\"/>\
                <path d=\"M65.5 79a3.5 3.5 0 0 0 3.5 3.5\" fill=\"none\" stroke=\"#fff\" \
                stroke-width=\"2\" stroke-linecap=\"round\"/>",
        }
    }

    fn preview(&self) -> String {
        let wallets = self.wallets();
        let total: u64 = wallets.iter().map(|(_, sats)| sats).sum();
        format!("<p>{total} sats given to {} wallet(s).</p>", wallets.len())
    }

    async fn open(
        self: Arc<Self>,
        session: String,
        force_resume: bool,
        out: Out,
    ) -> Result<Option<Arc<dyn Flow>>, String> {
        let Some((state, log)) = self.sessions.open(session, force_resume) else {
            return Ok(None);
        };
        let amount = {
            let mut state = state.lock().unwrap_or_else(|e| e.into_inner());
            if state.amount == 0 {
                state.amount = self.limits.request_sats.min(10);
            }
            if matches!(state.status, Status::Asked) {
                state.status = Status::None;
                state.asked = None;
            }
            state.amount
        };
        let flow = Arc::new(FaucetFlow {
            service: self,
            out: out.clone(),
            state,
            log,
        });
        out.signal(json!({"faucet": {"amount": amount}})).await?;
        flow.redraw().await?;
        Ok(Some(flow))
    }
}

/// What the client is told about their last attempt.
#[derive(Default)]
enum Status {
    #[default]
    None,
    Asked,
    Sending,
    Paid(String),
    Error(String),
}

#[derive(Default)]
struct State {
    amount: u64,
    status: Status,
    /// What the invoice the wallet answers with must ask for.
    asked: Option<u64>,
}

struct FaucetFlow {
    service: Arc<Faucet>,
    out: Out,
    state: Arc<Mutex<State>>,
    log: Log,
}

impl FaucetFlow {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn set(&self, status: Status) {
        self.state().status = status;
    }

    async fn fail(&self, error: impl Into<String>) -> Result<(), String> {
        self.set(Status::Error(error.into()));
        self.redraw().await
    }

    async fn redraw(&self) -> Result<(), String> {
        let page = page(self.service.limits, &self.state().status, self.log.card());
        self.out.redraw(page).await
    }
}

#[async_trait]
impl Flow for FaucetFlow {
    async fn action(&self, action: Action) -> Result<(), String> {
        match action.name.as_str() {
            "request" => {
                let limits = self.service.limits;
                let sats = match action.signal("faucet.amount") {
                    Some(hypereffect::Value::String(text)) => text.trim().parse().ok(),
                    Some(hypereffect::Value::Number(number)) => number.as_u64(),
                    _ => None,
                };
                let Some(sats) = sats.filter(|sats| (1..=limits.request_sats).contains(sats)) else {
                    return self
                        .fail(format!("1 to {} sats.", limits.request_sats))
                        .await;
                };
                if self.service.node.network_ids().is_empty() {
                    return self.fail("No networks open.").await;
                }
                {
                    let mut state = self.state();
                    state.amount = sats;
                    state.asked = Some(sats);
                    state.status = Status::Asked;
                }
                request_invoice(&self.out, sats, "Faucet").await?;
                self.redraw().await
            }
            other => {
                warn!("unknown action {other}");
                Ok(())
            }
        }
    }

    async fn invoice(&self, invoice: hypereffect::Value) -> Result<(), String> {
        let Some(sats) = self.state().asked.take() else {
            warn!("ignoring an invoice nobody asked for");
            return Ok(());
        };
        let invoice: Invoice = match serde_json::from_value(invoice) {
            Ok(invoice) => invoice,
            Err(error) => return self.fail(format!("Invalid invoice: {error}")).await,
        };
        if let Err(error) = invoice.verify() {
            return self.fail(format!("Invalid invoice: {error}")).await;
        }
        if invoice.body.amount_msat != sats * 1000 {
            return self.fail("Invoice amount does not match.").await;
        }
        if invoice.body.expires_at <= unix_now() + 30 {
            return self.fail("Invoice expires too soon.").await;
        }
        let ours = self.service.node.network_ids();
        let Some(from) = invoice
            .body
            .networks
            .iter()
            .map(|entry| entry.target.network().clone())
            .find(|network| ours.contains(network))
        else {
            return self.fail("No shared network.").await;
        };

        let wallet = invoice.receiver.payee().to_string();
        if let Err(left) = self.service.reserve(&wallet, sats) {
            return self
                .fail(format!("{left} sat(s) remaining for this wallet."))
                .await;
        }
        self.set(Status::Sending);
        self.redraw().await?;
        info!("faucet: paying {sats} sats to {} on {from}", short(&wallet));
        let paid = match self.service.node.pay(&invoice, &from, &[], &Default::default()).await {
            Ok(report) => report.into_result().map(|_| ()),
            Err(error) => Err(error),
        };
        match paid {
            Ok(()) => {
                self.log.record(sats, Deed::Received);
                let taken = self.service.taken().get(&wallet).copied().unwrap_or(sats);
                let left = self.service.limits.wallet_sats.saturating_sub(taken);
                self.set(Status::Paid(format!("Sent {sats} sats ({left} left).")));
            }
            Err(error) => {
                warn!("faucet: paying {} failed: {error}", short(&wallet));
                self.service.release(&wallet, sats);
                self.set(Status::Error(format!("Payment failed: {error}")));
            }
        }
        self.redraw().await
    }
}

fn page(limits: Limits, status: &Status, history: Option<vtree::Node>) -> vtree::Node {
    // The status is always a notice, so its type stays the same as it changes.
    let status = match status {
        Status::None => None,
        Status::Asked => Some(("info", "Waiting for invoice…".to_string())),
        Status::Sending => Some(("info", "Sending…".to_string())),
        Status::Paid(text) => Some(("success", text.clone())),
        Status::Error(text) => Some(("failure", text.clone())),
    };
    column("page")
        .children([
            column("heading").gap("none").children([
                text("title").variant("h1").text("Faucet"),
                text("subtitle").variant("caption").text(format!(
                    "Free test sats: up to {} a request, {} per wallet",
                    limits.request_sats, limits.wallet_sats
                )),
            ]),
            card("ask")
                .child(
                    row("ask-row").align("end").wrap(true).children([
                        input("amount")
                            .variant("number")
                            .label(format!("Sats (1-{})", limits.request_sats))
                            .bind("faucet.amount"),
                        button("request")
                            .variant("primary")
                            .label("Request")
                            .on_press(Handler::action("request")),
                    ]),
                )
                .children(status.map(|(variant, text)| notice("status").variant(variant).text(text))),
        ])
        .children(history)
}
