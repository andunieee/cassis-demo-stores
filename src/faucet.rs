//! The faucet: anyone asks it for sats, it asks their wallet for an
//! invoice with an `invoicerequest` and pays it from this node. Wallets
//! are told apart by the key of the payee of the invoices they send, and
//! each may take a limited sum in all.

use crate::cop::{request_invoice, Flow, Out, Service};
use async_trait::async_trait;
use cassis_core::{unix_now, Invoice};
use hypereffect::vtree::{self, button, column, input, notice, row, text};
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
}

impl Faucet {
    pub fn new(node: cassis_wallet::Node, path: PathBuf, limits: Limits) -> Result<Arc<Self>, String> {
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
        }))
    }

    /// Fires when what a wallet has taken changes.
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.changed.subscribe()
    }

    pub fn limits(&self) -> Limits {
        self.limits
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
        "Free sats, up to a limit for each wallet"
    }

    fn preview(&self) -> String {
        let wallets = self.wallets();
        let total: u64 = wallets.iter().map(|(_, sats)| sats).sum();
        format!(
            "<h2>Faucet</h2><p>Up to {} sats a request and {} sats a wallet. \
             {total} sats given to {} wallet(s) so far.</p>",
            self.limits.request_sats,
            self.limits.wallet_sats,
            wallets.len()
        )
    }

    async fn open(self: Arc<Self>, _session: String, out: Out) -> Result<Arc<dyn Flow>, String> {
        let flow = Arc::new(FaucetFlow {
            service: self,
            out: out.clone(),
            state: Default::default(),
        });
        out.signal(json!({"faucet": {"amount": flow.service.limits.request_sats.min(10)}}))
            .await?;
        flow.redraw().await?;
        Ok(flow)
    }
}

/// What the client is told about their last attempt.
#[derive(Default)]
enum Status {
    #[default]
    None,
    /// Their wallet was asked for an invoice of this many sats.
    Asked(u64),
    Sending,
    Paid(String),
    Error(String),
}

#[derive(Default)]
struct State {
    status: Status,
    /// What the invoice the wallet answers with must ask for.
    asked: Option<u64>,
}

struct FaucetFlow {
    service: Arc<Faucet>,
    out: Out,
    state: Mutex<State>,
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
        let page = page(self.service.limits, &self.state().status);
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
                        .fail(format!("Ask for 1 to {} sats.", limits.request_sats))
                        .await;
                };
                if self.service.node.network_ids().is_empty() {
                    return self.fail("The faucet has no network open right now.").await;
                }
                {
                    let mut state = self.state();
                    state.asked = Some(sats);
                    state.status = Status::Asked(sats);
                }
                request_invoice(&self.service.node, &self.out, sats, "Faucet").await?;
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
            Err(error) => return self.fail(format!("That is not an invoice: {error}")).await,
        };
        if let Err(error) = invoice.verify() {
            return self.fail(format!("The invoice is not valid: {error}")).await;
        }
        if invoice.body.amount_msat != sats * 1000 {
            return self
                .fail(format!(
                    "The invoice asks for {} msat, not the {sats} sats requested.",
                    invoice.body.amount_msat
                ))
                .await;
        }
        if invoice.body.expires_at <= unix_now() + 30 {
            return self.fail("The invoice expires too soon.").await;
        }
        let ours = self.service.node.network_ids();
        let Some(from) = invoice
            .body
            .networks
            .iter()
            .map(|entry| entry.target.network().clone())
            .find(|network| ours.contains(network))
        else {
            return self.fail("The invoice is on none of the faucet's networks.").await;
        };

        let wallet = invoice.receiver.payee().to_string();
        if let Err(left) = self.service.reserve(&wallet, sats) {
            return self
                .fail(format!(
                    "This wallet may take {left} more sat(s), and you asked for {sats}."
                ))
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
                let taken = self.service.taken().get(&wallet).copied().unwrap_or(sats);
                let left = self.service.limits.wallet_sats.saturating_sub(taken);
                self.set(Status::Paid(format!(
                    "Sent {sats} sats. This wallet has taken {taken} sats and may take {left} more."
                )));
            }
            Err(error) => {
                warn!("faucet: paying {} failed: {error}", short(&wallet));
                self.service.release(&wallet, sats);
                self.set(Status::Error(format!("The payment failed: {error}")));
            }
        }
        self.redraw().await
    }
}

fn page(limits: Limits, status: &Status) -> vtree::Node {
    // The status is always a notice, so its type stays the same as it changes.
    let status = match status {
        Status::None => None,
        Status::Asked(sats) => Some(("info", format!("Waiting for your wallet's invoice for {sats} sats…"))),
        Status::Sending => Some(("info", "Sending…".to_string())),
        Status::Paid(text) => Some(("success", text.clone())),
        Status::Error(text) => Some(("failure", text.clone())),
    };
    column("page").children([
        text("title").variant("h1").text("Faucet"),
        text("blurb").variant("caption").text(format!(
            "Ask for up to {} sats at a time. Each wallet may take {} sats in all.",
            limits.request_sats, limits.wallet_sats
        )),
        row("ask").children([
            input("amount")
                .variant("number")
                .label(format!("Sats (1-{})", limits.request_sats))
                .bind("faucet.amount"),
            button("request")
                .variant("primary")
                .label("Request")
                .on_press(Handler::action("request")),
        ]),
    ])
    .children(status.map(|(variant, text)| notice("status").variant(variant).text(text)))
}
