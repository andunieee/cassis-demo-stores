//! The casino: a ticket costs [`TICKET_SATS`] and goes into the pot, and
//! the moment it is paid it has a [`WIN_PERCENT`]% chance of winning
//! [`PRIZE_PERCENT`]% of the pot, ticket included. What is left of the pot
//! starts the next one. A prize is owed to the session that won it until
//! its wallet answers an `invoicerequest` and the payment goes through, so
//! a winner who left, or whose payment failed, claims it later.

use crate::cop::{request_invoice, request_payment, Flow, Out, Service, Sessions};
use crate::history::{Deed, History, Log};
use async_trait::async_trait;
use cassis_core::{unix_now, Invoice};
use hypereffect::vtree::{self, button, card, column, notice, row, text};
use hypereffect::{Action, Handler};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tracing::{info, warn};

const TICKET_SATS: u64 = 100;
const WIN_PERCENT: u64 = 10;
const PRIZE_PERCENT: u64 = 90;

/// What `casino.json` holds.
#[derive(Default, Serialize, Deserialize)]
struct Saved {
    pot: u64,
    tickets: u64,
    wins: u64,
    /// Prizes not paid yet, by session token.
    owed: BTreeMap<String, u64>,
}

pub struct Casino {
    node: cassis_wallet::Node,
    path: PathBuf,
    saved: Mutex<Saved>,
    changed: broadcast::Sender<()>,
    sessions: Sessions<State>,
}

impl Casino {
    pub fn new(node: cassis_wallet::Node, path: PathBuf, history: History) -> Result<Arc<Self>, String> {
        let saved = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| format!("{}: {e}", path.display()))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Saved::default(),
            Err(error) => return Err(format!("read {}: {error}", path.display())),
        };
        Ok(Arc::new(Self {
            node,
            path,
            saved: Mutex::new(saved),
            changed: broadcast::channel(16).0,
            sessions: Sessions::new(history, "casino"),
        }))
    }

    fn saved(&self) -> std::sync::MutexGuard<'_, Saved> {
        self.saved.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn pot(&self) -> u64 {
        self.saved().pot
    }

    fn owed(&self, session: &str) -> u64 {
        self.saved().owed.get(session).copied().unwrap_or(0)
    }

    /// Put a paid ticket of `session` in the pot and draw it: the prize,
    /// if it won, which is then owed to `session`.
    fn draw(&self, session: &str) -> Option<u64> {
        let won = roll() < WIN_PERCENT;
        let mut prize = None;
        self.change(|saved| {
            saved.pot += TICKET_SATS;
            saved.tickets += 1;
            if won {
                let sats = saved.pot * PRIZE_PERCENT / 100;
                saved.pot -= sats;
                saved.wins += 1;
                *saved.owed.entry(session.to_string()).or_default() += sats;
                prize = Some(sats);
            }
        });
        prize
    }

    /// Take what is owed to `session` if it is exactly `sats`, to pay it.
    fn take(&self, session: &str, sats: u64) -> bool {
        let mut taken = false;
        self.change(|saved| {
            if saved.owed.get(session) == Some(&sats) {
                saved.owed.remove(session);
                taken = true;
            }
        });
        taken
    }

    /// Owe back what [`Casino::take`] took, as it was not paid.
    fn give_back(&self, session: &str, sats: u64) {
        self.change(|saved| *saved.owed.entry(session.to_string()).or_default() += sats);
    }

    fn change(&self, change: impl FnOnce(&mut Saved)) {
        let json = {
            let mut saved = self.saved();
            change(&mut saved);
            serde_json::to_vec(&*saved).expect("serialize the casino")
        };
        let tmp = self.path.with_extension("tmp");
        if let Err(error) = std::fs::write(&tmp, json).and_then(|_| std::fs::rename(&tmp, &self.path)) {
            tracing::error!("save {}: {error}", self.path.display());
        }
        let _ = self.changed.send(());
    }
}

/// A uniform number below 100.
fn roll() -> u64 {
    let mut bytes = [0; 8];
    getrandom::fill(&mut bytes).expect("the system has randomness");
    // 2^64 is not a multiple of 100, but the bias is below 1 in 10^17.
    u64::from_le_bytes(bytes) % 100
}

#[async_trait]
impl Service for Casino {
    fn name(&self) -> &'static str {
        "Casino"
    }

    fn description(&self) -> &'static str {
        "100 sats a ticket, 10% chance to win 90% of the pot"
    }

    fn preview(&self) -> String {
        let saved = self.saved();
        format!(
            "<h2>Pot: {} sats</h2><p class=\"muted\">{} ticket(s), {} win(s).</p>",
            saved.pot, saved.tickets, saved.wins
        )
    }

    async fn open(
        self: Arc<Self>,
        session: String,
        force_resume: bool,
        out: Out,
    ) -> Result<Option<Arc<dyn Flow>>, String> {
        let Some((state, log)) = self.sessions.open(session.clone(), force_resume) else {
            return Ok(None);
        };
        {
            // An invoice asked for on a connection that is gone never comes.
            let mut state = state.lock().unwrap_or_else(|e| e.into_inner());
            if state.asked.take().is_some() {
                state.status = Status::None;
            }
        }
        let flow = Arc::new(CasinoFlow {
            service: self.clone(),
            out: out.clone(),
            token: session,
            state,
            log,
        });
        flow.redraw().await?;
        let mut changed = self.changed.subscribe();
        let watcher = flow.clone();
        out.spawn(async move {
            while !matches!(changed.recv().await, Err(broadcast::error::RecvError::Closed)) {
                if watcher.redraw().await.is_err() {
                    return;
                }
            }
        });
        Ok(Some(flow))
    }
}

/// What the player is told about their last ticket or prize.
#[derive(Default)]
enum Status {
    #[default]
    None,
    Lost,
    /// Won this many sats, and is being asked for an invoice.
    Won(u64),
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

struct CasinoFlow {
    service: Arc<Casino>,
    out: Out,
    /// The client's session token, which prizes are owed to.
    token: String,
    state: Arc<Mutex<State>>,
    log: Log,
}

impl CasinoFlow {
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
        let pot = self.service.pot();
        let owed = self.service.owed(&self.token);
        let page = page(pot, owed, &self.state().status, self.log.card());
        self.out.redraw(page).await
    }
}

/// Ask the wallet of a flow for an invoice of the `sats` owed to it.
async fn claim(node: &cassis_wallet::Node, out: &Out, state: &Mutex<State>, sats: u64) -> Result<(), String> {
    state.lock().unwrap_or_else(|e| e.into_inner()).asked = Some(sats);
    request_invoice(node, out, sats, "Casino prize").await
}

#[async_trait]
impl Flow for CasinoFlow {
    async fn action(&self, action: Action) -> Result<(), String> {
        match action.name.as_str() {
            "play" => {
                if self.service.node.network_ids().is_empty() {
                    return self.fail("No networks open.").await;
                }
                self.set(Status::None);
                let pot = self.service.pot();
                let flow = (
                    self.service.clone(),
                    self.out.clone(),
                    self.token.clone(),
                    self.state.clone(),
                    self.log.clone(),
                );
                request_payment(
                    &self.service.node,
                    &self.out,
                    TICKET_SATS,
                    format!("Casino ticket, pot {} sats", pot + TICKET_SATS),
                    async move {
                        let (service, out, token, state, log) = flow;
                        let prize = service.draw(&token);
                        log.record(TICKET_SATS, Deed::Played { won: prize.unwrap_or(0) });
                        match prize {
                            Some(sats) => {
                                info!("casino: a ticket won {sats} sats");
                                state.lock().unwrap_or_else(|e| e.into_inner()).status = Status::Won(sats);
                                let owed = service.owed(&token);
                                if let Err(error) = claim(&service.node, &out, &state, owed).await {
                                    warn!("casino: ask for a prize invoice: {error}");
                                }
                            }
                            None => state.lock().unwrap_or_else(|e| e.into_inner()).status = Status::Lost,
                        }
                        // Draw this flow even if the pot did not change.
                        let _ = service.changed.send(());
                    },
                )
                .await
            }
            "claim" => {
                let owed = self.service.owed(&self.token);
                if owed == 0 {
                    return self.redraw().await;
                }
                self.set(Status::Won(owed));
                claim(&self.service.node, &self.out, &self.state, owed).await?;
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
        if !self.service.take(&self.token, sats) {
            return self.fail("The prize changed, claim it again.").await;
        }
        self.set(Status::Sending);
        self.redraw().await?;
        info!("casino: paying a prize of {sats} sats on {from}");
        let paid = match self.service.node.pay(&invoice, &from, &[], &Default::default()).await {
            Ok(report) => report.into_result().map(|_| ()),
            Err(error) => Err(error),
        };
        match paid {
            Ok(()) => {
                self.log.record(sats, Deed::Won);
                self.set(Status::Paid(format!("Sent {sats} sats.")));
            }
            Err(error) => {
                warn!("casino: paying a prize failed: {error}");
                self.service.give_back(&self.token, sats);
                self.set(Status::Error(format!("Payment failed: {error}")));
            }
        }
        self.redraw().await
    }
}

fn page(pot: u64, owed: u64, status: &Status, history: Option<vtree::Node>) -> vtree::Node {
    // The status is always a notice, so its type stays the same as it changes.
    let status = match status {
        Status::None => None,
        Status::Lost => Some(("info", "No luck this time.".to_string())),
        Status::Won(sats) => Some(("success", format!("You won {sats} sats! Waiting for invoice…"))),
        Status::Sending => Some(("info", "Sending…".to_string())),
        Status::Paid(text) => Some(("success", text.clone())),
        Status::Error(text) => Some(("failure", text.clone())),
    };
    let claim = (owed > 0).then(|| {
        row("claim-row").justify("between").align("center").wrap(true).children([
            text("claim-hint").text(format!("{owed} sats are yours.")),
            button("claim")
                .variant("primary")
                .label(format!("Claim {owed} sats"))
                .on_press(Handler::action("claim")),
        ])
    });
    column("page")
        .children([
            column("heading").gap("none").children([
                text("title").variant("h1").text("Casino"),
                text("subtitle").variant("caption").text(format!(
                    "{TICKET_SATS} sats a ticket, {WIN_PERCENT}% chance to win {PRIZE_PERCENT}% of the pot"
                )),
            ]),
            card("pot")
                .title(format!("Pot: {pot} sats"))
                .children(claim)
                .children(status.map(|(variant, text)| notice("status").variant(variant).text(text)))
                .child(
                    row("play-actions").justify("between").align("center").wrap(true).children([
                        text("play-hint").variant("caption").text(format!(
                            "A win now pays {} sats.",
                            (pot + TICKET_SATS) * PRIZE_PERCENT / 100
                        )),
                        button("play")
                            .variant("primary")
                            .label(format!("Play ({TICKET_SATS} sats)"))
                            .on_press(Handler::action("play")),
                    ]),
                ),
        ])
        .children(history)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pages_render() {
        let log = History::in_memory().log("casino", "abc");
        log.record(TICKET_SATS, Deed::Played { won: 0 });
        log.record(90, Deed::Won);
        let statuses = [
            Status::None,
            Status::Lost,
            Status::Won(90),
            Status::Sending,
            Status::Paid("Sent 90 sats.".into()),
            Status::Error("Payment failed".into()),
        ];
        // vtree panics on an id used twice anywhere in the tree.
        for status in &statuses {
            vtree::render(&page(0, 0, status, None));
            vtree::render(&page(1000, 90, status, log.card()));
        }
    }
}
