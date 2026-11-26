//! Compute: a sat buys a credit, and a credit buys one calculation done
//! on this server. The calculator is a plain four-function one; every
//! operation it carries out, with `=` or by chaining operators, costs
//! [`CREDITS_PER_CALCULATION`]. Balances are kept by session token.

use crate::cop::{min_invoice_sats, request_payment, unpayable, Flow, Icon, Out, Service, Sessions};
use crate::history::{Deed, History, Log};
use async_trait::async_trait;
use hypereffect::vtree::{self, button, card, column, input, notice, row, text};
use hypereffect::{json, Action, Handler};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tracing::{info, warn};

const CREDITS_PER_CALCULATION: u64 = 1;
/// The most sats one payment may buy credits with, so the msat amount
/// cannot overflow.
const MAX_PURCHASE_SATS: u64 = 100_000_000;
const DEFAULT_PURCHASE_SATS: u64 = 100;
/// The most characters a number being typed may have.
const MAX_ENTRY: usize = 16;

/// What `compute.json` holds.
#[derive(Default, Serialize, Deserialize)]
struct Saved {
    /// Credits left, by session token.
    credits: BTreeMap<String, u64>,
    sold: u64,
    calculations: u64,
}

pub struct Compute {
    node: cassis_wallet::Node,
    path: PathBuf,
    saved: Mutex<Saved>,
    changed: broadcast::Sender<()>,
    sessions: Sessions<State>,
}

impl Compute {
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
            sessions: Sessions::new(history, "compute"),
        }))
    }

    /// Draw every flow again, as for a change of the networks that pay.
    pub fn refresh(&self) {
        let _ = self.changed.send(());
    }

    fn saved(&self) -> std::sync::MutexGuard<'_, Saved> {
        self.saved.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn credits(&self, session: &str) -> u64 {
        self.saved().credits.get(session).copied().unwrap_or(0)
    }

    /// Add `credits` bought by `session`.
    fn credit(&self, session: &str, credits: u64) {
        self.change(|saved| {
            *saved.credits.entry(session.to_string()).or_default() += credits;
            saved.sold += credits;
        });
    }

    /// Take the price of one calculation from `session`, if it has it.
    fn spend(&self, session: &str) -> bool {
        let mut spent = false;
        self.change(|saved| {
            if let Some(credits) = saved.credits.get_mut(session) {
                if *credits >= CREDITS_PER_CALCULATION {
                    *credits -= CREDITS_PER_CALCULATION;
                    saved.calculations += 1;
                    spent = true;
                }
            }
        });
        spent
    }

    fn change(&self, change: impl FnOnce(&mut Saved)) {
        let json = {
            let mut saved = self.saved();
            change(&mut saved);
            serde_json::to_vec(&*saved).expect("serialize the compute balances")
        };
        let tmp = self.path.with_extension("tmp");
        if let Err(error) = std::fs::write(&tmp, json).and_then(|_| std::fs::rename(&tmp, &self.path)) {
            tracing::error!("save {}: {error}", self.path.display());
        }
        let _ = self.changed.send(());
    }
}

#[async_trait]
impl Service for Compute {
    fn name(&self) -> &'static str {
        "Compute"
    }

    fn description(&self) -> &'static str {
        "Server-grade arithmetic: 1 sat a credit, 1 credit a calculation"
    }

    fn icon(&self) -> Icon {
        Icon {
            top: "#74c0fc",
            bottom: "#1864ab",
            shapes: "\
                <g fill=\"#d0ebff\"><rect x=\"30\" y=\"12\" width=\"6\" height=\"14\" rx=\"3\"/>\
                <rect x=\"47\" y=\"12\" width=\"6\" height=\"14\" rx=\"3\"/><rect x=\"64\" y=\"12\" width=\"6\" height=\"14\" rx=\"3\"/>\
                <rect x=\"30\" y=\"74\" width=\"6\" height=\"14\" rx=\"3\"/><rect x=\"47\" y=\"74\" width=\"6\" height=\"14\" rx=\"3\"/>\
                <rect x=\"64\" y=\"74\" width=\"6\" height=\"14\" rx=\"3\"/><rect x=\"12\" y=\"30\" width=\"14\" height=\"6\" rx=\"3\"/>\
                <rect x=\"12\" y=\"47\" width=\"14\" height=\"6\" rx=\"3\"/><rect x=\"12\" y=\"64\" width=\"14\" height=\"6\" rx=\"3\"/>\
                <rect x=\"74\" y=\"30\" width=\"14\" height=\"6\" rx=\"3\"/><rect x=\"74\" y=\"47\" width=\"14\" height=\"6\" rx=\"3\"/>\
                <rect x=\"74\" y=\"64\" width=\"14\" height=\"6\" rx=\"3\"/></g>\
                <rect x=\"22\" y=\"22\" width=\"56\" height=\"56\" rx=\"9\" fill=\"#fff\"/>\
                <rect x=\"30\" y=\"30\" width=\"40\" height=\"40\" rx=\"5\" fill=\"#1c7ed6\"/>\
                <path d=\"M36 42h12M42 36v12M53 42h11M36 58l10 0M53 55h11M53 61h11\" fill=\"none\" \
                stroke=\"#fff\" stroke-width=\"3.5\" stroke-linecap=\"round\"/>",
        }
    }

    fn preview(&self) -> String {
        let saved = self.saved();
        format!(
            "<h2>{} calculation(s) performed</h2><p class=\"muted\">{} credit(s) sold.</p>",
            saved.calculations, saved.sold
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
        let flow = Arc::new(ComputeFlow {
            service: self.clone(),
            out: out.clone(),
            token: session,
            state,
            log,
        });
        out.signal(json!({"compute": {"amount": DEFAULT_PURCHASE_SATS}})).await?;
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

/// What the user is told about their last purchase or calculation.
#[derive(Default)]
enum Status {
    #[default]
    None,
    Paid(String),
    Error(String),
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Op {
    Add,
    Sub,
    Mul,
    Div,
}

impl Op {
    fn parse(key: &str) -> Option<Self> {
        match key {
            "+" => Some(Op::Add),
            "-" => Some(Op::Sub),
            "*" => Some(Op::Mul),
            "/" => Some(Op::Div),
            _ => None,
        }
    }

    fn symbol(self) -> &'static str {
        match self {
            Op::Add => "+",
            Op::Sub => "−",
            Op::Mul => "×",
            Op::Div => "÷",
        }
    }

    fn apply(self, a: f64, b: f64) -> Result<f64, &'static str> {
        let result = match self {
            Op::Add => a + b,
            Op::Sub => a - b,
            Op::Mul => a * b,
            Op::Div if b == 0.0 => return Err("Cannot divide by zero."),
            Op::Div => a / b,
        };
        if result.is_finite() {
            Ok(result)
        } else {
            Err("Overflow.")
        }
    }
}

/// A four-function calculator that executes each operation as it is
/// entered, like a pocket one.
#[derive(Default)]
struct Calculator {
    /// The result so far, or the first operand.
    acc: Option<f64>,
    op: Option<Op>,
    /// The number being typed.
    entry: String,
}

/// What a key asks of the calculator.
#[derive(PartialEq, Debug)]
enum Press {
    /// Nothing to compute: the key only edited the calculator.
    Edited,
    /// `acc op entry` has to be computed, and paid for, first.
    Compute(f64, Op, f64),
}

impl Calculator {
    fn is_clear(&self) -> bool {
        self.acc.is_none() && self.op.is_none() && self.entry.is_empty()
    }

    fn display(&self) -> String {
        if !self.entry.is_empty() {
            self.entry.clone()
        } else {
            format_number(self.acc.unwrap_or(0.0))
        }
    }

    /// What is pending, such as `12 ×`.
    fn expression(&self) -> String {
        match (self.acc, self.op) {
            (Some(acc), Some(op)) => format!("{} {}", format_number(acc), op.symbol()),
            _ => String::new(),
        }
    }

    fn digit(&mut self, key: &str) {
        // A digit after `=` starts a new calculation.
        if self.op.is_none() && self.entry.is_empty() {
            self.acc = None;
        }
        if self.entry.len() >= MAX_ENTRY || (key == "." && self.entry.contains('.')) {
            return;
        }
        match (self.entry.as_str(), key) {
            ("", ".") => self.entry.push_str("0."),
            ("0", k) if k != "." => self.entry = k.to_string(),
            _ => self.entry.push_str(key),
        }
    }

    /// The typed number, which is valid as the keys only type digits and
    /// one point.
    fn operand(&self) -> f64 {
        self.entry.parse().unwrap_or(0.0)
    }

    /// Press `key`: what it asks for, or `None` for a key that is not one.
    fn press(&mut self, key: &str) -> Option<Press> {
        if key.len() == 1 && (key.as_bytes()[0].is_ascii_digit() || key == ".") {
            self.digit(key);
            return Some(Press::Edited);
        }
        if key == "C" {
            *self = Calculator::default();
            return Some(Press::Edited);
        }
        if let Some(op) = Op::parse(key) {
            if self.entry.is_empty() {
                // Change the operator, or start from the result or zero.
                self.acc.get_or_insert(0.0);
                self.op = Some(op);
                return Some(Press::Edited);
            }
            return Some(match (self.acc, self.op) {
                (Some(acc), Some(pending)) => Press::Compute(acc, pending, self.operand()),
                _ => {
                    self.acc = Some(self.operand());
                    self.entry.clear();
                    self.op = Some(op);
                    Press::Edited
                }
            });
        }
        if key == "=" {
            return Some(match (self.acc, self.op, self.entry.is_empty()) {
                (Some(acc), Some(op), false) => Press::Compute(acc, op, self.operand()),
                _ => Press::Edited,
            });
        }
        None
    }

    /// Take the `result` of the computation `key` asked for: `key` is the
    /// operator to go on with, or `=`.
    fn computed(&mut self, result: f64, key: &str) {
        self.acc = Some(result);
        self.entry.clear();
        self.op = Op::parse(key);
    }
}

/// `number` as a calculator shows it.
fn format_number(number: f64) -> String {
    if number == 0.0 {
        // Also -0.
        "0".into()
    } else if number.abs() >= 1e16 || number.abs() < 1e-9 {
        format!("{number:e}")
    } else {
        format!("{number}")
    }
}

#[derive(Default)]
struct State {
    status: Status,
    calculator: Calculator,
}

struct ComputeFlow {
    service: Arc<Compute>,
    out: Out,
    /// The client's session token, which credits belong to.
    token: String,
    state: Arc<Mutex<State>>,
    log: Log,
}

impl ComputeFlow {
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
        let credits = self.service.credits(&self.token);
        let min = min_invoice_sats(&self.service.node);
        let page = {
            let state = self.state();
            page(credits, &state.calculator, &state.status, min, self.log.card())
        };
        self.out.redraw(page).await
    }

    async fn buy(&self, action: &Action) -> Result<(), String> {
        let sats = match action.signal("compute.amount") {
            Some(hypereffect::Value::String(text)) => text.trim().parse().ok(),
            Some(hypereffect::Value::Number(number)) => number.as_u64(),
            _ => None,
        };
        let min = min_invoice_sats(&self.service.node);
        let Some(min) = min else {
            return self.fail("No networks open.").await;
        };
        let Some(sats) = sats.filter(|sats| (min.max(1)..=MAX_PURCHASE_SATS).contains(sats)) else {
            return self
                .fail(format!("{} to {MAX_PURCHASE_SATS} sats.", min.max(1)))
                .await;
        };
        self.set(Status::None);
        self.redraw().await?;
        let flow = (self.service.clone(), self.token.clone(), self.state.clone(), self.log.clone());
        request_payment(
            &self.service.node,
            &self.out,
            sats,
            format!("{sats} compute credits"),
            async move {
                let (service, token, state, log) = flow;
                log.record(sats, Deed::Credited);
                state.lock().unwrap_or_else(|e| e.into_inner()).status =
                    Status::Paid(format!("Added {sats} credits."));
                service.credit(&token, sats);
                info!("compute: {sats} credits bought");
            },
        )
        .await
    }

    async fn key(&self, key: &str) -> Result<(), String> {
        let press = {
            let mut state = self.state();
            state.status = Status::None;
            state.calculator.press(key)
        };
        match press {
            None => warn!("unknown calculator key {key:?}"),
            Some(Press::Edited) => {}
            Some(Press::Compute(a, op, b)) => match op.apply(a, b) {
                Err(error) => self.set(Status::Error(error.into())),
                Ok(_) if !self.service.spend(&self.token) => {
                    self.set(Status::Error("Out of credits: buy some more to compute.".into()))
                }
                Ok(result) => self.state().calculator.computed(result, key),
            },
        }
        self.redraw().await
    }
}

#[async_trait]
impl Flow for ComputeFlow {
    async fn action(&self, action: Action) -> Result<(), String> {
        match action.name.as_str() {
            "buy" => self.buy(&action).await,
            "key" => {
                let key = action.payload_as::<String>().unwrap_or_default();
                self.key(&key).await
            }
            other => {
                warn!("unknown action {other}");
                Ok(())
            }
        }
    }
}

/// The calculator's keys by row: what each sends, its label and its id.
const KEYS: [[(&str, &str, &str); 4]; 4] = [
    [("7", "7", "7"), ("8", "8", "8"), ("9", "9", "9"), ("/", "÷", "div")],
    [("4", "4", "4"), ("5", "5", "5"), ("6", "6", "6"), ("*", "×", "mul")],
    [("1", "1", "1"), ("2", "2", "2"), ("3", "3", "3"), ("-", "−", "sub")],
    [("0", "0", "0"), (".", ".", "dot"), ("=", "=", "eq"), ("+", "+", "add")],
];

fn page(
    credits: u64,
    calculator: &Calculator,
    status: &Status,
    min: Option<u64>,
    history: Option<vtree::Node>,
) -> vtree::Node {
    let status = match status {
        Status::None => None,
        Status::Paid(text) => Some(notice("status").variant("success").text(text.as_str())),
        Status::Error(text) => Some(notice("status").variant("failure").text(text.as_str())),
    };
    let unpayable = unpayable(min, 1);
    let buy_hint = unpayable.clone().or_else(|| {
        min.filter(|min| *min > 1)
            .map(|min| format!("Payments start at {min} sats."))
    });
    // Shown once there are credits, and kept while a calculation is on
    // screen, so running out does not take its result away.
    let calculator = (credits > 0 || !calculator.is_clear()).then(|| {
        card("calculator")
            .title("Compute")
            .child(
                column("screen").gap("none").align("end").children([
                    text("expression").variant("caption").text(calculator.expression()),
                    text("display").variant("h1").text(calculator.display()),
                ]),
            )
            .children(KEYS.iter().enumerate().map(|(i, keys)| {
                row(format!("keys-{i}")).justify("between").children(keys.iter().map(|(key, label, id)| {
                    button(format!("key-{id}"))
                        .variant(match *key {
                            "=" => "primary",
                            "+" | "-" | "*" | "/" => "secondary",
                            _ => "plain",
                        })
                        .label(*label)
                        .on_press(Handler::action("key").payload(*key))
                }))
            }))
            .child(
                row("calculator-actions").justify("between").align("center").children([
                    text("calculator-hint")
                        .variant("caption")
                        .text(format!("Each operation uses {CREDITS_PER_CALCULATION} credit.")),
                    button("key-clear")
                        .variant("failure")
                        .label("C")
                        .on_press(Handler::action("key").payload("C")),
                ]),
            )
    });
    column("page")
        .children([
            column("heading").gap("none").children([
                text("title").variant("h1").text("Compute"),
                text("subtitle")
                    .variant("caption")
                    .text("Arithmetic performed on our servers. 1 sat buys 1 credit, 1 credit buys 1 calculation."),
            ]),
            card("balance")
                .title(format!("Balance: {credits} credit{}", if credits == 1 { "" } else { "s" }))
                .child(
                    input("amount")
                        .variant("number")
                        .label("Credits to add (sats)")
                        .bind("compute.amount"),
                )
                .children(status)
                .child(
                    row("buy-actions")
                        .justify("end")
                        .align("center")
                        .wrap(true)
                        .children(
                            buy_hint.map(|why| text("buy-hint").variant("caption").text(why)),
                        )
                        .child(
                            button("buy")
                                .variant("primary")
                                .label("Add credits")
                                .disabled(unpayable.is_some())
                                .on_press(Handler::action("buy")),
                        ),
                ),
        ])
        .children(calculator)
        .children(history)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn press_all(calculator: &mut Calculator, keys: &str) -> Vec<Press> {
        let mut computed = Vec::new();
        for key in keys.chars().map(String::from) {
            match calculator.press(&key).expect("a key") {
                Press::Edited => {}
                Press::Compute(a, op, b) => {
                    computed.push(Press::Compute(a, op, b));
                    calculator.computed(op.apply(a, b).unwrap(), &key);
                }
            }
        }
        computed
    }

    #[test]
    fn calculates_like_a_pocket_calculator() {
        let mut calculator = Calculator::default();
        assert_eq!(calculator.display(), "0");
        let computed = press_all(&mut calculator, "12+3*2=");
        assert_eq!(computed, [Press::Compute(12.0, Op::Add, 3.0), Press::Compute(15.0, Op::Mul, 2.0)]);
        assert_eq!(calculator.display(), "30");
        // Going on from the result, then starting over with a digit.
        assert_eq!(press_all(&mut calculator, "/4=").len(), 1);
        assert_eq!(calculator.display(), "7.5");
        assert!(press_all(&mut calculator, "9").is_empty());
        assert_eq!(calculator.display(), "9");
        assert_eq!(calculator.acc, None);
        // Operators pressed twice, or `=` with nothing to do, cost nothing.
        assert!(press_all(&mut calculator, "+-==").is_empty());
        assert_eq!(calculator.expression(), "9 −");
        assert!(press_all(&mut calculator, "C").is_empty());
        assert!(calculator.is_clear());
        assert!(press_all(&mut calculator, ".5.5").is_empty());
        assert_eq!(calculator.display(), "0.55");
        assert_eq!(calculator.press("sqrt"), None);
        assert_eq!(Op::Div.apply(1.0, 0.0), Err("Cannot divide by zero."));
    }

    #[test]
    fn pages_render() {
        let log = History::in_memory().log("compute", "abc");
        log.record(100, Deed::Credited);
        let mut busy = Calculator::default();
        press_all(&mut busy, "12+");
        let statuses = [Status::None, Status::Paid("Added 100 credits.".into()), Status::Error("Out of credits".into())];
        // vtree panics on an id used twice anywhere in the tree.
        for status in &statuses {
            for min in [None, Some(1), Some(475)] {
                vtree::render(&page(0, &Calculator::default(), status, min, None));
                vtree::render(&page(5, &busy, status, min, log.card()));
            }
        }
    }
}
