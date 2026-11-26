//! The admin interface: a hyperb app over iroh, at `hyperb://<id>/admin`,
//! for the operators of this server, which can take texts off the wall,
//! wipe the wall and the picture, see what the faucet gave and let wallets
//! take from it again, and manage the wallets that get paid
//! (see `hyperb-cassis-wallet`).
//!
//! Only the keys listed in `operators.json` in the data directory are let
//! in. At every start a pairing link with a one-time secret is printed on
//! the console: the key that opens it becomes an operator. An operator can
//! let other keys in, and take them out again, from the Operators tab.

use crate::cop::{Icon, Out};
use crate::faucet::{short, Faucet};
use crate::pixels::Pixels;
use crate::wall::Wall;
use cassis_core::NetworkId;
use cassis_wallet::Node;
use hyperb_cassis_wallet::{Drafts, Host, WalletAdmin};
use hypereffect::vtree::{self, button, card, column, image, input, list, notice, row, tab, tabs, text};
use hypereffect::{json, Action, Handler, Metadata, RendererMessage, ServerMessage};
use iroh::endpoint::{Connection, RecvStream};
use iroh::Endpoint;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tracing::{info, warn};

/// How long a connection that is not an operator yet has to say hello.
const HELLO_TIMEOUT: Duration = Duration::from_secs(30);
/// How often the page is drawn again, for what no change announces.
const REFRESH: Duration = Duration::from_secs(5);
const CLOSE_NOT_ALLOWED: u32 = 403;

/// Gives the wallets' networks to the node that is paid on them, and
/// fires `changed` when they change, since what can be paid changes too.
pub struct NodeHost {
    pub node: Node,
    pub changed: tokio::sync::broadcast::Sender<()>,
}

impl Host for NodeHost {
    fn opened(&self, network: Arc<dyn cassis_core::Network>) {
        self.node.add_network(network);
        let _ = self.changed.send(());
    }

    fn closed(&self, id: &NetworkId) {
        self.node.remove_network(id);
        let _ = self.changed.send(());
    }
}

/// The keys that may use the interface, kept in a file.
struct Operators {
    path: PathBuf,
    keys: RwLock<Vec<String>>,
}

impl Operators {
    fn load(path: PathBuf) -> Result<Self, String> {
        let keys = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<Vec<String>>(&bytes)
                .map_err(|e| format!("{}: {e}", path.display()))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(format!("read {}: {error}", path.display())),
        };
        let mut valid = Vec::new();
        for key in keys {
            match operator_key(&key) {
                Ok(key) if !valid.contains(&key) => valid.push(key),
                Ok(_) => {}
                Err(error) => warn!("ignoring operator key {key:?} in {}: {error}", path.display()),
            }
        }
        Ok(Self { path, keys: RwLock::new(valid) })
    }

    fn any(&self) -> bool {
        !self.keys.read().unwrap_or_else(|e| e.into_inner()).is_empty()
    }

    fn has(&self, key: &str) -> bool {
        self.keys.read().unwrap_or_else(|e| e.into_inner()).iter().any(|k| k == key)
    }

    fn list(&self) -> Vec<String> {
        self.keys.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Let `key` in. Whether it was not one already.
    fn add(&self, key: &str) -> Result<bool, String> {
        let key = operator_key(key)?;
        let mut keys = self.keys.write().unwrap_or_else(|e| e.into_inner());
        if keys.contains(&key) {
            return Ok(false);
        }
        let mut next = keys.clone();
        next.push(key);
        self.save(&next)?;
        *keys = next;
        Ok(true)
    }

    /// Let `key` no longer in, unless it is `by` (the one asking). Whether
    /// it was one.
    fn remove(&self, key: &str, by: &str) -> Result<bool, String> {
        let key = operator_key(key)?;
        if key == operator_key(by)? {
            return Err("You cannot remove your own key".into());
        }
        let mut keys = self.keys.write().unwrap_or_else(|e| e.into_inner());
        if !keys.contains(&key) {
            return Ok(false);
        }
        let next: Vec<String> = keys.iter().filter(|k| **k != key).cloned().collect();
        self.save(&next)?;
        *keys = next;
        Ok(true)
    }

    fn save(&self, keys: &[String]) -> Result<(), String> {
        let json = serde_json::to_vec_pretty(keys).expect("serialize the operators");
        std::fs::write(&self.path, json)
            .map_err(|e| format!("couldn't save {}: {e}", self.path.display()))
    }
}

/// An operator key as it is kept: the lowercase hex of an iroh endpoint's key.
fn operator_key(key: &str) -> Result<String, String> {
    key.trim()
        .to_lowercase()
        .parse::<iroh::PublicKey>()
        .map(|key| key.to_string())
        .map_err(|_| "an operator key is the key of an iroh endpoint".into())
}

pub struct Admin {
    wall: Arc<Wall>,
    pixels: Arc<Pixels>,
    faucet: Arc<Faucet>,
    wallets: Arc<WalletAdmin>,
    operators: Operators,
    /// The secret of this start's pairing link, until a key uses it.
    setup: Mutex<Option<String>>,
}

impl Admin {
    pub fn new(
        data: &std::path::Path,
        wall: Arc<Wall>,
        pixels: Arc<Pixels>,
        faucet: Arc<Faucet>,
        wallets: Arc<WalletAdmin>,
    ) -> Result<Arc<Self>, String> {
        let operators = Operators::load(data.join("operators.json"))?;
        // A fresh pairing link at every start: whoever can read the console
        // may let one more key in, paired or not.
        let secret = Sha256::digest(cassis_core::keys::generate_mnemonic());
        let setup = secret.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
        Ok(Arc::new(Self {
            wall,
            pixels,
            faucet,
            wallets,
            operators,
            setup: Mutex::new(Some(setup)),
        }))
    }

    /// Tell the console where the interface is, and how to pair a key.
    pub fn announce(&self, endpoint: &Endpoint) {
        let address = format!("hyperb://{}/admin", endpoint.id());
        match &*self.setup.lock().unwrap_or_else(|e| e.into_inner()) {
            Some(secret) => {
                println!("admin: open this in hyperb to pair a key as an operator (it works once):");
                println!("{address}#setup={secret}");
            }
            None => println!("admin: {address}"),
        }
    }

    /// Pair `key` if `secret` is the pairing link's. Whether it did.
    fn claim(&self, key: &str, secret: &str) -> Result<bool, String> {
        let mut setup = self.setup.lock().unwrap_or_else(|e| e.into_inner());
        // Compared as hashes, so the time it takes says nothing about the secret.
        let hash = |text: &str| Sha256::digest(text.as_bytes());
        let same = setup.as_deref().is_some_and(|expected| hash(expected) == hash(secret));
        if !same {
            return Ok(false);
        }
        self.operators.add(key)?;
        *setup = None;
        info!("{key} opened the pairing link and is now an operator");
        Ok(true)
    }
}

pub async fn serve(admin: Arc<Admin>, endpoint: Endpoint) {
    while let Some(incoming) = endpoint.accept().await {
        let admin = admin.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(connection) => connect(admin, connection).await,
                Err(error) => warn!("admin connection failed: {error}"),
            }
        });
    }
}

async fn connect(admin: Arc<Admin>, connection: Connection) {
    let key = connection.remote_id().to_string().to_lowercase();
    // The handshake proved the key. An unpaired key is turned away before it
    // can say anything, unless a pairing link is still waiting to be used.
    let pairing = admin.setup.lock().unwrap_or_else(|e| e.into_inner()).is_some();
    if admin.operators.any() && !admin.operators.has(&key) && !pairing {
        warn!("the key {key} tried to open the admin interface; pair it or add it to operators.json");
        return connection.close(CLOSE_NOT_ALLOWED.into(), b"this key is not an operator");
    }
    let Ok((send, recv)) = connection.accept_bi().await else {
        return;
    };
    let mut lines = BufReader::new(recv).lines();
    let params = match tokio::time::timeout(HELLO_TIMEOUT, hello(&mut lines)).await {
        Ok(Some(params)) => params,
        _ => return connection.close(CLOSE_NOT_ALLOWED.into(), b"no hello"),
    };
    let mut result = None;
    if !admin.operators.has(&key) {
        match params.get("setup").map(|secret| admin.claim(&key, secret)) {
            Some(Ok(true)) => result = Some(Ok("Operator paired.".to_string())),
            Some(Err(error)) => result = Some(Err(error)),
            _ => {
                return connection
                    .close(CLOSE_NOT_ALLOWED.into(), b"open the pairing link printed by the server");
            }
        }
    }
    info!("admin session for {key}");
    let out = Out::new(send);
    if let Err(error) = session(admin, key.clone(), out.clone(), lines, result).await {
        warn!("admin session of {key} ended: {error}");
    }
    out.stop();
    connection.close(0u32.into(), b"bye");
}

async fn hello(lines: &mut Lines<BufReader<RecvStream>>) -> Option<HashMap<String, String>> {
    let line = lines.next_line().await.ok()??;
    match RendererMessage::parse(&line) {
        Ok(RendererMessage::Hello(body)) => Some(
            body.get("params")
                .and_then(|p| serde_json::from_value(p.clone()).ok())
                .unwrap_or_default(),
        ),
        _ => None,
    }
}

/// What a session shows besides what the server holds.
#[derive(Default)]
struct View {
    /// What the last action did.
    result: Option<Result<String, String>>,
    /// The wipe waiting to be confirmed.
    confirm: Option<Wipe>,
    drafts: Drafts,
}

#[derive(Clone, Copy, PartialEq)]
enum Wipe {
    Wall,
    Pixels,
    Faucet,
}

struct Session {
    admin: Arc<Admin>,
    /// The key this session was opened with.
    key: String,
    out: Out,
    view: Mutex<View>,
}

/// A gear: its teeth are the dashes of a thick stroke around its body.
const ICON: Icon = Icon {
    top: "#9775fa",
    bottom: "#5f3dc4",
    shapes: "\
        <circle cx=\"50\" cy=\"50\" r=\"30\" fill=\"none\" stroke=\"#fff\" stroke-width=\"11\" \
        stroke-dasharray=\"11.78 11.78\" stroke-dashoffset=\"5.89\"/>\
        <circle cx=\"50\" cy=\"50\" r=\"27\" fill=\"#fff\"/>\
        <circle cx=\"50\" cy=\"50\" r=\"10\" fill=\"#7048e8\"/>",
};

async fn session(
    admin: Arc<Admin>,
    key: String,
    out: Out,
    mut lines: Lines<BufReader<RecvStream>>,
    result: Option<Result<String, String>>,
) -> Result<(), String> {
    let metadata = Metadata::new()
        .name("Demo stores admin")
        .description("Moderate the stores, the faucet and the wallets that get paid")
        .icon(ICON.url());
    out.send(ServerMessage::from(metadata).to_line()).await?;
    let session = Arc::new(Session {
        admin: admin.clone(),
        key,
        out: out.clone(),
        view: Mutex::new(View { result, ..View::default() }),
    });
    session.redraw().await?;

    let watcher = session.clone();
    out.spawn(async move {
        // Balances are only kept fresh while a session is open.
        let _watching = watcher.admin.wallets.watch();
        let mut wall = watcher.admin.wall.subscribe();
        let mut pixels = watcher.admin.pixels.subscribe();
        let mut faucet = watcher.admin.faucet.subscribe();
        let mut wallets = watcher.admin.wallets.updates();
        let mut tick = tokio::time::interval(REFRESH);
        loop {
            tokio::select! {
                _ = wall.recv() => {}
                _ = pixels.recv() => {}
                _ = faucet.recv() => {}
                _ = wallets.recv() => {}
                _ = tick.tick() => {}
            }
            if watcher.redraw().await.is_err() {
                return;
            }
        }
    });

    while let Some(line) = lines.next_line().await.map_err(|e| e.to_string())? {
        if line.is_empty() {
            continue;
        }
        match RendererMessage::parse(&line) {
            Ok(RendererMessage::Action(action)) => session.action(action).await?,
            Ok(RendererMessage::Hello(_)) => warn!("ignoring a second hello"),
            Err(error) => warn!("ignoring message: {error}"),
        }
    }
    Ok(())
}

impl Session {
    fn view(&self) -> std::sync::MutexGuard<'_, View> {
        self.view.lock().unwrap_or_else(|e| e.into_inner())
    }

    async fn redraw(&self) -> Result<(), String> {
        let patch = self.admin.wallets.signals(&mut self.view().drafts);
        if let Some(patch) = patch {
            self.out.signal(patch).await?;
        }
        let page = self.page();
        self.out.redraw(page).await
    }

    async fn action(&self, action: Action) -> Result<(), String> {
        if let Some(handled) = self.admin.wallets.act(&action) {
            if let Some(patch) = handled.patch {
                self.out.signal(patch).await?;
            }
            return self.redraw().await;
        }
        let result = self.run(&action);
        match &result {
            Ok(Some(message)) => info!("admin: {message}"),
            Ok(None) => {}
            Err(error) => warn!("admin: {}: {error}", action.name),
        }
        // The key that was just added is of no more use in the field.
        if action.name == "operator.add" && matches!(&result, Ok(Some(_))) {
            self.out.signal(json!({"operator": {"key": ""}})).await?;
        }
        if let Some(result) = result.transpose() {
            self.view().result = Some(result);
        }
        self.redraw().await
    }

    /// Do what `action` says, returning what happened, if it is to be said.
    fn run(&self, action: &Action) -> Result<Option<String>, String> {
        let payload = action.payload.as_ref();
        let number = |key: &str| payload.and_then(|p| p.get(key)).and_then(|v| v.as_u64());
        match action.name.as_str() {
            "wall.delete" => {
                let (Some(wall), Some(index)) = (number("number"), number("index")) else {
                    return Err("which message?".into());
                };
                let text = payload.and_then(|p| p["text"].as_str()).unwrap_or_default();
                match self.admin.wall.remove(wall as usize, index as usize, text) {
                    true => Ok(Some("Message deleted.".into())),
                    false => Err("Message not found.".into()),
                }
            }
            "wall.reset" => self.ask(Wipe::Wall),
            "pixels.reset" => self.ask(Wipe::Pixels),
            "faucet.reset" => self.ask(Wipe::Faucet),
            "faucet.forget" => {
                let wallet = payload.and_then(|p| p["wallet"].as_str()).unwrap_or_default();
                match self.admin.faucet.forget(wallet) {
                    true => Ok(Some(format!("Reset {}.", short(wallet)))),
                    false => Err("Wallet not found.".into()),
                }
            }
            "operator.add" => {
                let key = action.signal("operator.key").and_then(|v| v.as_str()).unwrap_or_default();
                match self.admin.operators.add(key) {
                    Ok(true) => {
                        let key = key.trim().to_lowercase();
                        Ok(Some(format!("Added {}.", short(&key))))
                    }
                    Ok(false) => Err("Already an operator.".into()),
                    Err(error) => Err(error),
                }
            }
            "operator.remove" => {
                let key = payload.and_then(|p| p["key"].as_str()).unwrap_or_default();
                match self.admin.operators.remove(key, &self.key) {
                    Ok(true) => Ok(Some(format!("Removed {}.", short(key)))),
                    Ok(false) => Err("Not an operator.".into()),
                    Err(error) => Err(error),
                }
            }
            "wipe.cancel" => {
                self.view().confirm = None;
                Ok(None)
            }
            "wipe.confirm" => match self.view().confirm.take() {
                Some(Wipe::Wall) => {
                    self.admin.wall.clear();
                    Ok(Some("Wall reset.".into()))
                }
                Some(Wipe::Pixels) => {
                    self.admin.pixels.reset();
                    Ok(Some("Canvas reset.".into()))
                }
                Some(Wipe::Faucet) => {
                    self.admin.faucet.reset();
                    Ok(Some("Faucet reset.".into()))
                }
                None => Err("Nothing to confirm.".into()),
            },
            other => Err(format!("unknown action {other}")),
        }
    }

    fn ask(&self, wipe: Wipe) -> Result<Option<String>, String> {
        self.view().confirm = Some(wipe);
        Ok(None)
    }

    fn page(&self) -> vtree::Node {
        let (result, confirm) = {
            let view = self.view();
            (view.result.clone(), view.confirm)
        };
        let mut page = column("page").child(text("title").variant("h1").text("Demo stores admin"));
        match &result {
            Some(Ok(message)) => page = page.child(notice("result").variant("success").text(message.as_str())),
            Some(Err(error)) => page = page.child(notice("result").variant("failure").text(error.as_str())),
            None => {}
        }
        page.child(tabs("tabs").bind("_tab").children([
            tab("tab/wall")
                .label("Wall of text")
                .children(self.wall_tab(confirm == Some(Wipe::Wall))),
            tab("tab/pixels")
                .label("Pixel wall")
                .children(self.pixels_tab(confirm == Some(Wipe::Pixels))),
            tab("tab/faucet")
                .label("Faucet")
                .children(self.faucet_tab(confirm == Some(Wipe::Faucet))),
            tab("tab/wallets")
                .label("Wallets")
                .children(self.admin.wallets.view()),
            tab("tab/operators").label("Operators").children(self.operators_tab()),
        ]))
    }

    fn wall_tab(&self, confirming: bool) -> Vec<vtree::Node> {
        let (number, messages) = self.admin.wall.messages();
        let list = list("wall/messages")
            .empty("No messages.")
            .children(messages.iter().enumerate().rev().map(|(index, message)| {
                let id = format!("wall/{number}/{index}");
                row(id.clone()).children([
                    text(format!("{id}/text")).text(message.as_str()),
                    button(format!("{id}/delete"))
                        .variant("plain")
                        .label("Delete")
                        .on_press(Handler::action("wall.delete").payload(
                            json!({"number": number, "index": index, "text": message}),
                        )),
                ])
            }));
        vec![
            text("wall/blurb").variant("caption").text(format!(
                "Wall {number} ({})",
                messages.len()
            )),
            list,
            wipe("wall", "Reset wall", confirming),
        ]
    }

    fn faucet_tab(&self, confirming: bool) -> Vec<vtree::Node> {
        let wallets = self.admin.faucet.wallets();
        let total: u64 = wallets.iter().map(|(_, sats)| sats).sum();
        vec![
            text("faucet/blurb")
                .variant("caption")
                .text(format!("{total} sats given to {} wallet(s)", wallets.len())),
            list("faucet/wallets")
                .empty("None.")
                .children(wallets.iter().map(|(wallet, sats)| {
                    let id = format!("faucet/{wallet}");
                    row(id.clone()).children([
                        text(format!("{id}/text")).text(format!("{}: {sats} sats", short(wallet))),
                        button(format!("{id}/forget"))
                            .variant("plain")
                            .label("Reset")
                            .on_press(Handler::action("faucet.forget").payload(json!({"wallet": wallet}))),
                    ])
                })),
            wipe("faucet", "Reset faucet", confirming),
        ]
    }

    fn pixels_tab(&self, confirming: bool) -> Vec<vtree::Node> {
        vec![
            text("pixels/blurb")
                .variant("caption")
                .text(format!("{} painted", self.admin.pixels.painted())),
            image("pixels/canvas")
                .src(self.admin.pixels.image())
                .alt("Pixel wall")
                .variant("full"),
            wipe("pixels", "Reset canvas", confirming),
        ]
    }

    fn operators_tab(&self) -> Vec<vtree::Node> {
        let keys = self.admin.operators.list();
        let rows = keys.iter().map(|key| {
            let id = format!("operator/{key}");
            let mut row = row(id.clone()).children([
                text(format!("{id}/key")).text(key.as_str()),
            ]);
            if key == &self.key {
                row = row.child(text(format!("{id}/you")).variant("caption").text("you"));
            } else {
                row = row.child(
                    button(format!("{id}/remove"))
                        .variant("plain")
                        .label("Remove")
                        .on_press(Handler::action("operator.remove").payload(json!({"key": key}))),
                );
            }
            row
        });
        vec![
            list("operators/list").empty("No operators.").children(rows),
            card("operators/add").title("Add operator").children([
                input("key")
                    .label("iroh key")
                    .placeholder("64 hex characters")
                    .bind("operator.key"),
                row("operators/add-actions").justify("end").child(
                    button("operator/add")
                        .variant("primary")
                        .label("Add")
                        .on_press(Handler::action("operator.add")),
                ),
            ]),
        ]
    }
}

/// A button that wipes something, after asking.
fn wipe(what: &str, label: &str, confirming: bool) -> vtree::Node {
    match confirming {
        true => card(format!("{what}/wipe")).child(
            row(format!("{what}/wipe/buttons")).children([
                button(format!("{what}/wipe/confirm"))
                    .variant("failure")
                    .label("Confirm")
                    .on_press(Handler::action("wipe.confirm")),
                button(format!("{what}/wipe/cancel"))
                    .label("Cancel")
                    .on_press(Handler::action("wipe.cancel")),
            ]),
        ),
        false => card(format!("{what}/wipe")).child(
            button(format!("{what}/wipe/ask"))
                .variant("failure")
                .label(label)
                .on_press(Handler::action(format!("{what}.reset"))),
        ),
    }
}
