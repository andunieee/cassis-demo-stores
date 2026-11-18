//! The admin interface: a hyperb app over iroh, at `hyperb://<id>/admin`,
//! for the operators of this server, which can take texts off the wall,
//! wipe the wall and the picture, and manage the wallets that get paid
//! (see `hyperb-cassis-wallet`).
//!
//! Only the keys listed in `operators.json` in the data directory are let
//! in. While there is none, a setup link with a secret is printed on the
//! console: the key that opens it first becomes the operator.

use crate::cop::Out;
use crate::pixels::Pixels;
use crate::wall::Wall;
use cassis_core::NetworkId;
use cassis_wallet::Node;
use hyperb_cassis_wallet::{Drafts, Host, WalletAdmin};
use hypereffect::vtree::{self, button, card, column, image, list, notice, row, tab, tabs, text};
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

/// Gives the wallets' networks to the node that is paid on them.
pub struct NodeHost(pub Node);

impl Host for NodeHost {
    fn opened(&self, network: Arc<dyn cassis_core::Network>) {
        self.0.add_network(network);
    }

    fn closed(&self, id: &NetworkId) {
        self.0.remove_network(id);
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
        let keys = keys.iter().map(|key| key.to_lowercase()).collect();
        Ok(Self { path, keys: RwLock::new(keys) })
    }

    fn any(&self) -> bool {
        !self.keys.read().unwrap_or_else(|e| e.into_inner()).is_empty()
    }

    fn has(&self, key: &str) -> bool {
        self.keys.read().unwrap_or_else(|e| e.into_inner()).iter().any(|k| k == key)
    }

    fn add(&self, key: &str) -> Result<(), String> {
        let mut keys = self.keys.write().unwrap_or_else(|e| e.into_inner());
        keys.push(key.to_string());
        let json = serde_json::to_vec_pretty(&*keys).expect("serialize the operators");
        std::fs::write(&self.path, json)
            .map_err(|e| format!("couldn't save {}: {e}", self.path.display()))
    }
}

pub struct Admin {
    wall: Arc<Wall>,
    pixels: Arc<Pixels>,
    wallets: Arc<WalletAdmin>,
    operators: Operators,
    /// The secret of the setup link, while there is no operator.
    setup: Mutex<Option<String>>,
}

impl Admin {
    pub fn new(
        data: &std::path::Path,
        wall: Arc<Wall>,
        pixels: Arc<Pixels>,
        wallets: Arc<WalletAdmin>,
    ) -> Result<Arc<Self>, String> {
        let operators = Operators::load(data.join("operators.json"))?;
        let setup = (!operators.any())
            .then(|| {
                let secret = cassis_core::keys::new_network_secret();
                secret.iter().map(|byte| format!("{byte:02x}")).collect::<String>()
            });
        Ok(Arc::new(Self {
            wall,
            pixels,
            wallets,
            operators,
            setup: Mutex::new(setup),
        }))
    }

    /// Tell the console where the interface is.
    pub fn announce(&self, endpoint: &Endpoint) {
        let address = format!("hyperb://{}/admin", endpoint.id());
        match &*self.setup.lock().unwrap_or_else(|e| e.into_inner()) {
            Some(secret) => {
                println!("admin: nobody is the operator yet. Open this in hyperb to become it (it works once):");
                println!("{address}#setup={secret}");
            }
            None => println!("admin: {address}"),
        }
    }

    /// Let `key` in if `secret` is the setup link's. Whether it was.
    fn claim(&self, key: &str, secret: &str) -> Result<bool, String> {
        let mut setup = self.setup.lock().unwrap_or_else(|e| e.into_inner());
        // Compared as hashes, so the time it takes says nothing about the secret.
        let hash = |text: &str| Sha256::digest(text.as_bytes());
        let same = setup.as_deref().is_some_and(|expected| hash(expected) == hash(secret));
        if !same || self.operators.any() {
            return Ok(false);
        }
        *setup = None;
        info!("{key} opened the setup link and is now the operator");
        self.operators.add(key)?;
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
    // The handshake proved the key: someone else's is turned away before
    // it can say anything.
    if admin.operators.any() && !admin.operators.has(&key) {
        warn!("the key {key} tried to open the admin interface; add it to operators.json if it is yours");
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
            Some(Ok(true)) => result = Some(Ok("You are the operator of this server now.".to_string())),
            Some(Err(error)) => result = Some(Err(error)),
            _ => {
                return connection
                    .close(CLOSE_NOT_ALLOWED.into(), b"open the setup link printed by the server");
            }
        }
    }
    info!("admin session for {key}");
    let out = Out::new(send);
    if let Err(error) = session(admin, out.clone(), lines, result).await {
        warn!("admin session of {key} ended: {error}");
    }
    out.stop();
    connection.close(0u32.into(), b"bye");
}

async fn hello(lines: &mut Lines<BufReader<RecvStream>>) -> Option<HashMap<String, String>> {
    let line = lines.next_line().await.ok()??;
    match RendererMessage::parse(&line) {
        Ok(RendererMessage::Hello { params }) => Some(params.into_iter().collect()),
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
}

struct Session {
    admin: Arc<Admin>,
    out: Out,
    view: Mutex<View>,
}

async fn session(
    admin: Arc<Admin>,
    out: Out,
    mut lines: Lines<BufReader<RecvStream>>,
    result: Option<Result<String, String>>,
) -> Result<(), String> {
    let metadata = Metadata::new()
        .name("Demo stores admin")
        .description("Moderate the wall and the picture, and manage the wallets");
    out.send(ServerMessage::from(metadata).to_line()).await?;
    let session = Arc::new(Session {
        admin: admin.clone(),
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
        let mut wallets = watcher.admin.wallets.updates();
        let mut tick = tokio::time::interval(REFRESH);
        loop {
            tokio::select! {
                _ = wall.recv() => {}
                _ = pixels.recv() => {}
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
            Ok(RendererMessage::Hello { .. }) => warn!("ignoring a second hello"),
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
                    true => Ok(Some("Message taken off the wall".into())),
                    false => Err("That message is not on the wall anymore".into()),
                }
            }
            "wall.reset" => self.ask(Wipe::Wall),
            "pixels.reset" => self.ask(Wipe::Pixels),
            "wipe.cancel" => {
                self.view().confirm = None;
                Ok(None)
            }
            "wipe.confirm" => match self.view().confirm.take() {
                Some(Wipe::Wall) => {
                    self.admin.wall.clear();
                    Ok(Some("The wall is empty".into()))
                }
                Some(Wipe::Pixels) => {
                    self.admin.pixels.reset();
                    Ok(Some("The picture is blank".into()))
                }
                None => Err("Nothing to confirm".into()),
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
            tab("tab/wallets").label("Wallets").children(self.admin.wallets.view()),
        ]))
    }

    fn wall_tab(&self, confirming: bool) -> Vec<vtree::Node> {
        let (number, messages) = self.admin.wall.messages();
        let list = list("wall/messages")
            .empty("Nothing written on this wall yet.")
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
                "Wall {number}, {} message(s), newest first.",
                messages.len()
            )),
            list,
            wipe("wall", "Reset the wall", "This takes every message off the wall.", confirming),
        ]
    }

    fn pixels_tab(&self, confirming: bool) -> Vec<vtree::Node> {
        vec![
            text("pixels/blurb").variant("caption").text(format!(
                "{} pixel(s) painted.",
                self.admin.pixels.painted()
            )),
            image("pixels/canvas")
                .src(self.admin.pixels.image())
                .alt("The pixel wall")
                .variant("full"),
            wipe("pixels", "Reset the picture", "This paints every pixel blank.", confirming),
        ]
    }
}

/// A button that wipes something, after asking.
fn wipe(what: &str, label: &str, warning: &str, confirming: bool) -> vtree::Node {
    match confirming {
        true => card(format!("{what}/wipe")).children([
            notice(format!("{what}/wipe/warning"))
                .variant("warning")
                .text(format!("{warning} It cannot be undone.")),
            row(format!("{what}/wipe/buttons")).children([
                button(format!("{what}/wipe/confirm"))
                    .variant("failure")
                    .label(label)
                    .on_press(Handler::action("wipe.confirm")),
                button(format!("{what}/wipe/cancel"))
                    .label("Keep it")
                    .on_press(Handler::action("wipe.cancel")),
            ]),
        ]),
        false => card(format!("{what}/wipe")).child(
            button(format!("{what}/wipe/ask"))
                .variant("failure")
                .label(label)
                .on_press(Handler::action(format!("{what}.reset"))),
        ),
    }
}
