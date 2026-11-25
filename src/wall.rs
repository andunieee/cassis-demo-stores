//! The wall of text: anyone can write on the last wall, and each message
//! costs [`MESSAGE_SATS`]. Starting a new wall costs 100 sats times the
//! number the wall will have; only the last wall is shown.

use crate::cop::{request_payment, Flow, Out, Service, Sessions};
use async_trait::async_trait;
use hypereffect::vtree::{self, button, card, column, divider, input, list, notice, row, text};
use hypereffect::{json, Action, Handler};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tracing::info;

const MAX_CHARS: usize = 144;
const MESSAGE_SATS: u64 = 50;

/// What a payer has in a flow: their draft and what happened to their last attempt.
#[derive(Default)]
struct Session {
    text: String,
    status: Status,
}

pub struct Wall {
    node: cassis_wallet::Node,
    path: PathBuf,
    /// The messages of every wall, oldest first. There is always one wall.
    walls: Mutex<Vec<Vec<String>>>,
    changed: broadcast::Sender<()>,
    sessions: Sessions<Session>,
}

impl Wall {
    pub fn new(node: cassis_wallet::Node, path: PathBuf) -> Result<Arc<Self>, String> {
        let walls = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<Vec<Vec<String>>>(&bytes)
                .ok()
                .filter(|walls| !walls.is_empty())
                .ok_or_else(|| format!("{} holds no walls", path.display()))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => vec![vec![]],
            Err(error) => return Err(format!("read {}: {error}", path.display())),
        };
        Ok(Arc::new(Self {
            node,
            path,
            walls: Mutex::new(walls),
            changed: broadcast::channel(16).0,
            sessions: Default::default(),
        }))
    }

    /// The number of the last wall and its messages, newest first.
    fn last(&self) -> (usize, Vec<String>) {
        let walls = self.walls.lock().unwrap_or_else(|e| e.into_inner());
        let messages = walls.last().expect("there is always a wall");
        (walls.len(), messages.iter().rev().cloned().collect())
    }

    /// Fires when the wall changes.
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.changed.subscribe()
    }

    /// The number of the last wall and its messages, oldest first.
    pub fn messages(&self) -> (usize, Vec<String>) {
        let walls = self.walls.lock().unwrap_or_else(|e| e.into_inner());
        (walls.len(), walls.last().expect("there is always a wall").clone())
    }

    /// Take `message` out of wall `number`, where it was written at
    /// `index` (0 is the oldest), if it is still there.
    pub fn remove(&self, number: usize, index: usize, message: &str) -> bool {
        let mut removed = false;
        self.change(|walls| {
            if let Some(messages) = number.checked_sub(1).and_then(|i| walls.get_mut(i)) {
                if messages.get(index).map(String::as_str) == Some(message) {
                    messages.remove(index);
                    removed = true;
                }
            }
        });
        removed
    }

    /// Wipe every message off the last wall.
    pub fn clear(&self) {
        self.change(|walls| walls.last_mut().expect("there is always a wall").clear());
    }

    fn change(&self, change: impl FnOnce(&mut Vec<Vec<String>>)) {
        let json = {
            let mut walls = self.walls.lock().unwrap_or_else(|e| e.into_inner());
            change(&mut walls);
            serde_json::to_vec(&*walls).expect("serialize the walls")
        };
        let tmp = self.path.with_extension("tmp");
        if let Err(error) = std::fs::write(&tmp, json).and_then(|_| std::fs::rename(&tmp, &self.path)) {
            tracing::error!("save {}: {error}", self.path.display());
        }
        let _ = self.changed.send(());
    }
}

#[async_trait]
impl Service for Wall {
    fn name(&self) -> &'static str {
        "Wall of text"
    }

    fn description(&self) -> &'static str {
        "50 sats a message"
    }

    fn preview(&self) -> String {
        let (number, messages) = self.last();
        let mut html = format!("<h2>Wall {number}</h2>");
        if messages.is_empty() {
            html.push_str("<p class=\"muted\">No messages yet.</p>");
        }
        for message in messages {
            html.push_str(&format!(
                "<div class=\"card message\">{}</div>",
                crate::web::escape(&message)
            ));
        }
        html
    }

    async fn open(
        self: Arc<Self>,
        session: String,
        force_resume: bool,
        out: Out,
    ) -> Result<Option<Arc<dyn Flow>>, String> {
        let Some(session) = self.sessions.open(session, force_resume) else {
            return Ok(None);
        };
        let text = session.lock().unwrap_or_else(|e| e.into_inner()).text.clone();
        let flow = Arc::new(WallFlow {
            service: self.clone(),
            out: out.clone(),
            session,
        });
        out.signal(json!({"wall": {"text": text}})).await?;
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

/// What the payer is told about their last attempt.
#[derive(Default)]
enum Status {
    #[default]
    None,
    Paid(String),
    Error(String),
}

struct WallFlow {
    service: Arc<Wall>,
    out: Out,
    session: Arc<Mutex<Session>>,
}

impl WallFlow {
    fn set(&self, status: Status) {
        self.session.lock().unwrap_or_else(|e| e.into_inner()).status = status;
    }

    async fn redraw(&self) -> Result<(), String> {
        let (number, messages) = self.service.last();
        let page = {
            let session = self.session.lock().unwrap_or_else(|e| e.into_inner());
            page(number, &messages, &session.status)
        };
        self.out.redraw(page).await
    }
}

/// The price of the wall after `walls` walls.
fn wall_sats(walls: usize) -> u64 {
    100 * (walls as u64 + 1)
}

#[async_trait]
impl Flow for WallFlow {
    async fn action(&self, action: Action) -> Result<(), String> {
        match action.name.as_str() {
            "post" => {
                let message = action
                    .signal("wall.text")
                    .and_then(|text| text.as_str())
                    .unwrap_or_default()
                    .trim()
                    .to_string();
                self.session.lock().unwrap_or_else(|e| e.into_inner()).text = message.clone();
                if message.is_empty() || message.chars().count() > MAX_CHARS {
                    self.set(Status::Error(format!("1 to {MAX_CHARS} characters.")));
                    return self.redraw().await;
                }
                self.set(Status::None);
                // Whoever pays gets the price they were quoted, on the wall
                // they were quoted it for, even if the wall moved on since.
                let (number, messages) = self.service.last();
                let sats = MESSAGE_SATS;
                let flow = (self.service.clone(), self.out.clone(), self.session.clone());
                request_payment(
                    &self.service.node,
                    &self.out,
                    sats,
                    format!("Message {} on wall {number}", messages.len() + 1),
                    async move {
                        let (service, out, session) = flow;
                        {
                            let mut session = session.lock().unwrap_or_else(|e| e.into_inner());
                            session.text.clear();
                            session.status = Status::Paid("Message posted.".into());
                        }
                        let _ = out.signal(json!({"wall": {"text": ""}})).await;
                        service.change(|walls| walls[number - 1].push(message));
                        info!("message posted on wall {number}");
                    },
                )
                .await
            }
            "newwall" => {
                self.set(Status::None);
                let walls = self.service.walls.lock().unwrap_or_else(|e| e.into_inner()).len();
                let sats = wall_sats(walls);
                let flow = (self.service.clone(), self.session.clone());
                request_payment(
                    &self.service.node,
                    &self.out,
                    sats,
                    format!("A new wall, number {}", walls + 1),
                    async move {
                        let (service, session) = flow;
                        session.lock().unwrap_or_else(|e| e.into_inner()).status =
                            Status::Paid("New wall started.".into());
                        service.change(|walls| walls.push(vec![]));
                        info!("a new wall was started");
                    },
                )
                .await
            }
            other => {
                tracing::warn!("unknown action {other}");
                Ok(())
            }
        }
    }
}

fn page(number: usize, messages: &[String], status: &Status) -> vtree::Node {
    let status = match status {
        Status::None => None,
        Status::Paid(text) => Some(notice("status").variant("success").text(text.as_str())),
        Status::Error(text) => Some(notice("status").variant("failure").text(text.as_str())),
    };
    column("page").children([
        text("title").variant("h1").text("Wall of text"),
        card("write")
            .child(
                input("text")
                    .variant("multiline")
                    .label(format!("Message (max {MAX_CHARS} chars)"))
                    .bind("wall.text"),
            )
            .children(status)
            .child(
                row("write-actions").justify("end").child(
                    button("post")
                        .variant("primary")
                        .label(format!("Post ({MESSAGE_SATS} sats)"))
                        .on_press(Handler::action("post")),
                ),
            ),
        card("wall").title(format!("Wall {number}")).child(
            list("messages")
                .empty("No messages yet.")
                .children(messages.iter().enumerate().map(|(i, message)| {
                    // Ids name the message, not its place, so a new one does not
                    // rename the others.
                    text(format!("message-{number}-{}", messages.len() - i)).text(message.as_str())
                })),
        ),
        divider("divider"),
        button("newwall")
            .variant("plain")
            .label(format!("New wall ({} sats)", wall_sats(number)))
            .on_press(Handler::action("newwall")),
    ])
}
