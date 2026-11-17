//! The wall of text: anyone can write on the last wall, and the n-th
//! message of a wall costs n sats. Starting a new wall costs as many sats
//! as the wall will be numbered; only the last wall is shown.

use crate::cop::{request_payment, Flow, Out, Service};
use async_trait::async_trait;
use hypereffect::vtree::{self, button, column, divider, input, list, notice, text};
use hypereffect::{json, Action, Handler};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tracing::info;

const MAX_CHARS: usize = 144;

pub struct Wall {
    node: cassis_wallet::Node,
    path: PathBuf,
    /// The messages of every wall, oldest first. There is always one wall.
    walls: Mutex<Vec<Vec<String>>>,
    changed: broadcast::Sender<()>,
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
        }))
    }

    /// The number of the last wall and its messages, newest first.
    fn last(&self) -> (usize, Vec<String>) {
        let walls = self.walls.lock().unwrap_or_else(|e| e.into_inner());
        let messages = walls.last().expect("there is always a wall");
        (walls.len(), messages.iter().rev().cloned().collect())
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
        "Write on the wall: the n-th message costs n sats"
    }

    async fn open(self: Arc<Self>, _session: String, out: Out) -> Result<Arc<dyn Flow>, String> {
        let flow = Arc::new(WallFlow {
            service: self.clone(),
            out: out.clone(),
            status: Default::default(),
        });
        out.signal(json!({"wall": {"text": ""}})).await?;
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
        Ok(flow)
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
    status: Arc<Mutex<Status>>,
}

impl WallFlow {
    fn set(&self, status: Status) {
        *self.status.lock().unwrap_or_else(|e| e.into_inner()) = status;
    }

    async fn redraw(&self) -> Result<(), String> {
        let (number, messages) = self.service.last();
        let page = {
            let status = self.status.lock().unwrap_or_else(|e| e.into_inner());
            page(number, &messages, &status)
        };
        self.out.redraw(page).await
    }
}

/// The price of the next message on a wall with `messages` of them.
fn message_sats(messages: usize) -> u64 {
    messages as u64 + 1
}

/// The price of the wall after `walls` walls.
fn wall_sats(walls: usize) -> u64 {
    walls as u64 + 1
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
                if message.is_empty() || message.chars().count() > MAX_CHARS {
                    self.set(Status::Error(format!(
                        "A message is 1 to {MAX_CHARS} characters long."
                    )));
                    return self.redraw().await;
                }
                self.set(Status::None);
                // Whoever pays gets the price they were quoted, on the wall
                // they were quoted it for, even if the wall moved on since.
                let (number, messages) = self.service.last();
                let sats = message_sats(messages.len());
                let flow = (self.service.clone(), self.out.clone(), self.status.clone());
                request_payment(
                    &self.service.node,
                    &self.out,
                    sats,
                    format!("Message {} on wall {number}", messages.len() + 1),
                    async move {
                        let (service, out, status) = flow;
                        service.change(|walls| walls[number - 1].push(message));
                        info!("message posted on wall {number}");
                        let flow = WallFlow { service, out, status };
                        flow.set(Status::Paid("Paid, thank you! Your message is on the wall.".into()));
                        let _ = flow.out.signal(json!({"wall": {"text": ""}})).await;
                        let _ = flow.redraw().await;
                    },
                )
                .await
            }
            "newwall" => {
                self.set(Status::None);
                let walls = self.service.walls.lock().unwrap_or_else(|e| e.into_inner()).len();
                let sats = wall_sats(walls);
                let flow = (self.service.clone(), self.out.clone(), self.status.clone());
                request_payment(
                    &self.service.node,
                    &self.out,
                    sats,
                    format!("A new wall, number {}", walls + 1),
                    async move {
                        let (service, out, status) = flow;
                        service.change(|walls| walls.push(vec![]));
                        info!("a new wall was started");
                        let flow = WallFlow { service, out, status };
                        flow.set(Status::Paid("Paid, thank you! This is a new wall.".into()));
                        let _ = flow.redraw().await;
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
    let mut page = column("page").children([
        text("title").variant("h1").text("Wall of text"),
        text("blurb").variant("caption").text(format!(
            "Wall {number}. The n-th message on a wall costs n sats; a new wall costs {} sats.",
            wall_sats(number)
        )),
    ]);
    match status {
        Status::None => {}
        Status::Paid(text) => page = page.child(notice("status").variant("success").text(text.as_str())),
        Status::Error(text) => page = page.child(notice("status").variant("failure").text(text.as_str())),
    }
    page.children([
        input("text")
            .variant("multiline")
            .label("Your message")
            .placeholder("Write something")
            .bind("wall.text"),
        button("post")
            .variant("primary")
            .label(format!("Post for {} sats", message_sats(messages.len())))
            .on_press(Handler::action("post")),
        list("messages")
            .empty("Nothing written on this wall yet.")
            .children(messages.iter().enumerate().map(|(i, message)| {
                // Ids name the message, not its place, so a new one does not
                // rename the others.
                text(format!("message-{number}-{}", messages.len() - i)).text(message.as_str())
            })),
        divider("divider"),
        button("newwall")
            .variant("plain")
            .label(format!("Start a new wall for {} sats", wall_sats(number)))
            .on_press(Handler::action("newwall")),
    ])
}
