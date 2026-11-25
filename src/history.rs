//! `<data>/sessions.redb`: the sessions of every service, by the client's
//! `session` token, and what was done in each. A flow resumed with a token
//! shows what was done in it, even after a restart.

use cassis_core::unix_now;
use hypereffect::vtree::{self, badge, card, column, list, row, text};
use std::path::Path;
use std::sync::Arc;
use tracing::error;

pub use schema::*;

#[orangedb::schema]
pub mod schema {
    #[entity]
    #[derive(Debug, Clone)]
    pub struct Session {
        /// The slug of the service and the client's token.
        #[id((String, String))]
        pub key: SessionKey,
        pub opened_at: u64,
    }

    /// Something a payer did, once it was paid for.
    #[embed]
    #[derive(Debug, Clone, PartialEq)]
    pub enum Deed {
        /// Pixels by column, row and color.
        Painted { pixels: Vec<(u32, u32, [u8; 3])> },
        Posted { wall: u64, message: String },
        NewWall { wall: u64 },
        /// Sats the faucet sent.
        Received,
    }

    #[entity]
    #[derive(Debug, Clone)]
    pub struct Entry {
        #[id(auto)]
        pub id: EntryId,
        #[references(Session, on_delete = cascade)]
        pub session: SessionKey,
        pub at: u64,
        /// What was paid, or received for [`Deed::Received`].
        pub sats: u64,
        pub deed: Deed,
    }
}

/// The sessions of every service. Cheap to clone.
#[derive(Clone)]
pub struct History(Arc<Db>);

impl History {
    pub fn open(path: &Path) -> Result<Self, String> {
        let db = Db::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
        Ok(Self(Arc::new(db)))
    }

    #[cfg(test)]
    pub fn in_memory() -> Self {
        Self(Arc::new(Db::in_memory().expect("open an in-memory database")))
    }

    fn key(service: &str, token: &str) -> SessionKey {
        SessionKey::new((service.to_string(), token.to_string()))
    }

    /// Whether `service` ever had the session `token`.
    pub fn knows(&self, service: &str, token: &str) -> bool {
        self.0
            .sessions()
            .contains(Self::key(service, token))
            .unwrap_or_else(|e| {
                error!("look up a session: {e}");
                false
            })
    }

    /// The log of the session `token` of `service`, which starts now if it
    /// did not exist.
    pub fn log(&self, service: &str, token: &str) -> Log {
        let key = Self::key(service, token);
        let session = Session {
            key: key.clone(),
            opened_at: unix_now(),
        };
        let started = self.0.write(|tx| {
            if !tx.sessions().contains(&key)? {
                tx.sessions().insert(&session)?;
            }
            Ok::<_, orangedb::Error>(())
        });
        if let Err(error) = started {
            error!("save a session: {error}");
        }
        Log {
            db: self.0.clone(),
            key,
        }
    }
}

/// What was done in one session. Cheap to clone.
#[derive(Clone)]
pub struct Log {
    db: Arc<Db>,
    key: SessionKey,
}

impl Log {
    pub fn record(&self, sats: u64, deed: Deed) {
        let recorded = self.db.entries().create(|id| Entry {
            id,
            session: self.key.clone(),
            at: unix_now(),
            sats,
            deed,
        });
        if let Err(error) = recorded {
            error!("save what a session did: {error}");
        }
    }

    /// What was done, newest first.
    pub fn entries(&self) -> Vec<Entry> {
        let mut entries = self.db.entries().by_session(&self.key).unwrap_or_else(|e| {
            error!("read what a session did: {e}");
            Vec::new()
        });
        entries.reverse();
        entries
    }

    /// A card of what was done, or nothing if nothing was.
    pub fn card(&self) -> Option<vtree::Node> {
        let entries = self.entries();
        (!entries.is_empty()).then(|| {
            card("history").title("This session").child(list("history-list").children(
                entries.iter().map(|entry| {
                    let id = format!("entry-{}", entry.id.0);
                    let sats = match entry.deed {
                        Deed::Received => badge(format!("{id}-sats"))
                            .variant("success")
                            .text(format!("+{} sats", entry.sats)),
                        _ => badge(format!("{id}-sats")).text(format!("{} sats", entry.sats)),
                    };
                    row(id.clone()).justify("between").children([
                        column(format!("{id}-what")).gap("none").children([
                            text(format!("{id}-text")).text(entry.deed.describe()),
                            text(format!("{id}-at")).variant("caption").text(utc(entry.at)),
                        ]),
                        sats,
                    ])
                }),
            ))
        })
    }
}

impl Deed {
    pub fn describe(&self) -> String {
        match self {
            Deed::Painted { pixels } => {
                let places: Vec<String> = pixels
                    .iter()
                    .map(|(col, row, [r, g, b])| format!("({col}, {row}) #{r:02x}{g:02x}{b:02x}"))
                    .collect();
                match pixels.len() {
                    1 => format!("Painted {}", places[0]),
                    n => format!("Painted {n} pixels: {}", places.join(", ")),
                }
            }
            Deed::Posted { wall, message } => format!("Wrote on wall {wall}: “{message}”"),
            Deed::NewWall { wall } => format!("Started wall {wall}"),
            Deed::Received => "Received from the faucet".into(),
        }
    }
}

/// `at` as `YYYY-MM-DD HH:MM UTC`.
fn utc(at: u64) -> String {
    let (days, secs) = ((at / 86_400) as i64, at % 86_400);
    // Days since 1970-01-01 to a civil date (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year}-{month:02}-{day:02} {:02}:{:02} UTC",
        secs / 3_600,
        secs % 3_600 / 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates() {
        assert_eq!(utc(0), "1970-01-01 00:00 UTC");
        assert_eq!(utc(951_782_400 + 3_661), "2000-02-29 01:01 UTC");
        assert_eq!(utc(1_795_996_800), "2026-11-30 00:00 UTC");
    }

    #[test]
    fn a_session_keeps_what_was_done_newest_first() {
        let history = History::in_memory();
        assert!(!history.knows("wall", "abc"));
        let log = history.log("wall", "abc");
        assert!(history.knows("wall", "abc"));
        assert!(!history.knows("pixels", "abc"));
        assert!(log.card().is_none());

        log.record(50, Deed::Posted { wall: 1, message: "hi".into() });
        log.record(200, Deed::NewWall { wall: 2 });
        // Opening it again keeps what it had.
        let again = history.log("wall", "abc");
        let deeds: Vec<Deed> = again.entries().into_iter().map(|entry| entry.deed).collect();
        assert_eq!(deeds, [Deed::NewWall { wall: 2 }, Deed::Posted { wall: 1, message: "hi".into() }]);
        assert!(history.log("wall", "other").entries().is_empty());
    }
}
