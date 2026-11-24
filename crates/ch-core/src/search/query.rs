//! The search box syntax. See the [module docs](super) for the grammar.

/// One free-text part of a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Term {
    /// A word, matched as a token prefix by the FTS index (`прив` finds `Привет`).
    Word(String),
    /// `"a quoted phrase"`: its tokens adjacent and in order.
    Phrase(String),
    /// A token without letters or digits (emoji, `+`, `#`): the FTS tokenizer drops these, so
    /// it is matched as a case-sensitive substring of the text instead.
    Literal(String),
}

/// Media filter values (`media:`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MediaFilter {
    Photo,
    Video,
    Gif,
    Sticker,
    Voice,
    /// `round` / `video_note`.
    VideoNote,
    /// `music` / `audio`.
    Music,
    /// `file` / `document`.
    Document,
    Other,
    /// Messages with a link (URL entity or link preview).
    Link,
}

impl MediaFilter {
    fn parse(s: &str) -> Option<Self> {
        Some(match s.to_lowercase().as_str() {
            "photo" | "photos" => Self::Photo,
            "video" | "videos" => Self::Video,
            "gif" | "gifs" => Self::Gif,
            "sticker" | "stickers" => Self::Sticker,
            "voice" => Self::Voice,
            "round" | "video_note" => Self::VideoNote,
            "music" | "audio" => Self::Music,
            "file" | "files" | "document" | "doc" => Self::Document,
            "other" => Self::Other,
            "link" | "links" | "url" => Self::Link,
            _ => return None,
        })
    }

    /// The `message.media_kind` value (`None` for [`MediaFilter::Link`]).
    pub fn media_kind(self) -> Option<&'static str> {
        Some(match self {
            Self::Photo => "photo",
            Self::Video => "video",
            Self::Gif => "gif",
            Self::Sticker => "sticker",
            Self::Voice => "voice",
            Self::VideoNote => "video_note",
            Self::Music => "music",
            Self::Document => "document",
            Self::Other => "other",
            Self::Link => return None,
        })
    }
}

/// A calendar date from `before:` / `after:`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Date {
    pub year: i32,
    pub month: u32,
    pub day: u32,
}

impl Date {
    fn parse(s: &str) -> Option<Self> {
        let mut it = s.splitn(3, '-');
        let year: i32 = it.next()?.parse().ok()?;
        let month: u32 = it.next()?.parse().ok()?;
        let day: u32 = it.next()?.parse().ok()?;
        let ok = (1970..=9999).contains(&year)
            && (1..=12).contains(&month)
            && (1..=days_in_month(year, month)).contains(&day);
        ok.then_some(Self { year, month, day })
    }

    /// Unix ms of 00:00 on this date at `utc_offset_minutes` east of UTC.
    pub fn start_millis(self, utc_offset_minutes: i32) -> i64 {
        let days = days_from_civil(self.year, self.month, self.day);
        days * 86_400_000 - i64::from(utc_offset_minutes) * 60_000
    }
}

fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        2 if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days since 1970-01-01 (H. Hinnant's algorithm).
fn days_from_civil(y: i32, m: u32, d: u32) -> i64 {
    let y = i64::from(if m <= 2 { y - 1 } else { y });
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = i64::from(m);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// A parsed search box.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ParsedQuery {
    pub terms: Vec<Term>,
    /// `from:` values (OR): `me`, `@username`, or a name fragment.
    pub from: Vec<String>,
    /// `in:` values (OR): `@username`, or a chat title fragment.
    pub chats: Vec<String>,
    /// `media:` values (OR).
    pub media: Vec<MediaFilter>,
    /// `before:` (exclusive: earlier than 00:00 that day).
    pub before: Option<Date>,
    /// `after:` (inclusive: from 00:00 that day).
    pub after: Option<Date>,
    /// `is:deleted`: only kept-deleted messages.
    pub only_deleted: bool,
    /// Filters whose value did not parse (`before:yesterday`, `media:pdf`); ignored, for the UI
    /// to point out.
    pub invalid: Vec<String>,
}

impl ParsedQuery {
    /// Is there anything to search for?
    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
            && self.from.is_empty()
            && self.chats.is_empty()
            && self.media.is_empty()
            && self.before.is_none()
            && self.after.is_none()
            && !self.only_deleted
    }
}

/// Split into tokens; `"…"` groups (also as a filter value: `in:"Family chat"`).
fn tokens(s: &str) -> Vec<(String, bool)> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    let mut in_quotes = false;
    for c in s.chars() {
        match c {
            '"' if in_quotes => {
                in_quotes = false;
            }
            '"' => {
                in_quotes = true;
                quoted = true;
            }
            c if c.is_whitespace() && !in_quotes => {
                if !cur.is_empty() {
                    out.push((std::mem::take(&mut cur), quoted));
                }
                quoted = false;
            }
            c => cur.push(c),
        }
    }
    if !cur.is_empty() {
        out.push((cur, quoted));
    }
    out
}

/// Parse the search box. Never fails: anything that is not a well-formed filter is text.
pub fn parse_query(input: &str) -> ParsedQuery {
    let mut q = ParsedQuery::default();
    for (tok, quoted) in tokens(input) {
        if let Some((key, value)) = tok.split_once(':')
            && !value.is_empty()
        {
            let key_l = key.to_lowercase();
            let handled = match key_l.as_str() {
                "from" => {
                    q.from.push(value.to_owned());
                    true
                }
                "in" => {
                    q.chats.push(value.to_owned());
                    true
                }
                "media" | "has" => {
                    for v in value.split(',').filter(|v| !v.is_empty()) {
                        match MediaFilter::parse(v) {
                            Some(m) if !q.media.contains(&m) => q.media.push(m),
                            Some(_) => {}
                            None => q.invalid.push(format!("{key}:{v}")),
                        }
                    }
                    true
                }
                "before" | "after" => {
                    match Date::parse(value) {
                        Some(d) if key_l == "before" => q.before = Some(d),
                        Some(d) => q.after = Some(d),
                        None => q.invalid.push(tok.clone()),
                    }
                    true
                }
                "is" if value.eq_ignore_ascii_case("deleted") => {
                    q.only_deleted = true;
                    true
                }
                _ => false,
            };
            if handled {
                continue;
            }
        }
        if quoted {
            if tok.chars().any(char::is_alphanumeric) {
                q.terms.push(Term::Phrase(tok));
            } else if !tok.trim().is_empty() {
                q.terms.push(Term::Literal(tok.trim().to_owned()));
            }
            continue;
        }
        let word = tok.trim_matches(|c: char| !c.is_alphanumeric());
        if word.is_empty() {
            q.terms.push(Term::Literal(tok));
        } else {
            q.terms.push(Term::Word(word.to_owned()));
        }
    }
    q
}

/// The FTS5 MATCH expression for the word and phrase terms (`None` when there are none).
pub fn fts_expr(terms: &[Term]) -> Option<String> {
    let parts: Vec<String> = terms
        .iter()
        .filter_map(|t| match t {
            Term::Word(w) => Some(format!("\"{}\"*", w.replace('"', "\"\""))),
            Term::Phrase(p) => Some(format!("\"{}\"", p.replace('"', "\"\""))),
            Term::Literal(_) => None,
        })
        .collect();
    (!parts.is_empty()).then(|| parts.join(" "))
}
