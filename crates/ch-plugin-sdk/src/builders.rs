//! Constructors and chainable setters for the DTOs plugins build most (spec §9).
//!
//! `Conversation`, `Topic` and `Message` are merge-updated (§8.4): a field left `None` is not
//! written and means "unchanged", so an update carries only what changed. These traits (in
//! the prelude) give them a `new` taking the required fields, every optional field `None`,
//! plus `with_*` setters that take the plain value:
//!
//! ```
//! use ch_plugin_sdk::prelude::*;
//!
//! let chat = Conversation::new("c1", ConversationKind::Group)
//!     .with_title("Friends")
//!     .with_unread_count(3);
//! assert_eq!(
//!     serde_json::to_value(&chat).unwrap(),
//!     serde_json::json!({"id": "c1", "kind": "group", "title": "Friends", "unread_count": 3}),
//! );
//! ```
//!
//! Every field is public, so anything without a setter is a plain assignment
//! (`chat.permissions = Some(..)`) or struct-update syntax
//! (`Conversation { muted: Some(true), ..Conversation::new(id, kind) }`).

use ch_protocol::dto::{
    Contact, Conversation, ConversationKind, Delivery, Direction, Draft, Entity, Folder, Media,
    Message, ReplyRef, Topic,
};
use ch_protocol::{Id, Timestamp};

macro_rules! dto_builder {
    (
        $(#[$doc:meta])*
        $trait:ident for $ty:ident {
            $(#[$new_doc:meta])*
            fn new($($arg:ident: $arg_ty:ty),*) -> Self $new:block
            into { $($isetter:ident => $ifield:ident: $it:ty),* $(,)? }
            plain { $($psetter:ident => $pfield:ident: $pt:ty),* $(,)? }
        }
    ) => {
        $(#[$doc])*
        pub trait $trait: Sized {
            $(#[$new_doc])*
            fn new($($arg: $arg_ty),*) -> Self;
            $(
                #[doc = concat!("Set `", stringify!($ifield), "`.")]
                fn $isetter(self, value: impl Into<$it>) -> Self;
            )*
            $(
                #[doc = concat!("Set `", stringify!($pfield), "`.")]
                fn $psetter(self, value: $pt) -> Self;
            )*
        }

        impl $trait for $ty {
            fn new($($arg: $arg_ty),*) -> Self $new
            $(
                fn $isetter(mut self, value: impl Into<$it>) -> Self {
                    self.$ifield = Some(value.into());
                    self
                }
            )*
            $(
                fn $psetter(mut self, value: $pt) -> Self {
                    self.$pfield = Some(value);
                    self
                }
            )*
        }
    };
}

dto_builder! {
    /// `Conversation::new(id, kind)` and setters (spec §9.2).
    ConversationExt for Conversation {
        /// A chat with only `id` and `kind` set: as an update, it changes nothing else.
        fn new(id: impl Into<Id>, kind: ConversationKind) -> Self {
            Conversation {
                id: id.into(),
                kind,
                title: None,
                peer_user_id: None,
                parent_chat_id: None,
                username: None,
                about: None,
                archived: None,
                muted: None,
                mute_until: None,
                pinned: None,
                pin_order: None,
                folder_ids: None,
                unread_count: None,
                unread_mentions: None,
                unread_reactions: None,
                marked_unread: None,
                read_inbox_max_id: None,
                read_outbox_max_id: None,
                last_message_at: None,
                is_self_chat: None,
                avatar_path: None,
                member_count: None,
                can_post: None,
                is_forum: None,
                draft: None,
                color_index: None,
                scheduled_count: None,
                auto_delete_seconds: None,
                slow_mode_seconds: None,
                permissions: None,
                is_verified: None,
                is_scam: None,
                is_restricted: None,
                ext: None,
            }
        }
        into {
            with_title => title: String,
            with_peer_user_id => peer_user_id: Id,
            with_parent_chat_id => parent_chat_id: Id,
            with_username => username: String,
            with_about => about: String,
            with_folder_ids => folder_ids: Vec<Id>,
            with_read_inbox_max_id => read_inbox_max_id: Id,
            with_read_outbox_max_id => read_outbox_max_id: Id,
            with_avatar_path => avatar_path: String,
        }
        plain {
            with_archived => archived: bool,
            with_muted => muted: bool,
            with_mute_until => mute_until: Timestamp,
            with_pinned => pinned: bool,
            with_pin_order => pin_order: u64,
            with_unread_count => unread_count: u64,
            with_unread_mentions => unread_mentions: u64,
            with_unread_reactions => unread_reactions: u64,
            with_marked_unread => marked_unread: bool,
            with_last_message_at => last_message_at: Timestamp,
            with_is_self_chat => is_self_chat: bool,
            with_member_count => member_count: u64,
            with_can_post => can_post: bool,
            with_is_forum => is_forum: bool,
            with_draft => draft: Draft,
        }
    }
}

dto_builder! {
    /// `Message::new(id, chat_id, direction, sent_at)` and setters (spec §9.3).
    MessageExt for Message {
        /// A message with only its required fields set.
        fn new(id: impl Into<Id>, chat_id: impl Into<Id>, direction: Direction, sent_at: Timestamp) -> Self {
            Message {
                id: id.into(),
                chat_id: chat_id.into(),
                topic_id: None,
                direction,
                sent_at,
                sender_id: None,
                sender_name: None,
                sender_title: None,
                text: None,
                entities: None,
                media: None,
                reply_to: None,
                forward_from: None,
                grouped_id: None,
                reactions: None,
                reactors: None,
                poll: None,
                location: None,
                contact_card: None,
                dice: None,
                link_preview: None,
                buttons: None,
                views: None,
                replies_count: None,
                replies_chat_id: None,
                effect_id: None,
                action: None,
                mentioned: None,
                edited: None,
                edited_at: None,
                delivery: None,
                read_by: None,
                is_scheduled: None,
                is_pinned: None,
                silent: None,
                ttl_seconds: None,
                via_bot_id: None,
                ext: None,
            }
        }
        into {
            with_topic_id => topic_id: Id,
            with_sender_id => sender_id: Id,
            with_sender_name => sender_name: String,
            with_sender_title => sender_title: String,
            with_text => text: String,
            with_entities => entities: Vec<Entity>,
            with_grouped_id => grouped_id: Id,
            with_via_bot_id => via_bot_id: Id,
        }
        plain {
            with_media => media: Media,
            with_reply_to => reply_to: ReplyRef,
            with_mentioned => mentioned: bool,
            with_edited => edited: bool,
            with_edited_at => edited_at: Timestamp,
            with_delivery => delivery: Delivery,
            with_is_scheduled => is_scheduled: bool,
            with_is_pinned => is_pinned: bool,
            with_silent => silent: bool,
            with_ttl_seconds => ttl_seconds: u64,
        }
    }
}

dto_builder! {
    /// `Topic::new(id, chat_id, title)` and setters (spec §9.6).
    TopicExt for Topic {
        /// A forum topic with only its required fields set.
        fn new(id: impl Into<Id>, chat_id: impl Into<Id>, title: impl Into<String>) -> Self {
            Topic {
                id: id.into(),
                chat_id: chat_id.into(),
                title: title.into(),
                ..Topic::default()
            }
        }
        into {
            with_icon_emoji => icon_emoji: String,
            with_last_message_preview => last_message_preview: String,
        }
        plain {
            with_icon_color => icon_color: u32,
            with_is_general => is_general: bool,
            with_unread_count => unread_count: u64,
            with_unread_mentions => unread_mentions: u64,
            with_unread_reactions => unread_reactions: u64,
            with_pinned => pinned: bool,
            with_closed => closed: bool,
            with_hidden => hidden: bool,
            with_last_message_at => last_message_at: Timestamp,
        }
    }
}

dto_builder! {
    /// `Contact::new(id)` and setters (spec §9.1).
    ContactExt for Contact {
        /// A user with only `id` set.
        fn new(id: impl Into<Id>) -> Self {
            Contact { id: id.into(), ..Contact::default() }
        }
        into {
            with_display_name => display_name: String,
            with_first_name => first_name: String,
            with_last_name => last_name: String,
            with_username => username: String,
            with_phone => phone: String,
            with_avatar_path => avatar_path: String,
            with_bio => bio: String,
        }
        plain {
            with_is_bot => is_bot: bool,
            with_is_contact => is_contact: bool,
        }
    }
}

dto_builder! {
    /// `Folder::new(id, title)` and setters (spec §9.5).
    FolderExt for Folder {
        /// A folder with only `id` and `title` set.
        fn new(id: impl Into<Id>, title: impl Into<String>) -> Self {
            Folder { id: id.into(), title: title.into(), ..Folder::default() }
        }
        into {
            with_emoticon => emoticon: String,
            with_chat_ids => chat_ids: Vec<Id>,
        }
        plain {
            with_position => position: u64,
        }
    }
}

dto_builder! {
    /// `Draft::new(text)` and setters (spec §9.4).
    DraftExt for Draft {
        /// A draft with only `text` set (empty text means "no draft").
        fn new(text: impl Into<String>) -> Self {
            Draft { text: text.into(), ..Draft::default() }
        }
        into {
            with_entities => entities: Vec<Entity>,
            with_reply_to => reply_to: Id,
        }
        plain {
            with_updated_at => updated_at: Timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn new_writes_only_the_required_fields() {
        let ts = Timestamp::from_unix_millis(0).unwrap();
        let cases = [
            (
                serde_json::to_value(Conversation::new("c", ConversationKind::Group)).unwrap(),
                json!({"id": "c", "kind": "group"}),
            ),
            (
                serde_json::to_value(Message::new("m", "c", Direction::In, ts)).unwrap(),
                json!({"id": "m", "chat_id": "c", "direction": "in", "sent_at": "1970-01-01T00:00:00.000Z"}),
            ),
            (
                serde_json::to_value(Topic::new("t", "c", "General")).unwrap(),
                json!({"id": "t", "chat_id": "c", "title": "General"}),
            ),
            (
                serde_json::to_value(Contact::new("u").with_is_bot(true)).unwrap(),
                json!({"id": "u", "is_bot": true}),
            ),
            (
                serde_json::to_value(
                    Conversation::new("c", ConversationKind::Dm)
                        .with_unread_count(0)
                        .with_archived(false),
                )
                .unwrap(),
                json!({"id": "c", "kind": "dm", "unread_count": 0, "archived": false}),
            ),
        ];
        for (got, want) in cases {
            assert_eq!(got, want);
        }
    }
}
