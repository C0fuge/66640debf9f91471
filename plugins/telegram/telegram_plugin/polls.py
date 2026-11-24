"""Existing-contract polls using the exact locked layer 233 constructors."""
import asyncio
import copy
import logging
from collections import OrderedDict

from contact_hub_sdk import errors as rpc, types as dto
from telethon import errors as tg, functions, types as tl

from .actions import fetch
from .mapping import entities_to_tl, identifier, message, peer_id, resolve_chat
from .poll_info import option_id
from .session import platform_errors
from .uploads import confirmations, reply_header

MAX_OPTIONS = 10
MAX_TARGETS = 2048
log = logging.getLogger(__name__)


def lock(account):
    return account.data.setdefault("poll_lock", asyncio.Lock())


def observe(account, items):
    for item in items:
        if item is not None and item.poll is not None and item.poll.id is not None:
            targets = account.data.setdefault("poll_targets", OrderedDict())
            key = (item.poll.id, item.chat_id, item.id)
            targets[key] = None
            targets.move_to_end(key)
            while len(targets) > MAX_TARGETS:
                targets.popitem(last=False)


def updates(response):
    if isinstance(response, tl.UpdateShort):
        return [response.update]
    if isinstance(response, (tl.Updates, tl.UpdatesCombined)):
        return response.updates
    if isinstance(response, tl.UpdatesTooLong):
        return []  # A scoped read below must recover the actual snapshot.
    raise rpc.Internal("Telegram did not return a poll update acknowledgement.")


def matching_update(response, poll_id, chat_id, mid):
    for update in updates(response):
        if not isinstance(update, tl.UpdateMessagePoll) or update.poll_id != poll_id:
            continue
        if update.peer is not None and peer_id(update.peer) != chat_id:
            continue
        if update.msg_id is not None and update.msg_id != mid:
            continue
        if update.poll is not None and update.poll.id != poll_id:
            continue
        return update
    return None


def with_update(raw, update):
    if update.top_msg_id is not None and message(raw).topic_id != str(update.top_msg_id):
        raise rpc.Internal("Telegram returned a poll update for a different topic.")
    result = copy.copy(raw)
    result.media = copy.copy(raw.media)
    result.media.poll = update.poll or raw.media.poll
    result.media.results = update.results
    return result


async def target(client, chat_id, mid):
    peer = await resolve_chat(client, chat_id)
    raw, item = await fetch(client, peer, chat_id, mid)
    if item is None or not isinstance(getattr(raw, "media", None), tl.MessageMediaPoll):
        raise rpc.NotFound(kind="poll")
    return peer, raw


async def fresh(client, peer, chat_id, mid, poll_id):
    raw, item = await fetch(client, peer, chat_id, mid)
    if item is None or not isinstance(getattr(raw, "media", None), tl.MessageMediaPoll) or raw.media.poll.id != poll_id:
        raise rpc.NotFound(kind="poll")
    if raw.media.results.min or raw.media.results.results is None:
        # Hash 0 requests a fresh personal view, without accepting a cached
        # common/min result as evidence of the current user's chosen answers.
        response = await client(functions.messages.GetPollResultsRequest(peer=peer, msg_id=mid, poll_hash=0))
        update = matching_update(response, poll_id, chat_id, mid)
        if update is not None:
            raw = with_update(raw, update)
    return raw


async def after_write(client, peer, raw, response):
    chat_id, mid, poll_id = peer_id(raw.peer_id), raw.id, raw.media.poll.id
    update = matching_update(response, poll_id, chat_id, mid)
    if update is not None and update.top_msg_id is not None and message(raw).topic_id != str(update.top_msg_id):
        raise rpc.Internal("Telegram returned a poll update for a different topic.")
    if update is not None and not update.results.min and update.results.results is not None:
        return with_update(raw, update)
    try:
        return await fresh(client, peer, chat_id, mid, poll_id)
    except rpc.NotFound:
        raise rpc.Internal("Telegram accepted the poll operation without an available matching snapshot.") from None


def create_media(p):
    if not p.question:
        raise rpc.InvalidParams(field="question")
    if not 2 <= len(p.options) <= MAX_OPTIONS or any(not option for option in p.options) or len(set(p.options)) != len(p.options):
        raise rpc.InvalidParams("Select two to ten distinct nonempty answers.", field="options")
    if p.quiz:
        if type(p.correct_option) is not int or not 0 <= p.correct_option < len(p.options):
            raise rpc.InvalidParams("A quiz requires a valid correct-answer index.", field="correct_option")
    elif p.correct_option is not None or p.solution is not None or p.solution_entities is not None:
        field = "correct_option" if p.correct_option is not None else "solution" if p.solution is not None else "solution_entities"
        raise rpc.InvalidParams("Correct answers and solutions require quiz mode.", field=field)
    if p.close_after_seconds is not None and (type(p.close_after_seconds) is not int or not 0 < p.close_after_seconds < 2**31):
        raise rpc.InvalidParams(field="close_after_seconds")
    explanation = p.solution
    solution_entities = None
    if explanation is not None or p.solution_entities is not None:
        explanation = explanation or ""
        if len(explanation) > 200 or explanation.count("\n") > 2:
            raise rpc.InvalidParams("Quiz explanations allow at most 200 characters and two line breaks.", field="solution")
        try:
            solution_entities = entities_to_tl(explanation, p.solution_entities)
        except rpc.InvalidParams as error:
            raise rpc.InvalidParams(error.message, field="solution_entities") from None
    poll = tl.Poll(id=0, hash=0, question=tl.TextWithEntities(p.question, []),
                   answers=[tl.PollAnswer(tl.TextWithEntities(text, []), bytes([index])) for index, text in enumerate(p.options)],
                   public_voters=p.anonymous is False, multiple_choice=p.multiple, quiz=p.quiz,
                   close_period=p.close_after_seconds)
    # In layer 233 correct_answers is Vector<int> (indices), not option bytes.
    return tl.InputMediaPoll(poll=poll, correct_answers=[p.correct_option] if p.quiz else None,
                             solution=explanation, solution_entities=solution_entities)


async def create(account, client, p):
    media, reply = create_media(p), reply_header(p)
    async with lock(account):
        peer = await resolve_chat(client, p.chat_id)
        request = functions.messages.SendMediaRequest(peer=peer, media=media, message="", reply_to=reply, silent=p.silent)
        response = await client(request)
        items = await confirmations(client, peer, p.chat_id, response, [request.random_id])
        item = items[0]
        if item.poll is None or item.poll.id in (None, "0") or item.direction != "out" or (p.topic_id is not None and item.topic_id != p.topic_id):
            raise rpc.Internal("Telegram did not confirm the poll in the requested thread.")
        observe(account, items)
        return dto.MessageResult(message=item)


async def vote(account, client, p):
    mid = identifier(p.message_id, "message_id")
    async with lock(account):
        peer, raw = await target(client, p.chat_id, mid)
        poll = raw.media.poll
        if poll.closed:
            raise rpc.Forbidden("The poll is closed.")
        choices = {option_id(answer.option, poll.id): answer.option for answer in poll.answers}
        if len(choices) != len(poll.answers):
            raise rpc.Internal("Telegram returned ambiguous poll answer identifiers.")
        if len(set(p.option_ids)) != len(p.option_ids) or any(value not in choices for value in p.option_ids):
            raise rpc.InvalidParams(field="option_ids")
        if len(p.option_ids) > 1 and not poll.multiple_choice:
            raise rpc.InvalidParams("This poll accepts one answer.", field="option_ids")
        response = await client(functions.messages.SendVoteRequest(peer=peer, msg_id=mid,
                                      options=[choices[value] for value in p.option_ids]))
        updated = await after_write(client, peer, raw, response)
        results = updated.media.results
        chosen = {option_id(value.option, updated.media.poll.id) for value in results.results or [] if value.chosen}
        if results.min or results.results is None or chosen != set(p.option_ids):
            raise rpc.Internal("Telegram did not confirm the requested poll selection.")
        item = message(updated)
        observe(account, [item])
        return dto.OptionalMessageResult(message=item)


async def close(account, client, p):
    mid = identifier(p.message_id, "message_id")
    async with lock(account):
        peer, raw = await target(client, p.chat_id, mid)
        if raw.media.poll.closed:
            item = message(raw)
        else:
            if raw.media.attached_media is not None or raw.media.results.solution_media is not None:
                raise rpc.NotSupported("Closing polls with attached media is not supported.", capability="polls.close")
            closed = copy.copy(raw.media.poll)
            closed.closed = True
            try:
                response = await client(functions.messages.EditMessageRequest(peer=peer, id=mid, media=tl.InputMediaPoll(poll=closed)))
            except tg.MessageNotModifiedError:
                updated = await fresh(client, peer, p.chat_id, mid, raw.media.poll.id)
            else:
                updated = await after_write(client, peer, raw, response)
                if not updated.media.poll.closed:
                    updated = await fresh(client, peer, p.chat_id, mid, raw.media.poll.id)
            item = message(updated)
            if item is None or item.poll is None or not item.poll.closed:
                raise rpc.Internal("Telegram did not confirm that the poll is closed.")
        observe(account, [item])
        return dto.OptionalMessageResult(message=item)


async def refresh_live(account, client, update):
    known = account.data.get("poll_targets", {})
    targets = {(chat, mid) for poll, chat, mid in known if poll == str(update.poll_id)}
    if update.peer is not None and update.msg_id is not None:
        targets.add((peer_id(update.peer), str(update.msg_id)))
    if not targets:
        log.debug("Poll update has no observed message target.")
        return
    async with lock(account):
        for chat, mid in targets:
            try:
                with platform_errors():
                    peer = await resolve_chat(client, chat)
                    raw = await fresh(client, peer, chat, identifier(mid, "message_id"), update.poll_id)
                    item = message(raw)
                    addressed = update.peer is not None and update.msg_id is not None and chat == peer_id(update.peer) and mid == str(update.msg_id)
                    if addressed and update.top_msg_id is not None and item.topic_id != str(update.top_msg_id):
                        log.debug("Poll update no longer matches its addressed topic.")
                        continue
            except rpc.NotFound:
                account.data.get("poll_targets", {}).pop((str(update.poll_id), chat, mid), None)
                continue
            except rpc.FloodWait:
                log.debug("Telegram requested a wait before more poll refreshes.")
                break
            except rpc.PluginError:
                log.debug("Poll refresh is unavailable; a later scoped read can refresh it.")
                continue
            observe(account, [item])
            await account.messages_upsert([item])
