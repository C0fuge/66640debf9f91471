import asyncio
import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from contact_hub_sdk import errors as rpc, types as dto
from contact_hub_sdk.wire import to_wire
from telethon import functions, types as tl

from stub import NOW, row
from telegram_plugin.mapping import message
from telegram_plugin.poll_info import option_id
from telegram_plugin import polls
from telegram_plugin.actions import get
from telegram_plugin.sync import raw_update


def poll_media(*, minimum=False, results=None, **flags):
    poll = tl.Poll(707, tl.TextWithEntities("**😀Question**", []),
                   [tl.PollAnswer(tl.TextWithEntities("First", []), b"\x00\xff"),
                    tl.PollAnswer(tl.TextWithEntities("Second", []), b"\xff\x00")], hash=88, **flags)
    return tl.MessageMediaPoll(poll=poll, results=results or tl.PollResults(min=minimum))


def test_poll_mapping_preserves_literal_text_opaque_ids_and_known_results():
    results = tl.PollResults(results=[tl.PollAnswerVoters(b"\x00\xff", chosen=True, voters=2, correct=True, recent_voters=[]),
                                      tl.PollAnswerVoters(b"\xff\x00", voters=1, recent_voters=[])], total_voters=3,
                             solution="😀why", solution_entities=[tl.MessageEntityBold(2, 3)])
    media = poll_media(results=results, quiz=True, public_voters=True, close_date=NOW)
    mapped = message(row(110, media=media)).poll
    assert mapped.question == "**😀Question**" and mapped.id == "707"
    assert [value.id for value in mapped.options] == ["p:707:b:AP8", "p:707:b:_wA"]
    assert [(value.votes, value.chosen, value.correct) for value in mapped.options] == [(2, True, True), (1, False, False)]
    assert mapped.quiz and not mapped.anonymous and mapped.close_at == NOW
    assert mapped.total_voters == 3 and mapped.solution_entities[0].offset == 2
    assert mapped.solution == "😀why"


def test_minimum_poll_results_do_not_invent_personal_choices_or_missing_votes():
    results = tl.PollResults(min=True, results=[tl.PollAnswerVoters(b"\x00\xff", voters=4, recent_voters=[])], total_voters=4)
    mapped = message(row(110, media=poll_media(results=results))).poll
    assert mapped.options[0].votes == 4
    assert all(value.chosen is None and value.correct is None for value in mapped.options)
    assert mapped.options[1].votes is None
    missing = message(row(110, media=poll_media())).poll
    assert missing.total_voters is None and all(value.votes is None for value in missing.options)
    assert "chosen" not in to_wire(missing.options[0])
    assert missing.anonymous and not missing.closed and missing.close_at is None


def test_quiz_correctness_stays_unknown_until_server_reports_it():
    results = tl.PollResults(results=[tl.PollAnswerVoters(b"\x00\xff", voters=0, recent_voters=[]), tl.PollAnswerVoters(b"\xff\x00", voters=0, recent_voters=[])])
    mapped = message(row(110, media=poll_media(results=results, quiz=True))).poll
    assert all(value.correct is None for value in mapped.options)
    assert all(value.chosen is False for value in mapped.options)
    empty = message(row(110, media=poll_media(results=tl.PollResults(results=[])))).poll
    assert all(value.chosen is None and value.votes is None for value in empty.options)
    zero = message(row(110, media=poll_media(results=tl.PollResults(total_voters=0)))).poll
    assert all(value.chosen is False and value.votes is None for value in zero.options)
    assert option_id(b"", 707) == "p:707:b:"
    assert len(option_id(b"x" * 186, 707)) == 256
    assert len(option_id(b"x" * 187, 707)) == 77 and option_id(b"x" * 187, 707).startswith("p:707:sha256:")
    missing = message(row(110, media=poll_media(quiz=True, results=tl.PollResults(results=[
        tl.PollAnswerVoters(b"\x00\xff", correct=True)])))).poll
    assert missing.options[0].correct is True and missing.options[1].correct is None
    assert missing.options[0].chosen is False and missing.options[1].chosen is None


def owner():
    return SimpleNamespace(data={}, messages_upsert=AsyncMock())


def client(raw):
    result = AsyncMock()
    result.get_input_entity.return_value = tl.InputPeerUser(17, 0)
    result.get_messages.return_value = raw
    return result


def update_response(raw, results=None, *, header=True):
    return tl.Updates([tl.UpdateMessagePoll(raw.media.poll.id, results or raw.media.results,
                      poll=raw.media.poll if header else None, peer=raw.peer_id, msg_id=raw.id)], [], [], NOW, 1)


def test_create_uses_locked_integer_correct_indices_literal_text_and_utf16():
    p = dto.PollsCreateParams(account_id="a", chat_id="-1000000000008", topic_id="42",
         reply_to=dto.ReplyTarget(message_id="7"), question="**😀Question**", options=["**A**", "B"],
         quiz=True, correct_option=1, anonymous=False, multiple=False, silent=False,
         close_after_seconds=30, solution="😀why", solution_entities=[dto.Entity(type="bold", offset=2, length=3)])
    value = client(None)
    value.get_input_entity.return_value = tl.InputPeerChannel(8, 0)
    async def response(req):
        assert req.media.correct_answers == [1] and type(req.media.correct_answers[0]) is int
        assert req.media.poll.question.text == p.question and req.media.poll.question.entities == []
        assert [a.text.text for a in req.media.poll.answers] == p.options
        assert req.media.poll.hash == 0 and req.media.poll.close_period == 30
        assert req.media.poll.public_voters and req.media.poll.multiple_choice is False and req.silent is False
        assert req.reply_to.top_msg_id == 42 and req.reply_to.reply_to_msg_id == 7
        assert req.media.solution_entities[0].offset == 2
        assert bytes(req)  # Real locked serializer rejects the old Vector<bytes> shape.
        media = copy.deepcopy(req.media.poll)
        media.id = 707
        raw = row(110, tl.PeerChannel(8), out=True, media=tl.MessageMediaPoll(media, tl.PollResults()),
                  reply_to=tl.MessageReplyHeader(reply_to_msg_id=7, reply_to_top_id=42, forum_topic=True))
        return tl.Updates([tl.UpdateMessageID(110, req.random_id), tl.UpdateNewMessage(raw, 1, 1)], [], [], NOW, 1)
    value.side_effect = response
    result = asyncio.run(polls.create(owner(), value, p)).message
    assert (result.id, result.chat_id, result.topic_id, result.poll.question) == ("110", p.chat_id, "42", p.question)


def test_create_invalid_options_quiz_and_solution_fail_before_rpc():
    base = dict(account_id="a", chat_id="17", question="Question", options=["A", "B"])
    for changed, field in [({"options": ["A", "A"]}, "options"), ({"options": [str(i) for i in range(11)]}, "options"),
                            ({"correct_option": 0}, "correct_option"), ({"quiz": True}, "correct_option"),
                            ({"quiz": True, "correct_option": 2}, "correct_option"), ({"solution": "not a quiz"}, "solution"),
                            ({"close_after_seconds": 0}, "close_after_seconds"),
                            ({"quiz": True, "correct_option": 0, "solution": "😀", "solution_entities": [dto.Entity(type="bold", offset=1, length=1)]}, "solution_entities")]:
        with pytest.raises(rpc.InvalidParams) as error:
            polls.create_media(dto.PollsCreateParams(**{**base, **changed}))
        assert error.value.data["field"] == field


def test_vote_round_trips_opaque_bytes_and_retraction_without_guessing():
    raw = row(110, media=poll_media())
    value, account = client(raw), owner()
    calls = []
    async def response(req):
        assert isinstance(req, functions.messages.SendVoteRequest)
        calls.append(req.options)
        results = tl.PollResults(results=[tl.PollAnswerVoters(a.option, chosen=True if a.option in req.options else None,
                            voters=int(a.option in req.options), recent_voters=[]) for a in raw.media.poll.answers], total_voters=int(bool(req.options)))
        return update_response(raw, results)
    value.side_effect = response
    p = dto.PollsVoteParams(account_id="a", chat_id="17", message_id="110", option_ids=["p:707:b:AP8"])
    first = asyncio.run(polls.vote(account, value, p)).message.poll
    assert first.options[0].chosen and first.options[1].chosen is False
    p.option_ids = []
    cleared = asyncio.run(polls.vote(account, value, p)).message.poll
    assert all(a.chosen is False for a in cleared.options)
    large_option = b"x" * 191
    raw.media.poll.answers[0].option = large_option
    p.option_ids = [option_id(large_option, 707)]
    assert asyncio.run(polls.vote(account, value, p)).message.poll.options[0].chosen
    assert calls == [[b"\x00\xff"], [], [large_option]]


def test_vote_preflight_scopes_poll_and_rejects_unknown_or_duplicate_choices():
    raw = row(110, media=poll_media())
    value = client(raw)
    p = dto.PollsVoteParams(account_id="a", chat_id="17", message_id="110", option_ids=[])
    for options in [["missing"], ["p:999:b:AP8"], ["p:707:b:AP8", "p:707:b:AP8"], ["p:707:b:AP8", "p:707:b:_wA"]]:
        p.option_ids = options
        with pytest.raises(rpc.InvalidParams):
            asyncio.run(polls.vote(owner(), value, p))
    raw.media.poll.closed = True
    p.option_ids = ["p:707:b:AP8"]
    with pytest.raises(rpc.Forbidden):
        asyncio.run(polls.vote(owner(), value, p))
    value.get_messages.return_value = row(110, tl.PeerUser(18), media=poll_media())
    with pytest.raises(rpc.NotFound):
        asyncio.run(polls.vote(owner(), value, p))
    value.assert_not_awaited()


def test_lost_or_mismatched_vote_confirmation_never_replays_or_fabricates_choice():
    raw = row(110, media=poll_media())
    value = client(raw)
    value.side_effect = OSError("synthetic-private-poll-error")
    p = dto.PollsVoteParams(account_id="a", chat_id="17", message_id="110", option_ids=["p:707:b:AP8"])
    with pytest.raises(OSError):
        asyncio.run(polls.vote(owner(), value, p))
    assert value.await_count == 1
    value.reset_mock()
    value.side_effect = None
    value.return_value = update_response(raw, tl.PollResults(results=[tl.PollAnswerVoters(b"\xff\x00", chosen=True, voters=1, recent_voters=[])]))
    with pytest.raises(rpc.Internal):
        asyncio.run(polls.vote(owner(), value, p))
    assert value.await_count == 1


def test_close_preserves_poll_header_and_confirms_actual_state_before_success():
    async def exercise():
        raw = row(110, media=poll_media(results=tl.PollResults(results=[]), quiz=True, public_voters=True))
        raw.media.poll.open_answers = True
        updated = copy.deepcopy(raw)
        updated.media.poll.closed = True
        value = client(raw)
        async def response(req):
            assert isinstance(req, functions.messages.EditMessageRequest) and req.id == 110
            assert req.media.poll.closed and req.media.poll.id == 707 and req.media.poll.hash == 88
            assert req.media.poll.question.text == raw.media.poll.question.text and req.media.poll.open_answers
            assert req.message is None and req.media.correct_answers is None
            assert bytes(req)
            return update_response(updated)
        value.side_effect = response
        result = await polls.close(owner(), value, dto.ChatMessageParams(account_id="a", chat_id="17", message_id="110"))
        assert result.message.poll.closed and not raw.media.poll.closed
        value.get_messages.return_value = updated
        value.reset_mock()
        result = await polls.close(owner(), value, dto.ChatMessageParams(account_id="a", chat_id="17", message_id="110"))
        assert result.message.poll.closed
        value.assert_not_awaited()
        raw.media.attached_media = tl.MessageMediaPhoto(photo=tl.PhotoEmpty(1))
        value.get_messages.return_value = raw
        with pytest.raises(rpc.NotSupported):
            await polls.close(owner(), value, dto.ChatMessageParams(account_id="a", chat_id="17", message_id="110"))
        value.assert_not_awaited()
    asyncio.run(exercise())


def test_sparse_live_results_refetch_with_account_and_poll_identity():
    async def exercise():
        raw = row(110, media=poll_media(minimum=True))
        first, second, value = owner(), owner(), client(raw)
        polls.observe(first, [message(raw)])
        event = tl.UpdateMessagePoll(707, tl.PollResults(min=True, total_voters=99))
        full = tl.PollResults(results=[tl.PollAnswerVoters(b"\x00\xff", chosen=True, voters=5, recent_voters=[])], total_voters=5)
        async def response(req):
            assert isinstance(req, functions.messages.GetPollResultsRequest)
            assert req.msg_id == 110 and req.poll_hash == 0 and req.peer == tl.InputPeerUser(17, 0)
            return update_response(raw, full)
        value.side_effect = response
        await raw_update(first, event, value)
        item = first.messages_upsert.await_args.args[0][0]
        assert item.id == "110" and item.chat_id == "17" and item.poll.total_voters == 5
        assert item.poll.options[0].chosen and item.poll.options[1].votes is None
        value.reset_mock()
        await raw_update(second, event, value)
        value.get_messages.assert_not_awaited()
        second.messages_upsert.assert_not_awaited()
        value.get_messages.return_value = row(110, media=tl.MessageMediaPoll(copy.copy(raw.media.poll), full))
        value.get_messages.return_value.media.poll.id = 999
        await raw_update(first, event, value)
        assert first.messages_upsert.await_count == 1
    asyncio.run(exercise())


def test_addressed_live_poll_checks_actual_topic_and_target_index_is_bounded(monkeypatch):
    async def exercise():
        raw = row(110, tl.PeerChannel(8), media=poll_media(results=tl.PollResults(results=[])),
                  reply_to=tl.MessageReplyHeader(reply_to_msg_id=42, forum_topic=True))
        value, account = client(raw), owner()
        value.get_input_entity.return_value = tl.InputPeerChannel(8, 0)
        event = tl.UpdateMessagePoll(707, tl.PollResults(), peer=tl.PeerChannel(8), msg_id=110, top_msg_id=42)
        await raw_update(account, event, value)
        assert account.messages_upsert.await_args.args[0][0].topic_id == "42"
        event.top_msg_id = 43
        await raw_update(account, event, value)
        assert account.messages_upsert.await_count == 1
        monkeypatch.setattr(polls, "MAX_TARGETS", 2)
        for mid in [111, 112]:
            changed = copy.copy(raw)
            changed.id = mid
            polls.observe(account, [message(changed)])
        assert len(account.data["poll_targets"]) == 2
        assert all(key[2] != "110" for key in account.data["poll_targets"])
    asyncio.run(exercise())


def test_poll_refresh_and_write_reject_contradictory_topic_without_applying():
    async def exercise():
        raw = row(110, tl.PeerChannel(8), media=poll_media(minimum=True),
                  reply_to=tl.MessageReplyHeader(reply_to_msg_id=42, forum_topic=True))
        account, value = owner(), client(raw)
        value.get_input_entity.return_value = tl.InputPeerChannel(8, 0)
        results = tl.PollResults(results=[tl.PollAnswerVoters(b"\x00\xff", chosen=True)])
        conflicting = tl.Updates([tl.UpdateMessagePoll(707, results, peer=tl.PeerChannel(8), msg_id=110, top_msg_id=43)], [], [], NOW, 1)
        value.return_value = conflicting
        with pytest.raises(rpc.Internal):
            await polls.fresh(value, tl.InputPeerChannel(8, 0), "-1000000000008", 110, 707)
        assert raw.media.results.min and raw.reply_to.reply_to_msg_id == 42
        polls.observe(account, [message(raw)])
        # Even a consistent outer event must not allow a contradictory scoped
        # GetPollResults reply to escape through the fallback refresh.
        event = tl.UpdateMessagePoll(707, tl.PollResults(min=True), peer=tl.PeerChannel(8), msg_id=110, top_msg_id=42)
        await raw_update(account, event, value)
        account.messages_upsert.assert_not_awaited()
        value.reset_mock()
        p = dto.PollsVoteParams(account_id="a", chat_id="-1000000000008", message_id="110", option_ids=["p:707:b:AP8"])
        with pytest.raises(rpc.Internal):
            await polls.vote(account, value, p)
        assert value.await_count == 1, "a contradictory write acknowledgement must not be replayed"
        account.messages_upsert.assert_not_awaited()
    asyncio.run(exercise())


def test_get_hydrates_sparse_poll_once_and_keeps_the_topic_barrier():
    async def exercise():
        raw = row(110, tl.PeerChannel(8), media=poll_media(minimum=True),
                  reply_to=tl.MessageReplyHeader(reply_to_msg_id=42, forum_topic=True))
        value = client(raw)
        value.get_input_entity.return_value = tl.InputPeerChannel(8, 0)
        full = tl.PollResults(results=[tl.PollAnswerVoters(b"\x00\xff", chosen=True), tl.PollAnswerVoters(b"\xff\x00")])
        response = tl.Updates([tl.UpdateMessagePoll(707, full, peer=tl.PeerChannel(8), msg_id=110, top_msg_id=42)], [], [], NOW, 1)
        value.return_value = response
        p = dto.ChatMessageParams(account_id="a", chat_id="-1000000000008", message_id="110")
        result = await get(value, p, hydrate=polls.hydrate)
        assert result.message.topic_id == "42"
        assert [a.chosen for a in result.message.poll.options] == [True, False]
        value.get_messages.assert_awaited_once()
        assert isinstance(value.await_args.args[0], functions.messages.GetPollResultsRequest)
        assert value.await_args.args[0].poll_hash == 0
        value.reset_mock()
        response.updates[0].top_msg_id = 43
        with pytest.raises(rpc.Internal):
            await get(value, p, hydrate=polls.hydrate)
        value.get_messages.assert_awaited_once()
        assert value.await_count == 1
    asyncio.run(exercise())
