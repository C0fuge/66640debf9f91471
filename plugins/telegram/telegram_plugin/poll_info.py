"""Frozen poll DTOs from complete TL poll headers and possibly sparse results."""
import base64
import hashlib

from contact_hub_sdk import types as dto
from telethon import types as tl


def option_id(value, poll_id):
    # Never treat opaque option bytes as an integer or decode them as text.
    # Large IDs still fit the frozen 256-character Id; writes resolve them
    # against the target poll's actual answers rather than reversing a hash.
    prefix = f"p:{poll_id}:"
    if len(value) <= 190:
        encoded = prefix + "b:" + base64.urlsafe_b64encode(value).decode().rstrip("=")
        if len(encoded) <= 256:
            return encoded
    return prefix + "sha256:" + hashlib.sha256(value).hexdigest()


def poll_info(media, entities_from_tl):
    if not isinstance(media, tl.MessageMediaPoll):
        return None
    poll, results = media.poll, media.results
    answers = {value.option: value for value in results.results or []}
    personal = not results.min and results.results is not None
    nobody_voted = not results.min and results.total_voters == 0
    correctness_known = personal and any(value.correct for value in answers.values())
    options = []
    for answer in poll.answers:
        result = answers.get(answer.option)
        options.append(dto.PollOption(id=option_id(answer.option, poll.id), text=answer.text.text,
                       votes=result.voters if result else None,
                       chosen=bool(result.chosen) if result is not None and personal else False if nobody_voted else None,
                       correct=True if result and result.correct else False if result is not None and correctness_known else None))
    return dto.Poll(id=str(poll.id), question=poll.question.text, options=options,
                    total_voters=results.total_voters, closed=bool(poll.closed),
                    multiple=bool(poll.multiple_choice), quiz=bool(poll.quiz), anonymous=not poll.public_voters,
                    solution=results.solution,
                    solution_entities=entities_from_tl(results.solution_entities) if results.solution_entities is not None else None,
                    close_at=poll.close_date)
