"""Deterministic auth emission races; runnable with stdlib unittest or pytest."""

import asyncio
import unittest
from datetime import UTC, datetime

from contact_hub_sdk import AuthFlow


class Account:
    def __init__(self, emit):
        self.auth_step = emit
        self.id = "synthetic-account"
        self.running = True

    def spawn(self, coro, name=None):
        return asyncio.create_task(coro, name=name)


class AuthFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_submit_synchronously_while_emitting_step(self):
        expires = datetime(2026, 1, 1, tzinfo=UTC)
        seen = []

        async def emit(step):
            seen.append(step)
            self.assertTrue(flow.submit(step.step_id, "synthetic-code"))

        flow = AuthFlow(Account(emit), "sign_in")
        result = await flow.ask(
            "code", title="Verify", hint="Synthetic hint", placeholder="Code",
            error="Try again", can_resend=True, min_length=4, max_length=6,
            expires_at=expires,
        )
        self.assertEqual(result, "synthetic-code")
        self.assertEqual(len(seen), 1)
        step = seen[0]
        self.assertEqual((step.flow, step.kind, step.title), ("sign_in", "code", "Verify"))
        self.assertEqual((step.hint, step.placeholder, step.error), ("Synthetic hint", "Code", "Try again"))
        self.assertEqual((step.can_resend, step.min_length, step.max_length), (True, 4, 6))
        self.assertEqual(step.expires_at, expires)
        self.assertFalse(flow.submit(step.step_id, "duplicate"))

    async def test_submit_while_emission_is_still_awaiting(self):
        emitted, release = asyncio.Event(), asyncio.Event()
        steps = []

        async def emit(step):
            steps.append(step)
            emitted.set()
            await release.wait()

        flow = AuthFlow(Account(emit), "sign_in")
        task = asyncio.create_task(flow.ask("password"))
        await asyncio.wait_for(emitted.wait(), 1)
        self.assertTrue(flow.submit(steps[0].step_id, "synthetic-password"))
        self.assertFalse(task.done(), "the emission still has to finish")
        release.set()
        self.assertEqual(await asyncio.wait_for(task, 1), "synthetic-password")
        self.assertFalse(flow.submit(steps[0].step_id, "stale"))

    async def test_emission_failure_rejects_late_submission_and_next_step_works(self):
        steps = []

        async def emit(step):
            steps.append(step)
            if len(steps) == 1:
                raise RuntimeError("synthetic emission failure")
            self.assertTrue(flow.submit(step.step_id, "next-code"))

        flow = AuthFlow(Account(emit), "sign_in")
        with self.assertRaisesRegex(RuntimeError, "synthetic emission failure"):
            await flow.ask("code")
        self.assertFalse(flow.submit(steps[0].step_id, "late-code"))
        self.assertEqual(await flow.ask("code"), "next-code")
        self.assertNotEqual(steps[0].step_id, steps[1].step_id)
        self.assertFalse(flow._pending)

    async def test_cancel_during_emission_does_not_leave_a_waiter(self):
        emitted = asyncio.Event()
        steps = []

        async def emit(step):
            steps.append(step)
            emitted.set()
            await asyncio.Event().wait()

        flow = AuthFlow(Account(emit), "sign_in")
        task = flow.start(flow.ask("phone"))
        await asyncio.wait_for(emitted.wait(), 1)
        flow.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        self.assertFalse(flow.running)
        self.assertFalse(flow.submit(steps[0].step_id, "late-phone"))
        self.assertFalse(flow._pending)

    async def test_cancel_ask_task_during_emission_rejects_late_submit(self):
        emitted = asyncio.Event()
        steps = []

        async def emit(step):
            steps.append(step)
            emitted.set()
            await asyncio.Event().wait()

        flow = AuthFlow(Account(emit), "sign_in")
        task = asyncio.create_task(flow.ask("code"))
        await asyncio.wait_for(emitted.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        self.assertFalse(flow.submit(steps[0].step_id, "late-code"))
        self.assertFalse(flow._pending)

    async def test_old_emission_cleanup_does_not_remove_replacement_waiter(self):
        first_emitted, second_emitted = asyncio.Event(), asyncio.Event()
        release_first, release_second = asyncio.Event(), asyncio.Event()
        steps = []

        async def emit(step):
            steps.append(step)
            if len(steps) == 1:
                first_emitted.set()
                await release_first.wait()
            else:
                second_emitted.set()
                await release_second.wait()

        flow = AuthFlow(Account(emit), "sign_in")
        first = asyncio.create_task(flow.ask("code"))
        await asyncio.wait_for(first_emitted.wait(), 1)
        second = asyncio.create_task(flow.ask("code"))
        await asyncio.wait_for(second_emitted.wait(), 1)
        self.assertFalse(flow.submit(steps[0].step_id, "old-code"))
        release_first.set()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(first, 1)
        self.assertTrue(flow.submit(steps[1].step_id, "replacement-code"))
        release_second.set()
        self.assertEqual(await asyncio.wait_for(second, 1), "replacement-code")
        self.assertFalse(flow._pending)

    async def test_cancel_one_flow_preserves_another_flow_on_the_same_account(self):
        steps = {}
        both_emitted = asyncio.Event()

        async def emit(step):
            steps[step.flow] = step
            if len(steps) == 2:
                both_emitted.set()

        account = Account(emit)
        sign_in = AuthFlow(account, "sign_in")
        two_factor = AuthFlow(account, "two_factor")
        answers = []

        async def independent_prompt():
            answers.append(await two_factor.ask("password"))

        first = sign_in.start(sign_in.ask("code"))
        second = two_factor.start(independent_prompt())
        await asyncio.wait_for(both_emitted.wait(), 1)
        sign_in.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(first, 1)
        self.assertTrue(two_factor.submit(steps["two_factor"].step_id, "independent-password"))
        await asyncio.wait_for(second, 1)
        self.assertEqual(answers, ["independent-password"])
        self.assertFalse(sign_in.submit(steps["sign_in"].step_id, "late-code"))


if __name__ == "__main__":
    unittest.main()
