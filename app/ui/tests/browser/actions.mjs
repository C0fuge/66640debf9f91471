// With playwright-core available, run from app/ui: node tests/browser/actions.mjs.
// Uses the real App/ThreadPane/dialog/Composer and synthetic in-memory CoreClient.
import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import { createRequire } from "node:module";
import { join, resolve } from "node:path";
import { createServer } from "vite";
const { chromium } = createRequire(import.meta.url)("playwright-core");
const server = await createServer({ server: { host: "127.0.0.1", port: 53175, strictPort: false, open: false } });
let browser;
let page;
const screenshotDir = process.env.CH_SCREENSHOT_DIR ? resolve(process.env.CH_SCREENSHOT_DIR) : null;
try {
  await server.listen();
  browser = await chromium.launch({ executablePath: process.env.CH_CHROMIUM, headless: true });
  page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  const errors = [];
  page.on("pageerror", error => errors.push(error.name + ": " + error.message));
  await page.route("**/src/main.tsx", route => route.fulfill({ contentType: "text/javascript", body: 'import "/tests/browser/actionFixture.tsx";' }));
  await page.goto(server.resolvedUrls.local[0]);
  const button = name => page.getByRole("button", { name, exact: true });
  const dialog = page.getByRole("dialog");
  const outgoing = () => page.getByRole("article", { name: "You", exact: true }).filter({ hasText: "Alpha" });
  const incoming = () => page.getByRole("article", { name: "Fixture sender", exact: true });
  const calls = () => page.evaluate(() => window.actionFixture.calls());
  await page.getByRole("button").filter({ hasText: "Alpha chat" }).click();
  const composer = page.getByRole("textbox", { name: "Message", exact: true });
  await composer.fill("Alpha draft stays");
  const leftEdges = await page.locator(".thread-bubble").evaluateAll(bubbles => bubbles.map(bubble => bubble.getBoundingClientRect().left));
  assert.equal(new Set(leftEdges.map(left => Math.round(left))).size, 1, "incoming and outgoing messages stay left aligned");
  await outgoing().getByRole("button", { name: "Edit", exact: true }).click();
  await page.getByRole("textbox", { name: "Message text", exact: true }).fill("Alpha replacement 😀");
  if (screenshotDir) {
    await mkdir(screenshotDir, { recursive: true });
    await page.screenshot({ path: join(screenshotDir, "message-action-edit.png"), fullPage: true });
  }
  await dialog.getByRole("button", { name: "Edit message", exact: true }).click();
  await dialog.waitFor({ state: "hidden" });
  await page.getByText("Alpha replacement 😀", { exact: true }).waitFor();
  assert.equal(await composer.inputValue(), "Alpha draft stays");
  assert.deepEqual((await calls())[0], { method: "editText", params: { thread: { accountId: "Alpha", chatId: "chat", topicId: null }, messageId: "outgoing", text: "Alpha replacement 😀", entities: [] } });

  await outgoing().getByRole("button", { name: "Pin", exact: true }).click();
  await page.getByLabel("Pin silently", { exact: true }).check();
  await dialog.getByRole("button", { name: "Pin message", exact: true }).click();
  await dialog.waitFor({ state: "hidden" });
  await outgoing().getByRole("button", { name: "Unpin", exact: true }).click();
  await dialog.getByRole("button", { name: "Unpin message", exact: true }).click();
  await dialog.waitFor({ state: "hidden" });
  assert.equal((await calls())[1].params.silent, true);
  assert.equal((await calls())[2].method, "unpinMessage");

  const provisional = page.getByRole("article", { name: "You", exact: true }).filter({ hasText: "Provisional" });
  assert.equal(await provisional.getByRole("button", { name: "Edit", exact: true }).isDisabled(), true);
  assert.equal(await provisional.getByRole("button", { name: "Delete", exact: true }).isDisabled(), true);
  assert.equal(await incoming().getByRole("button", { name: "Edit", exact: true }).count(), 0);
  await incoming().getByRole("button", { name: "Delete", exact: true }).click();
  assert.equal(await page.getByLabel("Only me", { exact: true }).count(), 0);
  assert.equal(await dialog.getByRole("button", { name: "Delete message", exact: true }).isDisabled(), true);
  await page.keyboard.press("Escape");
  await dialog.waitFor({ state: "hidden" });
  assert.equal(await incoming().getByRole("button", { name: "Delete", exact: true }).evaluate(el => el === document.activeElement), true);

  // A pending Alpha mutation survives a programmatic account switch; its late
  // completion cannot close Beta's dialog or overwrite either composer draft.
  await page.evaluate(() => window.actionFixture.next("hold"));
  await outgoing().getByRole("button", { name: "Pin", exact: true }).click();
  await dialog.getByRole("button", { name: "Pin message", exact: true }).click();
  await page.waitForFunction(() => window.actionFixture.calls().length === 4);
  assert.equal(await dialog.getByRole("button", { name: "Applying…", exact: true }).isDisabled(), true);
  await page.evaluate(() => document.querySelector('[aria-label="Beta, Fixture, connected"]').click());
  await dialog.waitFor({ state: "hidden" });
  await page.getByRole("button").filter({ hasText: "Beta chat" }).click();
  await composer.fill("Beta draft stays");
  const betaOutgoing = page.getByRole("article", { name: "You", exact: true }).filter({ hasText: "Beta original" });
  await betaOutgoing.getByRole("button", { name: "Edit", exact: true }).click();
  await page.evaluate(() => window.actionFixture.release());
  assert.equal(await dialog.isVisible(), true);
  assert.equal(await page.getByRole("textbox", { name: "Message text", exact: true }).inputValue(), "Beta original 😀");
  await page.keyboard.press("Escape");
  assert.equal(await composer.inputValue(), "Beta draft stays");
  await button("Alpha, Fixture, connected").click();
  await page.getByRole("button").filter({ hasText: "Alpha chat" }).click();
  assert.equal(await composer.inputValue(), "Alpha draft stays");

  await page.evaluate(() => { window.actionFixture.capability("delete.for_me", true); window.actionFixture.next("unknown"); });
  await incoming().getByRole("button", { name: "Delete", exact: true }).click();
  await page.getByLabel("Only me", { exact: true }).check();
  await dialog.getByRole("button", { name: "Delete message", exact: true }).click();
  await dialog.waitFor({ state: "hidden" });
  await page.getByText(/The action may have completed/).waitFor();
  assert.equal((await page.locator("body").textContent()).includes("sensitive fixture"), false);
  assert.deepEqual((await calls())[4], { method: "deleteMessages", params: { thread: { accountId: "Alpha", chatId: "chat", topicId: "topic-1" }, messageIds: ["incoming"], revoke: false } });
  assert.equal(await outgoing().getByRole("button", { name: "Edit", exact: true }).isDisabled(), true);
  await page.evaluate(() => window.actionFixture.historyAvailable(false));
  await button("Reload before another action").click();
  await page.getByText("The connection is unavailable.", { exact: true }).waitFor();
  assert.equal(await outgoing().getByRole("button", { name: "Edit", exact: true }).isDisabled(), true);
  await page.evaluate(() => window.actionFixture.historyAvailable(true));
  await button("Reload before another action").click();
  await page.getByText("History reloaded. Review the message before choosing another action.", { exact: true }).waitFor();
  assert.equal(await incoming().count(), 0);
  assert.equal((await calls()).length, 5, "reload must not replay the deletion");
  assert.equal(await composer.inputValue(), "Alpha draft stays");
  await page.evaluate(() => window.actionFixture.capability("send.entities", false));
  await page.waitForFunction(() => [...document.querySelectorAll('.thread-message-actions button')].every(el => el.textContent !== "Edit"));
  assert.deepEqual(errors, []);
  console.log("Message actions passed Chromium checks.");
} catch (error) {
  if (page && screenshotDir) {
    await mkdir(screenshotDir, { recursive: true })
      .then(() => page.screenshot({ path: join(screenshotDir, "message-actions-failure.png"), fullPage: true }))
      .catch(() => {});
  }
  throw error;
} finally {
  await browser?.close();
  await server.close();
}
