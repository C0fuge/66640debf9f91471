// Remote Chromium only. Actual App, virtualized chat rows and synthetic whole-chat observations.
import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import { createRequire } from "node:module";
import { join, resolve } from "node:path";
import { createServer } from "vite";
const { chromium } = createRequire(import.meta.url)("playwright-core");
const server = await createServer({ server: { host: "127.0.0.1", port: 53181, strictPort: false, open: false } });
let browser;
try {
  await server.listen();
  browser = await chromium.launch({ executablePath: process.env.CH_CHROMIUM, headless: true });
  const page = await browser.newPage({ viewport: { width: 1280, height: 1000 } });
  const errors = [];
  page.on("pageerror", error => errors.push(error.name + ": " + error.message));
  await page.route("**/src/main.tsx", route => route.fulfill({ contentType: "text/javascript", body: 'import "/tests/browser/chatFlagFixture.tsx";' }));
  await page.goto(server.resolvedUrls.local[0]);
  const button = name => page.getByRole("button", { name, exact: true });
  const menu = (account = "Alpha", id = 0) => button(`Chat preferences for ${account} dialog ${id}`);
  const dialog = page.getByRole("dialog");
  const control = name => dialog.getByRole("button", { name, exact: true });
  const refresh = () => control("Refresh flags").click();
  const requests = () => page.evaluate(() => window.flagFixture.requests());
  const snap = async file => {
    if (!process.env.CH_SCREENSHOT_DIR) return;
    const dir = resolve(process.env.CH_SCREENSHOT_DIR); await mkdir(dir, { recursive: true });
    await page.screenshot({ path: join(dir, file) });
  };
  await menu().focus(); await menu().press("Enter"); await dialog.waitFor();
  assert.equal(await control("Close").evaluate(el => el === document.activeElement), true);
  assert.deepEqual(await requests(), []);
  await refresh(); await control("Pin chat").waitFor(); await snap("chat-flags-keyboard.png");
  for (const action of ["Pin chat", "Unpin chat", "Mark as unread", "Clear unread mark"]) {
    await control(action).click(); await dialog.getByRole("status").waitFor(); await refresh();
  }
  assert.deepEqual((await requests()).filter(r => r.method === "setChatFlag").map(r => [r.params.flag, r.params.value]),
    [["pinned", true], ["pinned", false], ["markedUnread", true], ["markedUnread", false]]);
  await page.evaluate(() => window.flagFixture.lose());
  await control("Pin chat").evaluate(el => { el.click(); el.click(); });
  await dialog.getByText("Pinned · Outcome unknown").waitFor();
  await page.evaluate(() => window.flagFixture.cache());
  await dialog.press("Escape"); assert.equal(await menu().evaluate(el => el === document.activeElement), true);
  await page.locator(".chat-scroll").evaluate(el => { el.scrollTop = el.scrollHeight; });
  await menu("Alpha", 99).waitFor(); await page.locator(".chat-scroll").evaluate(el => { el.scrollTop = 0; });
  await menu().click(); await dialog.getByText("Pinned · Outcome unknown").waitFor();
  await page.evaluate(() => window.flagFixture.partial(true)); await refresh(); await control("Mark as unread").waitFor();
  await dialog.getByText("Pinned · Outcome unknown").waitFor(); await snap("chat-flags-unknown.png");
  await page.evaluate(() => { window.flagFixture.partial(false); window.flagFixture.reset(); });
  await dialog.waitFor({ state: "hidden" }); await menu().click(); await dialog.getByText("Pinned · Outcome unknown").waitFor();
  await refresh(); await control("Unpin chat").waitFor();
  await page.evaluate(() => window.flagFixture.delay()); await refresh();
  const count = (await requests()).length;
  await page.getByRole("searchbox").fill("dialog 99"); await dialog.waitFor({ state: "hidden" });
  await button("Beta, Fixture, connected").click(); await menu("Beta").click();
  await page.evaluate(() => window.flagFixture.release());
  assert.equal((await requests()).length, count); assert.equal(await control("Unpin chat").count(), 0);
  await refresh(); await control("Pin chat").waitFor();
  assert.deepEqual((await requests()).at(-1).params.thread, { accountId: "Beta", chatId: "0", topicId: null });
  await page.evaluate(() => window.flagFixture.disableManual());
  await control("Pin chat").waitFor({ state: "hidden" }); await refresh();
  assert.equal(await control("Mark as unread").isDisabled(), true);
  await control("Close").click(); await button("Archive").click();
  await menu("Beta", 100).click(); await refresh(); await control("Pin chat").waitFor();
  await snap("chat-flags-archive.png");
  assert.equal((await requests()).filter(r => r.method === "setChatFlag").length, 5);
  assert.deepEqual(errors, []);
  console.log("Whole-chat preference controls passed Chromium checks.");
} finally { await browser?.close(); await server.close(); }
