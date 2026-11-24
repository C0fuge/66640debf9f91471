// With playwright-core available, run from app/ui: node tests/browser/accounts.mjs.
// CH_CHROMIUM may point to a system Chromium executable.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { createServer } from "vite";
const { chromium } = createRequire(import.meta.url)("playwright-core");
const server = await createServer({ server: { host: "127.0.0.1", port: 53174, strictPort: false, open: false } });
let browser;
try {
  await server.listen();
  browser = await chromium.launch({ executablePath: process.env.CH_CHROMIUM, headless: true });
  const page = await browser.newPage();
  const errors = [];
  page.on("pageerror", err => errors.push(err.name));
  // Only the fixture entry point is substituted. Vite serves the actual app and styles.
  await page.route("**/src/main.tsx", route => route.fulfill({ contentType: "text/javascript",
    body: 'import "/tests/browser/accountFixture.tsx";' }));
  await page.goto(server.resolvedUrls.local[0]);
  const button = name => page.getByRole("button", { name, exact: true });
  const dialog = page.getByRole("dialog");
  await button("Account settings").click();
  await page.getByLabel("Name", { exact: true }).fill("edited");
  assert.equal(await page.getByLabel("token", { exact: true }).inputValue(), "");
  await page.evaluate(() => window.accountFixture.refresh());
  await page.waitForFunction(() => document.activeElement?.value === "edited");
  await button("Save and restart").click();
  await dialog.waitFor({ state: "hidden" });
  assert.deepEqual(await page.evaluate(() => window.accountFixture.saved()), { name: "edited" });
  await button("Account settings").click();
  await page.getByLabel("Clear stored value").check();
  await button("Save and restart").click();
  await dialog.waitFor({ state: "hidden" });
  assert.equal(await page.evaluate(() => window.accountFixture.saved().token), null);

  await button("Sign in").click();
  await page.getByLabel("Sign-in method").selectOption("phone_code");
  assert.equal(await page.evaluate(() => window.accountFixture.begins()), 0);
  await button("Start sign-in").click();
  await page.getByLabel("Phone number").fill("+15550100000");
  await button("Continue").click();
  await page.getByLabel("Sign-in code").fill("12345");
  await page.evaluate(() => window.accountFixture.step("Alpha", "code", "replacement"));
  await page.waitForFunction(() => document.querySelector("dialog input")?.value === "");
  await page.getByLabel("Sign-in code").fill("12345");
  await page.keyboard.press("Escape");
  await dialog.waitFor({ state: "hidden" });
  assert.equal(await button("Sign in").evaluate(el => el === document.activeElement), true);
  await button("Sign in").click();
  assert.equal(await page.getByLabel("Sign-in code").inputValue(), "");
  await page.evaluate(() => window.accountFixture.step("Alpha", "password", "password-step"));
  const password = page.getByLabel("Password", { exact: true });
  await password.fill("fixture-only");
  assert.equal(await password.getAttribute("type"), "password");
  await page.keyboard.press("Control+k");
  assert.equal(await password.evaluate(el => el === document.activeElement), true);
  await page.keyboard.press("Escape");
  await button("Beta, Fixture, auth_required").click();
  await button("Sign in").click();
  await page.getByLabel("Sign-in method").selectOption("qr");
  await button("Start sign-in").click();
  await page.getByRole("img", { name: "Scan to sign in" }).waitFor();
  assert.equal((await dialog.textContent()).includes("ch-fixture://"), false);
  await button("Cancel sign-in").click();
  await button("Start sign-in").waitFor();
  await page.keyboard.press("Escape");
  await button("Add account").click();
  await page.getByLabel("Account label").fill("Gamma");
  await dialog.getByRole("button", { name: "Add account", exact: true }).click();
  await dialog.waitFor({ state: "hidden" });
  assert.equal(await button("Gamma, Fixture, auth_required").getAttribute("aria-pressed"), "true");
  assert.deepEqual(errors, []);
  console.log("Account dialogs passed Chromium checks.");
} finally {
  await browser?.close();
  await server.close();
}
