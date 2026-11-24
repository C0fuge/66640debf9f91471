// Run remotely from app/ui with playwright-core available: node tests/browser/media.mjs.
// Real App/ThreadPane, synthetic metadata only; no native picker, files or accounts.
import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import { createRequire } from "node:module";
import { join, resolve } from "node:path";
import { createServer } from "vite";
const { chromium } = createRequire(import.meta.url)("playwright-core");
const server = await createServer({ server: { host: "127.0.0.1", port: 53176, strictPort: false, open: false } });
let browser;
try {
  await server.listen();
  browser = await chromium.launch({ executablePath: process.env.CH_CHROMIUM, headless: true });
  const page = await browser.newPage({ viewport: { width: 1280, height: 1000 } });
  // Text visibility precedes ResizeObserver/virtualizer layout. Check the actual
  // rows on successive frames; never force measurements or mask persistent overlap.
  const settledRows = async label => page.evaluate(async label => {
    await document.fonts.ready;
    const deadline = performance.now() + 5000;
    let previous = "", stable = 0, snapshot;
    while (performance.now() < deadline) {
      await new Promise(resolve => requestAnimationFrame(resolve));
      const scroller = document.querySelector(".thread-scroll");
      const canvas = document.querySelector(".thread-virtual");
      const rows = [...document.querySelectorAll(".thread-row[data-index]")]
        .sort((a, b) => Number(a.dataset.index) - Number(b.dataset.index))
        .map(row => {
          const bounds = row.getBoundingClientRect();
          const bubble = row.querySelector(".thread-bubble")?.getBoundingClientRect();
          return { index: Number(row.dataset.index), top: bounds.top, bottom: bounds.bottom,
            bubbleTop: bubble?.top, bubbleBottom: bubble?.bottom, left: bubble?.left };
        });
      snapshot = { rows, scrollTop: scroller?.scrollTop, scrollLeft: scroller?.scrollLeft,
        scrollWidth: scroller?.scrollWidth, clientWidth: scroller?.clientWidth,
        canvasBottom: canvas?.getBoundingClientRect().bottom };
      const valid = scroller?.getAttribute("aria-busy") === "false" && rows.length === 6
        && rows.every((row, i) => row.index === i && row.bottom > row.top
          && row.bubbleTop >= row.top - 0.5 && row.bubbleBottom <= row.bottom + 0.5
          && Math.abs(row.left - rows[0].left) <= 0.5
          && (!i || row.top >= rows[i - 1].bottom - 0.5))
        && rows.at(-1).bottom <= snapshot.canvasBottom + 0.5
        && snapshot.scrollWidth <= snapshot.clientWidth + 1 && snapshot.scrollLeft === 0;
      const signature = JSON.stringify(snapshot);
      stable = valid ? (signature === previous ? stable + 1 : 1) : 0;
      previous = signature;
      if (stable >= 3) return snapshot;
    }
    throw new Error(`${label}: rows did not settle without overlap: ${JSON.stringify(snapshot)}`);
  }, label);
  const capture = async name => {
    await settledRows(name);
    if (!process.env.CH_SCREENSHOT_DIR) return;
    const directory = resolve(process.env.CH_SCREENSHOT_DIR);
    await mkdir(directory, { recursive: true });
    await page.screenshot({ path: join(directory, name), fullPage: true });
  };
  const errors = [];
  page.on("pageerror", error => errors.push(error.name + ": " + error.message));
  await page.route("**/src/main.tsx", route => route.fulfill({ contentType: "text/javascript", body: 'import "/tests/browser/mediaFixture.tsx";' }));
  await page.goto(server.resolvedUrls.local[0]);
  const choose = async account => {
    await page.getByRole("button", { name: `${account}, Fixture, connected`, exact: true }).click();
    await page.getByRole("button").filter({ hasText: `${account} media chat` }).click();
    await page.getByRole("heading", { name: `${account} media chat`, exact: true }).waitFor();
    await page.getByText("Voice caption", { exact: true }).waitFor();
    await settledRows(`${account} selected`);
  };
  const bubble = caption => page.getByRole("article").filter({ hasText: caption });
  const first = () => bubble("First spoiler caption");
  const second = () => bubble("Second spoiler caption");
  const reveal = row => row.getByRole("button", { name: "Reveal attachment details", exact: true });
  const body = () => page.locator("body").textContent();
  await choose("Alpha");
  assert.match(await bubble("Photo caption").textContent(), /photo\.jpg.*image\/jpeg.*2,048 bytes.*640 × 480 px/s);
  assert.match(await bubble("Voice caption").textContent(), /Voice message.*audio\/ogg.*1:05/s);
  assert.equal((await body()).includes("Alpha secret"), false);
  assert.equal((await body()).includes("Second hidden name"), false);
  assert.equal((await body()).includes("Deleted hidden"), false);
  assert.equal((await body()).includes("Local upload private"), false);
  assert.equal((await body()).includes("Local hidden MIME"), false);
  assert.equal((await body()).includes("Deleted caption"), false);
  await page.getByText("Pending caption", { exact: true }).waitFor();
  assert.equal(await reveal(bubble("Pending caption")).count(), 0);
  const initial = await settledRows("initial metadata");
  const beforeReveal = await page.evaluate(() => window.mediaFixture.requests());
  await capture("attachment-metadata-hidden.png");
  await reveal(first()).focus();
  await page.keyboard.press("Enter");
  await page.getByText("Alpha secret <img src=x onerror=fixture>.txt", { exact: true }).waitFor();
  assert.match(await first().textContent(), /First spoiler caption/);
  assert.match(await first().textContent(), /9,007,199,254,740,993 bytes/);
  assert.equal((await body()).includes("Second hidden name"), false);
  assert.equal(await page.locator(".thread-attachment img, .thread-attachment svg, .thread-attachment a").count(), 0);
  assert.deepEqual(await page.evaluate(() => window.mediaFixture.requests()), beforeReveal, "reveal is local and performs no request");
  await capture("attachment-metadata-revealed.png");
  assert.equal((await settledRows("first reveal")).scrollTop, initial.scrollTop, "reveal preserves scroll position");
  await page.evaluate(() => window.mediaFixture.changeAttachment("Alpha"));
  await reveal(first()).waitFor();
  assert.equal((await body()).includes("Alpha replaced secret"), false, "an edited attachment requires another reveal");
  await settledRows("edited attachment hidden");
  await reveal(first()).click();
  await page.getByText("Alpha replaced secret", { exact: true }).waitFor();
  await settledRows("edited attachment revealed");

  for (const width of [880, 1280]) {
    await page.setViewportSize({ width, height: 1000 });
    const layout = await settledRows(`viewport ${width}`);
    assert.equal(layout.scrollTop, initial.scrollTop, "resizing the short fixture preserves scroll position");
  }

  const bubbles = page.locator(".thread-bubble");
  const left = await bubbles.evaluateAll(rows => rows.map(row => Math.round(row.getBoundingClientRect().left)));
  assert.equal(new Set(left).size, 1, "both directions remain left aligned");
  const colours = await bubbles.evaluateAll(rows => rows.map(row => getComputedStyle(row).backgroundColor));
  await page.evaluate(() => document.documentElement.style.setProperty("--accent", "rgb(230, 20, 40)"));
  assert.deepEqual(await bubbles.evaluateAll(rows => rows.map(row => getComputedStyle(row).backgroundColor)), colours, "accent changes do not recolour bubbles");

  await choose("Beta"); // Same row/message/chat/topic IDs, different account.
  assert.equal((await body()).includes("Alpha replaced secret"), false);
  assert.equal((await body()).includes("Beta secret"), false);
  await reveal(first()).click();
  await page.getByText("Beta secret <img src=x onerror=fixture>.txt", { exact: true }).waitFor();
  await settledRows("other account revealed");
  assert.equal((await body()).includes("Second hidden name"), false);
  await choose("Alpha");
  assert.equal((await body()).includes("Alpha replaced secret"), false);
  await reveal(second()).click();
  await page.getByText("Second hidden name", { exact: true }).waitFor();
  await settledRows("second message revealed");
  assert.equal((await body()).includes("Alpha replaced secret"), false);
  assert.equal(await reveal(first()).count(), 1);
  assert.deepEqual(errors, []);
  console.log("Attachment metadata, scoped spoiler reveal and settled row geometry passed Chromium checks.");
} finally {
  await browser?.close();
  await server.close();
}
