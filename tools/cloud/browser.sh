#!/usr/bin/env bash
set -euo pipefail
# No provider credentials or real accounts: only synthetic UI fixtures.
chrome_bin="$(command -v google-chrome || command -v google-chrome-stable || true)"
if [ -z "$chrome_bin" ]; then echo "System Chrome missing from runner image" >&2; exit 1; fi
browser_prefix="$RUNNER_TEMP/ui-browser-tools"
npm install --prefix "$browser_prefix" --ignore-scripts --no-audit --no-fund --package-lock=false 'playwright-core@1.65.1'
export NODE_PATH="$browser_prefix/node_modules"
export CH_CHROMIUM="$chrome_bin"
export CH_SCREENSHOT_DIR="$RUNNER_TEMP/ci-result/browser"
mkdir -p "$CH_SCREENSHOT_DIR"
cd app/ui
node tests/browser/accounts.mjs
node tests/browser/actions.mjs
node tests/browser/media.mjs
node tests/browser/polls.mjs
node tests/browser/media-jobs.mjs
node tests/browser/reactions.mjs
node tests/browser/files.mjs
