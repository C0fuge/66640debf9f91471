#!/usr/bin/env bash
# Instantiate each plugin template into a temp dir with sample values (the same steps
# `ch plugin new` follows, see TEMPLATE_SPEC.md), point its SDK dependency at this
# checkout, and run its tests. Then uncomment the auth example and check it still builds.
#
# Usage: templates/test_templates.sh [python|rust]...   (default: both)
# KEEP=1 keeps the temp dir for inspection.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(dirname "$HERE")
TMP=$(mktemp -d "${TMPDIR:-/tmp}/ch-templates.XXXXXX")
if [[ -z "${KEEP:-}" ]]; then trap 'rm -rf "$TMP"' EXIT; else echo "keeping $TMP"; fi

# Keep the shared uv cache (it lives under XDG_CACHE_HOME) before isolating XDG_*.
export UV_CACHE_DIR=${UV_CACHE_DIR:-$(uv cache dir 2>/dev/null || echo "$TMP/uv-cache")}
for v in CH_DATA_DIR XDG_DATA_HOME XDG_CONFIG_HOME XDG_CACHE_HOME XDG_STATE_HOME; do
  export "$v=$TMP/env/${v,,}"
done
export CARGO_TARGET_DIR=$TMP/target

PLUGIN_ID=demo_chat
DISPLAY_NAME="Demo Chat"
AUTHOR="Test Author"
MODULE_NAME=demo_chat_plugin
CRATE_NAME=demo-chat-plugin

SUBST=(-e "s/{{plugin_id}}/$PLUGIN_ID/g" -e "s/{{display_name}}/$DISPLAY_NAME/g"
       -e "s/{{author}}/$AUTHOR/g" -e "s/{{module_name}}/$MODULE_NAME/g"
       -e "s/{{crate_name}}/$CRATE_NAME/g")

# instantiate <template dir> <dest>: copy, rename placeholder paths, substitute contents.
instantiate() {
  cp -r "$1" "$2"
  find "$2" -depth -name '*{{*}}*' | while read -r p; do
    mv "$p" "$(dirname "$p")/$(basename "$p" | sed "${SUBST[@]}")"
  done
  find "$2" -type f -exec sed -i "${SUBST[@]}" {} +
  if grep -rn '{{' "$2"; then echo "unsubstituted placeholders left" >&2; exit 1; fi
}

# check_manifest <plugin.json>: validate against schemas/manifest.json.
check_manifest() {
  cat > "$TMP/check_manifest.py" <<'EOF'
import json, sys
from pathlib import Path
from jsonschema import Draft202012Validator
from referencing import Registry, Resource
docs = [json.loads(p.read_text()) for p in (Path(sys.argv[1]) / "schemas").glob("*.json")]
reg = Registry().with_resources((d["$id"], Resource.from_contents(d)) for d in docs)
v = Draft202012Validator({"$ref": "https://contacthub.dev/schemas/v1/manifest.json"}, registry=reg,
                         format_checker=Draft202012Validator.FORMAT_CHECKER)
errors = [f"{list(e.path)}: {e.message}" for e in v.iter_errors(json.loads(Path(sys.argv[2]).read_text()))]
sys.exit("plugin.json: " + "; ".join(errors) if errors else 0)
EOF
  uv run --quiet --no-project --with jsonschema --with referencing python -I "$TMP/check_manifest.py" "$ROOT" "$1"
}

# sdk_source <file> <replacement line>: swap the line marked `ch:sdk-source`.
sdk_source() { sed -i "s|^.*# ch:sdk-source\$|$2|" "$1"; grep -q "$2" "$1"; }

# enable_auth <file> <comment prefix>: uncomment the BEGIN/END auth example blocks.
enable_auth() { sed -i "/BEGIN auth example/,/END auth example/{/auth example/!s|^\\([[:space:]]*\\)$2 \\?|\\1|}" "$1"; }

test_python() {
  local d=$TMP/python/$PLUGIN_ID
  mkdir -p "$TMP/python"
  instantiate "$HERE/python" "$d"
  check_manifest "$d/plugin.json"
  sdk_source "$d/pyproject.toml" "contact-hub-sdk = { path = \"$ROOT/sdk/python\" }"
  (cd "$d" && uv sync --quiet && uv run --quiet pytest -q)
  enable_auth "$d/src/$MODULE_NAME/plugin.py" "#"
  (cd "$d" && uv run --quiet python -c \
    "from $MODULE_NAME import PlatformPlugin as P; assert P().get_capabilities().get('auth.interactive'); print('python auth example: ok')")
}

test_rust() {
  local d=$TMP/rust/$PLUGIN_ID
  mkdir -p "$TMP/rust"
  instantiate "$HERE/rust" "$d"
  check_manifest "$d/plugin.json"
  sdk_source "$d/Cargo.toml" "ch-plugin-sdk = { path = \"$ROOT/crates/ch-plugin-sdk\" }"
  (cd "$d" && cargo test --quiet && cargo clippy --quiet --all-targets -- -D warnings)
  enable_auth "$d/src/main.rs" "//"
  (cd "$d" && cargo clippy --quiet -- -D warnings && echo "rust auth example: ok")
}

targets=("$@")
[[ ${#targets[@]} -gt 0 ]] || targets=(python rust)
for t in "${targets[@]}"; do
  echo "== $t template"
  "test_$t"
done
echo "== all template checks passed"
