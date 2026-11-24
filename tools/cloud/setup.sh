#!/usr/bin/env bash
set -euo pipefail
export UV_PYTHON_INSTALL_DIR="$RUNNER_TEMP/ci-python"
printf 'UV_PYTHON_INSTALL_DIR=%s\n' "$UV_PYTHON_INSTALL_DIR" >> "$GITHUB_ENV"
python3 -m venv "$RUNNER_TEMP/ci-uv"
"$RUNNER_TEMP/ci-uv/bin/pip" --disable-pip-version-check install 'uv==0.13.9'
echo "$RUNNER_TEMP/ci-uv/bin" >> "$GITHUB_PATH"
"$RUNNER_TEMP/ci-uv/bin/uv" python install 3.13
