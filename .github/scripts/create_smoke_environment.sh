#!/usr/bin/env bash
set -euo pipefail

venv_dir="$RUNNER_TEMP/pookiepy-skeleton-venv"
uv venv "$venv_dir" --python "$UV_PYTHON"
if [[ "$RUNNER_OS" == "Windows" ]]; then
  venv_python="$venv_dir/Scripts/python.exe"
else
  venv_python="$venv_dir/bin/python"
fi
echo "POOKIEPY_VENV_PYTHON=$venv_python" >> "$GITHUB_ENV"