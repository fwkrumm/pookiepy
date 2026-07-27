#!/usr/bin/env bash
set -euo pipefail

VERSION="$(python - <<'PY'
import pathlib
import re

text = pathlib.Path('__init__.py').read_text(encoding='utf-8')
match = re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']\s*$', text, re.MULTILINE)
if not match:
    raise SystemExit('Could not find __version__ in __init__.py')
print(match.group(1))
PY
)"

TAG="v${VERSION}"

git fetch --tags

if git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null; then
  TAG_SHA="$(git rev-list -n 1 "refs/tags/${TAG}")"
  HEAD_SHA="$(git rev-parse HEAD)"

  if [ "$TAG_SHA" = "$HEAD_SHA" ]; then
    echo "Tag ${TAG} already points to this commit; nothing to do."
    exit 0
  fi

  echo "Tag ${TAG} already exists on a different commit (${TAG_SHA}); refusing to move it."
  exit 1
fi

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git tag -a "$TAG" -m "Release $TAG"
git push origin "$TAG"