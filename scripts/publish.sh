#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# publish.sh — Build and upload dlis-builder to a private Artifactory PyPI repo
#
# Required environment variables:
#   ARTIFACTORY_USERNAME  — Artifactory username
#   ARTIFACTORY_PASSWORD  — Artifactory API key
#   ARTIFACTORY_REPO_URL  — Upload endpoint (no /simple suffix)
#                           e.g. https://artifactory.g42.ae/artifactory/api/pypi/energy-pypi-internal
#
# Optional:
#   ARTIFACTORY_INDEX_URL — pip index URL (/simple suffix)
#                           Defaults to ${ARTIFACTORY_REPO_URL}/simple
#
# Usage:
#   export ARTIFACTORY_USERNAME=...
#   export ARTIFACTORY_PASSWORD=...
#   export ARTIFACTORY_REPO_URL=https://artifactory.g42.ae/artifactory/api/pypi/energy-pypi-internal
#   bash scripts/publish.sh
# ---------------------------------------------------------------------------

set -euo pipefail

# ── Validate required variables ───────────────────────────────────────────────
if [[ -z "${ARTIFACTORY_USERNAME:-}" ]] || \
   [[ -z "${ARTIFACTORY_PASSWORD:-}" ]] || \
   [[ -z "${ARTIFACTORY_REPO_URL:-}" ]]; then
    echo "ERROR: Must set ARTIFACTORY_USERNAME, ARTIFACTORY_PASSWORD, ARTIFACTORY_REPO_URL" >&2
    exit 1
fi

ARTIFACTORY_INDEX_URL="${ARTIFACTORY_INDEX_URL:-${ARTIFACTORY_REPO_URL}/simple}"
PACKAGE_NAME="dlis-builder"

# ── Read library version ──────────────────────────────────────────────────────
echo "Reading library version..."
LIB_VERSION=$(python -c "
import re, pathlib
text = pathlib.Path('src/dlis_builder/_version.py').read_text()
print(re.search(r'\"(.+?)\"', text).group(1))
")
echo "Library version: ${LIB_VERSION}"

# ── Write .netrc so pip can reach the private index ──────────────────────────
ARTIFACTORY_HOST=$(echo "${ARTIFACTORY_REPO_URL}" | sed -E 's|https?://([^/]+)/.*|\1|')
echo -e "machine ${ARTIFACTORY_HOST}\nlogin ${ARTIFACTORY_USERNAME}\npassword ${ARTIFACTORY_PASSWORD}" > ~/.netrc
chmod 600 ~/.netrc

# ── Check whether this version is already published ──────────────────────────
echo "Checking Artifactory for existing versions..."
EXISTING=$(pip install "${PACKAGE_NAME}==" \
    --extra-index-url "${ARTIFACTORY_INDEX_URL}" 2>&1 || true)

if echo "${EXISTING}" | grep -qF "${LIB_VERSION}"; then
    echo "Version ${LIB_VERSION} is already published — skipping build."
    exit 0
fi

# ── Install build tools ───────────────────────────────────────────────────────
pip install --upgrade build twine

# ── Clean previous dist/ artefacts ───────────────────────────────────────────
rm -rf dist/

# ── Build wheel + sdist ───────────────────────────────────────────────────────
echo "Building ${PACKAGE_NAME} ${LIB_VERSION}..."
python -m build
echo "Built artefacts:"
ls -lh dist/

# ── Upload to Artifactory ─────────────────────────────────────────────────────
echo "Uploading to ${ARTIFACTORY_REPO_URL}..."
twine upload \
    --repository-url "${ARTIFACTORY_REPO_URL}" \
    -u "${ARTIFACTORY_USERNAME}" \
    -p "${ARTIFACTORY_PASSWORD}" \
    dist/*

echo "Successfully published ${PACKAGE_NAME} ${LIB_VERSION}"
