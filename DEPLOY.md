# Deploying dlis-builder to a Private Artifactory

## Overview

This document covers:
1. Building distributable packages (wheel + sdist)
2. Configuring pip to publish to Artifactory
3. Installing from Artifactory in other projects
4. CI/CD integration

---

## 1. Prerequisites

```bash
pip install build twine
```

---

## 2. Build the Package

From the `dlis_builder/` directory:

```bash
cd dlis_builder/

# Build both wheel (.whl) and source distribution (.tar.gz)
python -m build

# Output is placed in dist/
ls dist/
# dlis_builder-1.0.0-py3-none-any.whl
# dlis_builder-1.0.0.tar.gz
```

---

## 3. Configure Artifactory Credentials

### Option A — ~/.pypirc (per-user, local dev)

Create or edit `~/.pypirc`:

```ini
[distutils]
index-servers =
    artifactory

[artifactory]
repository: https://your-company.jfrog.io/artifactory/api/pypi/<REPO_NAME>/
username: <ARTIFACTORY_USERNAME>
password: <ARTIFACTORY_API_KEY>
```

> Use an API key (not your password) from **User Profile → API Key** in the
> Artifactory UI.

### Option B — Environment Variables (CI/CD recommended)

```bash
export TWINE_REPOSITORY_URL="https://your-company.jfrog.io/artifactory/api/pypi/<REPO_NAME>/"
export TWINE_USERNAME="<ARTIFACTORY_USERNAME>"
export TWINE_PASSWORD="<ARTIFACTORY_API_KEY>"
```

---

## 4. Publish to Artifactory

```bash
# Using ~/.pypirc
twine upload --repository artifactory dist/*

# Using environment variables (no pypirc needed)
twine upload --repository-url "$TWINE_REPOSITORY_URL" dist/*
```

---

## 5. Install from Artifactory in Other Projects

```bash
pip install dlis-builder \
    --index-url "https://<USER>:<API_KEY>@your-company.jfrog.io/artifactory/api/pypi/<REPO_NAME>/simple"
```

For persistent configuration, add to your project's `pip.conf` or `requirements.txt`:

**pip.conf / pip.ini:**
```ini
[global]
index-url = https://<USER>:<API_KEY>@your-company.jfrog.io/artifactory/api/pypi/<REPO_NAME>/simple
```

**requirements.txt:**
```
--index-url https://<USER>:<API_KEY>@your-company.jfrog.io/artifactory/api/pypi/<REPO_NAME>/simple
dlis-builder==1.0.0
dlis-builder[las]==1.0.0     # include lasio
dlis-builder[all]==1.0.0     # include all optional dependencies
```

**pyproject.toml in a consumer project:**
```toml
[tool.pip]
index-url = "https://<USER>:<API_KEY>@your-company.jfrog.io/artifactory/api/pypi/<REPO_NAME>/simple"

[project.dependencies]
dlis-builder = ">=1.0.0"
```

---

## 6. CI/CD Pipeline (GitHub Actions example)

```yaml
# .github/workflows/publish.yml
name: Build and Publish to Artifactory

on:
  push:
    tags: ["v*.*.*"]

jobs:
  publish:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: dlis_builder/

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install build tools
        run: pip install build twine

      - name: Build wheel + sdist
        run: python -m build

      - name: Publish to Artifactory
        env:
          TWINE_REPOSITORY_URL: ${{ secrets.ARTIFACTORY_URL }}
          TWINE_USERNAME:        ${{ secrets.ARTIFACTORY_USER }}
          TWINE_PASSWORD:        ${{ secrets.ARTIFACTORY_API_KEY }}
        run: twine upload dist/*
```

Required repository secrets:

| Secret | Value |
|--------|-------|
| `ARTIFACTORY_URL` | `https://your-company.jfrog.io/artifactory/api/pypi/<REPO_NAME>/` |
| `ARTIFACTORY_USER` | Artifactory service account username |
| `ARTIFACTORY_API_KEY` | API key from the service account |

---

## 7. Versioning

Update the version in one place — `src/dlis_builder/_version.py`:

```python
__version__ = "1.1.0"
```

`pyproject.toml` reads this via `hatchling`'s dynamic version source (or set
it statically in `[project] version = "1.1.0"`).

Tag the release:

```bash
git tag v1.1.0
git push origin v1.1.0
```

The CI pipeline triggers on the tag and publishes automatically.

---

## 8. Dependency Extras

`dlis-builder` uses optional dependency groups to keep the install minimal:

| Extra | Installs | Use case |
|-------|---------|---------|
| *(none)* | `numpy`, `dliswriter` | Programmatic builder only |
| `[las]` | + `lasio` | LAS → DLIS conversion |
| `[csv]` | + `pandas` | CSV → DLIS conversion |
| `[all]` | + `lasio`, `pandas` | All converters |
| `[verify]` | + `dlisio` | DLIS verification / round-trip tests |
| `[dev]` | All + `pytest`, `mypy`, `ruff` | Development |

Install with extras from Artifactory:

```bash
pip install "dlis-builder[all]" \
    --index-url "https://<USER>:<API_KEY>@your-company.jfrog.io/..."
```
