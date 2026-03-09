# Contributing

Thank you for considering contributing to dlis-builder!

## Getting started

```bash
git clone https://github.com/<your-org>/dlis-builder.git
cd dlis-builder

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -e ".[dev]"
```

## Running the tests

```bash
pytest                             # all tests
pytest --cov=dlis_builder          # with coverage report
```

## Code style

Linting is enforced by [Ruff](https://docs.astral.sh/ruff/):

```bash
ruff check src/          # lint
ruff check src/ --fix    # auto-fix safe issues
```

Type checking uses [mypy](https://mypy.readthedocs.io/):

```bash
mypy src/
```

CI runs both on every pull request.  Please fix any new errors before opening a PR.

## Pull requests

1. Fork the repository and create a branch from `main`.
2. Add tests for new behaviour — aim to keep overall coverage ≥ 75 %.
3. Run `pytest` and `ruff check src/` locally before pushing.
4. Open a pull request with a clear description of what changed and why.

## Reporting bugs

Open an issue with:
- Python version and OS
- Minimum reproducible example
- Full traceback

## Versioning

This project follows [Semantic Versioning](https://semver.org/).  Breaking
changes bump the major version; new backward-compatible features bump minor;
bug fixes bump patch.
