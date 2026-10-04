# Repository Guidelines

## Project Structure & Module Organization

This is a Python MovieLens analytics project. Application code belongs in `src/de_upskills_mock_prj/`; keep reusable ingestion, profiling, and transformation logic in importable modules there. `analytics.ipynb` is the analysis notebook. Assignment context lives in `data/` and `Mock Project.md`. Large source datasets should remain in `data/MovieLens/` and must not be copied into source modules or committed unless specifically needed. Add automated checks under `tests/` as the project grows.

## Build, Test, and Development Commands

Use `uv` to keep the environment consistent with `pyproject.toml` and `uv.lock`:

- `uv sync` installs the project and dependencies into the local environment.
- `uv run python -m de_upskills_mock_prj` runs the package module when an entry point is implemented.
- `uv run jupyter lab` starts Jupyter for working with `analytics.ipynb`.
- `uv run pytest` runs tests once a pytest suite has been added.

The project currently has no configured build, lint, format, or test command; add tooling in `pyproject.toml` before relying on it in contributions.

## Coding Style & Naming Conventions

Target Python 3.10+. Use four spaces, `snake_case` for modules, functions, and variables, and `PascalCase` for classes. Keep source code under `src/` and make data transformations small, named functions with clear input/output contracts. Preserve raw input values in ingestion code and keep cleansing rules explicit and documented. Use descriptive notebook headings and avoid duplicating substantial pipeline logic in notebook cells.

## Testing Guidelines

No test framework or coverage requirement is configured yet. Add focused pytest tests in `tests/`, named `test_*.py`; include small synthetic fixtures for parsing, deduplication, data-quality rules, and incremental reruns. Do not require the full MovieLens dataset for unit tests. Run `uv run pytest` before submitting changes when tests are available.

## Commit & Pull Request Guidelines

Git history contains only the initial commit, so no convention is established. Use concise imperative messages, such as `Add rating profile step`. Pull requests should explain the change and checks, link related requirements or issues, and include notebook output when results change. Keep generated files and large datasets out unless required deliverables.

## Data & Configuration

Treat source datasets as read-only inputs. Keep credentials and machine-specific settings out of Git; use environment variables or ignored local configuration for secrets. Document any synthetic batches or derived artifacts so results can be reproduced from the original inputs.
