# Releasing

CorpusLedger follows semantic versioning. Releases come only from a clean, reviewed `main` commit with passing checks.

1. Update `pyproject.toml`, public version exports, `CHANGELOG.md`, and `CITATION.cff`.
2. Run lint, format, strict typing, tests, build, and benchmark smoke checks.
3. Inspect the wheel and sdist, install the wheel in an empty environment, and exercise create/verify/sign workflows.
4. Add curated notes at `docs/releases/vX.Y.Z.md` when appropriate; the workflow falls back to generated notes when
   that file is absent.
5. Before the first release, a repository administrator must enable GitHub's immutable releases setting. Create a
   protected `vX.Y.Z` tag at the reviewed commit.
6. Automation records SHA-256 checksums and build provenance, then publishes every asset in the release creation
   operation so repository-level immutability can lock them.
7. PyPI publication is a separate approved dispatch from the release tag through a configured trusted publisher;
   long-lived PyPI tokens are not accepted.

Never replace an existing release artifact. Correct mistakes with a new patch release.
