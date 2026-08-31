# Contributing

Thank you for improving CorpusLedger.

1. Open an issue for behavior or format changes so compatibility can be discussed first.
2. Create a focused branch and add tests that demonstrate the intended behavior.
3. Run `pytest`, coverage, Ruff, mypy, and `python -m build` as documented in the README.
4. Update the format and architecture docs when persistence semantics change.
5. Keep runtime dependencies optional; the standard-library runtime is a deliberate constraint.

Pull requests should explain the problem, compatibility impact, threat-model impact, and exact verification performed.
Generated or AI-assisted changes are welcome when the contributor understands and reviews every line, validates the
result, and clearly discloses material assistance. Do not submit confidential corpus samples in issues or tests.

By participating, you agree to follow the Code of Conduct.
