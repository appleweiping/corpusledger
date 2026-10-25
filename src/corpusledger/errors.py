"""Errors that the command line reports as a one-line message with exit status 2."""


class CorpusLedgerError(Exception):
    """Base class for expected, user-facing errors."""


class CanonicalizationError(CorpusLedgerError):
    """A value cannot be represented as canonical JSON."""


class InputError(CorpusLedgerError):
    """A dataset file is missing, malformed or lacks the fields needed to match records."""


class SnapshotError(CorpusLedgerError):
    """A snapshot file is malformed or incompatible."""
