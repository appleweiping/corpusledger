"""Public exception hierarchy for CorpusLedger."""


class CorpusLedgerError(Exception):
    """Base class for expected, user-facing CorpusLedger errors."""


class CanonicalizationError(CorpusLedgerError):
    """Raised when a value cannot be represented canonically."""


class InputError(CorpusLedgerError):
    """Raised when corpus input is malformed or ambiguous."""


class DuplicateIdError(InputError):
    """Raised when two records use the same logical identifier."""


class ManifestError(CorpusLedgerError):
    """Raised when a manifest is invalid or incompatible."""
