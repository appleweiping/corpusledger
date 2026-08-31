"""Detached Ed25519 signatures for exact manifest bytes.

``cryptography`` is imported lazily so the core package remains dependency-free.
Private key bytes are read only inside :func:`sign_manifest` and are never placed
in an exception message, signature envelope, or log output.
"""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from .canonical import CanonicalPolicy, canonical_json
from .errors import SignatureError, SignatureVerificationError
from .strictjson import (
    StrictJsonError,
    bounded_int,
    finite_float,
    object_without_duplicates,
    reject_constant,
)

SIGNATURE_FORMAT = "corpusledger-signature/1"
SIGNATURE_ALGORITHM = "ed25519"
_HEX_DIGEST_LENGTH = 64


def _crypto() -> tuple[Any, Any, Any, Any, Any]:
    try:
        from cryptography.exceptions import InvalidSignature, UnsupportedAlgorithm
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.ed25519 import (
            Ed25519PrivateKey,
            Ed25519PublicKey,
        )
    except ImportError as exc:  # pragma: no cover - exercised in an isolated wheel smoke test
        raise SignatureError(
            "Ed25519 support requires the optional 'signing' extra: python -m pip install 'corpusledger[signing]'"
        ) from exc
    return InvalidSignature, UnsupportedAlgorithm, serialization, Ed25519PrivateKey, Ed25519PublicKey


def _strict_b64decode(value: str, name: str) -> bytes:
    try:
        return base64.b64decode(value, validate=True)
    except (ValueError, TypeError) as exc:
        raise SignatureError(f"signature envelope {name} is not valid base64") from exc


def _read_bytes(path: str | Path, description: str) -> bytes:
    try:
        return Path(path).read_bytes()
    except OSError as exc:
        raise SignatureError(f"cannot read {description} {path}: {exc}") from exc


@dataclass(frozen=True, slots=True)
class SignatureEnvelope:
    """Portable metadata for a detached signature; it contains no key material."""

    format: str
    algorithm: str
    key_id: str
    manifest_sha256: str
    signature: str

    def __post_init__(self) -> None:
        if self.format != SIGNATURE_FORMAT:
            raise SignatureError(f"unsupported signature format {self.format!r}")
        if self.algorithm != SIGNATURE_ALGORITHM:
            raise SignatureError(f"unsupported signature algorithm {self.algorithm!r}")
        for name in ("key_id", "manifest_sha256"):
            value = getattr(self, name)
            if (
                not isinstance(value, str)
                or len(value) != _HEX_DIGEST_LENGTH
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise SignatureError(f"signature envelope {name} must be a lowercase SHA-256 digest")
        if not isinstance(self.signature, str) or len(_strict_b64decode(self.signature, "signature")) != 64:
            raise SignatureError("signature envelope signature must encode 64 bytes")

    def to_dict(self) -> dict[str, str]:
        return {
            "algorithm": self.algorithm,
            "format": self.format,
            "key_id": self.key_id,
            "manifest_sha256": self.manifest_sha256,
            "signature": self.signature,
        }

    def save(self, path: str | Path) -> None:
        try:
            text = canonical_json(self.to_dict(), CanonicalPolicy(unicode_form="none")) + "\n"
            Path(path).write_text(text, encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise SignatureError(f"cannot save signature envelope {path}: {exc}") from exc

    @classmethod
    def load(cls, path: str | Path) -> SignatureEnvelope:
        try:
            raw = json.loads(
                Path(path).read_text(encoding="utf-8"),
                object_pairs_hook=object_without_duplicates,
                parse_constant=reject_constant,
                parse_float=finite_float,
                parse_int=bounded_int,
            )
        except (OSError, UnicodeError, json.JSONDecodeError, StrictJsonError) as exc:
            raise SignatureError(f"cannot load signature envelope {path}: {exc}") from exc
        if not isinstance(raw, dict):
            raise SignatureError("signature envelope root must be an object")
        required = {"format", "algorithm", "key_id", "manifest_sha256", "signature"}
        if set(raw) != required:
            raise SignatureError("signature envelope has missing or unknown fields")
        if not all(isinstance(raw[name], str) for name in required):
            raise SignatureError("signature envelope fields must be strings")
        return cls(**cast(dict[str, str], raw))


def _public_key_bytes(public_key: Any) -> bytes:
    _, _, serialization, _, _ = _crypto()
    return cast(
        bytes,
        public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        ),
    )


def public_key_id(public_key: Any) -> str:
    """Return the full SHA-256 fingerprint of an Ed25519 public key."""

    return hashlib.sha256(_public_key_bytes(public_key)).hexdigest()


def sign_manifest(
    manifest_path: str | Path,
    private_key_path: str | Path,
    *,
    password: bytes | None = None,
) -> SignatureEnvelope:
    """Sign exact manifest bytes using an unencrypted or encrypted PEM key."""

    _, UnsupportedAlgorithm, serialization, Ed25519PrivateKey, _ = _crypto()
    manifest_bytes = _read_bytes(manifest_path, "manifest")
    private_bytes = _read_bytes(private_key_path, "private key")
    try:
        private_key = serialization.load_pem_private_key(private_bytes, password=password)
    except (TypeError, ValueError, UnsupportedAlgorithm) as exc:
        raise SignatureError("cannot load private key; check its format and password") from exc
    if not isinstance(private_key, Ed25519PrivateKey):
        raise SignatureError("private key must be an Ed25519 PEM key")
    public_key = private_key.public_key()
    signature = private_key.sign(manifest_bytes)
    return SignatureEnvelope(
        format=SIGNATURE_FORMAT,
        algorithm=SIGNATURE_ALGORITHM,
        key_id=public_key_id(public_key),
        manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        signature=base64.b64encode(signature).decode("ascii"),
    )


def verify_manifest_signature(
    manifest_path: str | Path,
    signature_path: str | Path,
    public_key_path: str | Path,
) -> SignatureEnvelope:
    """Verify digest, trusted public-key identity, and Ed25519 signature."""

    InvalidSignature, UnsupportedAlgorithm, serialization, _, Ed25519PublicKey = _crypto()
    manifest_bytes = _read_bytes(manifest_path, "manifest")
    public_bytes = _read_bytes(public_key_path, "public key")
    envelope = SignatureEnvelope.load(signature_path)
    try:
        public_key = serialization.load_pem_public_key(public_bytes)
    except (TypeError, ValueError, UnsupportedAlgorithm) as exc:
        raise SignatureError("cannot load public key; expected an Ed25519 PEM public key") from exc
    if not isinstance(public_key, Ed25519PublicKey):
        raise SignatureError("public key must be an Ed25519 PEM key")
    if public_key_id(public_key) != envelope.key_id:
        raise SignatureVerificationError("signature key ID does not match the trusted public key")
    observed_digest = hashlib.sha256(manifest_bytes).hexdigest()
    if observed_digest != envelope.manifest_sha256:
        raise SignatureVerificationError("manifest digest does not match the signature envelope")
    try:
        public_key.verify(_strict_b64decode(envelope.signature, "signature"), manifest_bytes)
    except InvalidSignature as exc:
        raise SignatureVerificationError("Ed25519 signature verification failed") from exc
    return envelope
