from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.rsa import generate_private_key

from corpusledger.cli import _password_from_environment, run
from corpusledger.errors import InputError, SignatureError, SignatureVerificationError
from corpusledger.manifest import build_manifest
from corpusledger.signing import SignatureEnvelope, sign_manifest, verify_manifest_signature


@pytest.fixture
def key_paths(tmp_path: Path) -> tuple[Path, Path, Path]:
    private_key = Ed25519PrivateKey.generate()
    private_path = tmp_path / "private.pem"
    encrypted_path = tmp_path / "private-encrypted.pem"
    public_path = tmp_path / "public.pem"
    private_path.write_bytes(
        private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    encrypted_path.write_bytes(
        private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.BestAvailableEncryption(b"correct horse battery staple"),
        )
    )
    public_path.write_bytes(
        private_key.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    return private_path, encrypted_path, public_path


def _manifest(tmp_path: Path) -> Path:
    source = tmp_path / "corpus.jsonl"
    source.write_text('{"id":"a","text":"hello"}\n', encoding="utf-8")
    target = tmp_path / "manifest.json"
    build_manifest(source).save(target)
    return target


def test_detached_signature_roundtrip_and_no_key_material(
    tmp_path: Path,
    key_paths: tuple[Path, Path, Path],
) -> None:
    private_path, _, public_path = key_paths
    manifest = _manifest(tmp_path)
    signature_path = tmp_path / "manifest.sig"
    envelope = sign_manifest(manifest, private_path)
    envelope.save(signature_path)

    verified = verify_manifest_signature(manifest, signature_path, public_path)
    assert verified == envelope
    assert set(envelope.to_dict()) == {
        "algorithm",
        "format",
        "key_id",
        "manifest_sha256",
        "signature",
    }
    serialized = signature_path.read_bytes()
    assert private_path.read_bytes() not in serialized
    assert public_path.read_bytes() not in serialized


def test_encrypted_private_key_and_cli_password_environment(
    tmp_path: Path,
    key_paths: tuple[Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, encrypted_path, public_path = key_paths
    manifest = _manifest(tmp_path)
    signature = tmp_path / "manifest.sig"
    monkeypatch.setenv("TEST_CORPUSLEDGER_PASSWORD", "correct horse battery staple")
    assert (
        run(
            [
                "sign",
                str(manifest),
                "--private-key",
                str(encrypted_path),
                "--password-env",
                "TEST_CORPUSLEDGER_PASSWORD",
                "--output",
                str(signature),
            ]
        )
        == 0
    )
    assert run(["verify-signature", str(manifest), str(signature), "--public-key", str(public_path)]) == 0


def test_tampered_manifest_signature_and_wrong_key_are_rejected(
    tmp_path: Path,
    key_paths: tuple[Path, Path, Path],
) -> None:
    private_path, _, public_path = key_paths
    manifest = _manifest(tmp_path)
    signature_path = tmp_path / "manifest.sig"
    sign_manifest(manifest, private_path).save(signature_path)

    original = manifest.read_bytes()
    manifest.write_bytes(original + b" ")
    with pytest.raises(SignatureVerificationError, match="digest"):
        verify_manifest_signature(manifest, signature_path, public_path)
    manifest.write_bytes(original)

    raw = json.loads(signature_path.read_text(encoding="utf-8"))
    signature = bytearray(base64.b64decode(raw["signature"]))
    signature[0] ^= 1
    raw["signature"] = base64.b64encode(signature).decode("ascii")
    signature_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(SignatureVerificationError, match="verification failed"):
        verify_manifest_signature(manifest, signature_path, public_path)

    sign_manifest(manifest, private_path).save(signature_path)
    other_key = Ed25519PrivateKey.generate().public_key()
    other_public = tmp_path / "other-public.pem"
    other_public.write_bytes(
        other_key.public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    with pytest.raises(SignatureVerificationError, match="key ID"):
        verify_manifest_signature(manifest, signature_path, other_public)


@pytest.mark.parametrize(
    "content,match",
    [
        ('{"format":"corpusledger-signature/1","format":"duplicate"}', "duplicate"),
        ("[]", "root"),
        ('{"unexpected":true}', "missing or unknown"),
    ],
)
def test_signature_envelope_is_strict(tmp_path: Path, content: str, match: str) -> None:
    path = tmp_path / "bad.sig"
    path.write_text(content, encoding="utf-8")
    with pytest.raises(SignatureError, match=match):
        SignatureEnvelope.load(path)


def test_sign_cli_refuses_key_overwrite_and_missing_password(
    tmp_path: Path,
    key_paths: tuple[Path, Path, Path],
) -> None:
    private_path, _, _ = key_paths
    manifest = _manifest(tmp_path)
    with pytest.raises(InputError, match="must not overwrite"):
        run(
            [
                "sign",
                str(manifest),
                "--private-key",
                str(private_path),
                "--output",
                str(private_path),
            ]
        )
    invalid_output = tmp_path / "existing.sig"
    invalid_output.write_text("not a signature", encoding="utf-8")
    with pytest.raises(InputError, match="not a CorpusLedger signature"):
        run(
            [
                "sign",
                str(manifest),
                "--private-key",
                str(private_path),
                "--output",
                str(invalid_output),
            ]
        )

    key_alias = tmp_path / "private-key-alias.pem"
    try:
        key_alias.hardlink_to(private_path)
    except OSError as exc:
        pytest.skip(f"hard links are unavailable: {exc}")
    private_bytes = private_path.read_bytes()
    with pytest.raises(InputError, match="must not overwrite"):
        run(
            [
                "sign",
                str(manifest),
                "--private-key",
                str(private_path),
                "--output",
                str(key_alias),
            ]
        )
    assert private_path.read_bytes() == private_bytes


@pytest.mark.parametrize("name", ["", " BAD", "BAD\nNAME", "BAD=NAME"])
def test_password_environment_name_is_strict(name: str) -> None:
    with pytest.raises(InputError, match="name is invalid"):
        _password_from_environment(name)


@pytest.mark.parametrize(
    "changes,match",
    [
        ({"format": "wrong"}, "format"),
        ({"algorithm": "rsa"}, "algorithm"),
        ({"key_id": "not-a-digest"}, "key_id"),
        ({"manifest_sha256": "A" * 64}, "manifest_sha256"),
        ({"signature": "not base64!"}, "base64"),
        ({"signature": base64.b64encode(b"short").decode("ascii")}, "64 bytes"),
    ],
)
def test_signature_envelope_field_boundaries(changes: dict[str, str], match: str) -> None:
    baseline = {
        "format": "corpusledger-signature/1",
        "algorithm": "ed25519",
        "key_id": "0" * 64,
        "manifest_sha256": "1" * 64,
        "signature": base64.b64encode(b"x" * 64).decode("ascii"),
    }
    with pytest.raises(SignatureError, match=match):
        SignatureEnvelope(**(baseline | changes))


def test_key_and_io_error_boundaries(
    tmp_path: Path,
    key_paths: tuple[Path, Path, Path],
) -> None:
    private_path, encrypted_path, public_path = key_paths
    manifest = _manifest(tmp_path)
    with pytest.raises(SignatureError, match="password"):
        sign_manifest(manifest, encrypted_path, password=b"wrong")
    with pytest.raises(SignatureError, match="cannot read manifest"):
        sign_manifest(tmp_path / "missing.json", private_path)

    rsa_key = generate_private_key(public_exponent=65537, key_size=2048)
    rsa_private = tmp_path / "rsa-private.pem"
    rsa_public = tmp_path / "rsa-public.pem"
    rsa_private.write_bytes(
        rsa_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    rsa_public.write_bytes(
        rsa_key.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    with pytest.raises(SignatureError, match="private key must be an Ed25519"):
        sign_manifest(manifest, rsa_private)

    signature = tmp_path / "manifest.sig"
    sign_manifest(manifest, private_path).save(signature)
    with pytest.raises(SignatureError, match="public key must be an Ed25519"):
        verify_manifest_signature(manifest, signature, rsa_public)
    public_path.write_text("not a PEM key", encoding="utf-8")
    with pytest.raises(SignatureError, match="cannot load public key"):
        verify_manifest_signature(manifest, signature, public_path)
    with pytest.raises(SignatureError, match="cannot save"):
        sign_manifest(manifest, private_path).save(tmp_path)


def test_cli_signature_failure_returns_one(
    tmp_path: Path,
    key_paths: tuple[Path, Path, Path],
) -> None:
    private_path, encrypted_path, public_path = key_paths
    manifest = _manifest(tmp_path)
    signature = tmp_path / "manifest.sig"
    assert run(["sign", str(manifest), "--private-key", str(private_path), "--output", str(signature)]) == 0
    manifest.write_bytes(manifest.read_bytes() + b" ")
    assert run(["verify-signature", str(manifest), str(signature), "--public-key", str(public_path)]) == 1
    with pytest.raises(InputError, match="not set"):
        run(
            [
                "sign",
                str(manifest),
                "--private-key",
                str(encrypted_path),
                "--password-env",
                "DEFINITELY_NOT_SET_CORPUSLEDGER_PASSWORD",
            ]
        )
