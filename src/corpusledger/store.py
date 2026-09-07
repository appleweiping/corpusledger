"""Content-addressed objects and reproducible snapshot bundles."""

from __future__ import annotations

import hashlib
import os
import tempfile
import zipfile
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

from .canonical import CanonicalPolicy, canonical_json
from .manifest import Manifest


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _check_digest(value: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError("digest must be a lowercase SHA-256 hex string")
    return value


class ObjectStore:
    """A filesystem-backed, immutable SHA-256 object store.

    Objects are addressed by digest and written through a same-directory
    temporary file followed by ``os.replace``. Existing bytes are verified before
    a write is treated as a harmless deduplication hit.
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.objects = self.root / "objects"
        self.objects.mkdir(parents=True, exist_ok=True)

    def path(self, digest: str) -> Path:
        """Return the validated object path without reading it."""
        digest = _check_digest(digest)
        return self.objects / digest[:2] / digest[2:]

    def put_bytes(self, payload: bytes) -> str:
        """Store bytes once and return their SHA-256 digest."""
        if not isinstance(payload, bytes):
            raise TypeError("payload must be bytes")
        digest = _digest(payload)
        destination = self.path(digest)
        if destination.is_file():
            if destination.read_bytes() != payload:
                raise OSError(f"object collision or corruption for {digest}")
            return digest
        destination.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=".object-", dir=destination.parent)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, destination)
        except BaseException:
            with suppress(OSError):
                os.unlink(temporary)
            raise
        return digest

    def put_file(self, source: str | Path) -> str:
        """Read and store one file as an immutable object."""
        return self.put_bytes(Path(source).read_bytes())

    def put_manifest(self, manifest: Manifest) -> str:
        """Store canonical manifest JSON and return its object digest."""
        if not isinstance(manifest, Manifest):
            raise TypeError("manifest must be a Manifest")
        payload = (canonical_json(manifest.to_dict(), CanonicalPolicy(unicode_form="none")) + "\n").encode("utf-8")
        return self.put_bytes(payload)

    def read_bytes(self, digest: str) -> bytes:
        """Read and re-authenticate one object."""
        path = self.path(digest)
        payload = path.read_bytes()
        if _digest(payload) != digest:
            raise OSError(f"object {digest} failed digest verification")
        return payload

    def contains(self, digest: str) -> bool:
        """Return whether a valid object exists."""
        try:
            return self.path(digest).is_file()
        except ValueError:
            return False

    def digests(self) -> tuple[str, ...]:
        """List object digests in stable order, ignoring temporary files."""
        values: list[str] = []
        for prefix in sorted(self.objects.glob("[0-9a-f][0-9a-f]")):
            if not prefix.is_dir():
                continue
            for item in sorted(prefix.iterdir()):
                candidate = prefix.name + item.name
                if len(candidate) == 64 and all(char in "0123456789abcdef" for char in candidate):
                    values.append(candidate)
        return tuple(values)


@dataclass(frozen=True, slots=True)
class BundleReport:
    """Digest and inventory for one reproducible ZIP snapshot."""

    destination: str
    archive_digest: str
    manifest_digest: str
    files: tuple[str, ...]
    bytes: int


def bundle_snapshot(
    manifest: Manifest,
    source: str | Path,
    destination: str | Path,
    *,
    store: ObjectStore | None = None,
) -> BundleReport:
    """Create a deterministic ZIP containing a manifest and its source files.

    Archive timestamps, ordering, permissions, and compression settings are
    pinned so identical inputs produce identical bytes. The source is read only;
    the destination is replaced atomically after the archive is complete.
    """
    if not isinstance(manifest, Manifest):
        raise TypeError("manifest must be a Manifest")
    source_path = Path(source).resolve()
    destination_path = Path(destination).resolve()
    if source_path == destination_path:
        raise ValueError("bundle destination must differ from source")
    base = source_path if source_path.is_dir() else source_path.parent
    manifest_payload = (canonical_json(manifest.to_dict(), CanonicalPolicy(unicode_form="none")) + "\n").encode("utf-8")
    manifest_digest = store.put_bytes(manifest_payload) if store is not None else _digest(manifest_payload)
    files: list[tuple[str, bytes]] = [("manifest.json", manifest_payload)]
    for relative in sorted(manifest.files):
        candidate = (base / relative).resolve()
        try:
            candidate.relative_to(base)
        except ValueError as error:
            raise ValueError(f"manifest file escapes source root: {relative!r}") from error
        if not candidate.is_file():
            raise FileNotFoundError(candidate)
        files.append((f"source/{relative}", candidate.read_bytes()))
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{destination_path.name}.", suffix=".tmp", dir=destination_path.parent
    )
    os.close(descriptor)
    try:
        with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
            for name, payload in files:
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o644 << 16
                archive.writestr(info, payload)
        os.replace(temporary, destination_path)
    except BaseException:
        with suppress(OSError):
            os.unlink(temporary)
        raise
    payload = destination_path.read_bytes()
    return BundleReport(
        str(destination_path),
        _digest(payload),
        manifest_digest,
        tuple(name for name, _ in files),
        len(payload),
    )
