"""Content-addressed objects and reproducible snapshot bundles."""

from __future__ import annotations

import hashlib
import json
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


@dataclass(frozen=True, slots=True)
class BundleVerification:
    """Authenticated inventory of a snapshot archive."""

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


def verify_bundle(bundle: str | Path, *, expected_archive_digest: str | None = None) -> BundleVerification:
    """Verify archive structure, manifest identity, and safe member names.

    Verification is independent of the source filesystem: it authenticates the
    exact bytes shipped in a bundle and checks that every manifest file has a
    corresponding ``source/`` member. Duplicate members, path traversal, and
    symbolic-link entries are rejected before extraction.
    """

    bundle_path = Path(bundle)
    try:
        archive_bytes = bundle_path.read_bytes()
    except (OSError, UnicodeError) as error:
        raise ValueError(f"cannot read bundle {bundle_path}: {error}") from error
    archive_digest = _digest(archive_bytes)
    if expected_archive_digest is not None and archive_digest != _check_digest(expected_archive_digest):
        raise ValueError("bundle archive digest does not match expected digest")
    try:
        archive = zipfile.ZipFile(bundle_path)
    except (OSError, zipfile.BadZipFile) as error:
        raise ValueError(f"invalid bundle archive {bundle_path}: {error}") from error
    with archive:
        infos = archive.infolist()
        names = [info.filename for info in infos]
        if len(names) != len(set(names)):
            raise ValueError("bundle contains duplicate member names")
        for info in infos:
            name = info.filename.replace("\\", "/")
            if name != info.filename or name.startswith("/") or any(part == ".." for part in name.split("/")):
                raise ValueError(f"unsafe bundle member path: {info.filename!r}")
            if info.is_dir() or (info.external_attr >> 16) & 0o170000 == 0o120000:
                raise ValueError(f"bundle member is not a regular file: {info.filename!r}")
        if "manifest.json" not in names:
            raise ValueError("bundle is missing manifest.json")
        manifest_bytes = archive.read("manifest.json")
        manifest_digest = _digest(manifest_bytes)
        try:
            manifest = json.loads(manifest_bytes.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as error:
            raise ValueError("bundle manifest.json is not valid UTF-8 JSON") from error
        if not isinstance(manifest, dict) or manifest.get("format") != "corpusledger/1":
            raise ValueError("bundle manifest has an unsupported format")
        files = manifest.get("files")
        if not isinstance(files, dict) or not all(isinstance(name, str) for name in files):
            raise ValueError("bundle manifest files must be an object")
        expected = {f"source/{name}" for name in files}
        actual = {name for name in names if name.startswith("source/")}
        if actual != expected:
            missing = sorted(expected - actual)
            extra = sorted(actual - expected)
            detail = []
            if missing:
                detail.append("missing " + ", ".join(missing))
            if extra:
                detail.append("unexpected " + ", ".join(extra))
            raise ValueError("bundle source inventory mismatch (" + "; ".join(detail) + ")")
        return BundleVerification(archive_digest, manifest_digest, tuple(names), len(archive_bytes))


def extract_bundle(bundle: str | Path, destination: str | Path, *, overwrite: bool = False) -> BundleVerification:
    """Safely extract a verified bundle and return its authentication report."""

    verification = verify_bundle(bundle)
    target = Path(destination).resolve()
    target.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(bundle) as archive:
        for info in archive.infolist():
            output = (target / info.filename).resolve()
            try:
                output.relative_to(target)
            except ValueError as error:
                raise ValueError(f"bundle member escapes extraction root: {info.filename!r}") from error
            if output.exists() and not overwrite:
                raise FileExistsError(output)
            output.parent.mkdir(parents=True, exist_ok=True)
            temporary = output.with_name(f".{output.name}.tmp")
            temporary.write_bytes(archive.read(info.filename))
            os.replace(temporary, output)
    return verification
