"""Install a built wheel in isolation and exercise the public CLI."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: wheel_smoke.py WHEEL_DIRECTORY")
    wheels = tuple(Path(sys.argv[1]).glob("corpusledger-*.whl"))
    if len(wheels) != 1:
        raise SystemExit(f"expected exactly one CorpusLedger wheel, found {len(wheels)}")
    with tempfile.TemporaryDirectory(prefix="corpusledger-wheel-smoke-") as directory:
        root = Path(directory)
        install_root = root / "site-packages"
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--no-deps",
                "--no-index",
                "--target",
                str(install_root),
                str(wheels[0].resolve()),
            ],
            check=True,
        )
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(install_root)
        source = root / "corpus.jsonl"
        manifest = root / "manifest.json"
        source.write_text('{"id":"smoke","text":"installed wheel"}\n', encoding="utf-8")
        subprocess.run(
            [sys.executable, "-S", "-m", "corpusledger", "snapshot", str(source), str(manifest)],
            check=True,
            cwd=root,
            env=environment,
        )
        subprocess.run(
            [sys.executable, "-S", "-m", "corpusledger", "verify", str(manifest)],
            check=True,
            cwd=root,
            env=environment,
        )
        value = json.loads(manifest.read_text(encoding="utf-8"))
        if value["format"] != "corpusledger/1" or len(value["records"]) != 1:
            raise SystemExit("installed wheel produced an invalid smoke manifest")
        unavailable = subprocess.run(
            [
                sys.executable,
                "-S",
                "-m",
                "corpusledger",
                "sign",
                str(manifest),
                "--private-key",
                str(root / "not-read-without-extra.pem"),
            ],
            check=False,
            cwd=root,
            env=environment,
            capture_output=True,
            text=True,
        )
        if unavailable.returncode != 2 or "optional 'signing' extra" not in unavailable.stderr:
            raise SystemExit("missing signing extra did not fail with the documented concise error")
        subprocess.run(
            [sys.executable, "-S", "-c", "import corpusledger; assert corpusledger.__version__ == '0.2.0'"],
            check=True,
            cwd=root,
            env=environment,
        )


if __name__ == "__main__":
    main()
