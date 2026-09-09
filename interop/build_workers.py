"""Build standalone workers offline from a preverified dependency cache.

No dependency resolver or implicit download runs. Use --fetch explicitly to
retrieve the one checksum-pinned jar from Maven Central before compilation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCK = ROOT / "interop/java/dependencies.json"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("dependency redirects are not allowed")


def dependency(cache: Path, *, fetch: bool = False) -> Path:
    item = json.loads(LOCK.read_text(encoding="utf-8"))["artifacts"][0]
    path = cache / item["file"]
    if fetch and not path.exists():
        cache.mkdir(parents=True, exist_ok=True)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        with opener.open(item["url"], timeout=30) as response:
            data = response.read(2 * 1024 * 1024 + 1)
        if len(data) > 2 * 1024 * 1024 or hashlib.sha256(data).hexdigest() != item["sha256"]:
            raise ValueError("dependency download checksum/size mismatch")
        with tempfile.NamedTemporaryFile(dir=cache, prefix=".worker-jar-", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(data)
        try:
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)
    with path.open("rb") as stream:
        raw = stream.read(2 * 1024 * 1024 + 1)
    if len(raw) > 2 * 1024 * 1024 or hashlib.sha256(raw).hexdigest() != item["sha256"]:
        raise ValueError("cached dependency checksum/size mismatch")
    return path.resolve()


def build(output: Path, cache: Path, *, fetch: bool = False) -> dict[str, str]:
    jar = dependency(cache.resolve(), fetch=fetch)
    output = output.resolve()
    if output == ROOT or ROOT in output.parents or output in ROOT.parents:
        raise ValueError("build output must be a dedicated directory outside the repository")
    # Never erase/reuse a directory belonging to another build or task.
    output.mkdir(parents=True, exist_ok=False)
    classes = output / "java-classes"
    classes.mkdir()
    tools = {name: shutil.which(name) for name in ("go", "javac", "java", "jar")}
    if not all(tools.values()):
        raise ValueError("Go and JDK 21+ must already be installed")
    env = {**os.environ, "GOTOOLCHAIN": "local", "GOPROXY": "off", "GOSUMDB": "off"}
    binary = output / ("token-worker.exe" if os.name == "nt" else "token-worker")
    worker_jar = output / "reference-worker.jar"
    commands = [
        [tools["go"], "test", "./..."],
        [tools["go"], "build", "-trimpath", "-o", str(binary), "."],
        [
            tools["javac"],
            "--release",
            "21",
            "--add-modules",
            "jdk.httpserver",
            "-Xlint:all",
            "-Werror",
            "-cp",
            str(jar),
            "-d",
            str(classes),
            str(ROOT / "interop/java/ReferenceWorker.java"),
        ],
        [tools["jar"], "--create", "--file", str(worker_jar), "--date=2026-01-01T00:00:00Z", "-C", str(classes), "."],
    ]
    for command in commands:
        subprocess.run(command, cwd=ROOT / "interop/go", env=env, check=True, timeout=180)
    result = {"go_worker": str(binary), "java_worker": str(worker_jar), "jackson_core": str(jar)}
    result.update(
        {name + "_sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest() for name, path in tuple(result.items())}
    )
    (output / "build.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--dependency-cache", required=True, type=Path)
    parser.add_argument("--fetch", action="store_true", help="explicitly fetch the pinned jar if absent")
    args = parser.parse_args()
    print(json.dumps(build(args.output, args.dependency_cache, fetch=args.fetch), sort_keys=True))


if __name__ == "__main__":
    main()
