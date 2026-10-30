#!/usr/bin/env python3
"""Build every deployable artifact into ``dist/`` before any Terraform plan.

    uv run python scripts/build/build_artifacts.py          # build + dist/manifest.json
    uv run python scripts/build/smoke_artifacts.py          # import/invoke the built files

Outputs (stable S3 keys under ``case01/`` in the artifacts bucket, see infra/environments/dev):

    dist/lambda/bronze/file_ingestion.zip   handler module + own packages + pinned pyarrow/lxml
    dist/glue/bronze/db_ingest.py           Glue script_location
    dist/glue/load/gold_load.py             Glue script_location
    dist/glue/lib/erl_glue_lib.zip          own packages for --extra-py-files
    dist/manifest.json                      git commit, uv.lock hash, SHA-256 and runtime per artifact

Third-party wheels are resolved from uv.lock for the target platform (Lambda python3.12 /
x86_64 / manylinux_2_28). Archives are deterministic: sorted entries, fixed timestamps.
Terraform only uploads/references these files; it never builds them.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DIST = ROOT / "dist"
S3_PREFIX = "case01"

LAMBDA_RUNTIME = {"service": "lambda", "runtime": "python3.12", "architecture": "x86_64"}
LAMBDA_PLATFORM = ("x86_64-manylinux_2_28", "3.12")
GLUE_RUNTIME = {"service": "glue", "glue_version": "5.0", "python": "3.11", "spark": "3.5"}

# Import roots copied into the archives (each directory becomes a top-level package).
LAMBDA_PACKAGES = (
    "src/python/utils",
    "src/python/storage",
    "src/python/transform",
    "src/python/ingestion",
    "src/contracts",
)
LAMBDA_HANDLER = "lambda/bronze/file_ingestion.py"
LAMBDA_THIRD_PARTY = ("pyarrow", "lxml")  # boto3 comes from the Lambda runtime
GLUE_LIBRARY_PACKAGES = (
    "src/python/utils",
    "src/python/storage",
    "src/python/ingestion",
    "src/python/load",
    "src/contracts",
    "src/spark/readers",
    "src/spark/transforms",
    "src/spark/writers",
)
GLUE_SCRIPTS = ("glue/bronze/db_ingest.py", "glue/load/gold_load.py")

# Not needed at runtime: headers, test suites and Cython sources of the pyarrow wheel.
STRIP_PREFIXES = ("pyarrow/include/", "pyarrow/tests/", "pyarrow/src/")
STRIP_SUFFIXES = (".pyx", ".pxd", ".pyc")

ARTIFACTS = {
    "lambda/bronze/file_ingestion.zip": f"{S3_PREFIX}/lambda/bronze/file_ingestion.zip",
    "glue/bronze/db_ingest.py": f"{S3_PREFIX}/glue/bronze/db_ingest.py",
    "glue/load/gold_load.py": f"{S3_PREFIX}/glue/load/gold_load.py",
    "glue/lib/erl_glue_lib.zip": f"{S3_PREFIX}/glue/lib/erl_glue_lib.zip",
}
FIXED_ZIP_TIME = (2020, 1, 1, 0, 0, 0)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def locked_versions(names: tuple[str, ...]) -> dict[str, str]:
    """Versions pinned in uv.lock (the same versions the local tests use)."""
    out = subprocess.run(
        ["uv", "export", "--frozen", "--no-hashes", "--format", "requirements-txt"],  # noqa: S607
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    pins = {}
    for line in out.splitlines():
        name, sep, rest = line.partition("==")
        if sep and name.strip() in names:
            pins[name.strip()] = rest.split(";")[0].split()[0]
    missing = set(names) - set(pins)
    if missing:
        raise SystemExit(f"not pinned in uv.lock: {sorted(missing)}")
    return pins


def install_wheels(pins: dict[str, str], target: Path) -> None:
    platform, python = LAMBDA_PLATFORM
    subprocess.run(  # noqa: S603 - fixed argv, versions from uv.lock
        [  # noqa: S607
            "uv",
            "pip",
            "install",
            "--quiet",
            "--target",
            str(target),
            "--python-platform",
            platform,
            "--python-version",
            python,
            "--only-binary",
            ":all:",
            "--no-deps",
            *[f"{n}=={v}" for n, v in sorted(pins.items())],
        ],
        cwd=ROOT,
        check=True,
    )


def _keep(relative: str) -> bool:
    if "__pycache__/" in relative or relative.endswith(STRIP_SUFFIXES):
        return False
    return not relative.startswith(STRIP_PREFIXES)


def collect(packages: tuple[str, ...]) -> dict[str, Path]:
    """Map archive path -> source file for the given package directories."""
    files: dict[str, Path] = {}
    for package in packages:
        src = ROOT / package
        if not (src / "__init__.py").is_file():
            raise SystemExit(f"{package} is not an importable package")
        for path in sorted(src.rglob("*")):
            relative = f"{src.name}/{path.relative_to(src).as_posix()}"
            if path.is_file() and _keep(relative):
                if relative in files:
                    raise SystemExit(f"two roots provide {relative}")
                files[relative] = path
    return files


def write_zip(target: Path, files: dict[str, Path]) -> int:
    target.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name in sorted(files):
            info = zipfile.ZipInfo(name, date_time=FIXED_ZIP_TIME)
            info.external_attr = 0o644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, files[name].read_bytes())
    return len(files)


def git_state() -> dict:
    def git(*args: str) -> str:
        return subprocess.run(  # noqa: S603
            ["git", *args],  # noqa: S607
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

    # dirty = any uncommitted or untracked change in the build inputs (not docs or evidence)
    inputs = ("src", "lambda", "glue", "pyproject.toml", "uv.lock", "scripts/build")
    return {"commit": git("rev-parse", "HEAD"), "dirty": bool(git("status", "--porcelain", "--", *inputs))}


def build() -> dict:
    shutil.rmtree(DIST, ignore_errors=True)
    pins = locked_versions(LAMBDA_THIRD_PARTY)
    entries: dict[str, dict] = {}

    with tempfile.TemporaryDirectory() as tmp:
        wheels = Path(tmp) / "wheels"
        install_wheels(pins, wheels)
        files = {
            p.relative_to(wheels).as_posix(): p
            for p in wheels.rglob("*")
            if p.is_file() and _keep(p.relative_to(wheels).as_posix())
        }
        own = collect(LAMBDA_PACKAGES)
        if clash := set(own) & set(files):
            raise SystemExit(f"own packages collide with third-party files: {sorted(clash)[:5]}")
        files |= own
        files[Path(LAMBDA_HANDLER).name] = ROOT / LAMBDA_HANDLER
        n = write_zip(DIST / "lambda/bronze/file_ingestion.zip", files)
        entries["lambda/bronze/file_ingestion.zip"] = {
            "runtime": LAMBDA_RUNTIME,
            "handler_module": Path(LAMBDA_HANDLER).stem,
            "third_party": pins,
            "packages": [Path(p).name for p in LAMBDA_PACKAGES],
            "files": n,
        }

    n = write_zip(DIST / "glue/lib/erl_glue_lib.zip", collect(GLUE_LIBRARY_PACKAGES))
    entries["glue/lib/erl_glue_lib.zip"] = {
        "runtime": GLUE_RUNTIME,
        "packages": [Path(p).name for p in GLUE_LIBRARY_PACKAGES],
        "files": n,
    }
    for script in GLUE_SCRIPTS:
        target = DIST / script
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / script, target)
        entries[script] = {"runtime": GLUE_RUNTIME, "files": 1}

    for name, meta in entries.items():
        path = DIST / name
        meta |= {"s3_key": ARTIFACTS[name], "sha256": sha256_file(path), "bytes": path.stat().st_size}
    manifest = {
        "project": "enterprise-risk-liquidity",
        "git": git_state(),
        "uv_lock_sha256": sha256_file(ROOT / "uv.lock"),
        "artifacts": dict(sorted(entries.items())),
    }
    (DIST / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main() -> int:
    manifest = build()
    for name, meta in manifest["artifacts"].items():
        print(f"{name:40} {meta['bytes'] / 1048576:8.2f} MiB  sha256 {meta['sha256'][:16]}")
    print(f"git {manifest['git']['commit'][:12]} dirty={manifest['git']['dirty']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
