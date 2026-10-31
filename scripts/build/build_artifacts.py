#!/usr/bin/env python3
"""Build independent units offline: python scripts/build/build_artifacts.py [--unit glue/db-gb].

Each artifact vendors its pinned integrity dependency; no global mutable runtime ZIP.
The local hybrid application explicitly consumes the public parser APIs for local mode.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import tempfile
import tomllib
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DIST = ROOT / "dist"
ZIP_TIME = (2020, 1, 1, 0, 0, 0)
RUNTIMES = {
    "lambda": {"python": "3.12", "architecture": "x86_64", "platform": "manylinux_2_28"},
    "glue": {"glue": "5.0", "python": "3.11", "spark": "3.5"},
    "local": {"python": "3.12", "dependencies": "uv.lock"},
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def keep(name: str) -> bool:
    return (
        not name.startswith(".")
        and "__pycache__" not in name
        and not name.endswith((".pyc", ".pyx", ".pxd"))
        and not name.startswith(("pyarrow/include/", "pyarrow/tests/", "pyarrow/src/"))
    )


def tree(directory: Path, prefix: str = "") -> dict[str, Path]:
    return {
        prefix + p.relative_to(directory).as_posix(): p
        for p in sorted(directory.rglob("*"))
        if p.is_file() and keep(p.relative_to(directory).as_posix())
    }


def write_zip(path: Path, files: dict[str, Path]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name, source in sorted(files.items()):
            info = zipfile.ZipInfo(name, ZIP_TIME)
            info.external_attr = 0o644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, source.read_bytes())


def units(root: Path = ROOT) -> dict[str, Path]:
    return {
        p.parent.relative_to(root).as_posix(): p.parent
        for runtime in ("apps", "glue", "lambda")
        for p in sorted((root / runtime).glob("*/unit.json"))
    }


def locked_versions(names: set[str], root: Path = ROOT) -> dict[str, str]:
    packages = tomllib.loads((root / "uv.lock").read_text())["package"]
    result = {p["name"]: p["version"] for p in packages if p["name"] in names}
    if result.keys() != names:
        raise ValueError(f"dependencies not pinned in uv.lock: {sorted(names - result.keys())}")
    return result


def install_wheels(pins: dict[str, str], target: Path) -> None:
    # A build never implicitly authorises dependency downloads. CI warms this cache with uv sync.
    subprocess.run(  # noqa: S603 - fixed offline installer argv and locked versions
        [
            shutil.which("uv") or "uv",
            "pip",
            "install",
            "--offline",
            "--quiet",
            "--target",
            str(target),
            "--python-platform",
            "x86_64-manylinux_2_28",
            "--python-version",
            "3.12",
            "--only-binary",
            ":all:",
            "--no-deps",
            *[f"{name}=={version}" for name, version in sorted(pins.items())],
        ],
        check=True,
    )


def own_files(unit: Path, root: Path = ROOT) -> tuple[dict, dict[str, Path]]:
    spec = json.loads((unit / "unit.json").read_text())
    protocol = root / "protocols" / "integrity-v2" / "erl_protocol"
    if f'__version__ = "{spec["protocol_version"]}"' not in (protocol / "__init__.py").read_text():
        raise ValueError(f"{unit.name}: integrity protocol version mismatch")
    modules = spec.get("protocol_modules")
    common = {"erl_protocol/__init__.py": protocol / "__init__.py"}
    if modules:
        common |= {f"erl_protocol/{name}.py": protocol / f"{name}.py" for name in modules}
    else:
        common |= tree(protocol, "erl_protocol/")
    contract_names = spec.get("contracts", ["bronze_contract.json", "consumer_contract.json"])
    common |= {"contracts/__init__.py": root / "contracts/__init__.py"}
    common |= {f"contracts/{name}": root / "contracts" / name for name in contract_names}
    if spec["runtime"] == "lambda":
        common |= tree(unit / "python")
        common["handler.py"] = unit / spec["entrypoint"]
    elif spec["runtime"] == "glue":
        common |= tree(unit / "spark")
    else:
        common |= tree(unit / "python") | tree(unit / "sql", "sql/")
        if spec["entrypoint"] == "erl.py":
            common["erl.py"] = unit / spec["entrypoint"]
        for dependency in spec["public_dependencies"]:
            public = root / dependency / "python"
            if not public.is_dir():
                raise ValueError(f"missing declared public dependency: {dependency}")
            common |= tree(public)
    if spec.get("dependencies_file") and (unit / spec["dependencies_file"]).is_file():
        common["requirements.txt"] = unit / spec["dependencies_file"]
    if not all(p.is_file() for p in common.values()):
        raise ValueError(f"{unit.name}: missing declared build input")
    return spec, common


def build_unit(unit: Path, output: Path, dependencies: dict[str, dict[str, Path]], root: Path = ROOT) -> dict:
    spec, files = own_files(unit, root)
    relative = unit.relative_to(root).as_posix()
    destination = output / relative
    destination.mkdir(parents=True, exist_ok=True)
    if spec["runtime"] == "glue":
        (destination / "job.py").write_bytes((unit / spec["entrypoint"]).read_bytes())
        write_zip(destination / "lib.zip", files)
        artifact_names = ["job.py", "lib.zip"]
    else:
        for dependency in spec.get("dependencies", []):
            for name, path in dependencies[dependency].items():
                if name in files:
                    raise ValueError(f"duplicate artifact member: {name}")
                files[name] = path
        name = "function.zip" if spec["runtime"] == "lambda" else spec.get("artifact", "runner.zip")
        write_zip(destination / name, files)
        artifact_names = [name]
    artifacts = {}
    for name in artifact_names:
        path = destination / name
        digest = sha256_file(path)
        prefix = f"case01/{relative}"
        key = f"{prefix}/{digest}/{name}" if spec["runtime"] == "glue" else f"{prefix}/{name}"
        artifacts[name] = {"sha256": digest, "bytes": path.stat().st_size, "s3_key": key}
    result = {
        "unit": relative,
        "entrypoint": spec["entrypoint"],
        "handler": spec.get("handler"),
        "runtime": RUNTIMES[spec["runtime"]],
        "protocol_version": spec["protocol_version"],
        "contracts": spec.get("contracts", ["bronze_contract.json", "consumer_contract.json"]),
        "dependencies": locked_versions(set(spec.get("dependencies", [])), root),
        "dependency_file": spec.get("dependencies_file"),
        "external_dependencies": locked_versions(set(spec.get("external_dependencies", [])), root),
        "python_modules": [
            line.strip()
            for line in (unit / spec["dependencies_file"]).read_text().splitlines()
            if line.strip() and not line.startswith("#")
        ]
        if spec.get("dependencies_file") and spec["runtime"] == "glue"
        else [],
        "public_dependencies": spec.get("public_dependencies", []),
        "files": sorted(files),
        "artifacts": artifacts,
    }
    (destination / "manifest.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def git_state(root: Path) -> dict:
    def git(*args):
        return subprocess.run(  # noqa: S603 - fixed local git read commands
            [shutil.which("git") or "git", *args], cwd=root, capture_output=True, text=True, check=True
        ).stdout.strip()

    return {
        "commit": git("rev-parse", "HEAD"),
        "dirty": bool(
            git(
                "status",
                "--porcelain",
                "--",
                "apps",
                "contracts",
                "protocols",
                "lambda",
                "glue",
                "pyproject.toml",
                "uv.lock",
                "scripts/build",
            )
        ),
    }


def build(selected: list[str] | None = None) -> dict:
    discovered = units()
    chosen = selected or list(discovered)
    if unknown := set(chosen) - discovered.keys():
        raise ValueError(f"unknown units: {sorted(unknown)}")
    names = {
        name
        for key in chosen
        for name in json.loads((discovered[key] / "unit.json").read_text()).get("dependencies", [])
    }
    if selected is None:
        shutil.rmtree(DIST, ignore_errors=True)
    result = {}
    with tempfile.TemporaryDirectory() as tmp:
        dependency_files = {}
        for name, version in locked_versions(names).items():
            target = Path(tmp) / name
            install_wheels({name: version}, target)
            dependency_files[name] = tree(target)
        for key in chosen:
            result[key] = build_unit(discovered[key], DIST, dependency_files)
    manifest = {"git": git_state(ROOT), "uv_lock_sha256": sha256_file(ROOT / "uv.lock"), "units": result}
    for key, unit in result.items():
        unit["provenance"] = {k: manifest[k] for k in ("git", "uv_lock_sha256")}
        (DIST / key / "manifest.json").write_text(json.dumps(unit, indent=2) + "\n")
    DIST.mkdir(parents=True, exist_ok=True)
    (DIST / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unit", action="append", help="Build only a declared unit, e.g. glue/db-gb")
    result = build(parser.parse_args().unit)
    for key, unit in result["units"].items():
        for name, artifact in unit["artifacts"].items():
            print(f"{key}/{name}: {artifact['sha256']} ({artifact['bytes']} bytes)")


if __name__ == "__main__":
    main()
