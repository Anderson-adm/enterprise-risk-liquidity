"""Unit ownership and independent builds. Fixtures use no third-party installation or AWS."""

from __future__ import annotations

import ast
import json
import shutil
import zipfile
from pathlib import Path

import pytest

from tests.conftest import load_entrypoint

ROOT = Path(__file__).resolve().parents[1]
build = load_entrypoint("scripts/build/build_artifacts.py")


def test_cloud_units_import_only_their_own_implementation_and_public_protocol():
    public_parsers = {"market_data", "internal_xml", "external_xml"}
    for key, directory in build.units().items():
        spec, files = build.own_files(directory)
        if spec["runtime"] == "local":
            continue
        own = {spec.get("python_package"), "unit_spark", "erl_protocol"}
        for path in [*directory.rglob("*.py")]:
            for node in ast.walk(ast.parse(path.read_text())):
                imports = [node.module] if isinstance(node, ast.ImportFrom) and node.module else []
                imports += [a.name for a in node.names] if isinstance(node, ast.Import) else []
                for name in imports:
                    assert (
                        name.split(".")[0]
                        not in (public_parsers | {"pipeline", "sources", "load", "connect", "ingestion"})
                        - own
                    ), (key, name)
        assert "erl_protocol/__init__.py" in files
        assert not any(name.startswith("sql/") for name in files), "only the local SQL consumer needs SQL"


def test_integrity_dependency_has_one_version_and_selected_source_of_truth():
    for key, directory in build.units().items():
        spec, files = build.own_files(directory)
        assert spec["protocol_version"] == "2.0.0"
        for name, source in files.items():
            if name.startswith("erl_protocol/"):
                assert source == ROOT / "protocols/integrity-v2" / name
            elif name.startswith("contracts/"):
                assert source == ROOT / name
        if key == "lambda/market-data":
            assert "erl_protocol/xml_validation.py" not in files
            assert spec["dependencies"] == ["pyarrow"]


def test_protocol_packages_contain_only_reachable_modules():
    """Check the import closure, including deferred imports and bundled public dependencies."""
    protocol = ROOT / "protocols/integrity-v2/erl_protocol"
    for key, directory in build.units().items():
        spec, files = build.own_files(directory)
        pending = [
            p for name, p in files.items() if p.suffix == ".py" and not name.startswith("erl_protocol/")
        ]
        if spec["runtime"] == "glue":
            pending.append(directory / spec["entrypoint"])
        required = set()
        while pending:
            for node in ast.walk(ast.parse(pending.pop().read_text())):
                modules = []
                if isinstance(node, ast.ImportFrom) and node.module == "erl_protocol":
                    modules = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and (node.module or "").startswith("erl_protocol."):
                    modules = [node.module.split(".")[1]]
                elif isinstance(node, ast.Import):
                    modules = [
                        alias.name.split(".")[1]
                        for alias in node.names
                        if alias.name.startswith("erl_protocol.")
                    ]
                for name in modules:
                    if name not in required:
                        required.add(name)
                        pending.append(protocol / f"{name}.py")
        included = {
            Path(name).stem
            for name in files
            if name.startswith("erl_protocol/") and name != "erl_protocol/__init__.py"
        }
        assert included == required, (key, included - required, required - included)


def test_compatible_protocol_implementation_change_uses_hashes_not_a_version_bump(tmp_path):
    project = tmp_path / "project"
    for directory in ("apps", "glue", "lambda", "protocols", "contracts"):
        shutil.copytree(
            ROOT / directory, project / directory, ignore=shutil.ignore_patterns("__pycache__", "*.pyc")
        )
    shutil.copyfile(ROOT / "uv.lock", project / "uv.lock")

    def manifests():
        return {
            key: build.build_unit(path, tmp_path / "dist", {"pyarrow": {}, "lxml": {}}, project)
            for key, path in build.units(project).items()
        }

    before = manifests()
    source = project / "protocols/integrity-v2/erl_protocol/objects.py"
    old = "COPY_PART_BYTES = 1024**3"
    assert old in source.read_text()
    source.write_text(source.read_text().replace(old, "COPY_PART_BYTES = 1 << 30"))  # same integer
    after = manifests()
    changed = {key for key in before if before[key]["artifacts"] != after[key]["artifacts"]}
    assert changed == build.units(project).keys() - {"apps/source-generator"}
    for key, manifest in after.items():
        assert manifest["protocol_version"] == before[key]["protocol_version"] == "2.0.0"
        assert manifest["contracts"] == before[key]["contracts"]
        assert ("erl_protocol/objects.py" in manifest["files"]) == (key in changed)
        if key.startswith("glue/"):
            assert manifest["artifacts"]["job.py"] == before[key]["artifacts"]["job.py"]
    assert manifests() == after, "identical source produces identical hashes and manifests"


def test_exclusive_changes_addition_and_removal_do_not_propagate(tmp_path):
    project = tmp_path / "project"
    for directory in ("apps", "glue", "lambda", "protocols", "contracts"):
        shutil.copytree(
            ROOT / directory, project / directory, ignore=shutil.ignore_patterns("__pycache__", "*.pyc")
        )
    shutil.copyfile(ROOT / "uv.lock", project / "uv.lock")
    dependencies = {"pyarrow": {}, "lxml": {}}  # code-only ZIPs exercise the same deterministic builder

    def hashes():
        return {
            key: {
                name: artifact["sha256"]
                for name, artifact in build.build_unit(path, tmp_path / "dist", dependencies, project)[
                    "artifacts"
                ].items()
            }
            for key, path in build.units(project).items()
        }

    before = hashes()
    parser = project / "lambda/market-data/python/market_data/parser.py"
    parser.write_text(parser.read_text() + "\n# Exclusive market-data change.\n")
    changed = hashes()
    assert {key for key in before if before[key] != changed[key]} == {
        "lambda/market-data",
        "apps/hybrid-runner",
    }
    # The runner is a declared consumer of this public parser; other cloud units are independent.
    second = changed
    for unit in ("extract-liquidez", "extract-enquadramento", "export-risk-analytics"):
        for filename in ("spark/unit_spark.py", "job.py", "requirements.txt"):
            source = project / "glue" / unit / filename
            source.write_text(source.read_text() + "\n# Exclusive owner change.\n")
            changed_owner = hashes()
            assert {key for key in second if second[key] != changed_owner[key]} == {f"glue/{unit}"}
            second = changed_owner
    for unit, filename in [
        ("apps/publisher", "python/load/publisher.py"),
        ("apps/source-generator", "python/sources/synthetic.py"),
        ("lambda/internal-xml", "python/internal_xml/parser.py"),
        ("lambda/external-xml", "python/external_xml/parser.py"),
    ]:
        source = project / unit / filename
        source.write_text(source.read_text() + "\n# Exclusive owner change.\n")
        changed_owner = hashes()
        assert {key for key in second if second[key] != changed_owner[key]} == {unit, "apps/hybrid-runner"}
        second = changed_owner
    shutil.copytree(project / "glue/extract-liquidez", project / "glue/additional-job")
    spec = project / "glue/additional-job/unit.json"
    data = json.loads(spec.read_text())
    data["id"] = "additional-job"
    spec.write_text(json.dumps(data))
    added = hashes()
    assert all(added[key] == second[key] for key in second)
    for unit in ("extract-liquidez", "extract-enquadramento", "export-risk-analytics"):
        shutil.rmtree(project / "glue" / unit)
        remaining = hashes()
        assert all(remaining[key] == added[key] for key in remaining)
    shutil.rmtree(project / "lambda/market-data")
    for key, directory in build.units(project).items():
        if key == "apps/hybrid-runner":
            with pytest.raises(ValueError, match="declared public dependency"):
                build.build_unit(directory, tmp_path / "dist", dependencies, project)
        else:
            result = build.build_unit(directory, tmp_path / "dist", dependencies, project)
            assert {n: a["sha256"] for n, a in result["artifacts"].items()} == added[key]


@pytest.mark.parametrize("unit", ["extract-liquidez", "extract-enquadramento", "export-risk-analytics"])
def test_glue_packages_do_not_include_local_or_native_python_dependencies(tmp_path, unit):
    result = build.build_unit(ROOT / "glue" / unit, tmp_path, {}, ROOT)
    assert set(result["artifacts"]) == {"job.py", "lib.zip"}
    with zipfile.ZipFile(tmp_path / "glue" / unit / "lib.zip") as archive:
        names = archive.namelist()
    assert "unit_spark.py" in names
    assert not any(n.startswith(("pyarrow/", "sqlalchemy/", "lxml/", "pipeline/", "sources/")) for n in names)
    for name, metadata in result["artifacts"].items():
        assert metadata["s3_key"] == f"case01/glue/{unit}/{metadata['sha256']}/{name}"


@pytest.mark.parametrize("unit", ["extract-liquidez", "extract-enquadramento"])
@pytest.mark.parametrize("outcome", ["success", "write_failure", "copy_failure", "checksum_failure"])
def test_bronze_unit_flow_commits_only_complete_outputs(tmp_path, monkeypatch, unit, outcome):
    """Exercise each real job's control flow, stubbing only Spark processing (not a Spark test).

    The independently owned scripts must preserve checksum -> produce/copy all -> commit.
    Files, checksums, conditional copies and manifests are the actual integrity implementation.
    """
    import hashlib
    import sys
    from types import SimpleNamespace

    from erl_protocol.contracts import bronze_entity
    from erl_protocol.errors import ContractError
    from erl_protocol.objects import LocalStore

    class FlowStore(LocalStore):
        """Local fixture exposing the temporary cleanup method supplied by real S3Store."""

        def delete_prefix(self, prefix):
            keys = self.list(prefix)
            for key in keys:
                (self.root / key).unlink()
            return len(keys)

    store = FlowStore(tmp_path / "lake")
    run = "20261026T190000Z-abcdef"
    key = f"landing/db/fund_ops/fund/reference_date=2026-10-26/{run}"
    payload = b"Spark processing stub fixture, not Parquet data"
    store.put(key + ".parquet", payload)
    store.put(
        key + ".json",
        json.dumps(
            {
                "source_system": "fund_ops",
                "entity": "fund",
                "reference_date": "2026-10-26",
                "run_id": run,
                "object": key + ".parquet",
                "rows": 2,
                "object_sha256": "invalid"
                if outcome == "checksum_failure"
                else hashlib.sha256(payload).hexdigest(),
                "content_sha256": "c" * 64,
                "content_hash_version": 2,
            }
        ).encode(),
    )
    columns = [*bronze_entity("fund_ops", "fund").columns, "meta_row_number", "meta_row_hash"]
    frame = SimpleNamespace(columns=columns)
    calls = []

    def write_parts(frame, uri, **kwargs):
        calls.append("write")
        if outcome == "write_failure":
            raise RuntimeError("controlled processing failure")
        prefix = Path(uri).relative_to(store.root).as_posix() + "/"
        store.put(prefix + "part-1.parquet", b"one")
        store.put(prefix + "part-2.parquet", b"two")

    original_copy = LocalStore.copy_if_absent

    def copy(self, source, target):
        calls.append("copy")
        if outcome == "copy_failure" and calls.count("copy") == 2:
            raise RuntimeError("controlled copy failure")
        return original_copy(self, source, target)

    monkeypatch.setattr(LocalStore, "copy_if_absent", copy)
    monkeypatch.setitem(
        sys.modules,
        "unit_spark",
        SimpleNamespace(
            read_parquet=lambda *_: frame,
            with_constant_columns=lambda frame, *_: frame,
            select_in_order=lambda frame, *_: frame,
            single_row=lambda *_: frame,
            write_parquet=write_parts,
        ),
    )
    job = load_entrypoint(f"glue/{unit}/job.py")
    volume_profile = json.loads((ROOT / "glue" / unit / "unit.json").read_text())["volume_profile"]

    def invoke():
        return job.ingest(None, store, key + ".json", "conformance", volume_profile)

    if outcome == "success":
        accepted = invoke()
        assert accepted.status == "ACCEPTED" and accepted.input_hash_version == 2
        assert all(store.exists(path) for path in accepted.bronze_object.split(","))
        assert invoke() == accepted, "retry reuses the committed result without another write/copy"
        assert calls == ["write", "copy", "copy"]
    else:
        with pytest.raises(ContractError if outcome == "checksum_failure" else RuntimeError):
            invoke()
        assert store.list("manifests/ingestion/") == [], "incomplete output cannot become visible"
        if outcome == "checksum_failure":
            assert calls == [], "reject bad landing before processing"
