"""Offline consistency of the layout: import roots, SQL files vs contracts, build vs Terraform.

These checks read files only (no AWS, no Terraform state or backend). The built artifacts
themselves are exercised by scripts/build/smoke_artifacts.py; Terraform by terraform test."""

from __future__ import annotations

import ast
import importlib.metadata
import re
import sys
import tomllib
from pathlib import Path

import pytest

from connect.secrets import get_secret
from erl_protocol.contracts import bronze_entity
from erl_protocol.errors import ConfigurationError
from pipeline import extraction_catalog
from tests.conftest import load_entrypoint
from utils.sql_files import read_sql, render_identifiers

ROOT = Path(__file__).resolve().parents[1]
DEV = ROOT / "infra/environments/dev"
build = load_entrypoint("scripts/build/build_artifacts.py")


def _wheel() -> dict:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["hatch"]["build"]["targets"]["wheel"]


def _packages() -> list[str]:
    return _wheel()["only-include"]


def test_wheel_maps_every_package_to_a_top_level_name():
    """A non-editable wheel once contained python/connect instead of connect: hatch only strips
    what ``sources`` maps. Every included directory must map to its own name."""
    sources = _wheel()["sources"]
    for path in _packages():
        prefix = max((p for p in sources if path.startswith(p)), key=len)
        mapped = (sources[prefix] + path.removeprefix(prefix)).strip("/")
        assert mapped == Path(path).name, (path, mapped)


def test_every_import_root_package_is_declared_and_importable():
    declared = set(_packages())
    roots = [
        "apps/hybrid-runner/python",
        "apps/publisher/python",
        "apps/source-generator/python",
        "protocols/integrity-v2",
        *[f"lambda/{u}/python" for u in ("market-data", "internal-xml", "external-xml")],
    ]
    found = {
        p.parent.relative_to(ROOT).as_posix() for root in roots for p in (ROOT / root).glob("*/__init__.py")
    } | {"contracts", "apps/hybrid-runner/sql"}
    assert declared == found
    assert not (ROOT / "src").exists(), (
        "implementation belongs to an owning unit or pinned integrity protocol"
    )


def test_top_level_names_do_not_collide_with_stdlib_or_installed_distributions():
    names = {Path(p).name for p in _packages()}
    assert not names & set(sys.stdlib_module_names)
    owners = importlib.metadata.packages_distributions()
    foreign = {n: owners[n] for n in names if n in owners and owners[n] != ["erl-platform"]}
    assert foreign == {}


def test_artifact_contents_are_declared_packages():
    for key, unit in build.units().items():
        spec, files = build.own_files(unit)
        assert spec["protocol_version"] == "2.0.0"
        if spec["runtime"] != "local":
            assert not any(
                name.startswith(("pipeline/", "sources/", "load/", "ingestion/")) for name in files
            )
            assert all(
                p.is_relative_to(unit)
                or p.is_relative_to(ROOT / "protocols/integrity-v2")
                or p.is_relative_to(ROOT / "contracts")
                for p in files.values()
            ), key


@pytest.mark.parametrize(("key", "query"), sorted(extraction_catalog.EXTRACT_QUERIES.items()))
def test_extraction_sql_selects_exactly_the_contract_columns(key, query):
    sql = read_sql(query.sql_file)
    select = re.search(r"SELECT\s+(.*?)\s+FROM\s", sql, re.S | re.I).group(1)
    assert tuple(c.strip() for c in select.split(",")) == bronze_entity(*key).columns
    assert re.search(r":(?!reference_date\b)\w+", re.sub(r"--.*", "", sql)) is None, "only :reference_date"


def test_every_sql_file_is_referenced():
    referenced = {q.sql_file for q in extraction_catalog.EXTRACT_QUERIES.values()}
    referenced |= set(extraction_catalog.READINESS_QUERIES.values())
    code = "\n".join(p.read_text() for p in (ROOT / "apps/hybrid-runner/python").rglob("*.py"))
    for path in (ROOT / "apps/hybrid-runner/sql").rglob("*.sql"):
        relative = path.relative_to(ROOT / "apps/hybrid-runner/sql").as_posix()
        assert relative in referenced or relative in code, f"unused SQL file {relative}"


def test_identifiers_are_rendered_only_from_an_allowlist():
    sql = "SELECT {columns} FROM gold.{gold_model}"
    ok = render_identifiers(sql, {"a", "b", "m"}, columns=["a", "b"], gold_model="m")
    assert ok == "SELECT a, b FROM gold.m"
    for bad in ("m; DROP TABLE x", "unknown"):
        with pytest.raises(ValueError):
            render_identifiers(sql, {"a", "m"}, columns=["a"], gold_model=bad)


def test_secrets_never_appear_in_errors(monkeypatch):
    monkeypatch.setenv("ERL_TEST_SECRET", "s3cr3t-value")
    import os

    assert get_secret(os.environ, "ERL_TEST_SECRET") == "s3cr3t-value"
    with pytest.raises(ConfigurationError) as exc:
        get_secret({"OTHER": "s3cr3t-value"}, "ERL_TEST_SECRET")
    assert "s3cr3t" not in str(exc.value)


# ----------------------------------------------------------------- Terraform <-> build drift


def _tf(name: str) -> str:
    return (DEV / name).read_text()


def test_lambda_handlers_in_terraform_exist_in_the_entry_point():
    local = _tf("locals.tf")
    for unit in ("market-data", "internal-xml", "external-xml"):
        assert f'unit = "{unit}"' in local or f'unit     = "{unit}"' in local
        source = ast.parse((ROOT / "lambda" / unit / "handler.py").read_text())
        assert "handler" in {n.name for n in source.body if isinstance(n, ast.FunctionDef)}
    assert 'handler = "handler.handler"' in local or 'handler  = "handler.handler"' in local


def test_terraform_uploads_exactly_the_built_artifacts_under_the_same_keys():
    fixture = DEV / "tests/fixtures/dist"
    for unit in ("db-gb", "db-tb", "gold-load"):
        manifest = __import__("json").loads((fixture / "glue" / unit / "manifest.json").read_text())
        assert set(manifest["artifacts"]) == {"job.py", "lib.zip"}
        for filename, artifact in manifest["artifacts"].items():
            assert artifact["s3_key"] == f"case01/glue/{unit}/{artifact['sha256']}/{filename}"
            assert (fixture / "glue" / unit / filename).is_file()
    for unit in ("market-data", "internal-xml", "external-xml"):
        assert (fixture / "lambda" / unit / "function.zip").is_file()
    assert "key         = each.value.key" in _tf("glue.tf")
    assert "case01/lambda/${each.value.unit}/function.zip" in _tf("lambda.tf")


def test_terraform_never_builds_and_never_writes_code_to_the_state_or_data_bucket():
    infra = "\n".join(p.read_text() for p in (ROOT / "infra").rglob("*.tf") if ".terraform" not in p.parts)
    for forbidden in ("local-exec", "archive_file", "null_resource", 'data "external"'):
        assert forbidden not in infra, forbidden
    objects = re.findall(r'resource "aws_s3_object" "\w+" \{(.*?)\n\}', infra, re.S)
    assert objects and all("bucket      = module.artifacts_bucket.id" in o for o in objects)
    assert "aws_s3_object" not in (ROOT / "infra/bootstrap/main.tf").read_text()


def test_terraform_reads_the_contract_from_its_current_location():
    path = re.search(r'file\("\$\{path\.module\}/([^"]+bronze_contract\.json)"\)', _tf("locals.tf")).group(1)
    assert (DEV / path).resolve() == (ROOT / "contracts/bronze_contract.json").resolve()


def test_generated_artifacts_are_not_versioned():
    ignored = (ROOT / ".gitignore").read_text().split()
    assert "/dist/" in ignored and "*.tfstate" in ignored
