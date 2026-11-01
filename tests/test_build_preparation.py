"""Packaging dependency selection and the explicit preparation/offline-build boundary."""

from __future__ import annotations

import json
import sys

import pytest

from tests.conftest import load_entrypoint

build = load_entrypoint("scripts/build/build_artifacts.py")


def test_packaged_versions_follow_unit_declarations_and_lock(tmp_path):
    units = [tmp_path / "xml", tmp_path / "csv"]
    for unit, dependencies in zip(units, [["lxml", "pyarrow"], ["pyarrow"]], strict=True):
        unit.mkdir()
        (unit / "unit.json").write_text(json.dumps({"dependencies": dependencies}))
    lock = tmp_path / "uv.lock"
    lock.write_text(
        '[[package]]\nname = "lxml"\nversion = "1.0"\n[[package]]\nname = "pyarrow"\nversion = "2.0"\n'
    )
    assert build.packaged_pins(units, tmp_path) == {"lxml": "1.0", "pyarrow": "2.0"}
    lock.write_text(lock.read_text().replace('version = "1.0"', 'version = "1.1"'))
    assert build.packaged_pins(units, tmp_path)["lxml"] == "1.1"
    (units[0] / "unit.json").write_text(json.dumps({"dependencies": ["unlocked"]}))
    with pytest.raises(ValueError, match="not pinned"):
        build.packaged_pins(units, tmp_path)


def test_prepare_uses_the_same_target_and_build_installer_stays_offline(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(build.subprocess, "run", lambda args, **kw: calls.append((args, kw)))
    pins = {"lxml": "1.0", "pyarrow": "2.0"}
    build.install_wheels(pins, tmp_path)
    build.install_wheels(pins, tmp_path, offline=False)
    offline, online = calls
    assert "--offline" in offline[0] and "--offline" not in online[0]
    assert [arg for arg in offline[0] if arg != "--offline"] == online[0]
    for flag, value in {
        "--python-platform": "x86_64-manylinux_2_28",
        "--python-version": "3.12",
        "--only-binary": ":all:",
    }.items():
        assert online[0][online[0].index(flag) + 1] == value
    assert "--no-deps" in online[0]
    assert online[0][-2:] == ["lxml==1.0", "pyarrow==2.0"]
    assert offline[1] == online[1] == {"check": True}


def test_preparation_is_explicit_and_does_not_build_artifacts(monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(build, "install_wheels", lambda pins, target, **kw: calls.append((pins, kw)))
    monkeypatch.setattr(build, "build", lambda *_: pytest.fail("preparation must not build artifacts"))
    monkeypatch.setattr(sys, "argv", ["build_artifacts.py", "--prepare-wheels"])
    expected = build.packaged_pins(list(build.units().values()))
    build.main()
    assert calls == [(expected, {"offline": False})]
    assert json.loads(capsys.readouterr().out) == {"prepared_wheels": expected}


def test_preparation_cannot_be_mixed_with_a_unit_build(monkeypatch):
    monkeypatch.setattr(
        sys, "argv", ["build_artifacts.py", "--prepare-wheels", "--unit", "lambda/market-data"]
    )
    with pytest.raises(SystemExit) as error:
        build.main()
    assert error.value.code == 2
