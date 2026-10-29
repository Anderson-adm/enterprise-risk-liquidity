"""Minimal object store used for landing, Bronze, manifests, quarantine and publication packages.

Two implementations with identical semantics:

* ``LocalStore`` — a directory tree (development / tests).
* ``S3Store`` — an S3 bucket. ``put(..., if_absent=True)`` uses S3 conditional writes
  (``If-None-Match: *``) so an existing key is never silently overwritten.

Append-only is a property of how the platform writes (unique run-scoped keys + conditional
puts). It is *not* physical immutability: S3 Object Lock is not enabled in this project.
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from erl.errors import InfrastructureError, ObjectExistsError


class ObjectStore(Protocol):
    def put(self, key: str, data: bytes, *, if_absent: bool = True) -> str: ...
    def get(self, key: str) -> bytes: ...
    def exists(self, key: str) -> bool: ...
    def list(self, prefix: str) -> list[str]: ...
    def uri(self, key: str) -> str: ...


@dataclass(frozen=True)
class LocalStore:
    root: Path

    def _path(self, key: str) -> Path:
        path = (self.root / key).resolve()
        if not path.is_relative_to(self.root.resolve()):
            raise ValueError(f"key escapes store root: {key}")
        return path

    def put(self, key: str, data: bytes, *, if_absent: bool = True) -> str:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        if if_absent:
            try:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            except FileExistsError as exc:
                raise ObjectExistsError(f"refusing to overwrite {self.uri(key)}") from exc
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
        else:
            # atomic replace for mutable keys (none of the Bronze keys use this)
            with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
                tmp.write(data)
            os.replace(tmp.name, path)
        return self.uri(key)

    def get(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def list(self, prefix: str) -> list[str]:
        base = self._path(prefix) if prefix else self.root
        if base.is_file():
            return [prefix]
        if not base.exists():
            return []
        root = self.root.resolve()
        return sorted(p.resolve().relative_to(root).as_posix() for p in base.rglob("*") if p.is_file())

    def uri(self, key: str) -> str:
        return str(self.root / key)


class S3Store:
    def __init__(self, bucket: str, *, client=None) -> None:
        import boto3  # local import keeps LocalStore usable without AWS config

        self.bucket = bucket
        self._s3 = client or boto3.client("s3")

    def put(self, key: str, data: bytes, *, if_absent: bool = True) -> str:
        from botocore.exceptions import ClientError

        kwargs = {"Bucket": self.bucket, "Key": key, "Body": data, "ChecksumAlgorithm": "SHA256"}
        if if_absent:
            kwargs["IfNoneMatch"] = "*"
        try:
            self._s3.put_object(**kwargs)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in {"PreconditionFailed", "ConditionalRequestConflict"}:
                raise ObjectExistsError(f"refusing to overwrite {self.uri(key)}") from exc
            raise InfrastructureError(f"S3 put failed for {self.uri(key)}: {code}") from exc
        return self.uri(key)

    def get(self, key: str) -> bytes:
        return self._s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def exists(self, key: str) -> bool:
        # HEAD on a missing object can return 403 when ListBucket is prefix-scoped.
        # Listing the exact prefix uses that scoped permission and distinguishes a
        # real object from a longer key sharing its prefix without broadening IAM.
        objects = self._s3.list_objects_v2(Bucket=self.bucket, Prefix=key, MaxKeys=1).get("Contents", [])
        return bool(objects and objects[0]["Key"] == key)

    def list(self, prefix: str) -> list[str]:
        keys: list[str] = []
        for page in self._s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return sorted(keys)

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"
