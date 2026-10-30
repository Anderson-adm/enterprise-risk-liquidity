"""Minimal object store used for landing, Bronze, manifests, quarantine and publication packages.

Two implementations with identical semantics:

* ``LocalStore`` - a directory tree (development / tests).
* ``S3Store`` - an S3 bucket through an injected boto3 client. ``put(..., if_absent=True)`` and
  ``copy_if_absent`` use S3 conditional writes (``If-None-Match: *``), so an existing key is
  never silently overwritten.

Append-only is a property of how the platform writes (unique run-scoped keys + conditional
writes). It is *not* physical immutability: S3 Object Lock is not enabled in this project.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from base64 import b64decode
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from utils.errors import InfrastructureError, ObjectExistsError

COPY_PART_BYTES = 1024**3  # server-side copy part size (S3 allows 5 MiB .. 5 GiB per part)


class ObjectStore(Protocol):
    def put(self, key: str, data: bytes, *, if_absent: bool = True) -> str: ...
    def get(self, key: str) -> bytes: ...
    def exists(self, key: str) -> bool: ...
    def list(self, prefix: str) -> list[str]: ...
    def uri(self, key: str) -> str: ...
    def sha256_hex(self, key: str) -> str: ...


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

    def sha256_hex(self, key: str) -> str:
        h = hashlib.sha256()
        with self._path(key).open("rb") as fh:
            for block in iter(lambda: fh.read(1 << 20), b""):
                h.update(block)
        return h.hexdigest()

    def copy_if_absent(self, source: str, target: str) -> str:
        path = self._path(target)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self._path(source).open("rb") as src, open(path, "xb") as dst:
                shutil.copyfileobj(src, dst)
        except FileExistsError as exc:
            raise ObjectExistsError(f"refusing to overwrite {self.uri(target)}") from exc
        return self.uri(target)


class S3Store:
    """S3 bucket access. The caller owns the client (credentials, region, retries)."""

    def __init__(self, bucket: str, *, client: Any) -> None:
        self.bucket = bucket
        self._s3 = client

    def put(self, key: str, data: bytes, *, if_absent: bool = True) -> str:
        from botocore.exceptions import ClientError

        kwargs = {"Bucket": self.bucket, "Key": key, "Body": data, "ChecksumAlgorithm": "SHA256"}
        if if_absent:
            kwargs["IfNoneMatch"] = "*"
        try:
            self._s3.put_object(**kwargs)
        except ClientError as exc:
            self._raise(exc, key, "put")
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

    def sha256_hex(self, key: str) -> str:
        """SHA-256 stored by S3 at upload when available (no download); otherwise streamed."""
        head = self._s3.head_object(Bucket=self.bucket, Key=key, ChecksumMode="ENABLED")
        checksum = head.get("ChecksumSHA256")
        if checksum and "-" not in checksum:  # a multipart composite checksum is not the file hash
            return b64decode(checksum).hex()
        h = hashlib.sha256()
        for block in self._s3.get_object(Bucket=self.bucket, Key=key)["Body"].iter_chunks(1 << 20):
            h.update(block)
        return h.hexdigest()

    def copy_if_absent(self, source: str, target: str) -> str:
        """Server-side copy that never overwrites ``target``.

        CopyObject cannot be conditional, and HEAD-then-COPY races. A multipart upload whose
        parts are ``UploadPartCopy`` ranges is completed with ``If-None-Match: *``, so the
        object bytes never pass through this process (Spark driver) and the commit is atomic.
        Any failure after the upload is created aborts it (best effort, original error kept).
        """
        from botocore.exceptions import ClientError

        size = self._s3.head_object(Bucket=self.bucket, Key=source)["ContentLength"]
        upload_id = self._s3.create_multipart_upload(Bucket=self.bucket, Key=target)["UploadId"]
        try:
            parts = []
            for number, start in enumerate(range(0, max(size, 1), COPY_PART_BYTES), start=1):
                kwargs = {
                    "Bucket": self.bucket,
                    "Key": target,
                    "UploadId": upload_id,
                    "PartNumber": number,
                    "CopySource": {"Bucket": self.bucket, "Key": source},
                }
                if size:
                    kwargs["CopySourceRange"] = f"bytes={start}-{min(start + COPY_PART_BYTES, size) - 1}"
                etag = self._s3.upload_part_copy(**kwargs)["CopyPartResult"]["ETag"]
                parts.append({"ETag": etag, "PartNumber": number})
            self._s3.complete_multipart_upload(
                Bucket=self.bucket,
                Key=target,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
                IfNoneMatch="*",
            )
        except BaseException as exc:  # any failure once the upload exists, incl. transport errors
            try:
                self._s3.abort_multipart_upload(Bucket=self.bucket, Key=target, UploadId=upload_id)
            except Exception as abort_exc:  # never mask the original failure
                exc.add_note(
                    f"abort of multipart upload {upload_id} for {self.uri(target)} also failed "
                    f"({type(abort_exc).__name__}); its parts may remain until the bucket lifecycle "
                    "aborts incomplete uploads"
                )
            if isinstance(exc, ClientError):
                self._raise(exc, target, "copy")
            raise
        return self.uri(target)

    def delete_prefix(self, prefix: str) -> int:
        """Delete every object under ``prefix`` (temporary outputs only); returns the count."""
        keys = self.list(prefix)
        for i in range(0, len(keys), 1000):
            batch = [{"Key": k} for k in keys[i : i + 1000]]
            result = self._s3.delete_objects(Bucket=self.bucket, Delete={"Objects": batch, "Quiet": True})
            if result.get("Errors"):
                raise InfrastructureError(
                    f"S3 delete failed under {self.uri(prefix)}: {result['Errors'][:3]}"
                )
        return len(keys)

    def _raise(self, exc: Exception, key: str, action: str) -> None:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
        if code in {"PreconditionFailed", "ConditionalRequestConflict"}:
            raise ObjectExistsError(f"refusing to overwrite {self.uri(key)}") from exc
        raise InfrastructureError(f"S3 {action} failed for {self.uri(key)}: {code}") from exc
