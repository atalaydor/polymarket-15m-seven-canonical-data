"""Bounded, verified and restart-safe source acquisition."""

from __future__ import annotations

import hashlib
import os
import shutil
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Protocol

from canonical_data.errors import ResourceLimitError, SourceError
from canonical_data.httpclient import USER_AGENT
from canonical_data.inventory import SourceObject


class Response(Protocol):
    headers: object

    def read(self, size: int = -1) -> bytes: ...

    def __enter__(self) -> Response: ...

    def __exit__(self, *args: object) -> None: ...


@dataclass(frozen=True)
class AcquiredObject:
    source: SourceObject
    path: Path
    byte_length: int
    sha256: str
    etag: str | None


class BoundedAcquirer:
    def __init__(
        self,
        work_dir: Path,
        max_object_bytes: int = 800_000_000,
        min_free_bytes: int = 8_000_000_000,
    ):
        self.work_dir = work_dir
        self.max_object_bytes = max_object_bytes
        self.min_free_bytes = min_free_bytes

    def _verify_headroom(self) -> None:
        self.work_dir.mkdir(parents=True, exist_ok=True)
        if shutil.disk_usage(self.work_dir).free < self.min_free_bytes:
            raise ResourceLimitError("insufficient disk headroom")

    def acquire(self, source: SourceObject) -> AcquiredObject:
        self._verify_headroom()
        name = hashlib.sha256(source.url.encode()).hexdigest() + ".source"
        final = self.work_dir / name
        partial = final.with_suffix(".partial")
        if final.exists():
            return self._verify_existing(source, final)
        request = urllib.request.Request(
            source.url,
            headers={"Accept-Encoding": "identity", "User-Agent": USER_AGENT},
        )
        digest = hashlib.sha256()
        total = 0
        etag: str | None = None
        try:
            with (
                urllib.request.urlopen(request, timeout=60) as response,
                partial.open("wb") as output,
            ):
                headers = response.headers
                content_length = headers.get("Content-Length")
                etag = headers.get("ETag")
                if content_length is not None and int(content_length) > self.max_object_bytes:
                    raise ResourceLimitError("source object exceeds acquisition cap")
                while chunk := response.read(1_048_576):
                    total += len(chunk)
                    if total > self.max_object_bytes:
                        raise ResourceLimitError("stream exceeded acquisition cap")
                    output.write(chunk)
                    digest.update(chunk)
                output.flush()
                os.fsync(output.fileno())
            actual = digest.hexdigest()
            self._verify_claims(source, total, actual)
            os.replace(partial, final)
            return AcquiredObject(source, final, total, actual, etag)
        except Exception:
            partial.unlink(missing_ok=True)
            raise

    def _verify_existing(self, source: SourceObject, path: Path) -> AcquiredObject:
        digest = hashlib.sha256()
        total = 0
        with path.open("rb") as handle:
            while chunk := handle.read(1_048_576):
                total += len(chunk)
                digest.update(chunk)
        actual = digest.hexdigest()
        self._verify_claims(source, total, actual)
        return AcquiredObject(source, path, total, actual, None)

    @staticmethod
    def _verify_claims(source: SourceObject, total: int, actual: str) -> None:
        if source.expected_bytes is not None and total != source.expected_bytes:
            raise SourceError("source byte length mismatch")
        if source.expected_sha256 is not None and actual != source.expected_sha256:
            raise SourceError("source checksum mismatch")


def copy_bounded(source: BinaryIO, target: BinaryIO, max_bytes: int) -> tuple[int, str]:
    digest = hashlib.sha256()
    total = 0
    while chunk := source.read(min(1_048_576, max_bytes + 1 - total)):
        total += len(chunk)
        if total > max_bytes:
            raise ResourceLimitError("copy exceeded bound")
        target.write(chunk)
        digest.update(chunk)
    return total, digest.hexdigest()
