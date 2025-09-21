import hashlib
from io import BytesIO
from typing import BinaryIO


def sha256_bytes(data: bytes) -> str:
    """Compute SHA256 hex digest of a byte sequence."""
    return hashlib.sha256(data).hexdigest()


def sha256_stream(stream: BinaryIO, chunk_size: int = 1024 * 1024) -> str:
    """Compute SHA256 hex digest for a file-like stream."""
    h = hashlib.sha256()
    while True:
        chunk = stream.read(chunk_size)
        if not chunk:
            break
        h.update(chunk)
    return h.hexdigest()


def bytesio_copy(src: BytesIO) -> BytesIO:
    """Return a copy of a BytesIO without changing the source position."""
    pos = src.tell()
    src.seek(0)
    data = src.read()
    src.seek(pos)
    dup = BytesIO(data)
    dup.seek(0)
    return dup