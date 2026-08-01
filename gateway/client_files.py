"""Client file upload helpers for Gateway-owned reference images."""
import hashlib
import mimetypes
import re
import uuid
from pathlib import Path

from fastapi import UploadFile


ALLOWED_MIME_TYPES = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp"}
MAX_FILE_BYTES = 20 * 1024 * 1024


def storage_root(db_path: Path) -> Path:
    return db_path.parent / "client_files"


async def save_upload(db, db_path: Path, upload: UploadFile) -> dict:
    original = Path(upload.filename or "upload").name
    data = await upload.read()
    result = validate_image(original, upload.content_type, data)
    if not result["ok"]:
        return {"ok": False, "original_filename": original, "error": result["error"]}

    file_id = f"file_{uuid.uuid4().hex}"
    digest = hashlib.sha256(data).hexdigest()
    suffix = ALLOWED_MIME_TYPES[result["mime_type"]]
    stored_filename = f"{file_id}{suffix}"
    root = storage_root(db_path)
    root.mkdir(parents=True, exist_ok=True)
    (root / stored_filename).write_bytes(data)
    await db.execute(
        """
        INSERT INTO client_files(file_id, original_filename, stored_filename, mime_type, size_bytes, sha256)
        VALUES(?, ?, ?, ?, ?, ?)
        """,
        (file_id, original, stored_filename, result["mime_type"], len(data), digest),
    )
    await db.commit()
    return {
        "ok": True,
        "file_id": file_id,
        "original_filename": original,
        "mime_type": result["mime_type"],
        "size_bytes": len(data),
        "sha256": digest,
    }


def validate_image(filename: str, content_type: str | None, data: bytes) -> dict:
    if not data:
        return {"ok": False, "error": "empty_file"}
    if len(data) > MAX_FILE_BYTES:
        return {"ok": False, "error": "file_too_large"}
    sniffed = _sniff_mime(data)
    guessed = (content_type or mimetypes.guess_type(filename)[0] or "").lower()
    mime_type = sniffed or guessed
    if mime_type not in ALLOWED_MIME_TYPES:
        return {"ok": False, "error": "unsupported_image_type"}
    if guessed in ALLOWED_MIME_TYPES and guessed != mime_type:
        return {"ok": False, "error": "mime_type_mismatch"}
    return {"ok": True, "mime_type": mime_type}


def local_path_for_file(db_path: Path, file_row: dict) -> str:
    stored = str(file_row["stored_filename"])
    if not re.fullmatch(r"file_[0-9a-f]{32}\.(jpg|png|webp)", stored):
        raise ValueError("invalid stored filename")
    path = (storage_root(db_path) / stored).resolve()
    root = storage_root(db_path).resolve()
    if root not in path.parents:
        raise ValueError("stored file escapes upload directory")
    return str(path)


def _sniff_mime(data: bytes) -> str | None:
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return None
