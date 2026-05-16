"""Backfill missing resources.file_hash values from stored files.

Usage:
    python backend/backfill_file_hash.py

The script scans resources with a blank file_hash, reads the corresponding file
from backend/storage, computes SHA-256, and updates the database.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        if default is None:
            raise RuntimeError(f"Missing environment variable: {name}")
        return default
    return value


def _hash_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _resource_file_candidates(storage_dir: Path, file_path: str) -> Iterable[Path]:
    original = Path(file_path)
    yield original
    if not original.is_absolute():
        yield storage_dir / original.name
        yield storage_dir / original
        if original.parent.name:
            yield storage_dir / original.name


def main() -> None:
    database_url = _env("DATABASE_URL")
    storage_dir = (Path(__file__).resolve().parent / "storage").resolve()
    engine: Engine = create_engine(database_url, future=True)

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, file_path
                FROM resources
                WHERE file_hash IS NULL OR file_hash = ''
                ORDER BY id
                """
            )
        ).mappings().all()

        updated = 0
        missing = []
        seen_hashes: dict[str, int] = {}

        for row in rows:
            resource_id = int(row["id"])
            file_path = str(row["file_path"] or "")
            file_obj = None
            for candidate in _resource_file_candidates(storage_dir, file_path):
                if candidate.exists():
                    file_obj = candidate
                    break
            if file_obj is None:
                missing.append((resource_id, file_path))
                continue

            file_hash = _hash_file(file_obj)
            if file_hash in seen_hashes:
                # Keep the database update idempotent, but note duplicate content.
                pass
            else:
                seen_hashes[file_hash] = resource_id

            conn.execute(
                text("UPDATE resources SET file_hash = :file_hash WHERE id = :id"),
                {"file_hash": file_hash, "id": resource_id},
            )
            updated += 1

    print({"updated": updated, "missing": len(missing)})
    if missing:
        print("Missing files:")
        for item in missing[:20]:
            print(item)


if __name__ == "__main__":
    main()
