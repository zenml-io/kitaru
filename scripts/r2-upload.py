#!/usr/bin/env -S uv run --python 3.12 --with boto3 --with python-dotenv
# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3", "python-dotenv"]
# ///
"""
Upload images to the Kitaru R2 bucket (kitaru-assets).

Uploads go under `content/uploads/{sha8}/{filename}` by default.
Use --prefix to customize (e.g. `content/blog`).

Usage:
    uv run scripts/r2-upload.py path/to/image.avif
    uv run scripts/r2-upload.py path/to/image.avif --prefix content/blog
    uv run scripts/r2-upload.py img1.png img2.jpg img3.avif
    uv run scripts/r2-upload.py path/to/hero.avif --frontmatter

Environment variables (from .env or shell):
    CLOUDFLARE_ACCOUNT_ID  — Cloudflare account ID
    R2_ACCESS_KEY_ID       — R2 API token key
    R2_SECRET_ACCESS_KEY   — R2 API token secret
    R2_BUCKET              — Bucket name (default: kitaru-assets)
    R2_PUBLIC_BASE_URL     — Public URL base (default: https://assets.kitaru.ai)
"""

from __future__ import annotations

import argparse
import hashlib
import mimetypes
import os
import re
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

# ── Defaults ──────────────────────────────────────────────────────────
DEFAULT_BUCKET = os.getenv("R2_BUCKET", "kitaru-assets")
DEFAULT_PREFIX = "content/uploads"
DEFAULT_PUBLIC_BASE = os.getenv(
    "R2_PUBLIC_BASE_URL",
    "https://assets.kitaru.ai",
)

CONTENT_TYPE_OVERRIDES: dict[str, str] = {
    ".svg": "image/svg+xml",
    ".webp": "image/webp",
    ".avif": "image/avif",
    ".json": "application/json",
    ".pdf": "application/pdf",
}


def sha256_file(path: Path) -> str:
    """Return hex SHA-256 of file contents."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def guess_content_type(path: Path) -> str:
    ext = path.suffix.lower()
    if ext in CONTENT_TYPE_OVERRIDES:
        return CONTENT_TYPE_OVERRIDES[ext]
    ct, _ = mimetypes.guess_type(str(path))
    return ct or "application/octet-stream"


def sanitize_filename(name: str) -> str:
    """Lowercase, replace spaces/special chars with hyphens."""
    name = name.lower().strip()
    name = re.sub(r"[^\w.\-]", "-", name)
    name = re.sub(r"-{2,}", "-", name)
    return name.strip("-")


def build_key(prefix: str, sha8: str, filename: str) -> str:
    return f"{prefix}/{sha8}/{filename}"


def make_client():
    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")

    missing = []
    if not account_id:
        missing.append("CLOUDFLARE_ACCOUNT_ID")
    if not access_key:
        missing.append("R2_ACCESS_KEY_ID")
    if not secret_key:
        missing.append("R2_SECRET_ACCESS_KEY")

    if missing:
        print(f"Error: Missing environment variables: {', '.join(missing)}")
        print("Copy .env.example to .env and fill in your R2 credentials.")
        sys.exit(1)

    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
    )


def object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise
    else:
        return True


def upload_one(
    s3,
    local_path: Path,
    *,
    bucket: str,
    prefix: str,
    public_base: str,
    overwrite: bool,
) -> str | None:
    """Upload a single file. Returns the public URL, or None on skip/error."""
    if not local_path.exists():
        print(f"  Error: file not found: {local_path}")
        return None

    sha8 = sha256_file(local_path)[:8]
    filename = sanitize_filename(local_path.name)
    key = build_key(prefix, sha8, filename)
    content_type = guess_content_type(local_path)
    public_url = f"{public_base}/{key}"

    if not overwrite and object_exists(s3, bucket, key):
        print(f"  Already exists: {key}")
        print(f"  URL: {public_url}")
        return public_url

    print(f"  Uploading: {local_path.name} → {key} ({content_type})")
    s3.upload_file(
        str(local_path),
        bucket,
        key,
        ExtraArgs={"ContentType": content_type},
    )
    print(f"  URL: {public_url}")
    return public_url


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload images to the Kitaru R2 bucket",
        epilog="URLs are printed to stdout for pasting into content frontmatter.",
    )
    parser.add_argument("files", nargs="+", type=Path, help="Files to upload")
    parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        help=f"R2 key prefix (default: {DEFAULT_PREFIX})",
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"R2 bucket name (default: {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--public-base",
        default=DEFAULT_PUBLIC_BASE,
        help=f"Public URL base (default: {DEFAULT_PUBLIC_BASE})",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-upload even if key already exists in R2",
    )
    parser.add_argument(
        "--frontmatter",
        action="store_true",
        help="Print paste-ready YAML frontmatter snippet",
    )
    args = parser.parse_args()

    s3 = make_client()
    urls: list[str] = []

    for filepath in args.files:
        url = upload_one(
            s3,
            filepath,
            bucket=args.bucket,
            prefix=args.prefix,
            public_base=args.public_base,
            overwrite=args.overwrite,
        )
        if url:
            urls.append(url)

    if args.frontmatter and urls:
        print("\n# ── Paste into frontmatter ──")
        print(f'ogImage: "{urls[0]}"')
        print(f'image: "{urls[0]}"')

    if urls:
        print(f"\n✅ Uploaded {len(urls)} file(s)")
    else:
        print("\n⚠️  No files were uploaded")
        sys.exit(1)


if __name__ == "__main__":
    main()
