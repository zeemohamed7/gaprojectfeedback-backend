# backend/google_download.py
from __future__ import annotations

import os
from typing import Optional, Set
from urllib.parse import urlencode

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import AuthorizedSession

# --- MIME types we care about -------------------------------------------------
MT_FOLDER = "application/vnd.google-apps.folder"
MT_SHEET = "application/vnd.google-apps.spreadsheet"
MT_DOC = "application/vnd.google-apps.document"
MT_SLIDE = "application/vnd.google-apps.presentation"
MT_DRAWING = "application/vnd.google-apps.drawing"
MT_SHORTCUT = "application/vnd.google-apps.shortcut"

# --- Sheets export options (PDF) ----------------------------------------------
SHEETS_PDF_PARAMS = {
    # page setup
    "format": "pdf",
    "portrait": "true",  # "false" for landscape
    "size": "A4",  # A4/LETTER/etc
    "scale": "2",  # 1=100%, 2=fit width, 3=fit height, 4=fit page
    # content options
    "gridlines": "true",  # ✅ show gridlines
    "printnotes": "true",  # ✅ include notes
    "sheetnames": "false",
    "printtitle": "false",
    # repeat frozen rows/cols
    "fzr": "true",
    "fzc": "true",
}


def _safe(name: str) -> str:
    """Sanitize filenames for local filesystem."""
    bad = '<>:"/\\|?*'
    return "".join("_" if c in bad else c for c in name).strip().rstrip(".")


def _sheets_export_url(file_id: str, extra: Optional[dict] = None) -> str:
    params = SHEETS_PDF_PARAMS.copy()
    if extra:
        # normalize types → strings
        for k, v in extra.items():
            if v is None:
                continue
            if isinstance(v, bool):
                params[k] = "true" if v else "false"
            else:
                params[k] = str(v)
    return (
        f"https://docs.google.com/spreadsheets/d/{file_id}/export?{urlencode(params)}"
    )


def _download_sheet_pdf(
    authed: AuthorizedSession, file_id: str, name: str, dest_dir: str
) -> str:
    url = _sheets_export_url(file_id)
    r = authed.get(url, stream=True)
    # Raise for HTTP auth/permission issues (401/403) etc.
    r.raise_for_status()
    out = os.path.join(dest_dir, f"{_safe(name)}.pdf")
    with open(out, "wb") as f:
        for chunk in r.iter_content(1024 * 1024):
            if chunk:
                f.write(chunk)
    return out


def _export_pdf_via_drive(drive, file_id: str, name: str, dest_dir: str) -> str:
    request = drive.files().export_media(fileId=file_id, mimeType="application/pdf")
    out = os.path.join(dest_dir, f"{_safe(name)}.pdf")
    with open(out, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return out


def _download_binary(drive, file_id: str, name: str, dest_dir: str) -> str:
    request = drive.files().get_media(fileId=file_id)
    out = os.path.join(dest_dir, _safe(name))
    with open(out, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return out


def download_folder_as_pdfs(
    folder_id: str,
    dest_dir: str,
    skip_ids: Optional[Set[str]] = None,
    *,
    access_token: str,
) -> None:
    """
    Walk a Drive folder and download:
      - Google Sheets   → PDF via docs export URL (gridlines + notes)
      - Google Docs/Slides/Drawings → PDF via Drive export
      - Other files     → original binary
      - Shortcuts       → resolve and handle by target type

    Uses the provided OAuth access_token (from frontend). No token.pkl needed.
    """
    os.makedirs(dest_dir, exist_ok=True)

    creds = Credentials(token=access_token)  # bearer from frontend
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    authed = AuthorizedSession(creds)

    def walk(fid: str, out_dir: str):
        os.makedirs(out_dir, exist_ok=True)
        page_token = None

        while True:
            resp = (
                drive.files()
                .list(
                    q=f"'{fid}' in parents and trashed=false",
                    fields=(
                        "nextPageToken, files("
                        "id, name, mimeType, shortcutDetails(targetId, targetMimeType)"
                        ")"
                    ),
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    pageSize=1000,
                    pageToken=page_token,
                )
                .execute()
            )

            for f in resp.get("files", []):
                # Skip templates if caller provided a set of IDs
                if skip_ids and f["id"] in skip_ids:
                    continue

                fid2 = f["id"]
                fname = f.get("name", "")
                mt = f.get("mimeType", "")

                # Recurse into folders
                if mt == MT_FOLDER:
                    walk(fid2, os.path.join(out_dir, _safe(fname)))
                    continue

                # Resolve shortcuts to their target
                if mt == MT_SHORTCUT:
                    tgt = f.get("shortcutDetails") or {}
                    fid2 = tgt.get("targetId") or fid2
                    mt = tgt.get("targetMimeType") or mt
                    # Refresh metadata to get the real target's name
                    meta = (
                        drive.files()
                        .get(fileId=fid2, fields="id, name, mimeType")
                        .execute()
                    )
                    fname = meta.get("name", fname)
                    mt = meta.get("mimeType", mt)

                # Route by type
                if mt == MT_SHEET:
                    _download_sheet_pdf(authed, fid2, fname, out_dir)
                elif mt in (MT_DOC, MT_SLIDE, MT_DRAWING):
                    _export_pdf_via_drive(drive, fid2, fname, out_dir)
                else:
                    _download_binary(drive, fid2, fname, out_dir)

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    walk(folder_id, dest_dir)
