import os, io
from urllib.parse import urlencode
from typing import Optional, Set

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import AuthorizedSession

# --- Google MIME types ---
MT_FOLDER = "application/vnd.google-apps.folder"
MT_SHEET = "application/vnd.google-apps.spreadsheet"
MT_DOC = "application/vnd.google-apps.document"
MT_SLIDE = "application/vnd.google-apps.presentation"
MT_SHORTCUT = "application/vnd.google-apps.shortcut"

# --- Sheets PDF export options (gridlines + notes) ---
SHEETS_PDF_PARAMS = {
    "format": "pdf",
    "portrait": "true",
    "size": "A4",
    "scale": "2",  # 1=100%, 2=fit width, 3=fit height, 4=fit page
    "gridlines": "true",  # ✅ show gridlines
    "printnotes": "true",  # ✅ include notes
    "sheetnames": "false",
    "printtitle": "false",
    "fzr": "true",  # repeat frozen rows
    "fzc": "true",  # repeat frozen cols
}


def sheets_export_url(file_id: str, extra: dict | None = None) -> str:
    params = SHEETS_PDF_PARAMS.copy()
    if extra:
        params.update(
            {
                k: str(v).lower() if isinstance(v, bool) else str(v)
                for k, v in extra.items()
                if v is not None
            }
        )
    return (
        f"https://docs.google.com/spreadsheets/d/{file_id}/export?{urlencode(params)}"
    )


def _safe(name: str) -> str:
    bad = '<>:"/\\|?*'
    return "".join("_" if c in bad else c for c in name).strip().rstrip(".")


def _assert_pdf_response(resp):
    ct = resp.headers.get("Content-Type", "")
    if "pdf" not in ct.lower():
        text = ""
        try:
            # only read a small chunk to avoid big memory
            text = resp.text[:400]
        except Exception:
            pass
        raise RuntimeError(
            f"Expected PDF but got Content-Type='{ct}'. "
            f"(Not authorized? Shortcut? Body starts: {text!r})"
        )


def _download_sheet_pdf(
    authed: AuthorizedSession, file_id: str, name: str, dest_dir: str
) -> str:
    url = sheets_export_url(file_id)
    r = authed.get(url, stream=True)
    r.raise_for_status()
    _assert_pdf_response(r)
    out = os.path.join(dest_dir, f"{_safe(name)}.pdf")
    with open(out, "wb") as f:
        for chunk in r.iter_content(1024 * 1024):
            if chunk:
                f.write(chunk)
    return out


def _export_pdf_via_drive(drive, file_id: str, name: str, dest_dir: str) -> str:
    # For Docs/Slides export to PDF via Drive API
    request = drive.files().export_media(fileId=file_id, mimeType="application/pdf")
    out = os.path.join(dest_dir, f"{_safe(name)}.pdf")
    with open(out, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return out


def _download_binary(drive, file_id: str, name: str, dest_dir: str) -> str:
    # For non-Google files (e.g., PNG, PDF already, etc.)
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
      - Google Sheets: PDF (gridlines + notes)
      - Google Docs/Slides: PDF
      - Other files: original binary
      - Shortcuts: resolved and handled by target type
    Uses the provided OAuth access_token (no token.pkl).
    """
    os.makedirs(dest_dir, exist_ok=True)

    creds = Credentials(token=access_token)  # bearer token from frontend
    drive = build("drive", "v3", credentials=creds)
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
                if skip_ids and f["id"] in skip_ids:
                    continue

                fid2 = f["id"]
                fname = f.get("name", "")
                mt = f.get("mimeType", "")

                if mt == MT_FOLDER:
                    walk(fid2, os.path.join(out_dir, _safe(fname)))
                    continue

                # Resolve shortcuts
                if mt == MT_SHORTCUT:
                    tgt = f.get("shortcutDetails") or {}
                    fid2 = tgt.get("targetId") or fid2
                    mt = tgt.get("targetMimeType") or mt
                    # (Optional) refresh metadata for target name
                    meta = (
                        drive.files()
                        .get(fileId=fid2, fields="id, name, mimeType")
                        .execute()
                    )
                    fname = meta.get("name", fname)
                    mt = meta.get("mimeType", mt)

                if mt == MT_SHEET:
                    _download_sheet_pdf(authed, fid2, fname, out_dir)
                elif mt in (MT_DOC, MT_SLIDE):
                    _export_pdf_via_drive(drive, fid2, fname, out_dir)
                else:
                    _download_binary(drive, fid2, fname, out_dir)

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    walk(folder_id, dest_dir)
