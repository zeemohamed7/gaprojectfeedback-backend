import os, io, pickle, zipfile, tempfile, shutil
from urllib.parse import urlencode
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request as GoogleRequest, AuthorizedSession

TOKEN_PATH = "backend/token.pkl"


def get_creds():
    with open(TOKEN_PATH, "rb") as f:
        creds = pickle.load(f)
    if not creds.valid and creds.refresh_token:
        creds.refresh(GoogleRequest())
        with open(TOKEN_PATH, "wb") as f:
            pickle.dump(creds, f)
    return creds


def get_drive(creds):
    return build("drive", "v3", credentials=creds)


SHEETS_PDF_PARAMS = {
    "format": "pdf",
    "portrait": "true",  # set "false" for landscape
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


GOOGLE_SHEET = "application/vnd.google-apps.spreadsheet"
GOOGLE_DOC = "application/vnd.google-apps.document"
GOOGLE_SLIDE = "application/vnd.google-apps.presentation"
GOOGLE_FOLDER = "application/vnd.google-apps.folder"


def _safe(name: str) -> str:
    bad = '<>:"/\\|?*'
    return "".join("_" if c in bad else c for c in name).strip().rstrip(".")


def download_google_sheet_pdf(creds, file_id: str, name: str, dest_dir: str) -> str:
    authed = AuthorizedSession(creds)
    url = sheets_export_url(file_id)
    r = authed.get(url, stream=True)
    r.raise_for_status()
    out = os.path.join(dest_dir, f"{_safe(name)}.pdf")
    with open(out, "wb") as f:
        for chunk in r.iter_content(1024 * 1024):
            if chunk:
                f.write(chunk)
    return out


def download_drive_export_pdf(drive, file_id: str, name: str, dest_dir: str) -> str:
    request = drive.files().export_media(fileId=file_id, mimeType="application/pdf")
    out = os.path.join(dest_dir, f"{_safe(name)}.pdf")
    with open(out, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return out


def download_drive_binary(drive, file_id: str, name: str, dest_dir: str) -> str:
    request = drive.files().get_media(fileId=file_id)
    out = os.path.join(dest_dir, _safe(name))
    with open(out, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return out


def download_folder_as_pdfs(
    folder_id: str, dest_dir: str, skip_ids: set[str] | None = None
):
    creds = get_creds()
    drive = get_drive(creds)
    os.makedirs(dest_dir, exist_ok=True)

    def walk(fid: str, out_dir: str):
        os.makedirs(out_dir, exist_ok=True)
        page_token = None
        while True:
            resp = (
                drive.files()
                .list(
                    q=f"'{fid}' in parents and trashed=false",
                    fields="nextPageToken, files(id, name, mimeType)",
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
                fid2, fname, mt = f["id"], f["name"], f["mimeType"]
                if mt == GOOGLE_FOLDER:
                    walk(fid2, os.path.join(out_dir, _safe(fname)))
                elif mt == GOOGLE_SHEET:
                    download_google_sheet_pdf(creds, fid2, fname, out_dir)
                elif mt in (GOOGLE_DOC, GOOGLE_SLIDE):
                    download_drive_export_pdf(drive, fid2, fname, out_dir)
                else:
                    download_drive_binary(drive, fid2, fname, out_dir)
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    walk(folder_id, dest_dir)
