# backend/google_create.py
import os
import io
import pickle
import threading
from typing import Callable, Optional, Dict, Any, List, Tuple

import pandas as pd
from googleapiclient.discovery import build
from google.auth.transport.requests import Request as GoogleRequest

# If you keep SCOPES here, import as you had it before
from backend.config import SCOPES

# ---- Mime types ----
FOLDER_MT = "application/vnd.google-apps.folder"
SHEET_MT = "application/vnd.google-apps.spreadsheet"

# Accept these as “spreadsheet-ish” sources that we’ll auto-convert into native Sheets
SPREADSHEETISH = {
    SHEET_MT,  # already a Google Sheet
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # .xlsx
    "application/vnd.ms-excel",  # .xls
    "text/csv",  # .csv
    "application/vnd.oasis.opendocument.spreadsheet",  # .ods
}


# ------------------------------------------------------------------------------
# Auth (single-user token.pkl pattern)
# ------------------------------------------------------------------------------
def get_drive_service(
    scopes=SCOPES,
    token_file: str = "backend/token.pkl",
):
    """
    Build a Drive v3 client using a saved OAuth token (token.pkl).
    This is the legacy/single-user pattern used by the generation workers.
    """
    if not os.path.exists(token_file):
        raise RuntimeError(
            "No Google token found. Visit /login to authorize and create backend/token.pkl."
        )

    with open(token_file, "rb") as f:
        creds = pickle.load(f)

    if not creds.valid and creds.refresh_token:
        creds.refresh(GoogleRequest())
        with open(token_file, "wb") as f:
            pickle.dump(creds, f)

    return build("drive", "v3", credentials=creds)


# ------------------------------------------------------------------------------
# Drive helpers
# ------------------------------------------------------------------------------
def get_or_create_subfolder(drive, parent_folder_id: str, name: str) -> dict:
    """
    Return {'id','name','webViewLink'} for a subfolder named `name` under `parent_folder_id`.
    If it doesn't exist, create it.
    """
    q = (
        f"name = '{name}' and "
        f"mimeType = '{FOLDER_MT}' and "
        f"'{parent_folder_id}' in parents and trashed = false"
    )
    resp = (
        drive.files()
        .list(
            q=q,
            spaces="drive",
            fields="files(id, name, webViewLink)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=1,
        )
        .execute()
    )
    files = resp.get("files", [])
    if files:
        return files[0]

    folder_metadata = {
        "name": name,
        "mimeType": FOLDER_MT,
        "parents": [parent_folder_id],
    }
    created = (
        drive.files()
        .create(
            body=folder_metadata,
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )
    return created


def _file_meta(drive, file_id: str) -> dict:
    return (
        drive.files()
        .get(
            fileId=file_id,
            fields="id, name, mimeType, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )


def _is_spreadsheetish(mime: str) -> bool:
    return (mime or "").lower() in SPREADSHEETISH


def ensure_spreadsheetish(drive, file_id: str, label: str):
    """
    Light validation: allow Excel/CSV/ODS/Sheets; block PDFs/images/etc.
    """
    meta = _file_meta(drive, file_id)
    mt = meta.get("mimeType", "")
    if not _is_spreadsheetish(mt):
        raise ValueError(
            f"❌ {label} ({meta.get('name')}) is not a spreadsheet file.\n"
            f"MimeType: {mt}\n"
            f"Please choose a Google Sheet, Excel (.xlsx/.xls), CSV, or ODS."
        )


def copy_as_google_sheet(
    drive,
    source_file_id: str,
    *,
    name: str,
    parents: List[str],
) -> dict:
    """
    Force-copy any spreadsheet-ish file into a *native* Google Sheet by setting mimeType on copy.
    This preserves your downstream PDF export flags (gridlines, notes).
    """
    return (
        drive.files()
        .copy(
            fileId=source_file_id,
            body={
                "name": name,
                "parents": parents,
                "mimeType": SHEET_MT,
            },
            fields="id, name, mimeType, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )


def _normalize_group_label(val) -> str:
    """
    Normalize group labels like 1.0 -> '1', but pass strings through unchanged.
    """
    if pd.isna(val):
        return ""
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        return str(int(val)) if val.is_integer() else str(val).rstrip("0").rstrip(".")
    s = str(val).strip()
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except Exception:
        pass
    return s


# ------------------------------------------------------------------------------
# Main (legacy) generator kept for /generate
# ------------------------------------------------------------------------------
def create_sheets_from_df(
    df: pd.DataFrame,
    folder_id: str,
    group_template: Optional[str],
    indiv_group_template: Optional[str],
    indiv_template: Optional[str],
    *,
    do_group: bool = False,
    do_indiv_group: bool = False,
    do_indiv: bool = False,
    scopes=SCOPES,
    cancel_event: Optional[threading.Event] = None,
    progress_cb: Optional[Callable[[int, int, Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    """
    Legacy 'one function does all' flow (still used by /generate).
    - Auto-converts spreadsheet-ish templates (xlsx/csv/ods/gsheet) to native Google Sheets on copy.
    - Creates:
        * Group folder per row if Members > 1
        * Group Requirements sheet (from group_template) if do_group
        * Individual Contribution sheet per member (from indiv_group_template) if do_indiv_group
        * Solo students (Members == 1) copied to 'Individuals' folder (from indiv_template) if do_indiv
    """
    drive = get_drive_service(scopes)

    # Light validation for provided templates (only if that mode is enabled)
    if do_group and group_template:
        ensure_spreadsheetish(drive, group_template, "Group Template")
    if do_indiv_group and indiv_group_template:
        ensure_spreadsheetish(drive, indiv_group_template, "Indiv Group Template")
    if do_indiv and indiv_template:
        ensure_spreadsheetish(drive, indiv_template, "Individual Template")

    # Build Individuals subfolder only if needed
    individuals_folder_id = None
    individuals_folder_link = None
    if do_indiv:
        indiv_folder = get_or_create_subfolder(drive, folder_id, "Individuals")
        individuals_folder_id = indiv_folder["id"]
        individuals_folder_link = indiv_folder.get("webViewLink")

    results: List[Dict[str, Any]] = []

    # Count operations to drive progress
    total = 0
    for _, row in df.iterrows():
        members = [
            m.strip() for m in str(row.get("Members", "")).split(",") if m.strip()
        ]
        if len(members) > 1:
            if do_group:
                total += 1  # group sheet
            if do_indiv_group:
                total += len(members)  # per-member sheets
            total += 1  # the folder creation itself (always)
        else:
            if do_indiv:
                total += 1  # solo sheet

    step = 0

    # Process rows
    for _, row in df.iterrows():
        if cancel_event and cancel_event.is_set():
            return {"results": results, "cancelled": True}

        group_raw = row.get("Group")
        group_number = _normalize_group_label(group_raw)
        members = [
            m.strip() for m in str(row.get("Members", "")).split(",") if m.strip()
        ]

        if len(members) > 1:
            # Create/find "Group X" folder under parent folder
            folder_name = f"Group {group_number}"
            folder = (
                drive.files()
                .create(
                    body={
                        "name": folder_name,
                        "mimeType": FOLDER_MT,
                        "parents": [folder_id],
                    },
                    fields="id, name, webViewLink",
                    supportsAllDrives=True,
                )
                .execute()
            )
            folder_id_created = folder["id"]
            results.append(
                {"type": "folder", "group": group_number, "link": folder["webViewLink"]}
            )
            step += 1
            if progress_cb:
                progress_cb(
                    step,
                    total,
                    {"group": group_number, "members": members, "op": "folder"},
                )

            # Group requirements sheet
            if do_group and group_template:
                grp = copy_as_google_sheet(
                    drive,
                    group_template,
                    name=f"Group {group_number} - Requirements",
                    parents=[folder_id_created],
                )
                results.append(
                    {
                        "type": "group_sheet",
                        "group": group_number,
                        "link": grp["webViewLink"],
                    }
                )
                step += 1
                if progress_cb:
                    progress_cb(
                        step, total, {"group": group_number, "op": "group_sheet"}
                    )

            # Per-member contribution sheets
            if do_indiv_group and indiv_group_template:
                for member in members:
                    indiv = copy_as_google_sheet(
                        drive,
                        indiv_group_template,
                        name=f"{member} - Individual Contribution",
                        parents=[folder_id_created],
                    )
                    results.append(
                        {
                            "type": "indiv_sheet",
                            "member": member,
                            "group": group_number,
                            "link": indiv["webViewLink"],
                        }
                    )
                    step += 1
                    if progress_cb:
                        progress_cb(
                            step,
                            total,
                            {"group": group_number, "member": member, "op": "indiv"},
                        )

        else:
            # Solo → Individuals subfolder
            if not do_indiv or not indiv_template:
                # If solo row but indiv mode off, just skip
                continue
            solo_name = members[0] if members else ""
            solo = copy_as_google_sheet(
                drive,
                indiv_template,
                name=f"{solo_name} - Individual Feedback",
                parents=[individuals_folder_id],
            )
            results.append(
                {
                    "type": "solo_sheet",
                    "member": solo_name,
                    "folder": individuals_folder_link,
                    "link": solo["webViewLink"],
                }
            )
            step += 1
            if progress_cb:
                progress_cb(step, total, {"member": solo_name, "op": "solo"})

    return {"results": results, "cancelled": False}
