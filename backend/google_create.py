import os
import pickle
import threading
from typing import Callable, Optional, Dict, Any, List, Tuple

import pandas as pd
from googleapiclient.discovery import build
from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2.credentials import Credentials

from backend.config import SCOPES

# ---- Mime types ----
FOLDER_MT = "application/vnd.google-apps.folder"
SHEET_MT = "application/vnd.google-apps.spreadsheet"

# Spreadsheet-ish sources we will accept and auto-convert to native Google Sheets
SPREADSHEETISH = {
    SHEET_MT,  # Google Sheet
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # .xlsx
    "application/vnd.ms-excel",  # .xls
    "text/csv",  # .csv
    "application/vnd.oasis.opendocument.spreadsheet",  # .ods
}

# ------------------------------------------------------------------------------
# Auth / Service
# ------------------------------------------------------------------------------


def get_drive_service(
    scopes=SCOPES,
    access_token: str | None = None,
    token_file: str = "backend/token.pkl",
):
    """
    Prefer a per-request browser access_token (sent from frontend).
    Fall back to token.pkl only if you still support the old single-user flow.
    """
    if access_token:
        creds = Credentials(token=access_token, scopes=scopes)
        return build("drive", "v3", credentials=creds, cache_discovery=False)

    if os.path.exists(token_file):
        with open(token_file, "rb") as f:
            creds = pickle.load(f)
        if not creds.valid and creds.refresh_token:
            creds.refresh(GoogleRequest())
            with open(token_file, "wb") as f:
                pickle.dump(creds, f)
        return build("drive", "v3", credentials=creds, cache_discovery=False)

    raise RuntimeError(
        "No Google token found. Send 'X-Google-Access-Token' from the frontend."
    )


# ------------------------------------------------------------------------------
# Drive helpers
# ------------------------------------------------------------------------------


def _file_meta(drive, file_id: str) -> dict:
    return (
        drive.files()
        .get(
            fileId=file_id,
            fields="id,name,mimeType,webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )


def _is_spreadsheetish(mime: str) -> bool:
    return (mime or "").lower() in SPREADSHEETISH


def ensure_spreadsheetish(drive, file_id: Optional[str], label: str) -> None:
    """
    Validate a template is spreadsheet-ish (gsheet/xlsx/xls/csv/ods).
    Raises a friendly error otherwise.
    """
    if not file_id:
        return
    meta = _file_meta(drive, file_id)
    mt = meta.get("mimeType", "")
    if not _is_spreadsheetish(mt):
        raise ValueError(
            f"❌ {label} ({meta.get('name')}) is not a spreadsheet.\n"
            f"MimeType: {mt}\n"
            f"Please provide a Google Sheet, Excel (.xlsx/.xls), CSV, or ODS."
        )


def copy_as_google_sheet(
    drive, source_file_id: str, *, name: str, parents: List[str]
) -> dict:
    """
    Force-copy any spreadsheet-ish file as a *native Google Sheet* by specifying mimeType.
    """
    return (
        drive.files()
        .copy(
            fileId=source_file_id,
            body={"name": name, "parents": parents, "mimeType": SHEET_MT},
            fields="id,name,mimeType,webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )


def get_or_create_subfolder(drive, parent_folder_id: str, name: str) -> dict:
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
            fields="files(id,name,webViewLink)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=1,
        )
        .execute()
    )
    files = resp.get("files", [])
    if files:
        return files[0]
    return (
        drive.files()
        .create(
            body={"name": name, "mimeType": FOLDER_MT, "parents": [parent_folder_id]},
            fields="id,name,webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )


def _normalize_group_label(val) -> str:
    """
    Normalize group labels like 1.0 -> '1', but leave normal strings alone.
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
# Individuals helpers for legacy flow
# ------------------------------------------------------------------------------


def _collect_individual_names_from_df(df: pd.DataFrame) -> List[str]:
    cols = {c.lower(): c for c in df.columns}

    # single-column forms
    for key in ["student name", "name", "student", "full name", "member", "person"]:
        if key in cols:
            col = cols[key]
            out = []
            for _, row in df.iterrows():
                v = row.get(col)
                if pd.isna(v):
                    continue
                s = str(v).strip()
                if s:
                    out.append(s)
            return out

    # first+last form
    first = next(
        (cols[k] for k in ["first name", "firstname", "first"] if k in cols), None
    )
    last = next(
        (
            cols[k]
            for k in ["last name", "lastname", "last", "surname", "family name"]
            if k in cols
        ),
        None,
    )
    if first and last:
        out = []
        for _, row in df.iterrows():
            f = "" if pd.isna(row.get(first)) else str(row.get(first)).strip()
            l = "" if pd.isna(row.get(last)) else str(row.get(last)).strip()
            name = (f" {l}").strip()
            if name:
                out.append(name)
        return out

    raise ValueError(
        "Individuals-only expects either 'Student Name' (or Name/Student/Full Name) "
        "or a pair 'First Name' + 'Last Name'."
    )


# ------------------------------------------------------------------------------
# Legacy, mode-aware generator used by /generate (kept for compatibility)
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
    Legacy generator supporting three modes. NOW auto-converts any spreadsheet-ish template
    to native Google Sheets on copy, so later PDF export (gridlines + notes) works.
    """
    if not (do_group or do_indiv_group or do_indiv):
        raise ValueError("Pick at least one creation mode.")

    drive = get_drive_service(scopes)

    # Validate templates you'll actually use
    if do_group:
        ensure_spreadsheetish(drive, group_template, "Group Template")
    if do_indiv_group:
        ensure_spreadsheetish(drive, indiv_group_template, "Individual Group Template")
    if do_indiv:
        ensure_spreadsheetish(drive, indiv_template, "Individual Template")

    if df is None or df.empty:
        return {"results": [], "cancelled": False}

    cols = {c.lower(): c for c in df.columns}
    group_col = cols.get("group")
    members_col = cols.get("members")

    results: List[Dict[str, Any]] = []
    total = len(df)
    individuals_folder_id: Optional[str] = None
    individuals_folder_link: Optional[str] = None

    def ensure_individuals():
        nonlocal individuals_folder_id, individuals_folder_link
        if individuals_folder_id:
            return
        folder = get_or_create_subfolder(drive, folder_id, "Individuals")
        individuals_folder_id = folder["id"]
        individuals_folder_link = folder["webViewLink"]

    # Individuals-only CSV (no Group/Members headers)
    if not group_col and not members_col:
        if not do_indiv:
            raise ValueError(
                "CSV looks like individuals-only, but do_indiv=False. "
                "Enable do_indiv or provide Group/Members columns."
            )
        names = _collect_individual_names_from_df(df)
        ensure_individuals()
        for i, name in enumerate(names, start=1):
            if cancel_event and cancel_event.is_set():
                return {"results": results, "cancelled": True}
            # AUTO-CONVERT to native Google Sheet
            copy = copy_as_google_sheet(
                drive,
                indiv_template,
                name=f"{name} - Individual Feedback",
                parents=[individuals_folder_id],
            )
            results.append(
                {
                    "type": "solo_sheet",
                    "member": name,
                    "folder": individuals_folder_link,
                    "link": copy["webViewLink"],
                }
            )
            if progress_cb:
                progress_cb(i, total, {"group": None, "members": [name]})
        return {"results": results, "cancelled": False}

    # Group/Members layout
    for idx, (_, row) in enumerate(df.iterrows(), start=1):
        if cancel_event and cancel_event.is_set():
            return {"results": results, "cancelled": True}

        group_raw = row.get(group_col)
        group_number = _normalize_group_label(group_raw)
        raw_members = "" if pd.isna(row.get(members_col)) else str(row.get(members_col))
        members = [m.strip() for m in raw_members.split(",") if m.strip()]
        is_group_row = len(members) > 1
        is_solo_row = len(members) == 1

        # Group rows
        if is_group_row and (do_group or do_indiv_group):
            folder = (
                drive.files()
                .create(
                    body={
                        "name": f"Group {group_number}",
                        "mimeType": FOLDER_MT,
                        "parents": [folder_id],
                    },
                    fields="id,name,webViewLink",
                    supportsAllDrives=True,
                )
                .execute()
            )
            folder_id_created = folder["id"]
            results.append(
                {"type": "folder", "group": group_number, "link": folder["webViewLink"]}
            )

            # Group Requirements (AUTO-CONVERT)
            if do_group:
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

            # Member contribution (AUTO-CONVERT)
            if do_indiv_group:
                for member in members:
                    copy = copy_as_google_sheet(
                        drive,
                        indiv_group_template,
                        name=f"{member} - Individual Contribution",
                        parents=[folder_id_created],
                    )
                    results.append(
                        {
                            "type": "indiv_group_sheet",
                            "member": member,
                            "group": group_number,
                            "link": copy["webViewLink"],
                        }
                    )

        # Solo rows
        if is_solo_row and do_indiv:
            ensure_individuals()
            member = members[0]
            copy = copy_as_google_sheet(
                drive,
                indiv_template,
                name=f"{member} - Individual Feedback",
                parents=[individuals_folder_id],
            )
            # add folder link once
            if not any(r.get("type") == "individuals_folder" for r in results):
                results.append(
                    {"type": "individuals_folder", "link": individuals_folder_link}
                )
            results.append(
                {
                    "type": "solo_sheet",
                    "member": member,
                    "group": group_number,
                    "link": copy["webViewLink"],
                }
            )

        if progress_cb:
            progress_cb(idx, total, {"group": group_number, "members": members})

    return {"results": results, "cancelled": False}
