# backend/google_create.py
import os
import io
import pickle
import threading
from typing import Callable, Optional, Dict, Any, List

import pandas as pd
from googleapiclient.discovery import build
from google.auth.transport.requests import Request as GoogleRequest

from backend.config import SCOPES

FOLDER_MT = "application/vnd.google-apps.folder"
SHEET_MT = "application/vnd.google-apps.spreadsheet"


# --- Auth / Service -----------------------------------------------------------


from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import os, pickle
from google.auth.transport.requests import Request as GoogleRequest


def get_drive_service(
    scopes=SCOPES,
    access_token: str | None = None,
    token_file: str = "backend/token.pkl",
):
    """
    Prefer a per-request browser access_token (sent from frontend) so no /login is needed.
    Fall back to token.pkl only if you still support the old single-user flow.
    """
    # 1) Use the browser-provided token (stateless, per-user)
    if access_token:
        creds = Credentials(token=access_token, scopes=scopes)
        return build("drive", "v3", credentials=creds, cache_discovery=False)

    # 2) Optional fallback for single-user demo
    if os.path.exists(token_file):
        with open(token_file, "rb") as f:
            creds = pickle.load(f)
        if not creds.valid and creds.refresh_token:
            creds.refresh(GoogleRequest())
            with open(token_file, "wb") as f:
                pickle.dump(creds, f)
        return build("drive", "v3", credentials=creds, cache_discovery=False)

    # 3) Nothing available
    raise RuntimeError(
        "No Google token found. Send 'X-Google-Access-Token' from the frontend."
    )


# --- Drive helpers ------------------------------------------------------------


def check_template_is_sheet(drive_service, file_id: Optional[str], label: str):
    """Optional: verify a file is a native Google Sheet. No-op if file_id is falsy."""
    if not file_id:
        return
    meta = (
        drive_service.files()
        .get(
            fileId=file_id,
            fields="id,name,mimeType,webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )
    if meta["mimeType"] != SHEET_MT:
        raise ValueError(
            f"❌ {label} ({meta['name']}) is not a Google Sheet. "
            f"Open in Drive and File → Save as Google Sheets."
        )


def get_or_create_subfolder(drive_service, parent_folder_id: str, name: str) -> dict:
    """
    Return {'id','name','webViewLink'} for subfolder named `name` under `parent_folder_id`.
    If missing, create it.
    """
    q = (
        f"name = '{name}' and "
        f"mimeType = '{FOLDER_MT}' and "
        f"'{parent_folder_id}' in parents and trashed = false"
    )
    resp = (
        drive_service.files()
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

    created = (
        drive_service.files()
        .create(
            body={"name": name, "mimeType": FOLDER_MT, "parents": [parent_folder_id]},
            fields="id,name,webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )
    return created


# --- Back-compat generator (mode-aware) --------------------------------------
# You don’t *need* this for the new endpoints, but main.py imports it.
# Keeping a functional version avoids ImportErrors and lets you call /generate (legacy) if desired.


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
    Legacy, mode-aware generator:
      - do_group: copy group_template once per group into 'Group X' folder.
      - do_indiv_group: copy indiv_group_template per member inside 'Group X'.
      - do_indiv: copy indiv_template into shared 'Individuals/' for solo students,
                  or for rows in an individuals-only CSV.

    Returns: {"results": [...], "cancelled": bool}
    """
    if not (do_group or do_indiv_group or do_indiv):
        raise ValueError("Pick at least one creation mode.")

    drive = get_drive_service(scopes)

    # Validate only templates used
    if do_group:
        check_template_is_sheet(drive, group_template, "Group Template")
    if do_indiv_group:
        check_template_is_sheet(
            drive, indiv_group_template, "Individual Group Template"
        )
    if do_indiv:
        check_template_is_sheet(drive, indiv_template, "Individual Template")

    if df is None or df.empty:
        return {"results": [], "cancelled": False}

    # Try to detect group/members layout
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

    # Individuals-only input (no group/members headers)
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
            copy = (
                drive.files()
                .copy(
                    fileId=indiv_template,
                    body={
                        "name": f"{name} - Individual Feedback",
                        "parents": [individuals_folder_id],
                    },
                    fields="id, name, webViewLink",
                    supportsAllDrives=True,
                )
                .execute()
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

        group_number = str(row.get(group_col))
        raw_members = "" if pd.isna(row.get(members_col)) else str(row.get(members_col))
        members = [m.strip() for m in raw_members.split(",") if m.strip()]
        is_group_row = len(members) > 1
        is_solo_row = len(members) == 1

        # Group rows → create Group folder and contents
        if is_group_row and (do_group or do_indiv_group):
            folder = (
                drive.files()
                .create(
                    body={
                        "name": f"Group {group_number}",
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

            if do_group:
                grp = (
                    drive.files()
                    .copy(
                        fileId=group_template,
                        body={
                            "name": f"Group {group_number} - Requirements",
                            "parents": [folder_id_created],
                        },
                        fields="id, name, webViewLink",
                        supportsAllDrives=True,
                    )
                    .execute()
                )
                results.append(
                    {
                        "type": "group_sheet",
                        "group": group_number,
                        "link": grp["webViewLink"],
                    }
                )

            if do_indiv_group:
                for member in members:
                    copy = (
                        drive.files()
                        .copy(
                            fileId=indiv_group_template,
                            body={
                                "name": f"{member} - Individual Contribution",
                                "parents": [folder_id_created],
                            },
                            fields="id, name, webViewLink",
                            supportsAllDrives=True,
                        )
                        .execute()
                    )
                    results.append(
                        {
                            "type": "indiv_group_sheet",
                            "member": member,
                            "group": group_number,
                            "link": copy["webViewLink"],
                        }
                    )

        # Solo rows → Individuals folder (if enabled)
        if is_solo_row and do_indiv:
            ensure_individuals()
            member = members[0]
            copy = (
                drive.files()
                .copy(
                    fileId=indiv_template,
                    body={
                        "name": f"{member} - Individual Feedback",
                        "parents": [individuals_folder_id],
                    },
                    fields="id, name, webViewLink",
                    supportsAllDrives=True,
                )
                .execute()
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
