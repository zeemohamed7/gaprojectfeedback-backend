from fastapi import (
    FastAPI,
    Request,
    UploadFile,
    Form,
    File,
    BackgroundTasks,
    HTTPException,
)
from fastapi.responses import RedirectResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.background import BackgroundTask

import os
import io
import csv
import pickle
import tempfile
import zipfile
import shutil
import threading
from uuid import uuid4
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple

import pandas as pd
from dotenv import load_dotenv
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request as GoogleRequest

from backend.google_create import (
    create_sheets_from_df,  # kept for legacy /generate
    get_drive_service,
    get_or_create_subfolder,
)
from backend.google_download import download_folder_as_pdfs
from backend.config import SCOPES

# -----------------------------------------------------------------------------
# App & Config
# -----------------------------------------------------------------------------
load_dotenv()

app = FastAPI()


@app.get("/healthz")
def health():
    return {"ok": True}


VITE_REACT_APP_URL = os.getenv("VITE_REACT_APP_URL", "http://localhost:5173")
origins = [VITE_REACT_APP_URL]


app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://gaprojectfeedback.zainab.dev"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "X-Requested-With"],
)

CLIENT_SECRETS_FILE = "backend/credentials.json"
REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/auth/callback")


flow = Flow.from_client_secrets_file(
    "backend/credentials.json", scopes=SCOPES, redirect_uri=REDIRECT_URI
)

# -----------------------------------------------------------------------------
# Task registry (in-memory)
# -----------------------------------------------------------------------------

task_status: Dict[str, Dict[str, Any]] = {}
task_cancel: Dict[str, threading.Event] = {}


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _update_progress(task_id: str, step: int, total: int, meta: dict):
    st = task_status.get(task_id)
    if not st:
        return
    st["progress"] = {
        "current": step,
        "total": total,
        "percent": round(step * 100 / max(1, total), 2),
    }
    st["last"] = meta
    st["updated_at"] = datetime.now(timezone.utc).isoformat()


def _zip_dir(src_dir: str, zip_path: str):
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src_dir):
            for f in files:
                full = os.path.join(root, f)
                arc = os.path.relpath(full, src_dir)
                z.write(full, arc)


def _load_csv_df(file_bytes: bytes) -> pd.DataFrame:
    """Robust CSV loader: try encodings and sniff delimiter/quote."""
    last_err = None
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            text = file_bytes.decode(enc, errors="strict")
        except Exception as e:
            last_err = e
            continue

        sample = "\n".join(text.splitlines()[:50])
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
            delim = dialect.delimiter
            quotechar = dialect.quotechar or '"'
        except Exception:
            delim = ","
            quotechar = '"'

        try:
            return pd.read_csv(
                io.StringIO(text),
                sep=delim,
                engine="python",
                quotechar=quotechar,
                doublequote=True,
                skip_blank_lines=True,
                on_bad_lines="error",
            )
        except Exception as e:
            last_err = e
            continue
    raise ValueError(
        f"Could not parse CSV (encodings tried: UTF-8, UTF-8-SIG, cp1252, latin-1). Last error: {last_err}"
    )


# ---- CSV parsing for the three flows ----


def _parse_names_single_column(
    df: pd.DataFrame, header: str = "Student Name"
) -> List[str]:
    cols = {c.lower(): c for c in df.columns}
    key = cols.get(header.lower())
    if not key:
        raise ValueError(f"CSV must have a '{header}' header.")
    names: List[str] = []
    for _, row in df.iterrows():
        val = row.get(key)
        if pd.isna(val):
            continue
        name = str(val).strip()
        if name:
            names.append(name)
    return names


def _normalize_group_label(val) -> str:
    if pd.isna(val):
        return ""
    # If it's numeric like 1.0 → 1
    if isinstance(val, (int,)):
        return str(val)
    if isinstance(val, float):
        return str(int(val)) if val.is_integer() else str(val).rstrip("0").rstrip(".")
    # If it's a string like "1.0" → 1
    s = str(val).strip()
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except Exception:
        pass
    return s


def _parse_groups_members(df: pd.DataFrame) -> List[Tuple[str, List[str]]]:
    cols = {c.lower(): c for c in df.columns}
    group_col = cols.get("group")
    members_col = cols.get("members")
    if not group_col or not members_col:
        raise ValueError("CSV must have 'Group' and 'Members' headers.")
    rows: List[Tuple[str, List[str]]] = []
    for _, row in df.iterrows():
        g_raw = row.get(group_col)
        g = _normalize_group_label(g_raw)  # <<< sanitize here
        raw = "" if pd.isna(row.get(members_col)) else str(row.get(members_col))
        members = [m.strip() for m in raw.split(",") if m.strip()]
        if not members:
            continue
        rows.append((g, members))
    return rows


# ---- Drive helpers ----

FOLDER_MT = "application/vnd.google-apps.folder"


def _find_in_folder(drive, parent_id: str, name: str):
    q = f"name = '{name}' and '{parent_id}' in parents and trashed = false"
    resp = (
        drive.files()
        .list(
            q=q,
            fields="files(id, name, webViewLink, mimeType)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=1,
        )
        .execute()
    )
    files = resp.get("files", [])
    return files[0] if files else None


def _find_or_create_folder(drive, parent_id: str, name: str):
    existing = _find_in_folder(drive, parent_id, name)
    if existing and existing.get("mimeType") == FOLDER_MT:
        return existing
    return (
        drive.files()
        .create(
            body={"name": name, "mimeType": FOLDER_MT, "parents": [parent_id]},
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )


# -----------------------------------------------------------------------------
# Workers
# -----------------------------------------------------------------------------


def _run_generate_individuals_task(
    task_id: str, names: List[str], args: dict, cancel_evt: threading.Event
):
    try:
        task_status[task_id]["status"] = "running"
        drive = get_drive_service(SCOPES)

        indiv_folder = get_or_create_subfolder(drive, args["folderId"], "Individuals")
        indiv_folder_id = indiv_folder["id"]
        indiv_folder_link = indiv_folder["webViewLink"]

        results = []
        total = len(names)
        for i, name in enumerate(names, start=1):
            if cancel_evt and cancel_evt.is_set():
                task_status[task_id]["results"] = results
                task_status[task_id]["status"] = "cancelled"
                return

            target_name = f"{name} - Individual Feedback"
            existing = _find_in_folder(drive, indiv_folder_id, target_name)
            if existing:
                results.append(
                    {
                        "type": "solo_sheet_existing",
                        "member": name,
                        "folder": indiv_folder_link,
                        "link": existing.get("webViewLink"),
                    }
                )
                _update_progress(task_id, i, total, {"member": name, "skipped": True})
                continue

            copy = (
                drive.files()
                .copy(
                    fileId=args["indivTemplateId"],
                    body={"name": target_name, "parents": [indiv_folder_id]},
                    fields="id, name, webViewLink",
                    supportsAllDrives=True,
                )
                .execute()
            )
            results.append(
                {
                    "type": "solo_sheet",
                    "member": name,
                    "folder": indiv_folder_link,
                    "link": copy["webViewLink"],
                }
            )
            _update_progress(task_id, i, total, {"member": name})

        task_status[task_id]["results"] = results
        task_status[task_id]["status"] = "completed"
    except Exception as e:
        task_status[task_id]["status"] = "error"
        task_status[task_id]["error"] = repr(e)
    finally:
        task_status[task_id]["finished_at"] = datetime.now(timezone.utc).isoformat()


def _run_generate_groups_task(
    task_id: str,
    rows: List[Tuple[str, List[str]]],
    args: dict,
    cancel_evt: threading.Event,
):
    try:
        task_status[task_id]["status"] = "running"
        drive = get_drive_service(SCOPES)

        total = 0
        for _, members in rows:
            total += 1  # folder
            total += 1  # group sheet
            total += len(members)  # per-member sheets

        step = 0
        results = []

        for group_number, members in rows:
            if cancel_evt and cancel_evt.is_set():
                task_status[task_id]["results"] = results
                task_status[task_id]["status"] = "cancelled"
                return

            group_folder_name = f"Group {group_number}"
            folder = _find_or_create_folder(drive, args["folderId"], group_folder_name)
            folder_id = folder["id"]
            results.append(
                {"type": "folder", "group": group_number, "link": folder["webViewLink"]}
            )
            step += 1
            _update_progress(
                task_id, step, total, {"group": group_number, "op": "folder"}
            )

            group_sheet_name = f"Group {group_number} - Requirements"
            existing_group_sheet = _find_in_folder(drive, folder_id, group_sheet_name)
            if not existing_group_sheet:
                grp = (
                    drive.files()
                    .copy(
                        fileId=args["groupTemplateId"],
                        body={"name": group_sheet_name, "parents": [folder_id]},
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
            else:
                results.append(
                    {
                        "type": "group_sheet_existing",
                        "group": group_number,
                        "link": existing_group_sheet["webViewLink"],
                    }
                )
            step += 1
            _update_progress(
                task_id, step, total, {"group": group_number, "op": "group_sheet"}
            )

            for member in members:
                target = f"{member} - Individual Contribution"
                existing = _find_in_folder(drive, folder_id, target)
                if not existing:
                    copy = (
                        drive.files()
                        .copy(
                            fileId=args["indivGroupTemplateId"],
                            body={"name": target, "parents": [folder_id]},
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
                else:
                    results.append(
                        {
                            "type": "indiv_group_sheet_existing",
                            "member": member,
                            "group": group_number,
                            "link": existing["webViewLink"],
                        }
                    )
                step += 1
                _update_progress(
                    task_id,
                    step,
                    total,
                    {"group": group_number, "member": member, "op": "indiv"},
                )

        task_status[task_id]["results"] = results
        task_status[task_id]["status"] = "completed"
    except Exception as e:
        task_status[task_id]["status"] = "error"
        task_status[task_id]["error"] = repr(e)
    finally:
        task_status[task_id]["finished_at"] = datetime.now(timezone.utc).isoformat()


def _run_generate_mixed_task(
    task_id: str,
    rows: List[Tuple[str, List[str]]],
    solo_names: List[str],
    args: dict,
    cancel_evt: threading.Event,
):
    try:
        task_status[task_id]["status"] = "running"
        drive = get_drive_service(SCOPES)

        indiv_folder_id = None
        indiv_folder_link = None

        total = 0
        for _, members in rows:
            if len(members) > 1:
                total += 1 + 1 + len(members)
        total += len(solo_names)

        step = 0
        results = []

        for group_number, members in rows:
            if len(members) <= 1:
                continue
            if cancel_evt and cancel_evt.is_set():
                task_status[task_id]["results"] = results
                task_status[task_id]["status"] = "cancelled"
                return

            group_folder_name = f"Group {group_number}"
            folder = _find_or_create_folder(drive, args["folderId"], group_folder_name)
            folder_id = folder["id"]
            results.append(
                {"type": "folder", "group": group_number, "link": folder["webViewLink"]}
            )
            step += 1
            _update_progress(
                task_id, step, total, {"group": group_number, "op": "folder"}
            )

            group_sheet_name = f"Group {group_number} - Requirements"
            existing_group_sheet = _find_in_folder(drive, folder_id, group_sheet_name)
            if not existing_group_sheet:
                grp = (
                    drive.files()
                    .copy(
                        fileId=args["groupTemplateId"],
                        body={"name": group_sheet_name, "parents": [folder_id]},
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
            else:
                results.append(
                    {
                        "type": "group_sheet_existing",
                        "group": group_number,
                        "link": existing_group_sheet["webViewLink"],
                    }
                )
            step += 1
            _update_progress(
                task_id, step, total, {"group": group_number, "op": "group_sheet"}
            )

            for member in members:
                target = f"{member} - Individual Contribution"
                existing = _find_in_folder(drive, folder_id, target)
                if not existing:
                    copy = (
                        drive.files()
                        .copy(
                            fileId=args["indivGroupTemplateId"],
                            body={"name": target, "parents": [folder_id]},
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
                else:
                    results.append(
                        {
                            "type": "indiv_group_sheet_existing",
                            "member": member,
                            "group": group_number,
                            "link": existing["webViewLink"],
                        }
                    )
                step += 1
                _update_progress(
                    task_id,
                    step,
                    total,
                    {"group": group_number, "member": member, "op": "indiv"},
                )

        if solo_names:
            if indiv_folder_id is None:
                indiv = get_or_create_subfolder(drive, args["folderId"], "Individuals")
                indiv_folder_id = indiv["id"]
                indiv_folder_link = indiv["webViewLink"]
                results.append(
                    {"type": "individuals_folder", "link": indiv_folder_link}
                )

            for name in solo_names:
                if cancel_evt and cancel_evt.is_set():
                    task_status[task_id]["results"] = results
                    task_status[task_id]["status"] = "cancelled"
                    return

                target = f"{name} - Individual Feedback"
                existing = _find_in_folder(drive, indiv_folder_id, target)
                if not existing:
                    copy = (
                        drive.files()
                        .copy(
                            fileId=args["indivTemplateId"],
                            body={"name": target, "parents": [indiv_folder_id]},
                            fields="id, name, webViewLink",
                            supportsAllDrives=True,
                        )
                        .execute()
                    )
                    results.append(
                        {
                            "type": "solo_sheet",
                            "member": name,
                            "link": copy["webViewLink"],
                        }
                    )
                else:
                    results.append(
                        {
                            "type": "solo_sheet_existing",
                            "member": name,
                            "link": existing["webViewLink"],
                        }
                    )
                step += 1
                _update_progress(task_id, step, total, {"member": name, "op": "solo"})

        task_status[task_id]["results"] = results
        task_status[task_id]["status"] = "completed"
    except Exception as e:
        task_status[task_id]["status"] = "error"
        task_status[task_id]["error"] = repr(e)
    finally:
        task_status[task_id]["finished_at"] = datetime.now(timezone.utc).isoformat()


# -----------------------------------------------------------------------------
# Endpoints: three routes (individuals, groups, mixed)
# -----------------------------------------------------------------------------


@app.post("/generate-individuals")
async def generate_individuals(
    folderId: str = Form(...),
    indivTemplateId: str = Form(...),
    roster: UploadFile = File(...),  # CSV with 'Student Name'
    background_tasks: BackgroundTasks = None,
):
    if background_tasks is None:
        raise HTTPException(status_code=500, detail="Background tasks unavailable")

    fname = (roster.filename or "").lower()
    ctype = (roster.content_type or "").lower()
    if (not fname.endswith(".csv")) and ("csv" not in ctype):
        raise HTTPException(status_code=400, detail="Please upload a .csv file.")

    try:
        contents = await roster.read()
        df = _load_csv_df(contents)
        names = _parse_names_single_column(df, header="Student Name")
        if not names:
            raise ValueError("No names found under 'Student Name'.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read CSV: {e}")

    task_id = str(uuid4())
    cancel_evt = threading.Event()
    task_cancel[task_id] = cancel_evt
    task_status[task_id] = {
        "status": "queued",
        "progress": {"current": 0, "total": len(names), "percent": 0.0},
        "created_at": datetime.now(timezone.utc).isoformat(),
        "results": [],
    }

    background_tasks.add_task(
        _run_generate_individuals_task,
        task_id,
        names,
        {"folderId": folderId, "indivTemplateId": indivTemplateId},
        cancel_evt,
    )
    return {"task_id": task_id}


@app.post("/generate-groups")
async def generate_groups(
    folderId: str = Form(...),
    groupTemplateId: str = Form(...),
    indivGroupTemplateId: str = Form(...),
    roster: UploadFile = File(...),  # CSV with 'Group','Members'
    background_tasks: BackgroundTasks = None,
):
    if background_tasks is None:
        raise HTTPException(status_code=500, detail="Background tasks unavailable")

    fname = (roster.filename or "").lower()
    ctype = (roster.content_type or "").lower()
    if (not fname.endswith(".csv")) and ("csv" not in ctype):
        raise HTTPException(status_code=400, detail="Please upload a .csv file.")

    try:
        contents = await roster.read()
        df = _load_csv_df(contents)
        rows = _parse_groups_members(df)
        if not rows:
            raise ValueError("No groups found in CSV.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read CSV: {e}")

    total_ops = 0
    for _, members in rows:
        total_ops += 1 + 1 + len(members)

    task_id = str(uuid4())
    cancel_evt = threading.Event()
    task_cancel[task_id] = cancel_evt
    task_status[task_id] = {
        "status": "queued",
        "progress": {"current": 0, "total": total_ops, "percent": 0.0},
        "created_at": datetime.now(timezone.utc).isoformat(),
        "results": [],
    }

    background_tasks.add_task(
        _run_generate_groups_task,
        task_id,
        rows,
        {
            "folderId": folderId,
            "groupTemplateId": groupTemplateId,
            "indivGroupTemplateId": indivGroupTemplateId,
        },
        cancel_evt,
    )
    return {"task_id": task_id}


@app.post("/generate-mixed")
async def generate_mixed(
    folderId: str = Form(...),
    groupTemplateId: Optional[str] = Form(None),
    indivGroupTemplateId: Optional[str] = Form(None),
    indivTemplateId: Optional[str] = Form(None),
    roster: UploadFile = File(
        ...
    ),  # CSV with 'Group','Members' (rows w/ 1 member become solos)
    background_tasks: BackgroundTasks = None,
):
    if background_tasks is None:
        raise HTTPException(status_code=500, detail="Background tasks unavailable")

    fname = (roster.filename or "").lower()
    ctype = (roster.content_type or "").lower()
    if (not fname.endswith(".csv")) and ("csv" not in ctype):
        raise HTTPException(status_code=400, detail="Please upload a .csv file.")

    try:
        contents = await roster.read()
        df = _load_csv_df(contents)
        rows = _parse_groups_members(df)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read CSV: {e}")

    groups_rows = [(g, m) for g, m in rows if len(m) > 1]
    solo_names = [m[0] for _, m in rows if len(m) == 1]

    if groups_rows and (not groupTemplateId or not indivGroupTemplateId):
        raise HTTPException(
            status_code=400,
            detail="groupTemplateId and indivGroupTemplateId are required for group rows.",
        )
    if solo_names and not indivTemplateId:
        raise HTTPException(
            status_code=400, detail="indivTemplateId is required for solo rows."
        )

    total_ops = 0
    for _, members in groups_rows:
        total_ops += 1 + 1 + len(members)
    total_ops += len(solo_names)

    task_id = str(uuid4())
    cancel_evt = threading.Event()
    task_cancel[task_id] = cancel_evt
    task_status[task_id] = {
        "status": "queued",
        "progress": {"current": 0, "total": total_ops, "percent": 0.0},
        "created_at": datetime.now(timezone.utc).isoformat(),
        "results": [],
    }

    background_tasks.add_task(
        _run_generate_mixed_task,
        task_id,
        rows,
        solo_names,
        {
            "folderId": folderId,
            "groupTemplateId": groupTemplateId,
            "indivGroupTemplateId": indivGroupTemplateId,
            "indivTemplateId": indivTemplateId,
        },
        cancel_evt,
    )
    return {"task_id": task_id}


# ---- Legacy route (/generate) kept for back-compat: behaves like /generate-mixed ----
@app.post("/generate")
async def generate_legacy(
    folderId: str = Form(...),
    groupTemplateId: Optional[str] = Form(None),
    indivGroupTemplateId: Optional[str] = Form(None),
    indivTemplateId: Optional[str] = Form(None),
    groups: UploadFile = File(...),  # CSV with 'Group','Members'
    background_tasks: BackgroundTasks = None,
):
    return await generate_mixed(
        folderId=folderId,
        groupTemplateId=groupTemplateId,
        indivGroupTemplateId=indivGroupTemplateId,
        indivTemplateId=indivTemplateId,
        roster=groups,
        background_tasks=background_tasks,
    )


# -----------------------------------------------------------------------------
# Download endpoint
# -----------------------------------------------------------------------------


@app.get("/download-all")
def download_all(folderId: Optional[str] = None):
    folder_to_use = folderId or GA_FOLDER_ID
    workdir = tempfile.mkdtemp(prefix="ga_dl_")
    try:
        skip = (
            set(TEMPLATE_IDS) if isinstance(TEMPLATE_IDS, (list, set, tuple)) else None
        )
        download_folder_as_pdfs(folder_to_use, workdir, skip_ids=skip)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_path = os.path.join(tempfile.gettempdir(), f"GA_Downloads_{ts}.zip")
        _zip_dir(workdir, zip_path)

        def _cleanup():
            try:
                shutil.rmtree(workdir, ignore_errors=True)
                os.remove(zip_path)
            except Exception:
                pass

        return FileResponse(
            zip_path,
            media_type="application/zip",
            filename=os.path.basename(zip_path),
            background=BackgroundTask(_cleanup),
        )
    except Exception as e:
        shutil.rmtree(workdir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=f"Download failed: {e}")


# -----------------------------------------------------------------------------
# Task control
# -----------------------------------------------------------------------------


@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    st = task_status.get(task_id)
    if not st:
        raise HTTPException(status_code=404, detail="Task not found")
    return st


@app.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: str):
    evt = task_cancel.get(task_id)
    st = task_status.get(task_id)
    if not evt or not st:
        raise HTTPException(status_code=404, detail="Task not found")
    if st["status"] in {"completed", "cancelled", "error"}:
        return {"task_id": task_id, "status": st["status"]}
    evt.set()
    return {"task_id": task_id, "status": "cancelling"}


# -----------------------------------------------------------------------------
# Google OAuth (single-user demo)
# -----------------------------------------------------------------------------


@app.get("/login")
def login():
    auth_url, state = flow.authorization_url(
        access_type="offline", include_granted_scopes="true", prompt="consent"
    )
    return {"auth_url": auth_url}


@app.get("/auth/callback")
def auth_callback(request: Request):
    code = request.query_params.get("code")
    flow.fetch_token(code=code)
    creds = flow.credentials
    with open("backend/token.pkl", "wb") as token:
        pickle.dump(creds, token)
    return RedirectResponse(url=f"{VITE_REACT_APP_URL}/upload")


@app.get("/google/access-token")
def google_access_token():
    with open("backend/token.pkl", "rb") as f:
        creds = pickle.load(f)

    if not creds.valid and creds.refresh_token:
        creds.refresh(GoogleRequest())
        with open("backend/token.pkl", "wb") as token:
            pickle.dump(creds, token)

    return {
        "access_token": creds.token,
        "expires_at": (
            creds.expiry.astimezone(timezone.utc).isoformat() if creds.expiry else None
        ),
    }
