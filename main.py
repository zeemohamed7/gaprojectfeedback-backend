from fastapi import (
    FastAPI,
    Request,
    UploadFile,
    Form,
    File,
    BackgroundTasks,
    HTTPException,
    Depends,
    Header,
)
from pathlib import Path
from fastapi.responses import RedirectResponse, FileResponse, JSONResponse
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
import logging
import time
import uuid
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
    copy_as_google_sheet,  # ✅ use this everywhere we copy a template
)
from backend.google_download import download_folder_as_pdfs
from backend.config import SCOPES

# -----------------------------------------------------------------------------
# App & Config
# -----------------------------------------------------------------------------
load_dotenv()
app = FastAPI()


# --- Token extraction (header-based, stateless) --------------------------------
def get_access_token(
    x_google_access_token: str | None = Header(None),
    authorization: str | None = Header(None),
) -> str:
    if x_google_access_token:
        return x_google_access_token
    if authorization and authorization.lower().startswith("bearer "):
        return authorization.split(None, 1)[1]
    raise HTTPException(status_code=401, detail="Missing Google access token")


# --- Health --------------------------------------------------------------------
@app.get("/healthz")
def health():
    return {"ok": True}


# --- Logging middleware & global exception handler -----------------------------
logger = logging.getLogger("uvicorn.error")


@app.middleware("http")
async def log_requests(request, call_next):
    rid = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    start = time.time()
    try:
        response = await call_next(request)
    except Exception:
        logger.exception(
            "rid=%s unhandled error on %s %s", rid, request.method, request.url.path
        )
        raise
    duration_ms = (time.time() - start) * 1000
    logger.info(
        "rid=%s %s %s -> %s in %.1fms",
        rid,
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    response.headers["X-Request-ID"] = rid
    return response


@app.exception_handler(Exception)
async def all_exception_handler(request, exc):
    rid = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    logger.exception("rid=%s exception: %s", rid, exc)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "error": type(exc).__name__,
            "message": str(exc),
            "request_id": rid,
        },
        headers={"X-Request-ID": rid},
    )


# --- CORS ----------------------------------------------------------------------
FRONTEND_ORIGIN = os.getenv("VITE_REACT_APP_URL", "http://localhost:5173")
FRONTEND_ORIGIN_REGEX = os.getenv("FRONTEND_ORIGIN_REGEX", r"^https://.*\.vercel\.app$")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN],
    allow_origin_regex=FRONTEND_ORIGIN_REGEX,
    allow_credentials=False,  # we pass token via header, not cookies
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*", "X-Google-Access-Token"],
    expose_headers=["X-Request-ID", "Content-Disposition"],
    max_age=86400,
)

# --- OAuth client secrets / redirect ------------------------------------------
REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/auth/callback")

DEFAULT_SECRETS = Path("backend/credentials.json")
ALT_SECRETS = Path("/etc/secrets/credentials.json")
if DEFAULT_SECRETS.exists():
    secrets_path = str(DEFAULT_SECRETS)
elif ALT_SECRETS.exists():
    secrets_path = str(ALT_SECRETS)
else:
    raise FileNotFoundError(
        "Google client secrets not found. Add 'backend/credentials.json' "
        "or '/etc/secrets/credentials.json' (Render Secret File)."
    )

flow = Flow.from_client_secrets_file(
    secrets_path, scopes=SCOPES, redirect_uri=REDIRECT_URI
)

# If you still keep a single-user token as fallback:
TOKEN_FILE = Path(os.getenv("GOOGLE_TOKEN_FILE", "backend/token.pkl"))
TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)

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


def _parse_groups_members(df: pd.DataFrame) -> List[Tuple[str, List[str]]]:
    cols = {c.lower(): c for c in df.columns}
    group_col = cols.get("group")
    members_col = cols.get("members")
    if not group_col or not members_col:
        raise ValueError("CSV must have 'Group' and 'Members' headers.")
    rows: List[Tuple[str, List[str]]] = []
    for _, row in df.iterrows():
        g_raw = row.get(group_col)
        g = _normalize_group_label(g_raw)
        raw = "" if pd.isna(row.get(members_col)) else str(row.get(members_col))
        members = [m.strip() for m in raw.split(",") if m.strip()]
        if members:
            rows.append((g, members))
    return rows


# ---- Drive helpers in this file (small find/create utilities) ----------------
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
        drive = get_drive_service(SCOPES, access_token=args.get("access_token"))

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

            # ✅ copy & force native Google Sheet
            copy = copy_as_google_sheet(
                drive,
                args["indivTemplateId"],
                name=target_name,
                parents=[indiv_folder_id],
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
        drive = get_drive_service(SCOPES, access_token=args.get("access_token"))

        total = sum(1 + 1 + len(members) for _, members in rows)
        step = 0
        results = []

        for group_number, members in rows:
            if cancel_evt and cancel_evt.is_set():
                task_status[task_id]["results"] = results
                task_status[task_id]["status"] = "cancelled"
                return

            folder = _find_or_create_folder(
                drive, args["folderId"], f"Group {group_number}"
            )
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
                grp = copy_as_google_sheet(
                    drive,
                    args["groupTemplateId"],
                    name=group_sheet_name,
                    parents=[folder_id],
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
                    copy = copy_as_google_sheet(
                        drive,
                        args[
                            "indivGroupTemplateId"
                        ],  # ✅ correct template for indiv contribution
                        name=target,
                        parents=[folder_id],
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
        drive = get_drive_service(SCOPES, access_token=args.get("access_token"))

        indiv_folder_id = None
        indiv_folder_link = None

        total = sum(1 + 1 + len(m) for _, m in rows if len(m) > 1) + len(solo_names)
        step = 0
        results = []

        for group_number, members in rows:
            if len(members) <= 1:
                continue
            if cancel_evt and cancel_evt.is_set():
                task_status[task_id]["results"] = results
                task_status[task_id]["status"] = "cancelled"
                return

            folder = _find_or_create_folder(
                drive, args["folderId"], f"Group {group_number}"
            )
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
                grp = copy_as_google_sheet(
                    drive,
                    args["groupTemplateId"],
                    name=group_sheet_name,
                    parents=[folder_id],
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
                    copy = copy_as_google_sheet(
                        drive,
                        args["indivGroupTemplateId"],  # ✅ correct template
                        name=target,
                        parents=[folder_id],
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
                    copy = copy_as_google_sheet(
                        drive,
                        args["indivTemplateId"],  # ✅ correct template
                        name=target,
                        parents=[indiv_folder_id],
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
# Endpoints
# -----------------------------------------------------------------------------
@app.post("/generate-individuals")
async def generate_individuals(
    access_token: str = Depends(get_access_token),
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
        {
            "folderId": folderId,
            "indivTemplateId": indivTemplateId,
            "access_token": access_token,
        },
        cancel_evt,
    )
    return {"task_id": task_id}


@app.post("/generate-groups")
async def generate_groups(
    access_token: str = Depends(get_access_token),
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

    total_ops = sum(1 + 1 + len(members) for _, members in rows)
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
            "access_token": access_token,
        },
        cancel_evt,
    )
    return {"task_id": task_id}


@app.post("/generate-mixed")
async def generate_mixed(
    access_token: str = Depends(get_access_token),
    folderId: str = Form(...),
    groupTemplateId: Optional[str] = Form(None),
    indivGroupTemplateId: Optional[str] = Form(None),
    indivTemplateId: Optional[str] = Form(None),
    roster: UploadFile = File(...),  # CSV with Group, Members (1 member => solo)
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

    total_ops = sum(1 + 1 + len(m) for _, m in groups_rows) + len(solo_names)

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
            "access_token": access_token,
        },
        cancel_evt,
    )
    return {"task_id": task_id}


# ---- Legacy route (/generate) behaves like /generate-mixed --------------------
@app.post("/generate")
async def generate_legacy(
    access_token: str = Depends(get_access_token),
    folderId: str = Form(...),
    groupTemplateId: Optional[str] = Form(None),
    indivGroupTemplateId: Optional[str] = Form(None),
    indivTemplateId: Optional[str] = Form(None),
    groups: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
):
    return await generate_mixed(
        access_token=access_token,
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
def download_all(
    access_token: str = Depends(get_access_token),
    folderId: Optional[str] = None,
):
    if not folderId:
        raise HTTPException(status_code=400, detail="Missing folderId")
    workdir = tempfile.mkdtemp(prefix="ga_dl_")
    try:
        # If you want to skip template IDs, pass skip_ids={...} here later.
        download_folder_as_pdfs(
            folderId, workdir, skip_ids=None, access_token=access_token
        )
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_path = os.path.join(tempfile.gettempdir(), f"Sheets_Downloads_{ts}.zip")
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
# Google OAuth (single-user fallback – optional)
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
    if not code:
        raise HTTPException(status_code=400, detail="Missing ?code")
    flow.fetch_token(code=code)
    creds = flow.credentials
    with open(TOKEN_FILE, "wb") as f:
        pickle.dump(creds, f)
    return RedirectResponse(
        url=os.getenv("VITE_REACT_APP_URL", "http://localhost:5173")
    )
