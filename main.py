import hashlib
import hmac
import json
import os
import re
import time
from base64 import urlsafe_b64decode, urlsafe_b64encode
from urllib.parse import quote, unquote

import requests
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from sqlalchemy.orm import Session

from database import SessionLocal, engine
import models


load_dotenv()
models.Base.metadata.create_all(bind=engine)


app = FastAPI(title="KELYVO Portal API")


# ============================================================
# KELYVO PORTAL SESSION SECURITY
# ============================================================

KELYVO_SESSION_COOKIE = "kelyvo_session"
KELYVO_SESSION_TTL = 60 * 60 * 12
KELYVO_SESSION_SECRET = os.getenv("KELYVO_SESSION_SECRET", "").strip()

if not KELYVO_SESSION_SECRET:
    KELYVO_SESSION_SECRET = hashlib.sha256(
        (os.getenv("LABEL_STUDIO_API_TOKEN") or "kelyvo-local-session").encode("utf-8")
    ).hexdigest()


def _session_signing_value(value: str) -> str:
    return hmac.new(
        KELYVO_SESSION_SECRET.encode("utf-8"),
        value.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()


def create_session_token(email: str, role: str) -> str:
    expires_at = int(time.time()) + KELYVO_SESSION_TTL
    payload = f"{normalize_email(email)}|{role}|{expires_at}"
    signature = _session_signing_value(payload)
    raw = f"{payload}|{signature}".encode("utf-8")
    return urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def read_session_token(request: Request):
    token = request.cookies.get(KELYVO_SESSION_COOKIE, "")
    if not token:
        return None
    try:
        padding = "=" * (-len(token) % 4)
        decoded = urlsafe_b64decode((token + padding).encode("ascii")).decode("utf-8")
        email, role, expires_raw, signature = decoded.split("|", 3)
        expires_at = int(expires_raw)
    except Exception:
        return None

    if expires_at <= int(time.time()):
        return None

    normalized_email = normalize_email(email)
    payload = f"{normalized_email}|{role}|{expires_at}"
    expected = _session_signing_value(payload)
    if not hmac.compare_digest(signature, expected):
        return None

    return {
        "email": normalized_email,
        "role": role,
        "expires_at": expires_at,
    }


def require_portal_session(request: Request, allowed_roles=None):
    session = read_session_token(request)
    if not session:
        raise HTTPException(
            status_code=401,
            detail="A valid KELYVO session is required."
        )
    if allowed_roles and session.get("role") not in allowed_roles:
        raise HTTPException(
            status_code=403,
            detail="This portal area is not available for this role."
        )
    return session


def require_contributor_session(request: Request):
    return require_portal_session(request, {"contributor"})


def require_qa_session(request: Request):
    return require_portal_session(request, {"admin", "qa"})


@app.middleware("http")
async def intercept_legacy_label_studio_dm_users(
    request: Request,
    call_next
):
    # Label Studio frontend builds can request this legacy Data Manager
    # endpoint while the native editor is starting. It is not a current
    # Label Studio Data Manager endpoint and must never enter KELYVO's
    # contributor-scope fallback, which would return the contributor-mode 403.
    # Intercept it before FastAPI routing/authorization and keep the response
    # harmless for any frontend code that expects a user-list shape.
    request_path = request.scope.get("path", "")
    normalized_path = request_path.rstrip("/")

    if normalized_path == "/api/dm/users":
        return JSONResponse(
            content={
                "users": [],
                "results": [],
                "count": 0,
                "total": 0
            },
            status_code=200,
            headers={
                "X-KELYVO-DM-Users-Fix": "1"
            }
        )

    # Some Label Studio builds request the legacy /api/users endpoint with
    # the active project while the native labeling editor is starting. KELYVO
    # must not expose the global user-management endpoint, but this harmless
    # project-scoped user-list response is enough for the editor to initialize.
    if (
        request.method == "GET"
        and normalized_path == "/api/users"
        and request.query_params.get("project", "").isdigit()
    ):
        return JSONResponse(
            content=[],
            status_code=200,
            headers={
                "X-KELYVO-Users-Fix": "1"
            }
        )

    return await call_next(request)


@app.middleware("http")
async def intercept_legacy_label_studio_project_drafts(
    request: Request,
    call_next
):
    # Some Label Studio frontend builds still request the legacy project-level
    # drafts endpoint while opening the annotation editor:
    # GET /api/drafts?project=<id>. Current Label Studio task APIs expose
    # drafts under the assigned task, so this legacy request must not be
    # allowed to fall through KELYVO's contributor authorization layer.
    # Return an empty project-level list here. The actual assigned-task draft
    # endpoints remain available through the task-scoped proxy below.
    request_path = request.scope.get("path", "")
    normalized_path = request_path.rstrip("/")

    if (
        request.method == "GET"
        and normalized_path == "/api/drafts"
    ):
        project_value = request.query_params.get("project", "")

        if not project_value.isdigit():
            return JSONResponse(
                content={
                    "detail": "A project is required for contributor drafts."
                },
                status_code=400,
                headers={
                    "X-KELYVO-Drafts-Fix": "1"
                }
            )

        contributor_identity = get_browser_identity(request)
        assigned_task_id = get_reserved_task(
            contributor_identity,
            int(project_value)
        )

        if assigned_task_id is None:
            return JSONResponse(
                content={
                    "detail": "No active contributor task."
                },
                status_code=403,
                headers={
                    "X-KELYVO-Drafts-Fix": "1"
                }
            )

        # Label Studio uses this legacy project-level endpoint during editor
        # initialization to restore an in-progress annotation draft after a
        # browser refresh. Returning [] here makes every unsaved annotation
        # disappear on refresh even though the draft was successfully stored
        # under the assigned task.
        #
        # Resolve the request to KELYVO's single assigned task and return the
        # real task-scoped drafts. This preserves draft persistence without
        # exposing drafts from any other task in the project.
        draft_response = label_studio_request(
            "GET",
            f"/api/tasks/{int(assigned_task_id)}/drafts",
            params={
                "project": int(project_value)
            }
        )

        if draft_response.status_code >= 400:
            return JSONResponse(
                content=[],
                status_code=200,
                headers={
                    "Cache-Control": "no-store",
                    "X-KELYVO-Drafts-Fix": "1",
                    "X-KELYVO-Drafts-Fallback": "1"
                }
            )

        try:
            draft_payload = draft_response.json()
        except ValueError:
            draft_payload = []

        # Keep the native response shape. Different Label Studio versions
        # return either a list or an object containing a drafts/results list.
        return JSONResponse(
            content=draft_payload,
            status_code=200,
            headers={
                "Cache-Control": "no-store",
                "X-KELYVO-Drafts-Fix": "1",
                "X-KELYVO-Drafts-Task": str(int(assigned_task_id))
            }
        )

    return await call_next(request)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


LABEL_STUDIO_URL = os.getenv(
    "LABEL_STUDIO_URL",
    "http://localhost:8080"
).rstrip("/")

LABEL_STUDIO_REFRESH_TOKEN = os.getenv(
    "LABEL_STUDIO_API_TOKEN"
)


PROJECT_MAPPING = {
    "video": 6,
    "text": 5,
    "audio": 4,
    "image": 2,
}


TASK_ASSIGNMENTS = {}
TASK_ASSIGNMENT_TTL = 60 * 60

# Short-lived marker used to stop Label Studio's automatic post-submit
# next-task navigation. KELYVO controls task advancement itself via
# MARK TASK SUBMITTED, so native Submit must save the annotation without
# moving the contributor to another task.
KELYVO_RECENT_NATIVE_SUBMISSIONS = {}
KELYVO_RECENT_REVISION_UPDATES = {}
KELYVO_NATIVE_SUBMIT_TTL = 30


def mark_native_annotation_submitted(
    request: Request,
    project_id: int,
    task_id: int
):
    identity = get_browser_identity(request)
    cleanup_task_assignments()
    key = make_assignment_key(identity, project_id)
    KELYVO_RECENT_NATIVE_SUBMISSIONS[key] = {
        "task_id": int(task_id),
        "expires_at": time.time() + KELYVO_NATIVE_SUBMIT_TTL,
    }


def consume_recent_native_submission(
    request: Request,
    project_id: int,
    task_id: int
) -> bool:
    identity = get_browser_identity(request)
    key = make_assignment_key(identity, project_id)
    entry = KELYVO_RECENT_NATIVE_SUBMISSIONS.get(key)

    if not entry:
        return False

    if entry.get("expires_at", 0) <= time.time():
        KELYVO_RECENT_NATIVE_SUBMISSIONS.pop(key, None)
        return False

    if int(entry.get("task_id", 0)) != int(task_id):
        return False

    # The browser watcher may poll more than once before the UI finishes
    # closing the workspace. Keep the marker until TTL expiry rather than
    # consuming it on the first status check.
    return True


def _contributor_has_failed_revision_submission(
    request: Request,
    project_id: int,
    task_id: int,
) -> bool:
    """Return True only when the authenticated contributor owns a FAILED revision."""
    session = read_session_token(request)
    if not session or session.get("role") != "contributor":
        return False

    email = normalize_email(session.get("email", ""))
    if not email:
        return False

    reverse_mapping = {
        value: key
        for key, value in PROJECT_MAPPING.items()
    }
    task_type = reverse_mapping.get(int(project_id))
    if not task_type:
        return False

    db = SessionLocal()
    try:
        user = (
            db.query(models.User)
            .filter(models.User.email == email)
            .first()
        )
        if not user:
            return False

        failed_rows = (
            db.query(models.TaskSubmission)
            .filter(
                models.TaskSubmission.user_id == user.id,
                models.TaskSubmission.task_type == task_type,
                models.TaskSubmission.status == "FAILED",
            )
            .all()
        )

        for row in failed_rows:
            if _extract_kelyvo_task_id(row.task_title) == int(task_id):
                return True

        return False
    except Exception:
        return False
    finally:
        db.close()


def mark_native_revision_updated(
    request: Request,
    project_id: int,
    task_id: int,
):
    """Mark a successful Label Studio annotation update on a QA-returned task."""
    if not _contributor_has_failed_revision_submission(
        request,
        project_id,
        task_id,
    ):
        return

    identity = get_browser_identity(request)
    cleanup_task_assignments()
    key = make_assignment_key(identity, project_id)
    KELYVO_RECENT_REVISION_UPDATES[key] = {
        "task_id": int(task_id),
        "expires_at": time.time() + KELYVO_NATIVE_SUBMIT_TTL,
    }


@app.get("/api/native-submit-status")
def native_submit_status(
    request: Request,
    project_id: int,
    task_id: int,
    email: str = ""
):
    """Report a recent native Label Studio submission for this task."""
    cleanup_task_assignments()

    browser_identity = get_browser_identity(request)
    identity = browser_identity

    normalized_email = normalize_email(email)
    if normalized_email:
        email_identity = "user:" + normalized_email
        email_task_id = get_reserved_task(
            email_identity,
            project_id
        )

        if email_task_id is not None:
            sync_browser_assignment_for_request(
                request,
                project_id,
                int(email_task_id)
            )
            identity = browser_identity

    key = make_assignment_key(identity, project_id)
    entry = KELYVO_RECENT_NATIVE_SUBMISSIONS.get(key)

    revision_key = make_assignment_key(browser_identity, project_id)
    revision_entry = KELYVO_RECENT_REVISION_UPDATES.get(revision_key)

    revision_updated = False
    revision_task_id = int(task_id)

    if revision_entry:
        revision_expires_at = float(
            revision_entry.get("expires_at", 0) or 0
        )
        if revision_expires_at <= time.time():
            KELYVO_RECENT_REVISION_UPDATES.pop(
                revision_key,
                None,
            )
        else:
            revision_task_id = int(
                revision_entry.get("task_id", 0) or task_id
            )
            revision_updated = (
                revision_task_id == int(task_id)
            )

    if not entry:
        return {
            "submitted": False,
            "revision_updated": revision_updated,
            "task_id": revision_task_id,
        }

    expires_at = float(entry.get("expires_at", 0) or 0)

    if expires_at <= time.time():
        KELYVO_RECENT_NATIVE_SUBMISSIONS.pop(key, None)
        return {
            "submitted": False,
            "revision_updated": revision_updated,
            "task_id": revision_task_id,
        }

    matched_task_id = int(entry.get("task_id", 0) or 0)

    return {
        "submitted": matched_task_id == int(task_id),
        "revision_updated": revision_updated,
        "task_id": matched_task_id or revision_task_id,
    }


def cleanup_task_assignments():
    now = time.time()

    expired_keys = [
        key
        for key, assignment in TASK_ASSIGNMENTS.items()
        if assignment.get("expires_at", 0) <= now
    ]

    for key in expired_keys:
        TASK_ASSIGNMENTS.pop(key, None)


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def get_browser_identity(request: Request) -> str:
    forwarded_for = request.headers.get(
        "x-forwarded-for",
        ""
    )

    if forwarded_for:
        client_ip = forwarded_for.split(",")[0].strip()
    elif request.client:
        client_ip = request.client.host
    else:
        client_ip = "unknown"

    user_agent = request.headers.get(
        "user-agent",
        "unknown"
    )

    raw_identity = (
        client_ip
        + "|"
        + user_agent
    )

    digest = hashlib.sha256(
        raw_identity.encode("utf-8")
    ).hexdigest()

    return "browser:" + digest


def make_assignment_key(
    identity: str,
    project_id: int
) -> str:
    return f"{identity}:{project_id}"


def sync_browser_assignment_for_request(
    request: Request,
    project_id: int,
    task_id: int
):
    """Keep email and browser assignments synchronized."""
    browser_identity = get_browser_identity(request)
    browser_key = make_assignment_key(browser_identity, project_id)
    cleanup_task_assignments()
    existing = TASK_ASSIGNMENTS.get(browser_key)
    if existing and int(existing.get("task_id", 0)) != int(task_id):
        TASK_ASSIGNMENTS.pop(browser_key, None)
    reserve_task_for_contributor(browser_identity, project_id, int(task_id))


def reserve_task_for_contributor(
    identity: str,
    project_id: int,
    task_id: int
):
    cleanup_task_assignments()

    if not identity:
        raise HTTPException(
            status_code=400,
            detail="Unable to identify the contributor."
        )

    key = make_assignment_key(
        identity,
        project_id
    )

    existing = TASK_ASSIGNMENTS.get(key)

    if existing:
        if existing.get("task_id") != task_id:
            return existing["task_id"]

        existing["expires_at"] = (
            time.time()
            + TASK_ASSIGNMENT_TTL
        )

        return task_id

    TASK_ASSIGNMENTS[key] = {
        "identity": identity,
        "project_id": project_id,
        "task_id": task_id,
        "created_at": time.time(),
        "expires_at": (
            time.time()
            + TASK_ASSIGNMENT_TTL
        ),
    }

    return task_id


def get_reserved_task(
    identity: str,
    project_id: int
):
    cleanup_task_assignments()

    key = make_assignment_key(
        identity,
        project_id
    )

    assignment = TASK_ASSIGNMENTS.get(key)

    if not assignment:
        return None

    assignment["expires_at"] = (
        time.time()
        + TASK_ASSIGNMENT_TTL
    )

    return assignment.get("task_id")


def release_reserved_task(
    identity: str,
    project_id: int,
    task_id: int = None
):
    cleanup_task_assignments()

    key = make_assignment_key(
        identity,
        project_id
    )

    assignment = TASK_ASSIGNMENTS.get(key)

    if not assignment:
        return

    if task_id is not None:
        if assignment.get("task_id") != task_id:
            return

    TASK_ASSIGNMENTS.pop(
        key,
        None
    )


def release_browser_assignment_for_request(
    request: Request
):
    identity = get_browser_identity(request)

    cleanup_task_assignments()

    keys_to_remove = [
        key
        for key, assignment in TASK_ASSIGNMENTS.items()
        if assignment.get("identity") == identity
    ]

    for key in keys_to_remove:
        TASK_ASSIGNMENTS.pop(
            key,
            None
        )


def _extract_kelyvo_task_id(task_title: str):
    """Extract IDs only from KELYVO's canonical task titles."""
    title = str(task_title or "").strip()

    match = re.match(
        r"^(image|audio|video|text)\s+task\s+#(\d+)\s*$",
        title,
        re.IGNORECASE,
    )

    if not match:
        return None

    return int(match.group(2))


def _is_task_reserved_by_other_contributor(
    project_id: int,
    task_id: int,
    contributor_identity: str,
) -> bool:
    """Prevent the same task from being handed to two active contributors."""
    cleanup_task_assignments()

    for assignment in TASK_ASSIGNMENTS.values():
        if int(assignment.get("project_id", 0)) != int(project_id):
            continue

        if int(assignment.get("task_id", 0)) != int(task_id):
            continue

        if assignment.get("identity") == contributor_identity:
            continue

        return True

    return False


def _ensure_revision_draft_from_existing_annotation(
    project_id: int,
    task_id: int,
):
    """
    Preserve a QA-returned annotation as an editable Label Studio draft.

    KELYVO intentionally keeps the original submitted annotation intact when
    QA returns a task. Before the original contributor reopens that task,
    create a task/annotation draft from the existing annotation result.
    Label Studio can then preload the contributor's previous work into the
    editable labeling interface instead of starting from a blank task.

    This is deliberately server-side and uses KELYVO's existing Label Studio
    service token. Contributors never receive the Label Studio API token.
    """
    task_response = label_studio_request(
        "GET",
        f"/api/tasks/{int(task_id)}",
        params={
            "project": int(project_id),
            "resolve_uri": "true",
        },
    )

    if task_response.status_code == 404:
        raise HTTPException(
            status_code=404,
            detail="The Label Studio revision task could not be found.",
        )

    if task_response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail="Unable to retrieve the revision task from Label Studio.",
        )

    try:
        task_payload = task_response.json()
    except ValueError:
        raise HTTPException(
            status_code=502,
            detail="Label Studio returned invalid revision task data.",
        )

    if not isinstance(task_payload, dict):
        raise HTTPException(
            status_code=502,
            detail="Label Studio returned an invalid revision task object.",
        )

    drafts = task_payload.get("drafts", [])
    if isinstance(drafts, dict):
        drafts = drafts.get("drafts", drafts.get("results", []))
    if not isinstance(drafts, list):
        drafts = []

    # If Label Studio already has a draft, leave it untouched. This prevents
    # overwriting a contributor's in-progress revision.
    for draft in drafts:
        if isinstance(draft, dict) and draft.get("result"):
            return draft

    annotations = task_payload.get("annotations", [])
    if not isinstance(annotations, list):
        annotations = []

    usable_annotations = [
        annotation
        for annotation in annotations
        if isinstance(annotation, dict)
        and annotation.get("id") is not None
        and not bool(annotation.get("was_cancelled"))
        and isinstance(annotation.get("result"), list)
        and annotation.get("result")
    ]

    if not usable_annotations:
        # There is no prior annotation to carry forward. The normal empty
        # revision workspace remains valid, so do not fail the revision.
        return None

    # Use the most recently updated annotation when more than one exists.
    usable_annotations.sort(
        key=lambda item: str(item.get("updated_at") or item.get("created_at") or "")
    )
    annotation = usable_annotations[-1]
    annotation_id = int(annotation["id"])
    result = annotation.get("result") or []

    draft_response = label_studio_request(
        "POST",
        f"/api/tasks/{int(task_id)}/annotations/{annotation_id}/drafts",
        json={
            "lead_time": float(annotation.get("lead_time") or 0),
            "result": result,
        },
    )

    if draft_response.status_code in (200, 201):
        try:
            return draft_response.json()
        except ValueError:
            return {"result": result}

    # A concurrent Label Studio request may have created the draft between
    # our GET and POST. Re-read the task before treating this as an error.
    retry_response = label_studio_request(
        "GET",
        f"/api/tasks/{int(task_id)}",
        params={
            "project": int(project_id),
            "resolve_uri": "true",
        },
    )

    if retry_response.status_code == 200:
        try:
            retry_payload = retry_response.json()
        except ValueError:
            retry_payload = {}

        retry_drafts = (
            retry_payload.get("drafts", [])
            if isinstance(retry_payload, dict)
            else []
        )
        if isinstance(retry_drafts, list):
            for draft in retry_drafts:
                if isinstance(draft, dict) and draft.get("result"):
                    return draft

    raise HTTPException(
        status_code=502,
        detail=(
            "KELYVO could not preserve the previous annotation as an "
            "editable revision draft in Label Studio."
        ),
    )


def _reset_failed_label_studio_task_to_pool(
    project_id: int,
    task_id: int,
):
    """
    Clear the failed contributor's annotation/drafts so the task returns to
    Label Studio's original unlabeled pool in a clean state.

    This is intentionally server-side. QA reviewers never receive permission
    to delete annotations/tasks directly from Label Studio.
    """
    task_response = label_studio_request(
        "GET",
        f"/api/tasks/{int(task_id)}",
        params={
            "project": int(project_id),
            "resolve_uri": "true",
        },
    )

    if task_response.status_code == 404:
        raise HTTPException(
            status_code=404,
            detail="The failed Label Studio task could not be found.",
        )

    if task_response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail="Unable to retrieve the failed task from Label Studio.",
        )

    try:
        task_payload = task_response.json()
    except ValueError:
        raise HTTPException(
            status_code=502,
            detail="Label Studio returned invalid failed-task data.",
        )

    if not isinstance(task_payload, dict):
        raise HTTPException(
            status_code=502,
            detail="Label Studio returned an invalid failed-task object.",
        )

    annotations = task_payload.get("annotations", [])
    if not isinstance(annotations, list):
        annotations = []

    for annotation in annotations:
        if not isinstance(annotation, dict):
            continue

        annotation_id = annotation.get("id")
        if annotation_id is None:
            continue

        delete_response = label_studio_request(
            "DELETE",
            f"/api/annotations/{int(annotation_id)}",
            params={"project": int(project_id)},
        )

        if delete_response.status_code not in (200, 204, 404):
            raise HTTPException(
                status_code=502,
                detail=(
                    "KELYVO could not clear the failed annotation, "
                    "so the task was not requeued."
                ),
            )

    draft_response = label_studio_request(
        "GET",
        f"/api/tasks/{int(task_id)}/drafts",
        params={"project": int(project_id)},
    )

    if draft_response.status_code < 400:
        try:
            drafts_payload = draft_response.json()
        except ValueError:
            drafts_payload = []

        if isinstance(drafts_payload, dict):
            drafts = drafts_payload.get("drafts", [])
            if not isinstance(drafts, list):
                drafts = drafts_payload.get("results", [])
        else:
            drafts = drafts_payload

        if isinstance(drafts, list):
            for draft in drafts:
                if not isinstance(draft, dict):
                    continue

                draft_id = draft.get("id")
                if draft_id is None:
                    continue

                delete_response = label_studio_request(
                    "DELETE",
                    f"/api/drafts/{int(draft_id)}",
                    params={"project": int(project_id)},
                )

                if delete_response.status_code not in (200, 204, 404):
                    raise HTTPException(
                        status_code=502,
                        detail=(
                            "KELYVO could not clear the failed draft, "
                            "so the task was not requeued."
                        ),
                    )


def _get_requeued_tasks_for_contributor(
    db: Session,
    project_id: int,
    task_type: str,
    contributor_identity: str,
    contributor_user_id=None,
):
    """
    Return recently/previously failed canonical KELYVO tasks that have been
    reset to an unlabeled state and are therefore eligible for reassignment.

    The original contributor (and anyone else who previously failed the exact
    task) is excluded. Requeued tasks are returned before untouched tasks.
    """
    failed_rows = (
        db.query(models.TaskSubmission)
        .filter(
            models.TaskSubmission.task_type == task_type,
            models.TaskSubmission.status == "FAILED",
        )
        .order_by(models.TaskSubmission.id.desc())
        .all()
    )

    candidates = []
    seen_task_ids = set()

    failed_users_by_task_id = {}
    for failed_row in failed_rows:
        failed_task_id = _extract_kelyvo_task_id(failed_row.task_title)
        if failed_task_id is None:
            continue
        if failed_row.user_id is not None:
            failed_users_by_task_id.setdefault(
                failed_task_id,
                set(),
            ).add(int(failed_row.user_id))

    for failed_row in failed_rows:
        task_id = _extract_kelyvo_task_id(failed_row.task_title)
        if task_id is None or task_id in seen_task_ids:
            continue

        if failed_row.user_id is not None and contributor_user_id is not None:
            if int(failed_row.user_id) == int(contributor_user_id):
                continue

        previous_user_ids = failed_users_by_task_id.get(
            task_id,
            set(),
        )
        if (
            contributor_user_id is not None
            and int(contributor_user_id) in previous_user_ids
        ):
            continue

        if _is_task_reserved_by_other_contributor(
            project_id,
            task_id,
            contributor_identity,
        ):
            continue

        task_response = label_studio_request(
            "GET",
            f"/api/tasks/{int(task_id)}",
            params={
                "project": int(project_id),
                "resolve_uri": "true",
            },
        )

        if task_response.status_code != 200:
            continue

        try:
            task = task_response.json()
        except ValueError:
            continue

        if not isinstance(task, dict):
            continue

        annotations = task.get("annotations", [])
        cancelled_annotations = task.get("cancelled_annotations", 0)

        if annotations or cancelled_annotations:
            continue

        seen_task_ids.add(task_id)
        candidates.append(task)

    return candidates


def get_label_studio_access_token() -> str:
    if not LABEL_STUDIO_REFRESH_TOKEN:
        raise HTTPException(
            status_code=500,
            detail=(
                "Label Studio authentication "
                "is not configured."
            )
        )

    try:
        response = requests.post(
            f"{LABEL_STUDIO_URL}/api/token/refresh",
            json={
                "refresh":
                    LABEL_STUDIO_REFRESH_TOKEN
            },
            timeout=10,
        )
    except requests.RequestException:
        raise HTTPException(
            status_code=502,
            detail=(
                "Unable to connect to Label Studio."
            )
        )

    if response.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=(
                "Unable to authenticate "
                "with Label Studio."
            )
        )

    try:
        access_token = (
            response.json()
            .get("access")
        )
    except ValueError:
        access_token = None

    if not access_token:
        raise HTTPException(
            status_code=502,
            detail=(
                "Label Studio did not return "
                "an access token."
            )
        )

    return access_token


def label_studio_request(
    method: str,
    endpoint: str,
    **kwargs
):
    access_token = (
        get_label_studio_access_token()
    )

    headers = kwargs.pop(
        "headers",
        {}
    )

    headers["Authorization"] = (
        f"Bearer {access_token}"
    )

    if "timeout" not in kwargs:
        kwargs["timeout"] = 15

    url = (
        f"{LABEL_STUDIO_URL}"
        f"{endpoint}"
    )

    try:
        response = requests.request(
            method,
            url,
            headers=headers,
            **kwargs
        )
    except requests.RequestException:
        raise HTTPException(
            status_code=502,
            detail=(
                "Unable to communicate "
                "with Label Studio."
            )
        )

    return response


def rewrite_label_studio_media_urls(
    value
):
    if isinstance(value, dict):
        return {
            key:
                rewrite_label_studio_media_urls(
                    item
                )
            for key, item in value.items()
        }

    if isinstance(value, list):
        return [
            rewrite_label_studio_media_urls(
                item
            )
            for item in value
        ]

    if (
        isinstance(value, str)
        and value.startswith("/data/")
    ):
        return (
            "/api/label-studio-media?path="
            + quote(
                value,
                safe=""
            )
        )

    return value


def sanitize_contributor_task_payload(payload):
    """Hide persisted Label Studio annotations from contributor task payloads."""
    if not isinstance(payload, dict):
        return payload
    cleaned = dict(payload)
    cleaned.pop("annotations", None)
    cleaned.pop("annotation", None)
    return cleaned


def _cleanup_legacy_task12_drafts_once():
    """Remove stale task-12 drafts created by the earlier routing bug, once."""
    marker = os.path.join(
        ".kelyvo_task12_legacy_draft_repair_v1.done"
    )

    if os.path.exists(marker):
        return

    project_id = 2
    task_id = 12

    try:
        response = label_studio_request(
            "GET",
            f"/api/tasks/{task_id}",
            params={
                "project": project_id,
                "resolve_uri": "true",
            },
        )

        if response.status_code == 200:
            payload = response.json()
            drafts = (
                payload.get("drafts", [])
                if isinstance(payload, dict)
                else []
            )

            if isinstance(drafts, list):
                for draft in drafts:
                    if not isinstance(draft, dict):
                        continue
                    draft_id = draft.get("id")
                    if draft_id is None:
                        continue

                    delete_response = label_studio_request(
                        "DELETE",
                        f"/api/drafts/{int(draft_id)}",
                        params={"project": project_id},
                    )

                    if delete_response.status_code not in (200, 204, 404):
                        return

        with open(marker, "w", encoding="utf-8") as marker_file:
            marker_file.write("done\n")
    except Exception:
        return


@app.get("/api/label-studio-media")
def label_studio_media(
    path: str,
    request: Request
):
    try:
        decoded_path = (
            unquote(path)
            .strip()
        )
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="Invalid media path."
        )

    if not decoded_path.startswith(
        "/data/"
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid Label Studio "
                "media path."
            )
        )

    if (
        decoded_path.startswith("//")
        or "://" in decoded_path
        or "\x00" in decoded_path
        or ".." in decoded_path.split("/")
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid Label Studio "
                "media path."
            )
        )

    if not decoded_path.startswith(
        "/data/upload/"
    ):
        raise HTTPException(
            status_code=403,
            detail=(
                "Media path is not allowed."
            )
        )

    response = label_studio_request(
        "GET",
        decoded_path,
        stream=True
    )

    if response.status_code == 404:
        response.close()
        raise HTTPException(
            status_code=404,
            detail=(
                "Label Studio media file "
                "not found."
            )
        )

    if response.status_code >= 400:
        response.close()
        raise HTTPException(
            status_code=502,
            detail=(
                "Unable to retrieve media "
                "from Label Studio."
            )
        )

    content_type = response.headers.get(
        "content-type",
        "application/octet-stream"
    )

    content_length = response.headers.get(
        "content-length"
    )

    def stream_media():
        try:
            for chunk in response.iter_content(
                chunk_size=64 * 1024
            ):
                if chunk:
                    yield chunk
        finally:
            response.close()

    headers = {
        "Cache-Control":
            "private, no-store",
        "X-Content-Type-Options":
            "nosniff",
        "Content-Disposition":
            "inline",
    }

    if content_length:
        headers["Content-Length"] = (
            content_length
        )

    return StreamingResponse(
        stream_media(),
        media_type=content_type,
        headers=headers
    )


def hash_password(
    password: str
) -> str:
    return hashlib.sha256(
        password.encode("utf-8")
    ).hexdigest()


def get_db():
    db = SessionLocal()

    try:
        yield db
    finally:
        db.close()


@app.get(
    "/",
    response_class=HTMLResponse
)
def serve_portal():
    html_path = os.path.join(
        "templates",
        "index.html"
    )

    if os.path.exists(
        html_path
    ):
        with open(
            html_path,
            "r",
            encoding="utf-8"
        ) as f:
            return f.read()

    return "Template not found"


@app.post("/register")
def register_user(
    email: str,
    password: str,
    role: str = "contributor",
    db: Session = Depends(get_db)
):
    email = normalize_email(
        email
    )

    existing_user = (
        db.query(models.User)
        .filter(
            models.User.email == email
        )
        .first()
    )

    if existing_user:
        raise HTTPException(
            status_code=400,
            detail="Email already registered"
        )

    assigned_role = "contributor"

    hashed_pwd = hash_password(
        password
    )

    new_user = models.User(
        email=email,
        hashed_password=hashed_pwd,
        role=assigned_role,
        tasks_today=0,
        tasks_week=0,
        tasks_passed_qa=0,
        earnings=0.0
    )

    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    response = JSONResponse(
        content={
            "status": "success",
            "email": new_user.email,
            "role": new_user.role,
            "tasks_today": new_user.tasks_today,
            "tasks_week": new_user.tasks_week,
            "tasks_passed_qa": new_user.tasks_passed_qa,
            "earnings": new_user.earnings,
        }
    )

    response.set_cookie(
        KELYVO_SESSION_COOKIE,
        create_session_token(new_user.email, new_user.role),
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=KELYVO_SESSION_TTL,
        path="/",
    )

    return response


@app.post("/login")
def login_user(
    email: str,
    password: str,
    db: Session = Depends(get_db)
):
    email = normalize_email(
        email
    )

    hashed_pwd = hash_password(
        password
    )

    user = (
        db.query(models.User)
        .filter(
            models.User.email == email,
            models.User.hashed_password
                == hashed_pwd
        )
        .first()
    )

    if not user:
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid email or password"
            )
        )

    response = JSONResponse(
        content={
            "status": "success",
            "email": user.email,
            "role": user.role,
            "tasks_today": user.tasks_today,
            "tasks_week": user.tasks_week,
            "tasks_passed_qa": user.tasks_passed_qa,
            "earnings": user.earnings,
        }
    )

    response.set_cookie(
        KELYVO_SESSION_COOKIE,
        create_session_token(user.email, user.role),
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=KELYVO_SESSION_TTL,
        path="/",
    )

    return response


@app.post("/logout")
def logout_user(request: Request):
    response = JSONResponse(content={"status": "success"})
    response.delete_cookie(KELYVO_SESSION_COOKIE, path="/")
    return response


@app.get("/api/session")
def get_current_portal_session(
    request: Request,
    db: Session = Depends(get_db)
):
    """
    Return the currently authenticated KELYVO account from the secure
    HttpOnly session cookie.

    The frontend must use this endpoint to restore the dashboard instead
    of trusting localStorage as proof of authentication. This keeps the
    visible dashboard state synchronized with the server-side session.
    """
    session = require_portal_session(request)

    user = (
        db.query(models.User)
        .filter(
            models.User.email == normalize_email(session["email"])
        )
        .first()
    )

    if not user:
        raise HTTPException(
            status_code=401,
            detail="The KELYVO account for this session no longer exists."
        )

    return {
        "status": "success",
        "email": user.email,
        "role": user.role,
        "tasks_today": user.tasks_today,
        "tasks_week": user.tasks_week,
        "tasks_passed_qa": user.tasks_passed_qa,
        "earnings": user.earnings,
    }


@app.get("/api/start-task")
def start_task(
    request: Request,
    modality: str = "text",
    email: str = ""
):
    require_contributor_session(request)

    # Repair the one known task-10 draft corruption caused by the earlier
    # draft-routing patch. This runs only once because the migration writes
    # a marker file after successful cleanup.
    _repair_known_legacy_task10_drafts_once()
    _cleanup_legacy_task12_drafts_once()

    clean_input = (
        modality
        .strip()
        .lower()
    )

    email = normalize_email(
        email
    )

    if email:
        contributor_identity = (
            "user:" + email
        )
    else:
        contributor_identity = (
            get_browser_identity(
                request
            )
        )

    if clean_input.isdigit():
        project_id = int(
            clean_input
        )

        reverse_mapping = {
            value: key
            for key, value
            in PROJECT_MAPPING.items()
        }

        modality_key = (
            reverse_mapping.get(
                project_id
            )
        )

        if (
            project_id
            not in PROJECT_MAPPING.values()
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    "Unsupported Label Studio "
                    "project."
                )
            )
    else:
        modality_key = clean_input

        if (
            modality_key
            not in PROJECT_MAPPING
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Unsupported modality "
                    f"'{modality}'. "
                    f"Supported modalities: "
                    f"{', '.join(PROJECT_MAPPING.keys())}"
                )
            )

        project_id = (
            PROJECT_MAPPING[
                modality_key
            ]
        )

    contributor_user_id = None

    if email:
        contributor_user = None
        try:
            with SessionLocal() as start_task_db:
                contributor_user = (
                    start_task_db.query(models.User)
                    .filter(models.User.email == email)
                    .first()
                )
                if contributor_user:
                    contributor_user_id = int(contributor_user.id)
        except Exception:
            contributor_user_id = None

    reserved_task_id = (
        get_reserved_task(
            contributor_identity,
            project_id
        )
    )

    if reserved_task_id is not None:
        existing_response = (
            label_studio_request(
                "GET",
                f"/api/tasks/"
                f"{reserved_task_id}"
            )
        )

        if (
            existing_response.status_code
            == 200
        ):
            try:
                existing_task = (
                    existing_response.json()
                )
            except ValueError:
                existing_task = None

            if existing_task:
                existing_project = (
                    existing_task.get(
                        "project"
                    )
                )

                if (
                    existing_project is None
                    or int(existing_project)
                    == project_id
                ):
                    project_response = (
                        label_studio_request(
                            "GET",
                            f"/api/projects/"
                            f"{project_id}"
                        )
                    )

                    project = {}

                    if (
                        project_response.status_code
                        == 200
                    ):
                        try:
                            project = (
                                project_response.json()
                            )
                        except ValueError:
                            project = {}

                    task_for_frontend = (
                        rewrite_label_studio_media_urls(
                            existing_task
                        )
                    )

                    if email:
                        sync_browser_assignment_for_request(
                            request,
                            project_id,
                            int(reserved_task_id)
                        )

                    return {
                        "status":
                            "success",
                        "source":
                            "label_studio",
                        "modality":
                            modality_key,
                        "project_id":
                            project_id,
                        "config":
                            project.get(
                                "label_config",
                                ""
                            ),
                        "task":
                            task_for_frontend,
                        "assigned_task_id":
                            reserved_task_id,
                        "existing_assignment":
                            True,
                    }

        release_reserved_task(
            contributor_identity,
            project_id,
            reserved_task_id
        )

    project_response = (
        label_studio_request(
            "GET",
            f"/api/projects/{project_id}"
        )
    )

    if project_response.status_code == 404:
        raise HTTPException(
            status_code=404,
            detail=(
                "Label Studio project "
                "not found."
            )
        )

    if project_response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=(
                "Unable to retrieve "
                "Label Studio project."
            )
        )

    try:
        project = (
            project_response.json()
        )
    except ValueError:
        raise HTTPException(
            status_code=502,
            detail=(
                "Invalid response received "
                "from Label Studio."
            )
        )

    tasks_response = (
        label_studio_request(
            "GET",
            "/api/tasks",
            params={
                "project":
                    project_id,
                "page_size":
                    100,
            }
        )
    )

    if tasks_response.status_code == 404:
        raise HTTPException(
            status_code=404,
            detail=(
                "No tasks found for this "
                "Label Studio project."
            )
        )

    if tasks_response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=(
                "Unable to retrieve "
                "Label Studio tasks."
            )
        )

    try:
        tasks_payload = (
            tasks_response.json()
        )
    except ValueError:
        raise HTTPException(
            status_code=502,
            detail=(
                "Invalid task response "
                "received from Label Studio."
            )
        )

    if isinstance(
        tasks_payload,
        list
    ):
        tasks = tasks_payload
    else:
        tasks = (
            tasks_payload.get(
                "tasks",
                []
            )
        )

        if not tasks:
            tasks = (
                tasks_payload.get(
                    "results",
                    []
                )
            )

    if not tasks:
        raise HTTPException(
            status_code=404,
            detail=(
                "No tasks are currently "
                "available."
            )
        )

    # KELYVO determines whether a task is available from the actual
    # annotation state. The project-level /api/tasks response does not
    # reliably include the full annotation list for every task, so asking
    # only `task.get("annotations")` here can make an already-submitted
    # task look available again. Fetch each task's detail record and use
    # its real annotation list as the source of truth.
    #
    # Failed KELYVO tasks are reset server-side and returned to the original
    # pool. Prefer those clean requeued tasks for a contributor who has not
    # previously failed the task.
    requeue_db = SessionLocal()
    try:
        available_tasks = _get_requeued_tasks_for_contributor(
            db=requeue_db,
            project_id=project_id,
            task_type=modality_key,
            contributor_identity=contributor_identity,
            contributor_user_id=contributor_user_id,
        )
    finally:
        requeue_db.close()

    requeue_task_ids = {
        int(item.get("id"))
        for item in available_tasks
        if isinstance(item, dict) and item.get("id") is not None
    }

    for task in tasks:
        candidate_task_id = task.get("id")

        if candidate_task_id is None:
            continue

        detail_response = label_studio_request(
            "GET",
            f"/api/tasks/{candidate_task_id}",
            params={
                "project": project_id,
            }
        )

        if detail_response.status_code == 200:
            try:
                detail_task = detail_response.json()
            except ValueError:
                detail_task = task
        else:
            # Keep the original task payload as a fallback so a temporary
            # detail-request failure does not break the whole contributor
            # task picker.
            detail_task = task

        annotations = detail_task.get(
            "annotations",
            []
        )

        cancelled_annotations = detail_task.get(
            "cancelled_annotations",
            0
        )

        if annotations:
            continue

        if cancelled_annotations:
            continue

        if _is_task_reserved_by_other_contributor(
            project_id,
            int(candidate_task_id),
            contributor_identity,
        ):
            continue

        if int(candidate_task_id) in requeue_task_ids:
            # It is already present at the front of available_tasks.
            continue

        available_tasks.append(
            detail_task
        )

    if not available_tasks:
        raise HTTPException(
            status_code=404,
            detail=(
                "No unlabeled tasks are "
                "currently available."
            )
        )

    task = available_tasks[0]

    task_id = task.get(
        "id"
    )

    if task_id is None:
        raise HTTPException(
            status_code=502,
            detail=(
                "Label Studio returned "
                "a task without an ID."
            )
        )

    assigned_task_id = (
        reserve_task_for_contributor(
            contributor_identity,
            project_id,
            int(task_id)
        )
    )

    if email:
        sync_browser_assignment_for_request(
            request,
            project_id,
            int(assigned_task_id)
        )

    if (
        assigned_task_id
        != int(task_id)
    ):
        assigned_response = (
            label_studio_request(
                "GET",
                f"/api/tasks/"
                f"{assigned_task_id}"
            )
        )

        if (
            assigned_response.status_code
            == 200
        ):
            try:
                task = (
                    assigned_response.json()
                )
            except ValueError:
                raise HTTPException(
                    status_code=502,
                    detail=(
                        "Invalid assigned "
                        "task response."
                    )
                )
        else:
            release_reserved_task(
                contributor_identity,
                project_id,
                assigned_task_id
            )

            assigned_task_id = (
                reserve_task_for_contributor(
                    contributor_identity,
                    project_id,
                    int(task_id)
                )
            )

    task_for_frontend = (
        rewrite_label_studio_media_urls(
            task
        )
    )

    return {
        "status":
            "success",
        "source":
            "label_studio",
        "modality":
            modality_key,
        "project_id":
            project_id,
        "config":
            project.get(
                "label_config",
                ""
            ),
        "task":
            task_for_frontend,
        "assigned_task_id":
            assigned_task_id,
        "existing_assignment":
            False,
    }


@app.get("/api/current-task")
def current_task(
    request: Request,
    email: str = "",
    project_id: int = 0
):
    require_contributor_session(request)

    if not project_id:
        raise HTTPException(
            status_code=400,
            detail=(
                "Project ID is required."
            )
        )

    email = normalize_email(
        email
    )

    if email:
        contributor_identity = (
            "user:" + email
        )
    else:
        contributor_identity = (
            get_browser_identity(
                request
            )
        )

    task_id = (
        get_reserved_task(
            contributor_identity,
            project_id
        )
    )

    if task_id is None:
        return {
            "status":
                "success",
            "assigned":
                False,
            "task":
                None,
        }

    response = (
        label_studio_request(
            "GET",
            f"/api/tasks/{task_id}"
        )
    )

    if response.status_code != 200:
        release_reserved_task(
            contributor_identity,
            project_id,
            task_id
        )

        return {
            "status":
                "success",
            "assigned":
                False,
            "task":
                None,
        }

    try:
        task = (
            response.json()
        )
    except ValueError:
        raise HTTPException(
            status_code=502,
            detail=(
                "Invalid task response "
                "from Label Studio."
            )
        )

    task_project = task.get(
        "project"
    )

    if (
        task_project is not None
        and int(task_project)
        != int(project_id)
    ):
        release_reserved_task(
            contributor_identity,
            project_id,
            task_id
        )

        return {
            "status":
                "success",
            "assigned":
                False,
            "task":
                None,
        }

    if email:
        sync_browser_assignment_for_request(
            request,
            project_id,
            int(task_id)
        )

    return {
        "status":
            "success",
        "assigned":
            True,
        "project_id":
            project_id,
        "task_id":
            task_id,
        "task":
            rewrite_label_studio_media_urls(
                task
            ),
    }


@app.post("/api/skip-task")
def skip_task(
    request: Request,
    project_id: int,
    task_id: int,
    email: str = ""
):
    require_contributor_session(request)

    email = normalize_email(
        email
    )

    if email:
        contributor_identity = (
            "user:" + email
        )
    else:
        contributor_identity = (
            get_browser_identity(
                request
            )
        )

    assigned_task_id = (
        get_reserved_task(
            contributor_identity,
            project_id
        )
    )

    if assigned_task_id is None:
        raise HTTPException(
            status_code=403,
            detail=(
                "No task is assigned "
                "to this contributor."
            )
        )

    if (
        int(assigned_task_id)
        != int(task_id)
    ):
        raise HTTPException(
            status_code=403,
            detail=(
                "This task is not assigned "
                "to this contributor."
            )
        )

    release_reserved_task(
        contributor_identity,
        project_id,
        task_id
    )

    if email:
        release_reserved_task(
            get_browser_identity(request),
            project_id,
            task_id
        )

    return {
        "status":
            "success",
        "message":
            "Task skipped.",
        "task_id":
            task_id,
        "project_id":
            project_id,
    }


@app.post("/api/contributor/exit-task")
def exit_contributor_task(
    request: Request,
    project_id: int,
    task_id: int,
):
    """Release the contributor's active task when they explicitly exit the workspace."""
    session = require_contributor_session(request)
    email = normalize_email(session.get("email", ""))

    if not project_id or not task_id:
        raise HTTPException(
            status_code=400,
            detail="Project ID and task ID are required to exit the workspace.",
        )

    email_identity = "user:" + email
    browser_identity = get_browser_identity(request)

    email_task_id = get_reserved_task(
        email_identity,
        project_id,
    )
    browser_task_id = get_reserved_task(
        browser_identity,
        project_id,
    )

    if (
        email_task_id is not None
        and int(email_task_id) != int(task_id)
    ):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Task #{int(email_task_id)} is the active task for this contributor. "
                "Finish or exit that task before leaving this workspace."
            ),
        )

    if (
        browser_task_id is not None
        and int(browser_task_id) != int(task_id)
    ):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Task #{int(browser_task_id)} is the active browser task. "
                "Finish or exit that task before leaving this workspace."
            ),
        )

    release_reserved_task(
        email_identity,
        project_id,
        task_id,
    )
    release_reserved_task(
        browser_identity,
        project_id,
        task_id,
    )

    return {
        "status": "success",
        "message": "Workspace exited and task reservation released.",
        "task_id": int(task_id),
        "project_id": int(project_id),
    }


@app.post("/submit-task")
def submit_task(
    request: Request,
    email: str,
    task_type: str,
    task_title: str,
    task_id: int = 0,
    project_id: int = 0,
    db: Session = Depends(get_db)
):
    email = normalize_email(
        email
    )

    session = require_contributor_session(request)
    if email != session["email"]:
        raise HTTPException(
            status_code=403,
            detail="Submission account does not match the active KELYVO session."
        )

    user = (
        db.query(models.User)
        .filter(
            models.User.email == email
        )
        .first()
    )

    if not user:
        raise HTTPException(
            status_code=404,
            detail="User not found"
        )

    if task_id and project_id:
        contributor_identity = (
            "user:" + email
        )

        assigned_task_id = (
            get_reserved_task(
                contributor_identity,
                project_id
            )
        )

        if (
            assigned_task_id is None
            or int(assigned_task_id)
            != int(task_id)
        ):
            raise HTTPException(
                status_code=403,
                detail=(
                    "This task is not assigned "
                    "to this contributor."
                )
            )

    existing_submission = None

    try:
        existing_submission = (
            db.query(
                models.TaskSubmission
            )
            .filter(
                models.TaskSubmission.user_id
                == user.id,
                models.TaskSubmission.task_title
                == task_title,
                models.TaskSubmission.task_type
                == task_type,
                models.TaskSubmission.status
                == "PENDING_QA"
            )
            .first()
        )
    except Exception:
        existing_submission = None

    revision_submission = None

    if not existing_submission:
        try:
            revision_submission = (
                db.query(
                    models.TaskSubmission
                )
                .filter(
                    models.TaskSubmission.user_id
                    == user.id,
                    models.TaskSubmission.task_title
                    == task_title,
                    models.TaskSubmission.task_type
                    == task_type,
                    models.TaskSubmission.status
                    == "FAILED"
                )
                .order_by(
                    models.TaskSubmission.id.desc()
                )
                .first()
            )
        except Exception:
            revision_submission = None

    if revision_submission:
        # The contributor has corrected a QA-returned task. Reuse the same
        # submission record so the QA queue does not accumulate duplicates.
        revision_submission.status = "PENDING_QA"
        revision_submission.reviewer_notes = None
        db.commit()

        if task_id and project_id:
            release_reserved_task(
                "user:" + email,
                project_id,
                task_id
            )

        release_browser_assignment_for_request(
            request
        )

        return {
            "status": "success",
            "message": "Revision submitted for QA.",
            "tasks_today": user.tasks_today,
            "tasks_week": user.tasks_week,
            "revision": True,
            "submission_id": revision_submission.id,
        }

    if existing_submission:
        if task_id and project_id:
            release_reserved_task(
                "user:" + email,
                project_id,
                task_id
            )

        release_browser_assignment_for_request(
            request
        )

        return {
            "status":
                "success",
            "message":
                "Task has already been submitted.",
            "tasks_today":
                user.tasks_today,
            "tasks_week":
                user.tasks_week,
        }

    user.tasks_today += 1
    user.tasks_week += 1

    submission = (
        models.TaskSubmission(
            user_id=user.id,
            task_type=task_type,
            task_title=task_title,
            status="PENDING_QA",
            reviewer_notes=None
        )
    )

    db.add(
        submission
    )

    db.commit()

    db.refresh(
        user
    )

    if task_id and project_id:
        release_reserved_task(
            "user:" + email,
            project_id,
            task_id
        )

    release_browser_assignment_for_request(
        request
    )

    return {
        "status":
            "success",
        "tasks_today":
            user.tasks_today,
        "tasks_week":
            user.tasks_week
    }


@app.get("/admin/submissions")
def get_admin_submissions(
    request: Request,
    db: Session = Depends(get_db)
):
    require_qa_session(request)

    submissions = (
        db.query(
            models.TaskSubmission
        )
        .all()
    )

    result = []

    for sub in submissions:
        result.append({
            "id":
                sub.id,
            "contributor_email":
                (
                    sub.contributor.email
                    if sub.contributor
                    else
                    "Unassigned / Requeued"
                ),
            "task_type":
                sub.task_type,
            "task_title":
                sub.task_title,
            "status":
                sub.status,
            "reviewer_notes":
                sub.reviewer_notes
        })

    return result



@app.get("/api/contributor/submissions")
def get_contributor_submissions(
    request: Request,
    email: str,
    db: Session = Depends(get_db)
):
    """
    Return only the authenticated contributor's own submission records.

    The current KELYVO prototype uses the contributor email as the account
    identity, so the endpoint deliberately filters by the matching User row
    rather than exposing the global /admin/submissions collection.
    """
    session = require_contributor_session(request)
    if normalize_email(email) != session["email"]:
        raise HTTPException(
            status_code=403,
            detail="Contributor submission access is limited to the active account."
        )

    email = normalize_email(email)

    if not email:
        raise HTTPException(
            status_code=400,
            detail="Contributor email is required."
        )

    user = (
        db.query(
            models.User
        )
        .filter(
            models.User.email == email
        )
        .first()
    )

    if not user:
        raise HTTPException(
            status_code=404,
            detail="Contributor not found."
        )

    submissions = (
        db.query(
            models.TaskSubmission
        )
        .filter(
            models.TaskSubmission.user_id == user.id
        )
        .order_by(
            models.TaskSubmission.id.desc()
        )
        .all()
    )

    result = []

    for sub in submissions:
        task_id = None

        # TaskSubmission currently stores task_title rather than a dedicated
        # task_id column. Recover the numeric task id from the canonical title.
        match = re.search(
            r"(?:task|#)\s*#?\s*(\d+)\s*$",
            str(sub.task_title or ""),
            re.IGNORECASE
        )

        if match:
            task_id = int(
                match.group(1)
            )

        submitted_at = (
            getattr(
                sub,
                "submitted_at",
                None
            )
            or getattr(
                sub,
                "created_at",
                None
            )
        )

        if submitted_at is not None:
            try:
                submitted_at = submitted_at.isoformat()
            except AttributeError:
                submitted_at = str(
                    submitted_at
                )

        result.append({
            "id":
                sub.id,
            "task_id":
                task_id,
            "project_id":
                PROJECT_MAPPING.get(
                    str(sub.task_type or "").strip().lower(),
                    0
                ),
            "task_type":
                sub.task_type,
            "task_title":
                sub.task_title,
            "status":
                sub.status,
            "reviewer_notes":
                sub.reviewer_notes,
            "submitted_at":
                submitted_at
        })

    return {
        "status":
            "success",
        "email":
            user.email,
        "submissions":
            result
    }


@app.get("/api/qa/review/{submission_id}")
def get_qa_review_task(
    request: Request,
    submission_id: int,
    db: Session = Depends(get_db)
):
    """Return exactly one submitted task for the QA-only viewer."""
    require_qa_session(request)

    submission = (
        db.query(models.TaskSubmission)
        .filter(models.TaskSubmission.id == submission_id)
        .first()
    )

    if not submission:
        raise HTTPException(status_code=404, detail="Submission not found.")

    task_type = str(submission.task_type or "").strip().lower()
    project_id = PROJECT_MAPPING.get(task_type)
    if not project_id:
        raise HTTPException(
            status_code=400,
            detail="Unable to determine the project for this submission."
        )

    match = re.search(
        r"(?:task|#)\s*#?\s*(\d+)\s*$",
        str(submission.task_title or ""),
        re.IGNORECASE
    )
    if not match:
        raise HTTPException(
            status_code=400,
            detail="This submission does not contain enough task metadata for task-only QA review."
        )

    task_id = int(match.group(1))
    task_response = label_studio_request(
        "GET",
        f"/api/tasks/{task_id}",
        params={"project": project_id, "resolve_uri": "true"}
    )

    if task_response.status_code == 404:
        raise HTTPException(
            status_code=404,
            detail="The submitted Label Studio task could not be found."
        )
    if task_response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail="Unable to retrieve the submitted task from Label Studio."
        )

    try:
        task = task_response.json()
    except ValueError:
        raise HTTPException(
            status_code=502,
            detail="Label Studio returned invalid task data."
        )

    if not isinstance(task, dict):
        raise HTTPException(
            status_code=502,
            detail="Label Studio returned an invalid task object."
        )

    task = rewrite_label_studio_media_urls(task)

    annotations = task.get("annotations", [])
    if not isinstance(annotations, list):
        annotations = []

    return {
        "status": "success",
        "submission": {
            "id": submission.id,
            "contributor_email": (
                submission.contributor.email
                if submission.contributor
                else "Unassigned / Requeued"
            ),
            "task_type": task_type,
            "task_title": submission.task_title,
            "status": submission.status,
            "reviewer_notes": submission.reviewer_notes,
        },
        "task": task,
        "annotations": annotations,
        "task_id": task_id,
        "project_id": project_id,
    }


@app.post("/api/contributor/revision-task")
def start_contributor_revision(
    request: Request,
    submission_id: int,
    db: Session = Depends(get_db)
):
    """Reopen one contributor-owned QA-failed task for correction."""
    session = require_contributor_session(request)
    email = normalize_email(session["email"])

    user = (
        db.query(models.User)
        .filter(models.User.email == email)
        .first()
    )

    if not user:
        raise HTTPException(
            status_code=404,
            detail="Contributor not found."
        )

    submission = (
        db.query(models.TaskSubmission)
        .filter(
            models.TaskSubmission.id == int(submission_id),
            models.TaskSubmission.user_id == user.id,
        )
        .first()
    )

    if not submission:
        raise HTTPException(
            status_code=404,
            detail="Revision submission not found for this contributor."
        )

    if str(submission.status or "").upper() != "FAILED":
        raise HTTPException(
            status_code=409,
            detail="This task is not currently awaiting revision."
        )

    # QA-returned tasks follow a true revision workflow. The original
    # contributor reopens the same Label Studio task, sees the existing
    # annotation, corrects it, and submits it again.
    #
    # Do not reset/delete the Label Studio annotation here. The QA return
    # operation intentionally leaves the existing work intact for editing.
    task_type = str(submission.task_type or "").strip().lower()
    project_id = PROJECT_MAPPING.get(task_type)

    if not project_id:
        raise HTTPException(
            status_code=400,
            detail="Unable to determine the project for this revision."
        )

    match = re.search(
        r"(?:task|#)\s*#?\s*(\d+)\s*$",
        str(submission.task_title or ""),
        re.IGNORECASE
    )

    if not match:
        raise HTTPException(
            status_code=400,
            detail="This revision does not contain enough task metadata."
        )

    task_id = int(match.group(1))
    contributor_identity = "user:" + email

    # Enforce KELYVO's one-task-at-a-time rule. A contributor must not be able
    # to open a revision while another task is already actively assigned.
    current_task_id = get_reserved_task(
        contributor_identity,
        project_id
    )

    if (
        current_task_id is not None
        and int(current_task_id) != task_id
    ):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Task #{int(current_task_id)} is already active. "
                "Finish or exit that task before opening a revision."
            )
        )

    task_response = label_studio_request(
        "GET",
        f"/api/tasks/{task_id}",
        params={
            "project": int(project_id),
            "resolve_uri": "true",
        },
    )

    if task_response.status_code == 404:
        raise HTTPException(
            status_code=404,
            detail="The Label Studio task for this revision could not be found."
        )

    if task_response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail="Unable to retrieve the revision task from Label Studio."
        )

    try:
        task = task_response.json()
    except ValueError:
        raise HTTPException(
            status_code=502,
            detail="Label Studio returned invalid revision task data."
        )

    if not isinstance(task, dict):
        raise HTTPException(
            status_code=502,
            detail="Label Studio returned an invalid revision task object."
        )

    task_project = task.get("project")
    if (
        task_project is not None
        and int(task_project) != int(project_id)
    ):
        raise HTTPException(
            status_code=403,
            detail="The revision task does not belong to the expected project."
        )

    assigned_task_id = reserve_task_for_contributor(
        contributor_identity,
        project_id,
        task_id
    )

    sync_browser_assignment_for_request(
        request,
        project_id,
        task_id
    )

    # Convert the existing submitted annotation into an editable task draft
    # before the contributor enters Label Studio. The original annotation is
    # intentionally NOT deleted, so the contributor's previous work is carried
    # forward into the revision instead of starting from a blank canvas.
    _ensure_revision_draft_from_existing_annotation(
        int(project_id),
        int(task_id),
    )

    # Re-read the task after creating the draft so the frontend receives the
    # current draft state as well as the original annotation/task data.
    refreshed_task_response = label_studio_request(
        "GET",
        f"/api/tasks/{int(task_id)}",
        params={
            "project": int(project_id),
            "resolve_uri": "true",
        },
    )

    if refreshed_task_response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail="Unable to refresh the revision task after preserving its annotation.",
        )

    try:
        task = refreshed_task_response.json()
    except ValueError:
        raise HTTPException(
            status_code=502,
            detail="Label Studio returned invalid refreshed revision task data.",
        )

    task_for_frontend = rewrite_label_studio_media_urls(task)

    # Clear the old QA note only when the contributor actually begins the
    # revision. The review record remains FAILED until a new Submit arrives.
    return {
        "status": "success",
        "source": "kelyvo_revision",
        "modality": task_type,
        "project_id": int(project_id),
        "assigned_task_id": int(assigned_task_id),
        "submission_id": int(submission.id),
        "reviewer_notes": submission.reviewer_notes or "",
        "config": "",
        "task": task_for_frontend,
    }


@app.post("/admin/review-task")
def review_task(
    request: Request,
    submission_id: int,
    decision: str,
    notes: str = "",
    db: Session = Depends(get_db)
):
    require_qa_session(request)

    sub = (
        db.query(
            models.TaskSubmission
        )
        .filter(
            models.TaskSubmission.id
            == submission_id
        )
        .first()
    )

    if not sub:
        raise HTTPException(
            status_code=404,
            detail="Submission not found"
        )

    allowed_decisions = {
        "PASSED",
        "FAILED",
        "PENDING_QA"
    }

    decision = (
        decision
        .strip()
        .upper()
    )

    if (
        decision
        not in allowed_decisions
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid decision. "
                "Use PASSED, FAILED, "
                "or PENDING_QA."
            )
        )

    previous_status = (
        sub.status
    )

    if decision == "FAILED":
        task_type = str(sub.task_type or "").strip().lower()
        project_id = PROJECT_MAPPING.get(task_type)
        task_id = _extract_kelyvo_task_id(sub.task_title)

        if not project_id or task_id is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "This KELYVO submission cannot be returned for revision "
                    "because its task metadata is incomplete."
                )
            )

        # Preserve the submitted Label Studio annotation/draft. The original
        # contributor will reopen this exact task through
        # /api/contributor/revision-task, correct the work, and submit it
        # again. submit_task() will then reuse this same TaskSubmission record
        # and move it back to PENDING_QA.
        #
        # Intentionally do not call _reset_failed_label_studio_task_to_pool().
        pass

    sub.status = decision
    sub.reviewer_notes = notes.strip()

    if (
        decision == "PASSED"
        and previous_status != "PASSED"
        and sub.contributor
    ):
        sub.contributor.tasks_passed_qa += 1
        sub.contributor.earnings += 500.0

    # FAILED is an audited result for the original contributor. The
    # contributor receives an explicit revision action and can reopen the
    # existing task, correct it, and submit it back into QA.

    db.commit()

    return {
        "status":
            "success",
        "new_status":
            decision
    }


@app.post("/update-payout")
def update_payout(
    email: str,
    payout_method: str,
    payout_details: str,
    db: Session = Depends(get_db)
):
    email = normalize_email(
        email
    )

    user = (
        db.query(models.User)
        .filter(
            models.User.email == email
        )
        .first()
    )

    if not user:
        raise HTTPException(
            status_code=404,
            detail="User not found"
        )

    if user.payout_details:
        raise HTTPException(
            status_code=400,
            detail=(
                "Payout details are locked "
                "for security and cannot "
                "be changed."
            )
        )

    user.payout_method = (
        payout_method
    )

    user.payout_details = (
        payout_details
    )

    db.commit()

    return {
        "status":
            "success",
        "payout_method":
            user.payout_method,
        "payout_details":
            user.payout_details
    }


@app.get(
    "/admin/test-label-studio"
)
def test_label_studio():
    response = (
        label_studio_request(
            "GET",
            "/api/projects"
        )
    )

    if response.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=(
                "Label Studio connection "
                "test failed."
            )
        )

    try:
        data = (
            response.json()
        )
    except ValueError:
        raise HTTPException(
            status_code=502,
            detail=(
                "Label Studio returned "
                "invalid data."
            )
        )

    return {
        "status":
            "success",
        "message":
            "Successfully connected "
            "to Label Studio.",
        "project_count":
            data.get(
                "count",
                len(
                    data.get(
                        "results",
                        []
                    )
                )
            )
    }


@app.get(
    "/api/pipeline/projects"
)
def get_label_studio_projects(request: Request):
    require_contributor_session(request)

    response = (
        label_studio_request(
            "GET",
            "/api/projects"
        )
    )

    if response.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=(
                "Unable to retrieve "
                "Label Studio projects."
            )
        )

    try:
        data = (
            response.json()
        )
    except ValueError:
        raise HTTPException(
            status_code=502,
            detail=(
                "Invalid project data "
                "received from Label Studio."
            )
        )

    projects = []

    for project in data.get(
        "results",
        []
    ):
        project_id = (
            project.get("id")
        )

        modality = None

        for key, mapped_id in (
            PROJECT_MAPPING.items()
        ):
            if mapped_id == project_id:
                modality = key
                break

        task_count = 0

        if (
            project_id
            in PROJECT_MAPPING.values()
        ):
            task_response = (
                label_studio_request(
                    "GET",
                    "/api/tasks",
                    params={
                        "project":
                            project_id,
                        "page_size":
                            1,
                    }
                )
            )

            if (
                task_response.status_code
                == 200
            ):
                try:
                    task_data = (
                        task_response.json()
                    )

                    task_count = (
                        task_data.get(
                            "total",
                            len(
                                task_data.get(
                                    "tasks",
                                    task_data.get(
                                        "results",
                                        []
                                    )
                                )
                            )
                        )
                    )
                except ValueError:
                    task_count = 0

        projects.append({
            "id":
                project_id,
            "title":
                project.get(
                    "title",
                    ""
                ),
            "description":
                project.get(
                    "description",
                    ""
                ),
            "task_count":
                task_count,
            "modality":
                modality,
        })

    return {
        "status":
            "success",
        "source":
            "label_studio",
        "projects":
            projects
    }


@app.get(
    "/api/contributor/project"
)
def contributor_project(
    modality: str,
    request: Request
):
    require_contributor_session(request)

    clean_modality = (
        modality
        .strip()
        .lower()
    )

    if (
        clean_modality
        not in PROJECT_MAPPING
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Unsupported contributor "
                "modality."
            )
        )

    project_id = (
        PROJECT_MAPPING[
            clean_modality
        ]
    )

    response = (
        label_studio_request(
            "GET",
            f"/api/projects/"
            f"{project_id}"
        )
    )

    if response.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=(
                "Unable to retrieve "
                "contributor project."
            )
        )

    try:
        project = (
            response.json()
        )
    except ValueError:
        raise HTTPException(
            status_code=502,
            detail=(
                "Invalid project response."
            )
        )

    return {
        "status":
            "success",
        "project_id":
            project_id,
        "modality":
            clean_modality,
        "title":
            project.get(
                "title",
                ""
            ),
        "config":
            project.get(
                "label_config",
                ""
            ),
    }


@app.get(
    "/api/contributor/access"
)
def contributor_access(
    email: str,
    request: Request
):
    require_contributor_session(request)

    email = normalize_email(
        email
    )

    if not email:
        raise HTTPException(
            status_code=400,
            detail=(
                "Contributor email "
                "is required."
            )
        )

    return {
        "status":
            "success",
        "contributor":
            True,
        "direct_label_studio_access":
            False,
        "single_task_mode":
            True,
        "can_browse_projects":
            False,
        "can_browse_task_list":
            False,
        "can_access_label_studio_admin":
            False,
    }


def _validate_workspace_assignment(
    request: Request,
    project_id: int,
    task_id: int
):
    contributor_identity = (
        get_browser_identity(
            request
        )
    )

    assigned_task_id = (
        get_reserved_task(
            contributor_identity,
            project_id
        )
    )

    if assigned_task_id is None:
        raise HTTPException(
            status_code=403,
            detail=(
                "This contributor does not "
                "have an active task."
            )
        )

    if (
        int(assigned_task_id)
        != int(task_id)
    ):
        raise HTTPException(
            status_code=403,
            detail=(
                "This task is not assigned "
                "to this contributor."
            )
        )


def _is_contributor_next_task_action(
    request: Request,
    clean_path: str,
    project_id: int,
) -> bool:
    """Return True only for Label Studio's task-navigation action for this project."""
    requested_project = request.query_params.get("project")

    return (
        request.method == "POST"
        and clean_path == "/api/dm/actions"
        and request.query_params.get("id") == "next_task"
        and requested_project is not None
        and requested_project.isdigit()
        and int(requested_project) == int(project_id)
    )


KELYVO_LEGACY_DRAFT_REPAIR_MARKER = os.path.join(
    ".kelyvo_task10_legacy_draft_repair_v1.done"
)


def _repair_known_legacy_task10_drafts_once():
    """
    One-time cleanup for the draft corruption caused by the earlier
    contributor draft-routing patch.

    Step 7 incorrectly routed /api/tasks/11/drafts into task 10. That
    permanently attached the old task's draft to task 10. This migration
    removes task 10's existing drafts once, then writes a marker so normal
    drafts are never deleted on later restarts.
    """
    if os.path.exists(KELYVO_LEGACY_DRAFT_REPAIR_MARKER):
        return

    project_id = 2
    task_id = 10

    try:
        response = label_studio_request(
            "GET",
            f"/api/tasks/{task_id}",
            params={
                "project": project_id,
                "resolve_uri": "true",
            },
        )

        if response.status_code == 200:
            payload = response.json()

            drafts = (
                payload.get("drafts", [])
                if isinstance(payload, dict)
                else []
            )

            if isinstance(drafts, list):
                for draft in drafts:
                    if not isinstance(draft, dict):
                        continue

                    draft_id = draft.get("id")

                    if draft_id is None:
                        continue

                    delete_response = label_studio_request(
                        "DELETE",
                        f"/api/drafts/{int(draft_id)}",
                        params={"project": project_id},
                    )

                    if delete_response.status_code not in (
                        200,
                        204,
                        404
                    ):
                        # Do not create the marker if a draft could not be removed.
                        return

        with open(
            KELYVO_LEGACY_DRAFT_REPAIR_MARKER,
            "w",
            encoding="utf-8",
        ) as marker:
            marker.write("done\n")

    except Exception:
        # Leave the marker absent so the migration can safely retry next time.
        return


def _kelyvo_assigned_next_task_response(
    project_id: int,
    task_id: int,
):
    """
    Return KELYVO's already-assigned task directly instead of allowing
    Label Studio's native next_task selector to run.

    This is deliberately a direct task fetch. The native next_task action
    can select another task and record that task in Label Studio stream
    history. KELYVO contributor mode must never allow that.

    The returned payload is the complete Label Studio task object, which is
    what the working contributor editor already accepts as its active task.
    """
    response = label_studio_request(
        "GET",
        f"/api/tasks/{int(task_id)}",
        params={
            "project": int(project_id),
            "resolve_uri": "true",
        },
    )

    if response.status_code >= 400:
        return None

    try:
        payload = response.json()
    except ValueError:
        return None

    if not isinstance(payload, dict):
        return None

    # The direct assigned-task response bypasses the normal proxy response
    # rewriting below, so rewrite Label Studio's internal /data/... media URL
    # here before the task reaches the browser. Otherwise the browser tries
    # to request /data/upload/... from KELYVO and receives a 404.
    payload = rewrite_label_studio_media_urls(payload)

    payload["queue"] = "KELYVO contributor queue"

    return Response(
        content=json.dumps(payload).encode("utf-8"),
        status_code=200,
        headers={
            "Content-Type": "application/json",
            "Cache-Control": "no-store",
            "X-KELYVO-Assigned-Next-Task": str(int(task_id)),
        },
    )


def _rewrite_label_studio_workspace_html(
    html: str,
    project_id: int,
    task_id: int
) -> str:
    hide_ui_css = """
    <style id="kelyvo-strict-navbar-lock">
        /* Aggressively hide the Label Studio top navigation bar */
        header,
        .ls-header,
        [class*="header"],
        [class*="breadcrumbs"],
        [class*="hamburger-menu"],
        [class*="menu-wrapper"] {
            display: none !important;
            visibility: hidden !important;
            height: 0 !important;
            width: 0 !important;
            overflow: hidden !important;
            position: absolute !important;
        }

        /* Remove padding/margins so the editor takes up the full iframe */
        body, html, #root, #label-studio {
            margin: 0 !important;
            padding: 0 !important;
            height: 100vh !important;
            width: 100vw !important;
            overflow: hidden !important;
        }

        /* Hide the native pagination so users cannot skip around the queue manually */
        .ls-pagination,
        [class*="pagination"] {
            display: none !important;
        }

        /* Hide only the native Previous/Next task-history controls.
           The server-side history lock remains the real security boundary. */
        button[aria-label*="Previous task" i],
        button[aria-label*="Next task" i],
        button[title*="Previous task" i],
        button[title*="Next task" i],
        [data-testid*="previous-task" i],
        [data-testid*="next-task" i] {
            display: none !important;
            visibility: hidden !important;
            pointer-events: none !important;
        }
    </style>

    <script id="kelyvo-task-history-ui-lock">
    (function () {
        function lockTaskHistoryButtons() {
            const selectors = [
                'button[aria-label*="Previous task" i]',
                'button[aria-label*="Next task" i]',
                'button[title*="Previous task" i]',
                'button[title*="Next task" i]',
                '[data-testid*="previous-task" i]',
                '[data-testid*="next-task" i]'
            ];

            document.querySelectorAll(selectors.join(','))
                .forEach(function (element) {
                    element.style.setProperty('display', 'none', 'important');
                    element.style.setProperty('visibility', 'hidden', 'important');
                    element.style.setProperty('pointer-events', 'none', 'important');
                });
        }

        lockTaskHistoryButtons();

        const observer = new MutationObserver(lockTaskHistoryButtons);

        observer.observe(document.documentElement, {
            childList: true,
            subtree: true,
            attributes: true,
            attributeFilter: ['aria-label', 'title', 'data-testid']
        });
    })();
    </script>
    """

    if "</head>" in html:
        html = html.replace(
            "</head>",
            f"{hide_ui_css}\n</head>"
        )
    else:
        html = hide_ui_css + html

    return html


def _kelyvo_task_history_response(task_id: int):
    """Return only the contributor's currently assigned task in task history."""
    return JSONResponse(
        content=[
            {
                "taskId": int(task_id),
                "annotationId": None,
            }
        ],
        status_code=200,
        headers={
            "Cache-Control": "no-store",
            "X-KELYVO-Task-History-Lock": "1",
        },
    )


@app.get(
    "/api/label-studio-task-workspace",
    response_class=HTMLResponse
)
def label_studio_task_workspace(
    request: Request,
    project_id: int,
    task_id: int,
    modality: str = "",
    embed_id: str = ""
):
    _validate_workspace_assignment(
        request,
        project_id,
        task_id
    )

    clean_modality = (
        modality or ""
    ).strip().lower()

    if clean_modality:
        mapped_project_id = (
            PROJECT_MAPPING.get(
                clean_modality
            )
        )

        if (
            mapped_project_id is None
            or int(mapped_project_id) != int(project_id)
        ):
            raise HTTPException(
                status_code=403,
                detail=(
                    "This project is not available "
                    "for this contributor workspace."
                )
            )

    target = (
        f"/projects/{int(project_id)}/data"
        f"?labeling=1&task={int(task_id)}"
    )

    return RedirectResponse(
        url=target,
        status_code=307
    )


@app.get(
    "/projects/{project_id}/data",
    response_class=HTMLResponse
)
def contributor_label_studio_native_workspace(
    request: Request,
    project_id: int
):
    contributor_identity = get_browser_identity(request)

    assigned_task_id = get_reserved_task(
        contributor_identity,
        int(project_id)
    )

    requested_task = request.query_params.get(
        "task",
        ""
    )

    if assigned_task_id is None:
        raise HTTPException(
            status_code=403,
            detail="No active contributor task."
        )

    if (
        not requested_task.isdigit()
        or int(requested_task) != int(assigned_task_id)
    ):
        raise HTTPException(
            status_code=403,
            detail=(
                "This Label Studio workspace is restricted "
                "to the assigned task."
            )
        )

    if int(project_id) not in PROJECT_MAPPING.values():
        raise HTTPException(
            status_code=403,
            detail="This project is not available in contributor mode."
        )

    page_response = label_studio_request(
        "GET",
        f"/projects/{int(project_id)}/data",
        params={
            "labeling": "1",
            "task": str(int(assigned_task_id))
        }
    )

    if page_response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=(
                "Label Studio could not open the assigned task workspace."
            )
        )

    final_html = _rewrite_label_studio_workspace_html(
        page_response.text,
        int(project_id),
        int(assigned_task_id)
    )

    response = HTMLResponse(
        content=final_html,
        status_code=200
    )

    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["Content-Security-Policy"] = (
        "frame-ancestors 'self'; "
        "default-src 'self' 'unsafe-inline' 'unsafe-eval' "
        "data: blob: http: https:;"
    )

    return response


@app.get(
    "/api/label-studio-react/{path:path}"
)
def label_studio_react_proxy(
    path: str
):
    clean_path = (
        "/react-app/" +
        (path or "").lstrip("/")
    )

    response = label_studio_request(
        "GET",
        clean_path
    )

    if response.status_code >= 400:
        raise HTTPException(
            status_code=response.status_code,
            detail="Label Studio React asset not found."
        )

    headers = {}

    for name in (
        "content-type",
        "cache-control",
        "etag",
        "last-modified"
    ):
        value = response.headers.get(name)

        if value:
            headers[name] = value

    return Response(
        content=response.content,
        status_code=response.status_code,
        headers=headers
    )


@app.api_route(
    "/api/label-studio-proxy/{path:path}",
    methods=[
        "GET",
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "OPTIONS",
        "HEAD"
    ]
)
async def label_studio_browser_api_proxy(
    request: Request,
    path: str
):
    raw_path = "/" + (
        path or ""
    ).lstrip("/")

    react_app_path = raw_path.startswith("/react-app/")

    clean_path = raw_path

    if (
        not react_app_path
        and not clean_path.startswith("/api/")
    ):
        clean_path = (
            "/api" +
            clean_path
        )

    contributor_identity = (
        get_browser_identity(
            request
        )
    )

    active_assignment = None

    cleanup_task_assignments()

    matching_assignments = [
        assignment
        for assignment in TASK_ASSIGNMENTS.values()
        if assignment.get("identity") == contributor_identity
    ]

    if matching_assignments:
        active_assignment = max(
            matching_assignments,
            key=lambda item: item.get("created_at", 0)
        )

    if not active_assignment:
        raise HTTPException(
            status_code=403,
            detail="No active contributor task."
        )

    project_id = int(
        active_assignment.get(
            "project_id"
        )
    )

    task_id = int(
        active_assignment.get(
            "task_id"
        )
    )

    contributor_next_task_action = _is_contributor_next_task_action(
        request,
        clean_path,
        project_id,
    )

    # After native Submit, Label Studio immediately fires next_task.
    # Consume that one automatic navigation request and return success
    # with no replacement task. This keeps the contributor on the same
    # task until KELYVO's MARK TASK SUBMITTED action advances the queue.
    if contributor_next_task_action and consume_recent_native_submission(
        request,
        project_id,
        task_id,
    ):
        locked_next_response = _kelyvo_assigned_next_task_response(
            project_id,
            task_id,
        )
        locked_next_response.headers["X-KELYVO-Post-Submit-Lock"] = "1"
        return locked_next_response

    # Lock Label Studio's Previous/Next task history to this contributor's
    # single KELYVO-assigned task. This is a server-side security boundary;
    # the UI arrows may still render, but they cannot expose other tasks.
    if (
        request.method == "GET"
        and clean_path.rstrip("/") == f"/api/projects/{project_id}/label-stream-history"
    ):
        return _kelyvo_task_history_response(task_id)

    # CRITICAL:
    # Intercept Label Studio's automatic startup task-navigation action
    # BEFORE native Data Manager code can execute.
    #
    # We deliberately do NOT forward POST /api/dm/actions?id=next_task.
    # Native Label Studio can select another task and write it into its
    # stream history. KELYVO already assigned exactly one task, so the
    # assigned task is returned directly.
    if contributor_next_task_action:
        assigned_next_response = _kelyvo_assigned_next_task_response(
            project_id,
            task_id,
        )

        if assigned_next_response is not None:
            return assigned_next_response

        raise HTTPException(
            status_code=502,
            detail="Unable to load the assigned contributor task."
        )

    project_prefix = (
        f"/api/projects/{project_id}"
    )

    task_prefix = (
        f"/api/tasks/{task_id}"
    )

    blocked_exact_paths = {
        "/api/projects",
        "/api/users",
        "/api/user",
        "/api/organizations",
        "/api/organization",
        "/api/admin",
        "/api/settings",
    }

    blocked_prefixes = (
        "/api/users/",
        "/api/organizations/",
        "/api/organization/",
        "/api/admin/",
        "/api/memberships/",
        "/api/invitations/",
    )

    allowed = react_app_path

    if (
        clean_path in blocked_exact_paths
        or any(
            clean_path.startswith(prefix)
            for prefix in blocked_prefixes
        )
    ):
        allowed = False

    if (
        clean_path == project_prefix
        or clean_path == project_prefix + "/"
        or clean_path.startswith(project_prefix + "/")
        or clean_path == task_prefix
        or clean_path == task_prefix + "/"
        or clean_path.startswith(task_prefix + "/")
    ):
        allowed = True

    if clean_path == "/api/tasks":
        requested_task = (
            request.query_params.get("id")
            or request.query_params.get("task")
        )

        allowed = (
            requested_task is not None
            and requested_task.isdigit()
            and int(requested_task) == task_id
        )

    requested_project_id = request.query_params.get("project")

    if (
        request.method == "GET"
        and requested_project_id is not None
        and requested_project_id.isdigit()
        and int(requested_project_id) == project_id
        and clean_path in {
            "/api/dm/project",
            "/api/dm/columns",
            "/api/dm/views",
            "/api/dm/actions",
            "/api/label_links",
        }
    ):
        allowed = True

    workspace_api_prefixes = (
        "/api/annotations",
        "/api/drafts",
        "/api/reviews",
        "/api/comments",
        "/api/predictions",
        "/api/activities",
        "/api/interactive",
        "/api/label-config",
        "/api/label-configs",
        "/api/data",
        "/api/columns",
        "/api/filters",
        "/api/health",
        "/api/version",
        "/api/config",
        "/api/dm",
        "/api/ml",
        "/api/current-user",
        "/api/label_links",
        "/api/label-links",
    )

    if any(
        clean_path == prefix
        or clean_path.startswith(prefix + "/")
        for prefix in workspace_api_prefixes
    ):
        allowed = True

    if clean_path.rstrip("/") == "/api/native-submit-status":
        project_value = request.query_params.get("project_id", "")
        task_value = request.query_params.get("task_id", "")
        email_value = normalize_email(
            request.query_params.get("email", "")
        )

        if not project_value.isdigit() or not task_value.isdigit():
            return JSONResponse(
                content={"submitted": False, "task_id": 0},
                status_code=200,
                headers={"Cache-Control": "no-store"}
            )

        project_number = int(project_value)
        task_number = int(task_value)
        browser_identity = get_browser_identity(request)

        if email_value:
            email_task_id = get_reserved_task(
                "user:" + email_value,
                project_number
            )
            if email_task_id is not None:
                sync_browser_assignment_for_request(
                    request,
                    project_number,
                    int(email_task_id)
                )

        key = make_assignment_key(
            browser_identity,
            project_number
        )
        entry = KELYVO_RECENT_NATIVE_SUBMISSIONS.get(key)

        if not entry or float(entry.get("expires_at", 0) or 0) <= time.time():
            if entry:
                KELYVO_RECENT_NATIVE_SUBMISSIONS.pop(key, None)
            return JSONResponse(
                content={
                    "submitted": False,
                    "task_id": task_number
                },
                status_code=200,
                headers={"Cache-Control": "no-store"}
            )

        matched_task_id = int(entry.get("task_id", 0) or 0)
        return JSONResponse(
            content={
                "submitted": matched_task_id == task_number,
                "task_id": matched_task_id or task_number
            },
            status_code=200,
            headers={"Cache-Control": "no-store"}
        )

    if (
        clean_path == "/api/current-user"
        or clean_path.startswith("/api/current-user/")
    ):
        allowed = True

    if (
        clean_path.startswith("/api/import")
        or clean_path.startswith("/api/export")
    ):
        allowed = False

    blocked_project_subpaths = (
        project_prefix + "/tasks",
        project_prefix + "/data",
        project_prefix + "/members",
        project_prefix + "/users",
        project_prefix + "/settings",
    )

    if any(
        clean_path == prefix
        or clean_path.startswith(prefix + "/")
        for prefix in blocked_project_subpaths
    ):
        allowed = False

    if not allowed:
        raise HTTPException(
            status_code=403,
            detail=(
                "This Label Studio resource is not available in "
                "contributor task mode."
            )
        )

    query_string = request.url.query
    endpoint = clean_path

    if query_string:
        endpoint += "?" + query_string

    body = await request.body()

    forwarded_headers = {}

    content_type = request.headers.get(
        "content-type"
    )

    if content_type:
        forwarded_headers[
            "Content-Type"
        ] = content_type

    response = label_studio_request(
        request.method,
        endpoint,
        data=body if body else None,
        headers=forwarded_headers
    )

    if (
        request.method == "POST"
        and clean_path.rstrip("/")
        == f"/api/tasks/{task_id}/annotations"
        and response.status_code in (200, 201)
    ):
        mark_native_annotation_submitted(
            request,
            project_id,
            task_id,
        )

    if (
        request.method in ("PUT", "PATCH")
        and response.status_code in (200, 201)
        and (
            re.match(
                r"^/api/annotations/\d+/?$",
                clean_path,
            )
            or re.match(
                rf"^/api/tasks/{int(task_id)}/annotations/\d+/?$",
                clean_path,
            )
        )
    ):
        mark_native_revision_updated(
            request,
            project_id,
            task_id,
        )

    response_headers = {}

    response_content_type = (
        response.headers.get(
            "content-type"
        )
    )

    if response_content_type:
        response_headers[
            "Content-Type"
        ] = response_content_type

    content = response.content

    if (
        response_content_type
        and
        "application/json"
        in response_content_type.lower()
    ):
        try:
            payload = response.json()

            if (
                clean_path == "/api/tasks"
                and request.query_params.get("project")
                and request.query_params.get("project").isdigit()
                and int(request.query_params.get("project")) == project_id
            ):
                if isinstance(payload, list):
                    payload = [
                        item
                        for item in payload
                        if isinstance(item, dict)
                        and str(item.get("id")) == str(task_id)
                    ]

                elif isinstance(payload, dict):
                    if isinstance(payload.get("tasks"), list):
                        payload["tasks"] = [
                            item
                            for item in payload["tasks"]
                            if isinstance(item, dict)
                            and str(item.get("id")) == str(task_id)
                        ]

                        if "total" in payload:
                            payload["total"] = len(
                                payload["tasks"]
                            )

                    if isinstance(payload.get("results"), list):
                        payload["results"] = [
                            item
                            for item in payload["results"]
                            if isinstance(item, dict)
                            and str(item.get("id")) == str(task_id)
                        ]

                        if "count" in payload:
                            payload["count"] = len(
                                payload["results"]
                            )

            payload = rewrite_label_studio_media_urls(
                payload
            )

            content = json.dumps(
                payload
            ).encode("utf-8")

            response_headers["Content-Type"] = (
                "application/json"
            )

        except ValueError:
            pass

    return Response(
        content=content,
        status_code=response.status_code,
        headers=response_headers
    )


@app.get(
    "/api/label-studio-static/{path:path}"
)
def label_studio_static_proxy(
    path: str
):
    clean_path = (
        "/" +
        (path or "").lstrip("/")
    )

    response = label_studio_request(
        "GET",
        "/static" + clean_path
    )

    if response.status_code >= 400:
        raise HTTPException(
            status_code=404,
            detail="Label Studio asset not found."
        )

    headers = {}

    content_type = response.headers.get(
        "content-type"
    )

    if content_type:
        headers[
            "Content-Type"
        ] = content_type

    content = response.content

    if (
        content_type
        and
        "text/css"
        in content_type.lower()
    ):
        try:
            content = (
                content.decode("utf-8")
                .replace(
                    "/static/",
                    "/api/label-studio-static/"
                )
                .encode("utf-8")
            )
        except UnicodeDecodeError:
            pass

    headers[
        "Cache-Control"
    ] = "private, max-age=3600"

    return Response(
        content=content,
        status_code=response.status_code,
        headers=headers
    )


def _proxy_label_studio_root_asset(endpoint: str):
    response = label_studio_request(
        "GET",
        endpoint
    )

    if response.status_code >= 400:
        raise HTTPException(
            status_code=response.status_code,
            detail="Label Studio asset not found."
        )

    content_type = response.headers.get(
        "content-type",
        "application/octet-stream"
    )

    content = response.content

    if (
        "text/css" in content_type.lower()
        or "javascript" in content_type.lower()
    ):
        try:
            content = (
                content.decode("utf-8")
                .replace(
                    "/static/",
                    "/api/label-studio-static/"
                )
                .replace(
                    "/react-app/",
                    "/api/label-studio-react/"
                )
                .encode("utf-8")
            )
        except UnicodeDecodeError:
            pass

    return Response(
        content=content,
        status_code=response.status_code,
        headers={
            "Content-Type": content_type,
            "Cache-Control": "private, max-age=3600",
        }
    )


@app.get("/react-app/{path:path}")
def label_studio_root_react_proxy(path: str):
    return label_studio_react_proxy(path)


@app.get("/static/{path:path}")
def label_studio_root_static_proxy(path: str):
    clean_path = "/" + (path or "").lstrip("/")

    return _proxy_label_studio_root_asset(
        "/static" + clean_path
    )


@app.get("/sw.js")
def label_studio_service_worker():
    return _proxy_label_studio_root_asset(
        "/sw.js"
    )


@app.get("/health")
def health_check():
    return {
        "status":
            "ok",
        "service":
            "kelyvo-portal"
    }


@app.api_route(
    "/api/{path:path}",
    methods=[
        "GET",
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "OPTIONS",
        "HEAD"
    ]
)
async def native_label_studio_api_fallback(
    request: Request,
    path: str
):
    clean_path = "/api/" + (
        path or ""
    ).lstrip("/")

    kelyvo_paths = {
        "/start-task",
        "/current-task",
        "/skip-task",
        "/label-studio-media",
        "/label-studio-task-workspace",
    }

    kelyvo_prefixes = (
        "/pipeline/",
        "/contributor/",
        "/label-studio-react/",
        "/label-studio-proxy/",
        "/label-studio-static/",
    )

    if (
        clean_path in kelyvo_paths
        or any(
            clean_path.startswith(prefix)
            for prefix in kelyvo_prefixes
        )
    ):
        raise HTTPException(
            status_code=404,
            detail="KELYVO API endpoint not found."
        )

    contributor_identity = get_browser_identity(
        request
    )

    cleanup_task_assignments()

    assignments = [
        assignment
        for assignment in TASK_ASSIGNMENTS.values()
        if assignment.get("identity") == contributor_identity
    ]

    if not assignments:
        raise HTTPException(
            status_code=403,
            detail="No active contributor task."
        )

    requested_project_id = None
    requested_task_id = None

    path_parts = clean_path.strip(
        "/"
    ).split("/")

    if (
        len(path_parts) >= 3
        and path_parts[0] == "api"
        and path_parts[1] == "projects"
        and path_parts[2].isdigit()
    ):
        requested_project_id = int(
            path_parts[2]
        )

    if (
        len(path_parts) >= 3
        and path_parts[0] == "api"
        and path_parts[1] == "tasks"
        and path_parts[2].isdigit()
    ):
        requested_task_id = int(
            path_parts[2]
        )

    if clean_path == "/api/tasks":
        query_task = (
            request.query_params.get("id")
            or request.query_params.get("task")
            or request.query_params.get("pk")
        )

        if (
            query_task
            and query_task.isdigit()
        ):
            requested_task_id = int(
                query_task
            )

    active_assignment = None

    if requested_task_id is not None:
        for assignment in assignments:
            if (
                int(assignment.get("task_id"))
                == requested_task_id
            ):
                active_assignment = assignment
                break

    if (
        active_assignment is None
        and requested_project_id is not None
    ):
        for assignment in assignments:
            if (
                int(assignment.get("project_id"))
                == requested_project_id
            ):
                active_assignment = assignment
                break

    if active_assignment is None:
        active_assignment = max(
            assignments,
            key=lambda item: item.get(
                "created_at",
                0
            )
        )

    if active_assignment is None:
        raise HTTPException(
            status_code=403,
            detail=(
                "This Label Studio resource is not available in "
                "contributor task mode."
            )
        )

    project_id = int(
        active_assignment.get(
            "project_id"
        )
    )

    task_id = int(
        active_assignment.get(
            "task_id"
        )
    )

    contributor_next_task_action = _is_contributor_next_task_action(
        request,
        clean_path,
        project_id,
    )

    # After native Submit, Label Studio immediately fires next_task.
    # Consume that one automatic navigation request and return success
    # with no replacement task. This keeps the contributor on the same
    # task until KELYVO's MARK TASK SUBMITTED action advances the queue.
    if contributor_next_task_action and consume_recent_native_submission(
        request,
        project_id,
        task_id,
    ):
        locked_next_response = _kelyvo_assigned_next_task_response(
            project_id,
            task_id,
        )
        locked_next_response.headers["X-KELYVO-Post-Submit-Lock"] = "1"
        return locked_next_response

    # Lock Label Studio's Previous/Next task history to this contributor's
    # single KELYVO-assigned task.
    if (
        request.method == "GET"
        and clean_path.rstrip("/")
        == f"/api/projects/{project_id}/label-stream-history"
    ):
        return _kelyvo_task_history_response(
            task_id
        )

    # CRITICAL:
    # Intercept Label Studio's automatic startup task-navigation action
    # BEFORE native Data Manager code can execute.
    #
    # This means Label Studio never gets to run its own task-selection
    # algorithm for a KELYVO contributor.
    if contributor_next_task_action:
        assigned_next_response = (
            _kelyvo_assigned_next_task_response(
                project_id,
                task_id,
            )
        )

        if assigned_next_response is not None:
            return assigned_next_response

        raise HTTPException(
            status_code=502,
            detail="Unable to load the assigned contributor task."
        )

    blocked_exact_paths = {
        "/api/projects",
        "/api/users",
        "/api/user",
        "/api/organizations",
        "/api/organization",
        "/api/admin",
        "/api/settings",
        "/api/memberships",
        "/api/invitations",
    }

    blocked_prefixes = (
        "/api/users/",
        "/api/organizations/",
        "/api/organization/",
        "/api/admin/",
        "/api/memberships/",
        "/api/invitations/",
    )

    if (
        clean_path in blocked_exact_paths
        or any(
            clean_path.startswith(prefix)
            for prefix in blocked_prefixes
        )
    ):
        raise HTTPException(
            status_code=403,
            detail=(
                "This Label Studio resource is not available in "
                "contributor task mode."
            )
        )

    project_prefix = (
        f"/api/projects/{project_id}"
    )

    task_prefix = (
        f"/api/tasks/{task_id}"
    )

    allowed = False

    if (
        clean_path == project_prefix
        or clean_path == project_prefix + "/"
        or clean_path.startswith(
            project_prefix + "/"
        )
        or clean_path == task_prefix
        or clean_path == task_prefix + "/"
        or clean_path.startswith(
            task_prefix + "/"
        )
    ):
        allowed = True

    if clean_path == "/api/tasks":
        requested_task = (
            request.query_params.get("id")
            or request.query_params.get("task")
            or request.query_params.get("pk")
        )

        requested_project = (
            request.query_params.get("project")
        )

        if (
            requested_project is not None
            and requested_project.isdigit()
            and int(requested_project)
            == project_id
            and request.method == "GET"
        ):
            allowed = True

        elif (
            requested_task is not None
            and requested_task.isdigit()
            and int(requested_task)
            == task_id
        ):
            allowed = True

    requested_project_id = (
        request.query_params.get(
            "project"
        )
    )

    if (
        request.method == "GET"
        and requested_project_id is not None
        and requested_project_id.isdigit()
        and int(requested_project_id)
        == project_id
        and (
            clean_path == "/api/dm/users"
            or clean_path == "/api/ml"
            or clean_path.startswith("/api/ml/")
        )
    ):
        allowed = True

    workspace_api_prefixes = (
        "/api/annotations",
        "/api/drafts",
        "/api/reviews",
        "/api/comments",
        "/api/predictions",
        "/api/activities",
        "/api/interactive",
        "/api/label-config",
        "/api/label-configs",
        "/api/data",
        "/api/columns",
        "/api/filters",
        "/api/health",
        "/api/version",
        "/api/config",
        "/api/dm",
        "/api/ml",
        "/api/current-user",
        "/api/label_links",
        "/api/label-links",
        "/api/label-stream-history",
    )

    if any(
        clean_path == prefix
        or clean_path.startswith(
            prefix + "/"
        )
        for prefix in workspace_api_prefixes
    ):
        allowed = True

    if (
        clean_path == "/api/current-user"
        or clean_path.startswith(
            "/api/current-user/"
        )
    ):
        allowed = True

    if (
        clean_path.startswith("/api/import")
        or clean_path.startswith("/api/export")
    ):
        allowed = False

    blocked_project_subpaths = (
        project_prefix + "/tasks",
        project_prefix + "/data",
        project_prefix + "/members",
        project_prefix + "/users",
        project_prefix + "/settings",
    )

    if any(
        clean_path == prefix
        or clean_path.startswith(
            prefix + "/"
        )
        for prefix in blocked_project_subpaths
    ):
        allowed = False

    if not allowed:
        raise HTTPException(
            status_code=403,
            detail=(
                "This Label Studio resource is not available in "
                "contributor task mode."
            )
        )

    query_string = request.url.query

    endpoint = clean_path

    if query_string:
        endpoint += "?" + query_string

    body = await request.body()

    forwarded_headers = {}

    for header_name in (
        "content-type",
        "accept",
        "accept-language",
        "x-requested-with",
        "x-csrftoken",
    ):
        value = request.headers.get(
            header_name
        )

        if value:
            forwarded_headers[
                header_name
            ] = value

    response = label_studio_request(
        request.method,
        endpoint,
        data=body if body else None,
        headers=forwarded_headers
    )

    if (
        request.method == "POST"
        and clean_path.rstrip("/")
        == f"/api/tasks/{task_id}/annotations"
        and response.status_code in (200, 201)
    ):
        mark_native_annotation_submitted(
            request,
            project_id,
            task_id,
        )

    if (
        request.method in ("PUT", "PATCH")
        and response.status_code in (200, 201)
        and (
            re.match(
                r"^/api/annotations/\d+/?$",
                clean_path,
            )
            or re.match(
                rf"^/api/tasks/{int(task_id)}/annotations/\d+/?$",
                clean_path,
            )
        )
    ):
        mark_native_revision_updated(
            request,
            project_id,
            task_id,
        )

    response_headers = {}

    for header_name in (
        "content-type",
        "cache-control",
        "etag",
        "last-modified",
        "content-disposition",
    ):
        value = response.headers.get(
            header_name
        )

        if value:
            response_headers[
                header_name
            ] = value

    content = response.content

    response_content_type = (
        response.headers.get(
            "content-type",
            ""
        )
    )

    if (
        "application/json"
        in response_content_type.lower()
    ):
        try:
            payload = response.json()

            if (
                clean_path == "/api/tasks"
                and request.query_params.get("project")
                and request.query_params.get("project").isdigit()
                and int(
                    request.query_params.get("project")
                ) == project_id
            ):
                if isinstance(
                    payload,
                    list
                ):
                    payload = [
                        item
                        for item in payload
                        if isinstance(
                            item,
                            dict
                        )
                        and str(
                            item.get("id")
                        ) == str(task_id)
                    ]

                elif isinstance(
                    payload,
                    dict
                ):
                    if isinstance(
                        payload.get("tasks"),
                        list
                    ):
                        payload["tasks"] = [
                            item
                            for item in payload["tasks"]
                            if isinstance(
                                item,
                                dict
                            )
                            and str(
                                item.get("id")
                            ) == str(task_id)
                        ]

                        if "total" in payload:
                            payload["total"] = len(
                                payload["tasks"]
                            )

                    if isinstance(
                        payload.get("results"),
                        list
                    ):
                        payload["results"] = [
                            item
                            for item in payload["results"]
                            if isinstance(
                                item,
                                dict
                            )
                            and str(
                                item.get("id")
                            ) == str(task_id)
                        ]

                        if "count" in payload:
                            payload["count"] = len(
                                payload["results"]
                            )

                    if (
                        not payload.get("tasks")
                        and not payload.get("results")
                    ):
                        assigned_response = (
                            label_studio_request(
                                "GET",
                                f"/api/tasks/{task_id}",
                                params={
                                    "project":
                                        project_id
                                }
                            )
                        )

                        if (
                            assigned_response.status_code
                            == 200
                        ):
                            try:
                                assigned_payload = (
                                    assigned_response.json()
                                )
                            except ValueError:
                                assigned_payload = None

                            if isinstance(
                                assigned_payload,
                                dict
                            ):
                                if "tasks" in payload:
                                    payload["tasks"] = [
                                        assigned_payload
                                    ]

                                    if "total" in payload:
                                        payload["total"] = 1

                                elif "results" in payload:
                                    payload["results"] = [
                                        assigned_payload
                                    ]

                                    if "count" in payload:
                                        payload["count"] = 1

            payload = rewrite_label_studio_media_urls(
                payload
            )

            content = json.dumps(
                payload
            ).encode("utf-8")

            response_headers[
                "Content-Type"
            ] = "application/json"

        except ValueError:
            pass

    return Response(
        content=content,
        status_code=response.status_code,
        headers=response_headers
    )

