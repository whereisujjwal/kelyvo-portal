import hashlib
import hmac
import json
import os
import re
import time
import threading
from datetime import datetime, timedelta, timezone
from base64 import urlsafe_b64decode, urlsafe_b64encode
from urllib.parse import quote, unquote
from typing import Optional

import requests
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from sqlalchemy import Table, Column, String, Text, Integer, Float, Boolean, inspect, text
from sqlalchemy.orm import Session, joinedload

from database import SessionLocal, engine
import models
from storage_service import storage_provider


load_dotenv()
models.Base.metadata.create_all(bind=engine)


# ============================================================
# WORKFORCE PROFILE DETAIL STORE
# ============================================================
# The current enterprise models already contain ContributorProfile, skills and
# languages. This small supplemental table stores the remaining workforce
# attributes that are intentionally not part of authentication or billing.
# Keeping it here avoids destructive schema rewrites while the product is still
# running on the existing SQLite database.

workforce_profile_details = Table(
    "workforce_profile_details",
    models.Base.metadata,
    Column("user_id", Integer, primary_key=True),
    Column("workforce_type", String, nullable=True),
    Column("dialects", Text, nullable=True),
    Column("domain_expertise", Text, nullable=True),
    Column("experience_summary", Text, nullable=True),
    Column("availability_hours_per_week", Float, nullable=True),
    Column("availability_status", String, nullable=False, default="unspecified"),
    Column("preferred_shift", String, nullable=True),
    Column("worker_tier", String, nullable=False, default="general"),
    Column("qualification_status", String, nullable=False, default="pending"),
    Column("qualification_score", Float, nullable=True),
    Column("qualification_notes", Text, nullable=True),
    Column("project_eligibility", Text, nullable=True),
    Column("test_scores", Text, nullable=True),
    Column("payment_method", String, nullable=True),
    Column("upi_id", String, nullable=True),
    Column("bank_account_name", String, nullable=True),
    Column("bank_account_number", String, nullable=True),
    Column("bank_ifsc", String, nullable=True),
    Column("bank_name", String, nullable=True),
    Column("bank_branch", String, nullable=True),
    Column("bank_account_type", String, nullable=True),
    Column("profile_locked", Boolean, nullable=False, default=False),
    Column("payment_locked", Boolean, nullable=False, default=False),
    Column("profile_locked_at", String, nullable=True),
    Column("payment_locked_at", String, nullable=True),
    Column("profile_unlock_reason", Text, nullable=True),
    Column("payment_unlock_reason", Text, nullable=True),
    Column("updated_at", String, nullable=True),
)

workforce_profile_versions = Table(
    "workforce_profile_versions",
    models.Base.metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, nullable=False),
    Column("version_type", String, nullable=False),
    Column("version_number", Integer, nullable=False),
    Column("snapshot", Text, nullable=False),
    Column("changed_by_user_id", Integer, nullable=True),
    Column("reason", Text, nullable=True),
    Column("created_at", String, nullable=False),
)

models.Base.metadata.create_all(bind=engine)


# ============================================================
# CONTRIBUTOR WORK PREFERENCES
# ============================================================
# Preferences are onboarding/matching signals only. They are deliberately
# separate from the locked workforce profile and do not imply qualification.

contributor_work_interest_catalog = Table(
    "contributor_work_interest_catalog",
    models.Base.metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("slug", String, nullable=False, unique=True),
    Column("name", String, nullable=False),
    Column("description", Text, nullable=True),
    Column("status", String, nullable=False, default="active"),
    Column("sort_order", Integer, nullable=False, default=0),
    Column("created_at", String, nullable=False),
)

contributor_work_interests = Table(
    "contributor_work_interests",
    models.Base.metadata,
    Column("user_id", Integer, primary_key=True),
    Column("interest_id", Integer, primary_key=True),
    Column("created_at", String, nullable=False),
)

contributor_interest_languages = Table(
    "contributor_interest_languages",
    models.Base.metadata,
    Column("user_id", Integer, primary_key=True),
    Column("language_code", String, primary_key=True),
    Column("language_name", String, nullable=False),
    Column("created_at", String, nullable=False),
)

models.Base.metadata.create_all(bind=engine)

CONTRIBUTOR_WORK_INTEREST_CATALOG = [
    ("image", "Image", "Image annotation, classification, segmentation and visual data work."),
    ("video", "Video", "Video annotation, tracking, events and temporal labeling."),
    ("audio_speech", "Audio / Speech", "Speech and audio data work, review and annotation."),
    ("transcription", "Transcription", "Speech-to-text, transcription and text normalization."),
    ("translation_localization", "Translation / Localization", "Translation, localization and language adaptation work."),
    ("llm_ai_evaluation", "LLM / AI Evaluation", "Human evaluation, ranking, safety and AI response assessment."),
    ("text_labeling", "Text / Data Labeling", "Text classification, tagging and other structured labeling."),
    ("data_collection", "Data Collection", "Human data collection and field/data contribution work."),
    ("other", "Other", "Other AI-data or human-in-the-loop work."),
]

CONTRIBUTOR_LANGUAGE_CATALOG = [
    ("hi", "Hindi"), ("en", "English"), ("hinglish", "Hinglish"),
    ("bn", "Bengali"), ("te", "Telugu"), ("mr", "Marathi"), ("ta", "Tamil"),
    ("gu", "Gujarati"), ("ur", "Urdu"), ("kn", "Kannada"), ("ml", "Malayalam"),
    ("or", "Odia"), ("pa", "Punjabi"), ("as", "Assamese"), ("mai", "Maithili"),
    ("ne", "Nepali"), ("kok", "Konkani"), ("ks", "Kashmiri"), ("sd", "Sindhi"),
    ("mni", "Manipuri"), ("doi", "Dogri"), ("sat", "Santali"),
    ("bho", "Bhojpuri"), ("raj", "Rajasthani"), ("other", "Other / Not listed"),
]


def _ensure_contributor_preferences_schema():
    """Seed the preference catalog without changing existing user records."""
    db = SessionLocal()
    try:
        for sort_order, (slug, name, description) in enumerate(CONTRIBUTOR_WORK_INTEREST_CATALOG, start=1):
            row = db.execute(
                contributor_work_interest_catalog.select().where(
                    contributor_work_interest_catalog.c.slug == slug
                )
            ).mappings().first()
            values = {
                "name": name,
                "description": description,
                "status": "active",
                "sort_order": sort_order,
            }
            if row is None:
                db.execute(
                    contributor_work_interest_catalog.insert().values(
                        slug=slug,
                        **values,
                        created_at=datetime.now(timezone.utc).isoformat(),
                    )
                )
            else:
                db.execute(
                    contributor_work_interest_catalog.update()
                    .where(contributor_work_interest_catalog.c.slug == slug)
                    .values(**values)
                )
        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


# ============================================================
# WORKFORCE PROFILE DETAIL STORE

def _ensure_workforce_profile_detail_schema():
    """Apply only additive, non-destructive schema updates for workforce details."""
    try:
        existing_columns = {
            column.get("name")
            for column in inspect(engine).get_columns("workforce_profile_details")
        }
        additive_columns = {
            "workforce_type": "VARCHAR",
            "payment_method": "VARCHAR",
            "upi_id": "VARCHAR",
            "bank_account_name": "VARCHAR",
            "bank_account_number": "VARCHAR",
            "bank_ifsc": "VARCHAR",
            "bank_name": "VARCHAR",
            "bank_branch": "VARCHAR",
            "bank_account_type": "VARCHAR",
            "profile_locked": "BOOLEAN DEFAULT 0",
            "payment_locked": "BOOLEAN DEFAULT 0",
            "profile_locked_at": "VARCHAR",
            "payment_locked_at": "VARCHAR",
            "profile_unlock_reason": "TEXT",
            "payment_unlock_reason": "TEXT",
        }
        missing_columns = [
            (name, definition)
            for name, definition in additive_columns.items()
            if name not in existing_columns
        ]
        if missing_columns:
            with engine.begin() as connection:
                for name, definition in missing_columns:
                    connection.execute(
                        text(
                            f"ALTER TABLE workforce_profile_details "
                            f"ADD COLUMN {name} {definition}"
                        )
                    )
    except Exception:
        # Keep startup compatible with the existing database. The endpoint will
        # still fail loudly if the schema is genuinely unusable.
        pass


_ensure_workforce_profile_detail_schema()


# ============================================================
# DATA COLLECTOR PROFILE SCHEMA MIGRATION
# ============================================================

def _ensure_data_collector_profile_schema():
    """Apply only additive, non-destructive schema updates for data collectors."""
    try:
        existing_columns = {
            column.get("name")
            for column in inspect(engine).get_columns("data_collector_profiles")
        }
        additive_columns = {
            "pin_code": "VARCHAR",
            "area_type": "VARCHAR",
            "collection_environment": "TEXT",
            "payment_method": "VARCHAR",
            "upi_id": "VARCHAR",
            "bank_account_name": "VARCHAR",
            "bank_account_number": "VARCHAR",
            "bank_ifsc": "VARCHAR",
            "bank_name": "VARCHAR",
            "bank_branch": "VARCHAR",
            "bank_account_type": "VARCHAR",
            "consent_accepted": "BOOLEAN DEFAULT 0",
            "consent_version": "VARCHAR",
            "consent_accepted_at": "DATETIME",
            "profile_locked": "BOOLEAN DEFAULT 0",
            "payment_locked": "BOOLEAN DEFAULT 0",
            "profile_locked_at": "DATETIME",
            "payment_locked_at": "DATETIME",
            "profile_unlock_reason": "TEXT",
            "payment_unlock_reason": "TEXT",
            "submitted_at": "DATETIME",
            "reviewed_at": "DATETIME",
            "reviewed_by_user_id": "INTEGER",
            "rejection_reason": "TEXT",
        }
        missing_columns = [
            (name, definition)
            for name, definition in additive_columns.items()
            if name not in existing_columns
        ]
        if missing_columns:
            with engine.begin() as connection:
                for name, definition in missing_columns:
                    connection.execute(
                        text(
                            f"ALTER TABLE data_collector_profiles "
                            f"ADD COLUMN {name} {definition}"
                        )
                    )
    except Exception:
        # Keep startup compatible with the existing database.
        pass


_ensure_data_collector_profile_schema()


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

    # The database is the source of truth for the user's current role.
    # This prevents a stale session cookie from blocking access after an Admin
    # changes a user's role while that user remains signed in.
    current_role = str(session.get("role") or "").strip().lower()
    db = SessionLocal()
    try:
        user = (
            db.query(models.User)
            .filter(models.User.email == normalize_email(session.get("email", "")))
            .first()
        )
        if user is None:
            raise HTTPException(
                status_code=401,
                detail="The KELYVO account for this session no longer exists."
            )

        current_role = str(user.role or "").strip().lower()
        if current_role not in {"contributor", "data_collector", "qa", "admin"}:
            raise HTTPException(
                status_code=500,
                detail="This account has an invalid portal role configuration."
            )

        membership = _get_user_membership(db, int(user.id)) if "_get_user_membership" in globals() else None
        if membership is not None and str(membership.status or "active").strip().lower() == "suspended":
            raise HTTPException(
                status_code=403,
                detail="This KELYVO account is currently suspended. Contact an administrator."
            )
    finally:
        db.close()

    if allowed_roles and current_role not in allowed_roles:
        raise HTTPException(
            status_code=403,
            detail="This portal area is not available for this role."
        )

    session = dict(session)
    session["role"] = current_role
    return session


def require_contributor_session(request: Request):
    return require_portal_session(request, {"contributor"})


def require_data_collector_session(request: Request):
    return require_portal_session(request, {"data_collector"})


def require_admin_session(request: Request):
    return require_portal_session(request, {"admin"})


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
            int(project_value),
            user_id=_get_authenticated_user_id(request),
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


# ============================================================
# ENTERPRISE PROJECT REGISTRY BRIDGE
# ============================================================
#
# KELYVO's enterprise database is being introduced without breaking the
# currently working Label Studio workflow.  For this first migration step,
# the database becomes the registry for organizations/workspaces/projects,
# while task reservations are now persisted in TaskAssignment.  Later steps can
# safely expand the persistent task lifecycle without breaking the working
# Label Studio execution flow.
#
# Label Studio remains the execution engine for now.  Its project IDs are
# stored on the KELYVO Project records instead of being the only place where
# KELYVO knows how its projects are structured.


def _enterprise_role_for_user(user_role: str) -> str:
    role = (user_role or "contributor").strip().lower()
    if role == "admin":
        return "admin"
    if role == "qa":
        return "qa"
    if role == "data_collector":
        return "data_collector"
    return "contributor"


def _ensure_enterprise_project_registry():
    """Create/synchronize KELYVO's initial enterprise project registry.

    This is intentionally idempotent and backward-compatible.  It never
    deletes or changes existing contributor/submission records and it does not
    depend on Label Studio being online.
    """
    db = SessionLocal()
    try:
        organization = (
            db.query(models.Organization)
            .filter(models.Organization.slug == "kelyvo")
            .first()
        )

        if organization is None:
            organization = models.Organization(
                name="KELYVO",
                slug="kelyvo",
                organization_type="internal",
                status="active",
            )
            db.add(organization)
            db.flush()

        workspace = (
            db.query(models.Workspace)
            .filter(
                models.Workspace.organization_id == organization.id,
                models.Workspace.slug == "operations",
            )
            .first()
        )

        if workspace is None:
            workspace = models.Workspace(
                organization_id=organization.id,
                name="KELYVO Operations",
                slug="operations",
                description=(
                    "Core KELYVO data operations workspace for annotation, "
                    "QA, workforce and project execution."
                ),
                status="active",
            )
            db.add(workspace)
            db.flush()

        project_names = {
            "image": "Image Data Operations",
            "audio": "Audio Data Operations",
            "video": "Video Data Operations",
            "text": "Text Data Operations",
        }

        project_descriptions = {
            "image": "KELYVO image annotation operations.",
            "audio": "KELYVO audio annotation operations.",
            "video": "KELYVO video annotation operations.",
            "text": "KELYVO text annotation operations.",
        }

        for modality, external_project_id in PROJECT_MAPPING.items():
            project_code = f"KELYVO-{modality.upper()}"

            project = (
                db.query(models.Project)
                .filter(models.Project.project_code == project_code)
                .first()
            )

            if project is None:
                project = models.Project(
                    workspace_id=workspace.id,
                    name=project_names.get(
                        modality,
                        f"{modality.title()} Data Operations",
                    ),
                    project_code=project_code,
                    description=project_descriptions.get(modality),
                    modality=modality,
                    status="active",
                    external_engine="label_studio",
                    external_project_id=int(external_project_id),
                )
                db.add(project)
            else:
                project.workspace_id = workspace.id
                project.modality = modality
                project.status = "active"
                project.external_engine = "label_studio"
                project.external_project_id = int(external_project_id)

        # Put all currently registered portal users into the initial internal
        # organization.  Existing memberships are never overwritten.
        users = db.query(models.User).all()
        for user in users:
            membership = (
                db.query(models.OrganizationMember)
                .filter(
                    models.OrganizationMember.organization_id == organization.id,
                    models.OrganizationMember.user_id == user.id,
                )
                .first()
            )

            if membership is None:
                db.add(
                    models.OrganizationMember(
                        organization_id=organization.id,
                        user_id=user.id,
                        role=_enterprise_role_for_user(user.role),
                        status="active",
                    )
                )

        db.commit()
    except Exception:
        db.rollback()
        # The enterprise registry is a compatibility layer at this stage.
        # A registry problem must never prevent the existing portal from
        # starting while the migration is being rolled out.
    finally:
        db.close()


def _get_enterprise_project_for_modality(modality: str):
    """Resolve a KELYVO project from the persistent project registry."""
    modality = (modality or "").strip().lower()
    db = SessionLocal()
    try:
        return (
            db.query(models.Project)
            .filter(
                models.Project.modality == modality,
                models.Project.status == "active",
            )
            .order_by(models.Project.created_at.asc())
            .first()
        )
    except Exception:
        return None
    finally:
        db.close()


def _sync_enterprise_task(
    project_id: int,
    task_id: int,
    task_payload=None,
    status: str = "available",
):
    """Mirror one Label Studio task into KELYVO's canonical task table.

    This is deliberately best-effort during the migration.  The current
    Label Studio workflow remains authoritative for execution until the
    persistent assignment engine is introduced in a later step.
    """
    if task_id is None:
        return None

    db = SessionLocal()
    try:
        project = (
            db.query(models.Project)
            .filter(
                models.Project.external_engine == "label_studio",
                models.Project.external_project_id == int(project_id),
                models.Project.status == "active",
            )
            .first()
        )

        if project is None:
            return None

        task = (
            db.query(models.Task)
            .filter(
                models.Task.project_id == project.id,
                models.Task.external_task_id == int(task_id),
            )
            .first()
        )

        payload = task_payload if isinstance(task_payload, dict) else {}
        title = payload.get("title")
        if title is None:
            title = f"{project.modality} task #{int(task_id)}"

        if task is None:
            task = models.Task(
                project_id=project.id,
                task_number=int(task_id),
                external_task_id=int(task_id),
                external_engine="label_studio",
                external_project_id=int(project_id),
                title=str(title),
                task_type=project.modality,
                status=status,
                priority=0,
                is_locked=False,
            )
            db.add(task)
        else:
            task.task_number = int(task_id)
            task.external_engine = "label_studio"
            task.external_project_id = int(project_id)
            task.title = str(title)
            task.task_type = project.modality
            task.status = status

        db.commit()
        db.refresh(task)
        return task.id
    except Exception:
        db.rollback()
        return None
    finally:
        db.close()


# Seed the enterprise registry once when the backend process loads.  This is
# independent of Label Studio availability and therefore safe during cold
# starts or temporary annotation-engine outages.
_ensure_enterprise_project_registry()


TASK_ASSIGNMENT_TTL = 24 * 60 * 60

KELYVO_BLOCKED_SUBMISSION_STATUSES = {
    "PENDING_QA",
    "PASSED",
    "FAILED",
}


def _extract_task_id_from_submission_title(task_title):
    match = re.search(
        r"(?:task|#)\s*#?\s*(\d+)\s*$",
        str(task_title or ""),
        re.IGNORECASE
    )

    if not match:
        return None

    return int(match.group(1))


def _get_kelyvo_blocked_task_ids(
    db: Session,
    task_type: str,
):
    """Return task IDs that KELYVO must not offer as fresh work."""
    rows = (
        db.query(
            models.TaskSubmission.task_title
        )
        .filter(
            models.TaskSubmission.task_type
            == str(task_type).strip().lower(),
            models.TaskSubmission.status.in_(
                KELYVO_BLOCKED_SUBMISSION_STATUSES
            ),
        )
        .all()
    )

    blocked_ids = set()

    for row in rows:
        task_id = _extract_task_id_from_submission_title(
            row.task_title
        )

        if task_id is not None:
            blocked_ids.add(int(task_id))

    return blocked_ids



def _utc_now():
    return datetime.now(timezone.utc)


_ensure_contributor_preferences_schema()


def _get_authenticated_user_id(request: Request):
    session = read_session_token(request)
    if not session or not session.get("email"):
        return None

    db = SessionLocal()
    try:
        user = (
            db.query(models.User)
            .filter(models.User.email == normalize_email(session.get("email", "")))
            .first()
        )
        return int(user.id) if user else None
    except Exception:
        return None
    finally:
        db.close()


def _get_user_id_for_identity(identity: str):
    if not identity:
        return None

    if identity.startswith("user:"):
        email = normalize_email(identity[5:])
        if not email:
            return None
        db = SessionLocal()
        try:
            user = (
                db.query(models.User)
                .filter(models.User.email == email)
                .first()
            )
            return int(user.id) if user else None
        except Exception:
            return None
        finally:
            db.close()

    return None


def _clear_label_studio_task_work(project_id: int, task_id: int) -> bool:
    """Clear all Label Studio annotation/draft work before an expired task is re-pooled.

    A task is only released after its persisted contributor work has been
    successfully removed. This prevents an expired contributor's annotation
    from leaking to the next contributor.
    """
    try:
        task_response = label_studio_request(
            "GET",
            f"/api/tasks/{int(task_id)}",
            params={
                "project": int(project_id),
                "resolve_uri": "true",
            },
            timeout=8,
        )

        if task_response.status_code == 404:
            # The task is already gone from Label Studio, so there is no work
            # that can leak to another contributor.
            return True

        if task_response.status_code >= 400:
            return False

        try:
            task_payload = task_response.json()
        except ValueError:
            return False

        annotation_ids = set()
        if isinstance(task_payload, dict):
            annotations = task_payload.get("annotations", [])
            if isinstance(annotations, list):
                for annotation in annotations:
                    if not isinstance(annotation, dict):
                        continue
                    annotation_id = annotation.get("id")
                    if annotation_id is not None:
                        annotation_ids.add(int(annotation_id))

        for annotation_id in annotation_ids:
            delete_response = label_studio_request(
                "DELETE",
                f"/api/annotations/{annotation_id}",
                params={"project": int(project_id)},
                timeout=8,
            )
            if delete_response.status_code not in (200, 204, 404):
                return False

        draft_ids = set()
        if isinstance(task_payload, dict):
            drafts = task_payload.get("drafts", [])
            if isinstance(drafts, dict):
                drafts = drafts.get("drafts", drafts.get("results", []))
            if isinstance(drafts, list):
                for draft in drafts:
                    if not isinstance(draft, dict):
                        continue
                    draft_id = draft.get("id")
                    if draft_id is not None:
                        draft_ids.add(int(draft_id))

        draft_response = label_studio_request(
            "GET",
            f"/api/tasks/{int(task_id)}/drafts",
            params={"project": int(project_id)},
            timeout=8,
        )

        if draft_response.status_code not in (200, 404):
            return False

        if draft_response.status_code == 200:
            try:
                draft_payload = draft_response.json()
            except ValueError:
                return False

            if isinstance(draft_payload, dict):
                endpoint_drafts = (
                    draft_payload.get("drafts")
                    or draft_payload.get("results")
                    or draft_payload.get("data")
                    or []
                )
            else:
                endpoint_drafts = draft_payload

            if isinstance(endpoint_drafts, list):
                for draft in endpoint_drafts:
                    if not isinstance(draft, dict):
                        continue
                    draft_id = draft.get("id")
                    if draft_id is not None:
                        draft_ids.add(int(draft_id))

        for draft_id in draft_ids:
            delete_response = label_studio_request(
                "DELETE",
                f"/api/drafts/{draft_id}",
                params={"project": int(project_id)},
                timeout=8,
            )
            if delete_response.status_code not in (200, 204, 404):
                return False

        return True
    except Exception:
        return False


def _cleanup_expired_persistent_task_assignments():
    now = _utc_now()
    db = SessionLocal()
    try:
        expired = (
            db.query(models.TaskAssignment)
            .options(joinedload(models.TaskAssignment.task))
            .filter(
                models.TaskAssignment.status == "reserved",
                models.TaskAssignment.expires_at.isnot(None),
                models.TaskAssignment.expires_at <= now,
            )
            .all()
        )

        if not expired:
            return

        for assignment in expired:
            project_id = None
            task_id = None
            if assignment.task is not None:
                project_id = assignment.task.external_project_id
                task_id = assignment.task.external_task_id

            # Never expose unfinished work to another contributor. If Label
            # Studio cannot be cleared right now, keep the reservation active
            # and retry on the next cleanup pass.
            if (
                project_id is not None
                and task_id is not None
                and not _clear_label_studio_task_work(
                    int(project_id),
                    int(task_id),
                )
            ):
                continue

            assignment.status = "expired"
            assignment.released_at = now
            assignment.expires_at = now
            if assignment.task is not None:
                assignment.task.is_locked = False
                assignment.task.status = "available"

        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


def _get_active_persistent_assignment(
    db: Session,
    user_id: int,
    project_id: int = None,
    task_id: int = None,
):
    if user_id is None:
        return None

    query = (
        db.query(models.TaskAssignment)
        .options(joinedload(models.TaskAssignment.task))
        .join(models.Task, models.Task.id == models.TaskAssignment.task_id)
        .filter(
            models.TaskAssignment.user_id == int(user_id),
            models.TaskAssignment.status == "reserved",
        )
    )

    if project_id is not None:
        query = query.filter(
            models.Task.external_project_id == int(project_id)
        )

    if task_id is not None:
        query = query.filter(
            models.Task.external_task_id == int(task_id)
        )

    return (
        query
        .order_by(models.TaskAssignment.assigned_at.desc())
        .first()
    )



def cleanup_task_assignments():
    _cleanup_expired_persistent_task_assignments()


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def get_browser_identity(request: Request) -> str:
    """Return a contributor-specific fallback identity for browser-scoped flows.

    When an authenticated KELYVO session exists, use the authenticated email as
    the identity seed. This prevents two contributor accounts using the same
    browser/device/IP from sharing the same browser assignment namespace.
    Anonymous requests continue to use the IP + user-agent fallback.
    """
    session = read_session_token(request)
    session_email = normalize_email(session.get("email", "")) if session else ""

    if session_email:
        raw_identity = "user:" + session_email
    else:
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
    """Keep the browser view synchronized with the authenticated user's persistent assignment."""
    user_id = _get_authenticated_user_id(request)
    if user_id is None:
        return None

    return reserve_task_for_contributor(
        "user:" + normalize_email(read_session_token(request).get("email", "")),
        project_id,
        int(task_id),
        user_id=user_id,
    )


def reserve_task_for_contributor(
    identity: str,
    project_id: int,
    task_id: int,
    user_id: int = None,
):
    cleanup_task_assignments()

    if not identity:
        raise HTTPException(
            status_code=400,
            detail="Unable to identify the contributor."
        )

    if user_id is None:
        user_id = _get_user_id_for_identity(identity)

    if user_id is None:
        raise HTTPException(
            status_code=401,
            detail="Unable to resolve the authenticated contributor."
        )

    db = SessionLocal()
    try:
        task = (
            db.query(models.Task)
            .filter(
                models.Task.external_engine == "label_studio",
                models.Task.external_project_id == int(project_id),
                models.Task.external_task_id == int(task_id),
            )
            .first()
        )

        if task is None:
            task_id_uuid = _sync_enterprise_task(
                project_id=int(project_id),
                task_id=int(task_id),
                status="reserved",
            )
            if task_id_uuid is not None:
                db.expire_all()
                task = db.query(models.Task).filter(
                    models.Task.id == task_id_uuid
                ).first()

        if task is None:
            raise HTTPException(
                status_code=404,
                detail="The task could not be registered in KELYVO."
            )

        active_assignment = _get_active_persistent_assignment(
            db,
            int(user_id),
            project_id=int(project_id),
        )

        if active_assignment is not None:
            active_task_external_id = active_assignment.task.external_task_id
            if int(active_task_external_id or 0) != int(task_id):
                return int(active_task_external_id)

            active_assignment.status = "reserved"
            active_assignment.released_at = None
            active_assignment.completed_at = None
            active_assignment.task.is_locked = True
            active_assignment.task.status = "reserved"
            db.commit()
            return int(task_id)

        task_assignment = (
            db.query(models.TaskAssignment)
            .options(joinedload(models.TaskAssignment.task))
            .join(models.Task, models.Task.id == models.TaskAssignment.task_id)
            .filter(
                models.TaskAssignment.task_id == task.id,
                models.TaskAssignment.status == "reserved",
            )
            .first()
        )

        if task_assignment is not None:
            if int(task_assignment.user_id) == int(user_id):
                task_assignment.task.is_locked = True
                task_assignment.task.status = "reserved"
                db.commit()
                return int(task_id)
            raise HTTPException(
                status_code=409,
                detail="This task is already reserved by another contributor."
            )

        assignment = models.TaskAssignment(
            task_id=task.id,
            user_id=int(user_id),
            status="reserved",
            assigned_at=_utc_now(),
            expires_at=_utc_now() + timedelta(seconds=TASK_ASSIGNMENT_TTL),
        )
        db.add(assignment)
        task.is_locked = True
        task.status = "reserved"
        db.commit()
        return int(task_id)
    except HTTPException:
        db.rollback()
        raise
    except Exception:
        db.rollback()
        return None
    finally:
        db.close()


def get_reserved_task(
    identity: str,
    project_id: int,
    user_id: int = None,
):
    cleanup_task_assignments()

    if user_id is None:
        user_id = _get_user_id_for_identity(identity)

    if user_id is None:
        return None

    db = SessionLocal()
    try:
        assignment = _get_active_persistent_assignment(
            db,
            int(user_id),
            project_id=int(project_id),
        )
        if assignment is None:
            return None

        assignment.task.is_locked = True
        assignment.task.status = "reserved"
        db.commit()
        return int(assignment.task.external_task_id)
    except Exception:
        db.rollback()
        return None
    finally:
        db.close()


def release_reserved_task(
    identity: str,
    project_id: int,
    task_id: int = None,
    user_id: int = None,
    final_task_status: str = "available",
):
    cleanup_task_assignments()

    if user_id is None:
        user_id = _get_user_id_for_identity(identity)

    if user_id is None:
        return

    db = SessionLocal()
    try:
        assignment = _get_active_persistent_assignment(
            db,
            int(user_id),
            project_id=int(project_id),
            task_id=int(task_id) if task_id is not None else None,
        )
        if assignment is None:
            return

        now = _utc_now()
        if final_task_status == "submitted":
            assignment.status = "completed"
            assignment.completed_at = now
        else:
            assignment.status = "released"
            assignment.released_at = now

        assignment.expires_at = now
        assignment.task.is_locked = False
        assignment.task.status = final_task_status
        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


def release_browser_assignment_for_request(
    request: Request,
    final_task_status: str = "available",
):
    user_id = _get_authenticated_user_id(request)
    if user_id is None:
        return

    session = read_session_token(request)
    email = normalize_email(session.get("email", "")) if session else ""
    if not email:
        return

    db = SessionLocal()
    try:
        assignments = (
            db.query(models.TaskAssignment)
            .options(joinedload(models.TaskAssignment.task))
            .join(models.Task, models.Task.id == models.TaskAssignment.task_id)
            .filter(
                models.TaskAssignment.user_id == int(user_id),
                models.TaskAssignment.status == "reserved",
            )
            .all()
        )

        now = _utc_now()
        for assignment in assignments:
            if final_task_status == "submitted":
                assignment.status = "completed"
                assignment.completed_at = now
            else:
                assignment.status = "released"
                assignment.released_at = now
            assignment.expires_at = now
            assignment.task.is_locked = False
            assignment.task.status = final_task_status

        if assignments:
            db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


def _is_task_reserved_by_other_contributor(
    project_id: int,
    task_id: int,
    contributor_identity: str,
    contributor_user_id=None,
) -> bool:
    """Prevent the same task from being handed to two active contributors."""
    cleanup_task_assignments()

    if contributor_user_id is None:
        contributor_user_id = _get_user_id_for_identity(contributor_identity)

    db = SessionLocal()
    try:
        assignment = (
            db.query(models.TaskAssignment)
            .options(joinedload(models.TaskAssignment.task))
            .join(models.Task, models.Task.id == models.TaskAssignment.task_id)
            .filter(
                models.TaskAssignment.status == "reserved",
                models.Task.external_project_id == int(project_id),
                models.Task.external_task_id == int(task_id),
            )
            .first()
        )
        if assignment is None:
            return False
        if contributor_user_id is not None and int(assignment.user_id) == int(contributor_user_id):
            return False
        return True
    except Exception:
        # If the reservation state cannot be read, fail closed. Returning False
        # here could allow two contributors to receive the same task during a
        # transient database error.
        return True
    finally:
        db.close()


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



def get_recent_kelyvo_completed_submission(
    request: Request,
    project_id: int = None,
    max_age_seconds: int = 15 * 60,
):
    """
    Compatibility bridge for Label Studio requests that can arrive immediately
    after KELYVO has completed and released the contributor assignment.
    """
    session = read_session_token(request)
    if not session or not session.get("email"):
        return None

    user_id = _get_authenticated_user_id(request)
    if user_id is None:
        return None

    db = SessionLocal()
    try:
        rows = (
            db.query(models.TaskSubmission)
            .filter(
                models.TaskSubmission.user_id == int(user_id),
                models.TaskSubmission.status == "PENDING_QA",
            )
            .order_by(models.TaskSubmission.id.desc())
            .limit(10)
            .all()
        )

        now = _utc_now()

        for submission in rows:
            task_type = str(
                getattr(submission, "task_type", "") or ""
            ).strip().lower()
            mapped_project_id = PROJECT_MAPPING.get(task_type)

            if project_id is not None and mapped_project_id != int(project_id):
                continue

            task_id = _extract_kelyvo_task_id(
                getattr(submission, "task_title", "")
            )
            if task_id is None:
                continue

            submitted_at = (
                getattr(submission, "submitted_at", None)
                or getattr(submission, "created_at", None)
            )

            if submitted_at is not None:
                try:
                    if submitted_at.tzinfo is None:
                        submitted_at = submitted_at.replace(tzinfo=timezone.utc)
                    age = (now - submitted_at).total_seconds()
                    if age < 0 or age > int(max_age_seconds):
                        continue
                except (AttributeError, TypeError, ValueError):
                    pass

            return {
                "task_id": int(task_id),
                "project_id": int(mapped_project_id or 0),
                "task_type": task_type,
                "submission_id": int(submission.id),
            }

        return None
    except Exception:
        return None
    finally:
        db.close()


def mark_kelyvo_submission_completed(
    request: Request,
    project_id: int,
    task_id: int,
):
    """
    Persist the authoritative KELYVO Submit-to-QA transition into the
    enterprise task/annotation/submission tables.

    TaskSubmission remains the current QA-queue compatibility record.
    Enterprise synchronization is deliberately best-effort so a problem in
    the migration layer can never turn a successful contributor submission
    into a 500 response.
    """
    if not project_id or not task_id:
        return None

    user_id = _get_authenticated_user_id(request)
    if user_id is None:
        return None

    task_payload = {}

    try:
        response = label_studio_request(
            "GET",
            f"/api/tasks/{int(task_id)}",
            params={
                "project": int(project_id),
                "resolve_uri": "true",
            },
        )

        if response.status_code == 200:
            try:
                task_payload = response.json()
            except ValueError:
                task_payload = {}
    except Exception:
        task_payload = {}

    if not isinstance(task_payload, dict):
        task_payload = {}

    task_type = None

    for modality, mapped_project_id in PROJECT_MAPPING.items():
        if int(mapped_project_id) == int(project_id):
            task_type = modality
            break

    task_title = task_payload.get("title")

    if not task_title:
        task_title = (
            f"{str(task_type or 'task').upper()} "
            f"Task #{int(task_id)}"
        )

    annotations = task_payload.get("annotations", [])

    if not isinstance(annotations, list):
        annotations = []

    usable_annotations = [
        item
        for item in annotations
        if (
            isinstance(item, dict)
            and item.get("id") is not None
            and not bool(item.get("was_cancelled"))
            and isinstance(item.get("result"), list)
            and item.get("result")
        )
    ]

    usable_annotations.sort(
        key=lambda item: str(
            item.get("updated_at")
            or item.get("created_at")
            or item.get("submitted_at")
            or ""
        )
    )

    latest_annotation = (
        usable_annotations[-1]
        if usable_annotations
        else None
    )

    db = SessionLocal()

    try:
        canonical_task = (
            db.query(models.Task)
            .filter(
                models.Task.external_engine == "label_studio",
                models.Task.external_project_id == int(project_id),
                models.Task.external_task_id == int(task_id),
            )
            .first()
        )

        if canonical_task is None:
            canonical_task_id = _sync_enterprise_task(
                project_id=int(project_id),
                task_id=int(task_id),
                task_payload=task_payload,
                status="submitted",
            )

            if canonical_task_id is not None:
                canonical_task = (
                    db.query(models.Task)
                    .filter(models.Task.id == canonical_task_id)
                    .first()
                )

        if canonical_task is None:
            db.rollback()
            return None

        canonical_task.title = str(task_title)
        canonical_task.task_type = str(
            task_type
            or canonical_task.task_type
            or "unknown"
        )
        canonical_task.status = "submitted"
        canonical_task.is_locked = False

        annotation_record = None

        if (
            latest_annotation is not None
            and hasattr(models, "Annotation")
        ):
            external_annotation_id = int(
                latest_annotation["id"]
            )

            annotation_record = (
                db.query(models.Annotation)
                .filter(
                    models.Annotation.task_id == canonical_task.id,
                    models.Annotation.annotator_id == int(user_id),
                    models.Annotation.external_annotation_id
                    == external_annotation_id,
                )
                .first()
            )

            annotation_data = json.dumps(
                latest_annotation,
                ensure_ascii=False,
                default=str,
            )

            if annotation_record is None:
                annotation_record = models.Annotation(
                    task_id=canonical_task.id,
                    annotator_id=int(user_id),
                    external_annotation_id=external_annotation_id,
                    version=1,
                    status="submitted",
                    annotation_data=annotation_data,
                    submitted_at=_utc_now(),
                )

                db.add(annotation_record)
                db.flush()
            else:
                annotation_record.status = "submitted"
                annotation_record.annotation_data = annotation_data
                annotation_record.submitted_at = _utc_now()

                if hasattr(annotation_record, "updated_at"):
                    annotation_record.updated_at = _utc_now()

        if hasattr(models, "Submission"):
            enterprise_submission = (
                db.query(models.Submission)
                .filter(
                    models.Submission.task_id == canonical_task.id,
                    models.Submission.contributor_id == int(user_id),
                )
                .order_by(
                    models.Submission.submitted_at.desc()
                )
                .first()
            )

            if enterprise_submission is None:
                enterprise_submission = models.Submission(
                    task_id=canonical_task.id,
                    annotation_id=(
                        annotation_record.id
                        if annotation_record is not None
                        else None
                    ),
                    contributor_id=int(user_id),
                    status="pending_qa",
                    attempt_number=1,
                    submitted_at=_utc_now(),
                    updated_at=_utc_now(),
                )
                db.add(enterprise_submission)
            else:
                current_status = str(
                    getattr(
                        enterprise_submission,
                        "status",
                        "",
                    )
                    or ""
                ).strip().lower()

                if current_status in {
                    "failed",
                    "returned",
                    "revision",
                    "rejected",
                }:
                    enterprise_submission.attempt_number = (
                        int(
                            getattr(
                                enterprise_submission,
                                "attempt_number",
                                1,
                            )
                            or 1
                        )
                        + 1
                    )

                enterprise_submission.annotation_id = (
                    annotation_record.id
                    if annotation_record is not None
                    else getattr(
                        enterprise_submission,
                        "annotation_id",
                        None,
                    )
                )
                enterprise_submission.status = "pending_qa"
                enterprise_submission.submitted_at = _utc_now()
                enterprise_submission.updated_at = _utc_now()

        db.commit()

        return {
            "task_id": int(task_id),
            "project_id": int(project_id),
            "enterprise_task_id": canonical_task.id,
            "annotation_id": (
                annotation_record.id
                if annotation_record is not None
                else None
            ),
        }

    except Exception:
        db.rollback()
        return None
    finally:
        db.close()


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
            contributor_user_id=contributor_user_id,
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

        # A returned/FAILED task is intentionally eligible for rework even if
        # Label Studio still contains the contributor's previous annotation.
        # KELYVO's FAILED submission state is what puts it into the requeue pool.
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
            html_content = f.read()

        response = HTMLResponse(
            content=html_content,
            status_code=200
        )
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    return "Template not found"


@app.post("/register")
def register_user(
    email: str,
    password: str,
    role: str = "contributor",
    registration_code: str = "",
    db: Session = Depends(get_db)
):
    email = normalize_email(
        email
    )

    requested_role = (role or "contributor").strip().lower()

    if requested_role not in {
        "contributor",
        "data_collector",
        "qa",
        "admin"
    }:
        raise HTTPException(
            status_code=400,
            detail="Invalid registration role."
        )

    if requested_role == "admin":
        configured_admin_code = os.getenv(
            "KELYVO_ADMIN_REGISTRATION_CODE",
            ""
        ).strip()

        supplied_admin_code = (registration_code or "").strip()

        if (
            not configured_admin_code
            or not supplied_admin_code
            or not hmac.compare_digest(
                supplied_admin_code,
                configured_admin_code
            )
        ):
            raise HTTPException(
                status_code=403,
                detail=(
                    "A valid Admin Control registration code "
                    "is required."
                )
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

    assigned_role = requested_role

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
    db.flush()

    if requested_role == "data_collector" and hasattr(models, "DataCollectorProfile"):
        db.add(
            models.DataCollectorProfile(
                user_id=int(new_user.id),
                display_name="",
                country="India",
                onboarding_status="pending",
            )
        )

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

    membership = _get_user_membership(db, int(user.id))
    if membership is not None and str(membership.status or "active").strip().lower() == "suspended":
        raise HTTPException(
            status_code=403,
            detail="This KELYVO account is currently suspended. Contact an administrator."
        )

    # Login is intentionally role-agnostic.
    # The persisted database role is the only source of truth.
    actual_role = str(
        user.role or "contributor"
    ).strip().lower()

    if actual_role not in {
        "contributor",
        "data_collector",
        "qa",
        "admin"
    }:
        raise HTTPException(
            status_code=500,
            detail="This account has an invalid portal role configuration."
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


def _get_kelyvo_blocked_fresh_task_ids(
    user_id: int,
    task_type: str,
):
    """Return task IDs that KELYVO must not offer as fresh work.

    This exclusion is GLOBAL across contributors. Once a task has a KELYVO
    TaskSubmission in PENDING_QA, FAILED, or PASSED state, that task is no
    longer fresh work for anybody. FAILED tasks are handled only by the
    dedicated revision path for the original contributor; PENDING_QA tasks
    belong to the QA queue; PASSED tasks are permanently complete.

    The user_id argument is retained for call-site compatibility, but it is
    intentionally NOT used for the fresh-work exclusion. Using it here would
    allow contributor B to receive contributor A's already-submitted task.

    Label Studio state is deliberately ignored here. KELYVO is the canonical
    workflow source of truth, so deleting an annotation/draft in Label Studio
    must never make a submitted KELYVO task fresh again.
    """
    task_type = str(task_type or "").strip().lower()
    if not task_type:
        return set()

    db = SessionLocal()
    try:
        rows = (
            db.query(models.TaskSubmission)
            .filter(
                models.TaskSubmission.task_type == task_type,
                models.TaskSubmission.status.in_([
                    "PENDING_QA",
                    "FAILED",
                    "PASSED",
                ]),
            )
            .all()
        )

        blocked = set()
        for row in rows:
            task_id = _extract_task_id_from_submission_title(row.task_title)
            if task_id is not None:
                blocked.add(int(task_id))

        return blocked
    except Exception:
        db.rollback()
        return blocked if "blocked" in locals() else set()
    finally:
        db.close()


def _fetch_label_studio_project_tasks(project_id: int):
    """Fetch the complete Label Studio task pool for a project.

    Label Studio's task-list response is paginated and its serializer differs
    between Community Edition versions.  We therefore use the documented
    /api/tasks endpoint first, explicitly request unfiltered tasks, and fall
    back to the project task endpoint.  A task being annotated is NOT used by
    this function to decide KELYVO availability; KELYVO workflow state does
    that separately.
    """
    project_id = int(project_id)
    collected = []
    seen_ids = set()
    last_status = 0

    # The documented Label Studio endpoint is /api/tasks/ with project as a
    # query parameter.  Try both slash variants because older CE releases
    # differ in their redirect handling.
    for endpoint in ("/api/tasks/", "/api/tasks"):
        for page in range(1, 11):
            response = label_studio_request(
                "GET",
                endpoint,
                params={
                    "project": project_id,
                    "page_size": 100,
                    "page": page,
                    "only_annotated": "false",
                    "fields": "all",
                },
            )
            last_status = response.status_code

            if response.status_code >= 400:
                break

            try:
                payload = response.json()
            except ValueError:
                break

            if isinstance(payload, list):
                page_tasks = payload
            elif isinstance(payload, dict):
                page_tasks = (
                    payload.get("tasks")
                    or payload.get("results")
                    or []
                )
            else:
                page_tasks = []

            if not isinstance(page_tasks, list) or not page_tasks:
                break

            for task in page_tasks:
                if not isinstance(task, dict):
                    continue
                task_id = task.get("id")
                if task_id is None:
                    continue
                try:
                    task_id = int(task_id)
                except (TypeError, ValueError):
                    continue
                if task_id in seen_ids:
                    continue
                seen_ids.add(task_id)
                collected.append(task)

            # If fewer than page_size tasks were returned, there is no next
            # page for the normal paginated response.
            if len(page_tasks) < 100:
                break

        if collected:
            return collected

    # Compatibility fallback for older Label Studio builds.
    response = label_studio_request(
        "GET",
        f"/api/projects/{project_id}/tasks/",
        params={"page_size": 100, "page": 1},
    )
    last_status = response.status_code

    if response.status_code < 400:
        try:
            payload = response.json()
        except ValueError:
            payload = []

        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            return payload.get("tasks") or payload.get("results") or []

    if last_status == 404:
        raise HTTPException(
            status_code=404,
            detail="No tasks found for this Label Studio project.",
        )

    return []


@app.get("/api/start-task")
def start_task(
    request: Request,
    modality: str = "text",
    email: str = ""
):
    require_contributor_session(request)

    auth_user_id = _get_authenticated_user_id(request)
    if auth_user_id is None:
        raise HTTPException(status_code=401, detail="A valid contributor session is required.")
    profile_db = SessionLocal()
    try:
        profile_user = profile_db.query(models.User).filter(models.User.id == int(auth_user_id)).first()
        if profile_user is None:
            raise HTTPException(status_code=404, detail="Contributor account not found.")
        completion = _get_contributor_profile_completion(profile_db, profile_user)
    finally:
        profile_db.close()
    if not completion["complete"]:
        raise HTTPException(
            status_code=428,
            detail={
                "code": "CONTRIBUTOR_PROFILE_REQUIRED",
                "message": "Complete your KELYVO workforce profile before starting a new task.",
                "missing": completion["missing"],
                "completed_fields": completion["completed_fields"],
                "required_fields": completion["required_fields"],
            }
        )

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

    # Prefer the persistent KELYVO project registry when it is available.
    # PROJECT_MAPPING remains the compatibility fallback during this first
    # migration step so an enterprise-registry issue cannot break task flow.
    enterprise_project = _get_enterprise_project_for_modality(modality_key)
    if (
        enterprise_project is not None
        and enterprise_project.external_project_id is not None
    ):
        project_id = int(enterprise_project.external_project_id)

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
            project_id,
            user_id=_get_authenticated_user_id(request),
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

                    _sync_enterprise_task(
                        project_id=project_id,
                        task_id=int(reserved_task_id),
                        task_payload=existing_task,
                        status="reserved",
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

    tasks = _fetch_label_studio_project_tasks(project_id)

    if not tasks:
        raise HTTPException(
            status_code=404,
            detail=(
                "No tasks are currently available."
            )
        )

    blocked_fresh_task_ids = _get_kelyvo_blocked_fresh_task_ids(
        contributor_user_id,
        modality_key,
    )

    # Failed KELYVO tasks are handled through the dedicated revision/requeue
    # path. Fresh-work selection is blocked only by KELYVO workflow state and
    # active reservations; old Label Studio annotation objects do not by
    # themselves make a task unavailable.
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

        candidate_task_id = int(candidate_task_id)

        # Label Studio's explicit is_labeled=false is the strongest signal
        # that an old KELYVO PENDING_QA row is stale: there is currently no
        # submitted annotation on the task. In that case the task is safe to
        # return to fresh work. This specifically repairs tasks left poisoned
        # by the previous save/submission loop without reopening genuinely
        # submitted or PASSED tasks.
        task_is_explicitly_unlabeled = (
            isinstance(task, dict)
            and task.get("is_labeled") is False
        )

        if candidate_task_id in blocked_fresh_task_ids:
            if not task_is_explicitly_unlabeled:
                continue

            repair_db = SessionLocal()
            try:
                stale_pending_rows = (
                    repair_db.query(models.TaskSubmission)
                    .filter(
                        models.TaskSubmission.user_id == int(contributor_user_id),
                        models.TaskSubmission.task_type == modality_key,
                        models.TaskSubmission.status == "PENDING_QA",
                    )
                    .all()
                )
                repaired_any = False
                for stale_row in stale_pending_rows:
                    stale_task_id = _extract_task_id_from_submission_title(
                        stale_row.task_title
                    )
                    if stale_task_id == int(candidate_task_id):
                        stale_row.status = "CANCELLED"
                        stale_row.reviewer_notes = (
                            "Automatically reset because Label Studio reports "
                            "the task as explicitly unlabeled; the previous "
                            "KELYVO submission state was stale."
                        )
                        repaired_any = True
                if repaired_any:
                    repair_db.commit()
                blocked_fresh_task_ids.discard(int(candidate_task_id))
            except Exception:
                repair_db.rollback()
            finally:
                repair_db.close()

            if candidate_task_id in blocked_fresh_task_ids:
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

        # KELYVO is the workflow source of truth. Label Studio can contain
        # imported, historical, or draft annotation objects that do not mean
        # the task has been completed inside KELYVO. Fresh-work eligibility is
        # therefore decided from KELYVO submission state plus reservations,
        # not from `annotations` or `cancelled_annotations` in Label Studio.

        if _is_task_reserved_by_other_contributor(
            project_id,
            int(candidate_task_id),
            contributor_identity,
            contributor_user_id=contributor_user_id,
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
                "Label Studio has tasks for this project, but KELYVO has "
                "no eligible fresh task for this contributor. "
                "Any PASSED task remains permanently excluded; an active "
                "PENDING_QA task remains excluded until QA/revision changes "
                "its workflow state."
            )
        )

    # Final claim phase: the picker checks reservations while building the
    # candidate list, but another contributor can claim a candidate between
    # that check and the actual database reservation. Attempt the candidates in
    # order and skip any task that loses that race. This is the authoritative
    # hand-off point for fresh work.
    selected_task = None
    assigned_task_id = None

    for candidate in available_tasks:
        if not isinstance(candidate, dict):
            continue

        candidate_task_id = candidate.get("id")
        if candidate_task_id is None:
            continue

        try:
            candidate_task_id = int(candidate_task_id)
        except (TypeError, ValueError):
            continue

        if _is_task_reserved_by_other_contributor(
            project_id,
            candidate_task_id,
            contributor_identity,
            contributor_user_id=contributor_user_id,
        ):
            continue

        try:
            claimed_task_id = reserve_task_for_contributor(
                contributor_identity,
                project_id,
                candidate_task_id,
                user_id=contributor_user_id,
            )
        except HTTPException as exc:
            if exc.status_code == 409:
                # Another contributor won the reservation race. Try the next
                # eligible candidate rather than surfacing a false task error.
                continue
            raise

        if claimed_task_id is None:
            # A failed reservation must never be treated as a successful claim.
            continue

        if int(claimed_task_id) != candidate_task_id:
            # This should only happen if the contributor already had an active
            # reservation; do not accidentally return a task different from the
            # candidate we just selected.
            continue

        selected_task = candidate
        assigned_task_id = int(claimed_task_id)
        break

    if selected_task is None or assigned_task_id is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "All currently eligible tasks were claimed by other "
                "contributors before this task could be reserved. "
                "Please request a new task."
            )
        )

    task = selected_task
    task_id = int(task.get("id"))

    if email:
        sync_browser_assignment_for_request(
            request,
            project_id,
            int(assigned_task_id)
        )

    _sync_enterprise_task(
        project_id=project_id,
        task_id=int(assigned_task_id),
        task_payload=task,
        status="reserved",
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
            project_id,
            user_id=_get_authenticated_user_id(request),
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
            project_id,
            user_id=_get_authenticated_user_id(request),
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
    """Leave the workspace without releasing the contributor's reservation."""
    session = require_contributor_session(request)
    user_id = _get_authenticated_user_id(request)

    if not project_id or not task_id:
        raise HTTPException(
            status_code=400,
            detail="Project ID and task ID are required to exit the workspace.",
        )

    if user_id is None:
        raise HTTPException(
            status_code=401,
            detail="Contributor session is not authenticated.",
        )

    db = SessionLocal()
    try:
        assignment = _get_active_persistent_assignment(
            db,
            int(user_id),
            project_id=int(project_id),
            task_id=int(task_id),
        )

        if assignment is None:
            raise HTTPException(
                status_code=404,
                detail="This task is no longer reserved by this contributor.",
            )

        # Exit is deliberately NOT a release action. The task stays reserved
        # for this contributor until they submit, skip, or the fixed 24-hour
        # reservation expires. The browser may close and later reconnect; the
        # server-side assignment remains authoritative.
        if assignment.task is not None:
            assignment.task.is_locked = True
            assignment.task.status = "reserved"

        db.commit()

        expires_at = assignment.expires_at.isoformat() if assignment.expires_at else None
        return {
            "status": "success",
            "message": "Workspace exited. Your task remains reserved for you until submission, skip, or the 24-hour reservation expiry.",
            "task_id": int(task_id),
            "project_id": int(project_id),
            "expires_at": expires_at,
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as exc:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail="KELYVO could not preserve the active task reservation.",
        ) from exc
    finally:
        db.close()


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
    email = normalize_email(email)

    session = require_contributor_session(request)
    if email != session["email"]:
        raise HTTPException(
            status_code=403,
            detail="Submission account does not match the active KELYVO session."
        )

    user = (
        db.query(models.User)
        .filter(models.User.email == email)
        .first()
    )

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if task_id and project_id:
        contributor_identity = "user:" + email
        assigned_task_id = get_reserved_task(
            contributor_identity,
            project_id,
            user_id=_get_authenticated_user_id(request),
        )

        if assigned_task_id is None or int(assigned_task_id) != int(task_id):
            raise HTTPException(
                status_code=403,
                detail="This task is not currently assigned to this contributor."
            )

    # Serialize concurrent submit clicks for the same contributor. SQLite may
    # not support row-level locking, so the durable status checks below remain
    # the final idempotency safeguard.
    try:
        locked_user = (
            db.query(models.User)
            .with_for_update()
            .filter(models.User.id == int(user.id))
            .first()
        )
        if locked_user is not None:
            user = locked_user
    except Exception:
        pass

    existing_submission = None
    try:
        existing_submission = (
            db.query(models.TaskSubmission)
            .filter(
                models.TaskSubmission.user_id == user.id,
                models.TaskSubmission.task_title == task_title,
                models.TaskSubmission.task_type == task_type,
                models.TaskSubmission.status == "PENDING_QA"
            )
            .first()
        )
    except Exception:
        existing_submission = None

    revision_submission = None
    if not existing_submission:
        try:
            revision_submission = (
                db.query(models.TaskSubmission)
                .filter(
                    models.TaskSubmission.user_id == user.id,
                    models.TaskSubmission.task_title == task_title,
                    models.TaskSubmission.task_type == task_type,
                    models.TaskSubmission.status == "FAILED"
                )
                .order_by(models.TaskSubmission.id.desc())
                .first()
            )
        except Exception:
            revision_submission = None

    # IMPORTANT: determine whether this is a revision BEFORE checking Label
    # Studio. A revision already has an older finalized annotation on the task.
    # In revision mode the helper must therefore look for the new draft first,
    # promote it when changed, and only then fall back to the prior annotation.
    if task_id and project_id:
        _ensure_label_studio_annotation_saved(
            int(project_id),
            int(task_id),
            revision_mode=bool(revision_submission),
        )

    if revision_submission:
        # Reuse the same KELYVO submission row. This keeps one durable QA record
        # for the task while increasing the enterprise attempt number when the
        # revised annotation is persisted below.
        revision_submission.status = "PENDING_QA"
        revision_submission.reviewer_notes = None
        revision_submission.submitted_at = _utc_now()
        db.commit()

        if task_id and project_id:
            try:
                mark_kelyvo_submission_completed(
                    request,
                    project_id,
                    task_id,
                )
            except Exception:
                pass

            release_reserved_task(
                "user:" + email,
                project_id,
                task_id,
                user_id=int(user.id),
                final_task_status="submitted",
            )

        release_browser_assignment_for_request(
            request,
            final_task_status="submitted",
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
        # The durable KELYVO record already proves this task is in QA. Repeated
        # requests must not create duplicate rows or increment counters.
        if task_id and project_id:
            try:
                mark_kelyvo_submission_completed(
                    request,
                    project_id,
                    task_id,
                )
            except Exception:
                pass

            release_reserved_task(
                "user:" + email,
                project_id,
                task_id,
                user_id=int(user.id),
                final_task_status="submitted",
            )

        release_browser_assignment_for_request(
            request,
            final_task_status="submitted",
        )

        pending_qa_count = (
            db.query(models.TaskSubmission)
            .filter(models.TaskSubmission.status == "PENDING_QA")
            .count()
        )

        contributor_pending_qa_count = (
            db.query(models.TaskSubmission)
            .filter(
                models.TaskSubmission.user_id == user.id,
                models.TaskSubmission.status == "PENDING_QA",
            )
            .count()
        )

        return {
            "status": "success",
            "message": "Task was already submitted and is in the QA queue.",
            "created": False,
            "already_submitted": True,
            "tasks_today": int(user.tasks_today or 0),
            "tasks_week": int(user.tasks_week or 0),
            "submission_id": existing_submission.id,
            "queue_count": contributor_pending_qa_count,
            "qa_pending_count": pending_qa_count,
        }

    user.tasks_today += 1
    user.tasks_week += 1

    submission = models.TaskSubmission(
        user_id=user.id,
        task_type=task_type,
        task_title=task_title,
        status="PENDING_QA",
        reviewer_notes=None
    )

    db.add(submission)
    db.commit()
    db.refresh(user)

    if task_id and project_id:
        try:
            mark_kelyvo_submission_completed(
                request,
                project_id,
                task_id,
            )
        except Exception:
            pass

        release_reserved_task(
            "user:" + email,
            project_id,
            task_id,
            user_id=int(user.id),
            final_task_status="submitted",
        )

    release_browser_assignment_for_request(request)

    pending_qa_count = (
        db.query(models.TaskSubmission)
        .filter(models.TaskSubmission.status == "PENDING_QA")
        .count()
    )

    contributor_pending_qa_count = (
        db.query(models.TaskSubmission)
        .filter(
            models.TaskSubmission.user_id == user.id,
            models.TaskSubmission.status == "PENDING_QA",
        )
        .count()
    )

    return {
        "status": "success",
        "message": "Task submitted successfully and sent for QA review.",
        "created": True,
        "tasks_today": user.tasks_today,
        "tasks_week": user.tasks_week,
        "submission_id": submission.id,
        "queue_count": contributor_pending_qa_count,
        "qa_pending_count": pending_qa_count,
    }



def _ensure_label_studio_annotation_saved(
    project_id: int,
    task_id: int,
    revision_mode: bool = False,
):
    """Confirm the annotation for the CURRENT KELYVO submission attempt.

    Normal submission:
      1. wait for a finalized annotation;
      2. if only a draft exists, promote the draft.

    Revision submission:
      1. inspect the current revision draft FIRST;
      2. compare its result with the latest finalized annotation;
      3. if the draft contains changed work, promote that NEW result to a new
         finalized annotation;
      4. only fall back to the existing annotation when there is no changed
         revision draft.

    This prevents the previous finalized annotation from being mistaken for the
    newly edited revision.
    """
    project_id = int(project_id)
    task_id = int(task_id)
    revision_mode = bool(revision_mode)

    timeout_seconds = 15.0
    started = time.monotonic()
    poll_delays = [
        0.0,
        0.35,
        0.50,
        0.75,
        1.00,
        1.25,
        1.50,
        2.00,
        2.50,
        3.00,
    ]

    def has_result(item):
        return (
            isinstance(item, dict)
            and isinstance(item.get("result"), list)
            and bool(item.get("result"))
            and not bool(item.get("was_cancelled"))
        )

    def result_fingerprint(result):
        try:
            return json.dumps(
                result or [],
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
        except (TypeError, ValueError):
            return repr(result or [])

    def sort_latest(items):
        usable = [item for item in (items or []) if has_result(item)]
        usable.sort(
            key=lambda item: str(
                item.get("updated_at")
                or item.get("created_at")
                or item.get("submitted_at")
                or item.get("id")
                or ""
            )
        )
        return usable

    def read_annotations():
        response = label_studio_request(
            "GET",
            f"/api/tasks/{task_id}/annotations/",
            params={"project": project_id},
            timeout=5,
        )

        if response.status_code == 200:
            try:
                payload = response.json()
            except ValueError:
                payload = []

            if isinstance(payload, dict):
                payload = (
                    payload.get("annotations")
                    or payload.get("results")
                    or payload.get("data")
                    or []
                )

            if isinstance(payload, list):
                return sort_latest(payload), None
            return [], None

        return None, response.status_code

    def read_task():
        response = label_studio_request(
            "GET",
            f"/api/tasks/{task_id}",
            params={
                "project": project_id,
                "resolve_uri": "true",
            },
            timeout=5,
        )

        if response.status_code != 200:
            return None

        try:
            payload = response.json()
        except ValueError:
            return None

        return payload if isinstance(payload, dict) else None

    def extract_drafts(task_payload):
        drafts = (
            task_payload.get("drafts", [])
            if isinstance(task_payload, dict)
            else []
        )

        if isinstance(drafts, dict):
            drafts = drafts.get("drafts", drafts.get("results", []))

        return sort_latest(drafts if isinstance(drafts, list) else [])

    def read_drafts_from_endpoint():
        draft_response = label_studio_request(
            "GET",
            f"/api/tasks/{task_id}/drafts",
            params={"project": project_id},
            timeout=5,
        )

        if draft_response.status_code >= 400:
            return []

        try:
            draft_payload = draft_response.json()
        except ValueError:
            return []

        if isinstance(draft_payload, dict):
            draft_payload = (
                draft_payload.get("drafts")
                or draft_payload.get("results")
                or draft_payload.get("data")
                or []
            )

        return sort_latest(draft_payload if isinstance(draft_payload, list) else [])

    def promote_draft(draft):
        result = draft.get("result") or []

        body = {
            "task": task_id,
            "project": project_id,
            "lead_time": float(draft.get("lead_time") or 0),
            "result": result,
        }

        completed_by = draft.get("completed_by")
        if completed_by is not None:
            try:
                body["completed_by"] = int(completed_by)
            except (TypeError, ValueError):
                pass

        create_response = label_studio_request(
            "POST",
            f"/api/tasks/{task_id}/annotations/",
            json=body,
            headers={"Content-Type": "application/json"},
            timeout=8,
        )

        if create_response.status_code in (200, 201):
            annotations, _ = read_annotations()
            if annotations:
                task_payload = read_task()
                return task_payload or {"annotations": annotations}

        # Another save may have won the race between our read and POST.
        annotations, _ = read_annotations()
        if annotations:
            task_payload = read_task()
            return task_payload or {"annotations": annotations}

        return None

    for delay in poll_delays:
        elapsed = time.monotonic() - started
        if elapsed >= timeout_seconds:
            break

        if delay > 0:
            remaining = timeout_seconds - elapsed
            time.sleep(min(delay, max(0.0, remaining)))

        task_payload = read_task()
        annotations, _ = read_annotations()

        if task_payload is None:
            task_payload = {
                "id": task_id,
                "project": project_id,
            }

        task_annotations = annotations or []
        if not task_annotations and isinstance(task_payload.get("annotations"), list):
            task_annotations = sort_latest(task_payload.get("annotations"))

        drafts = extract_drafts(task_payload)
        if not drafts:
            drafts = read_drafts_from_endpoint()

        # Revision is fundamentally different: an older finalized annotation is
        # expected to exist, so it must NEVER win over a changed current draft.
        if revision_mode:
            latest_annotation = task_annotations[-1] if task_annotations else None
            latest_draft = drafts[-1] if drafts else None

            if latest_draft is not None:
                draft_fp = result_fingerprint(latest_draft.get("result"))
                annotation_fp = result_fingerprint(
                    latest_annotation.get("result") if latest_annotation else None
                )

                if latest_annotation is None or draft_fp != annotation_fp:
                    promoted = promote_draft(latest_draft)
                    if promoted is not None:
                        return promoted

            if latest_annotation is not None:
                task_payload["annotations"] = [latest_annotation]
                return task_payload

        else:
            # Normal first submission: a finalized annotation is the preferred
            # source. Otherwise promote the newest saved draft.
            if task_annotations:
                task_payload["annotations"] = [task_annotations[-1]]
                return task_payload

            if drafts:
                promoted = promote_draft(drafts[-1])
                if promoted is not None:
                    return promoted

        if time.monotonic() - started >= timeout_seconds:
            break

    raise HTTPException(
        status_code=409,
        detail=(
            "Label Studio is still saving this annotation. "
            "KELYVO waited for the save but could not confirm the current "
            "submission yet. Please wait a moment and try Submit again."
        ),
    )



@app.get("/api/task-submission-ready")
def task_submission_ready(
    request: Request,
    project_id: int,
    task_id: int,
):
    """Verify and, when necessary, finalize the current Label Studio draft."""
    require_contributor_session(request)

    user_id = _get_authenticated_user_id(request)
    if user_id is None:
        raise HTTPException(
            status_code=401,
            detail="Contributor session is not authenticated.",
        )

    db = SessionLocal()
    try:
        assignment = _get_active_persistent_assignment(
            db,
            int(user_id),
            project_id=int(project_id),
            task_id=int(task_id),
        )
    finally:
        db.close()

    if assignment is None:
        raise HTTPException(
            status_code=403,
            detail="This task is not currently assigned to this contributor.",
        )

    payload = _ensure_label_studio_annotation_saved(
        int(project_id),
        int(task_id),
    )

    annotations = payload.get("annotations", []) if isinstance(payload, dict) else []
    if not isinstance(annotations, list):
        annotations = []

    usable_annotations = [
        item
        for item in annotations
        if (
            isinstance(item, dict)
            and isinstance(item.get("result"), list)
            and bool(item.get("result"))
            and not bool(item.get("was_cancelled"))
        )
    ]

    return {
        "ready": bool(usable_annotations),
        "annotation_count": len(annotations),
        "current_annotation_count": len(usable_annotations),
        "draft_count": 0,
        "current_draft_count": 0,
        "task_id": int(task_id),
        "project_id": int(project_id),
    }


@app.post("/admin/reset-qa-queue")
def reset_qa_queue(
    request: Request,
    confirm: str = "",
    db: Session = Depends(get_db),
):
    """Hard-reset KELYVO's local QA/test state for a clean QA2 run.

    This intentionally preserves users, projects, tasks and payout/audit history.
    It removes the compatibility/canonical QA records and releases reservations.
    """
    require_qa_session(request)

    if str(confirm).strip().upper() != "RESET_QA2":
        raise HTTPException(
            status_code=400,
            detail="Confirmation is required. Use confirm=RESET_QA2.",
        )

    deleted = {
        "legacy_task_submissions": 0,
        "qa_reviews": 0,
        "canonical_submissions": 0,
        "canonical_annotations": 0,
        "released_assignments": 0,
    }

    try:
        # Delete child records before their parent canonical submissions.
        if hasattr(models, "QAReview"):
            rows = db.query(models.QAReview).all()
            deleted["qa_reviews"] = len(rows)
            for row in rows:
                db.delete(row)
            db.flush()

        if hasattr(models, "Submission"):
            rows = db.query(models.Submission).all()
            deleted["canonical_submissions"] = len(rows)
            for row in rows:
                db.delete(row)
            db.flush()

        if hasattr(models, "Annotation"):
            rows = db.query(models.Annotation).all()
            deleted["canonical_annotations"] = len(rows)
            for row in rows:
                db.delete(row)
            db.flush()

        rows = db.query(models.TaskSubmission).all()
        deleted["legacy_task_submissions"] = len(rows)
        for row in rows:
            db.delete(row)
        db.flush()

        if hasattr(models, "TaskAssignment"):
            assignments = db.query(models.TaskAssignment).all()
            for assignment in assignments:
                if assignment.status in {"reserved", "completed"}:
                    assignment.status = "released"
                    assignment.released_at = _utc_now()
                    assignment.completed_at = None
                    deleted["released_assignments"] += 1

                    if getattr(assignment, "task", None) is not None:
                        assignment.task.is_locked = False
                        assignment.task.status = "available"

        db.commit()

        return {
            "status": "success",
            "message": "QA2 state has been hard-reset for a fresh test run.",
            **deleted,
            "preserved": [
                "users",
                "projects",
                "tasks",
                "payout_ledger",
                "audit_logs",
            ],
        }
    except Exception as exc:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail="KELYVO could not reset the QA2 state.",
        ) from exc


def _get_kelyvo_organization(db):
    if not hasattr(models, "Organization"):
        return None
    return db.query(models.Organization).filter(models.Organization.slug == "kelyvo").first()


def _get_user_membership(db, user_id: int):
    organization = _get_kelyvo_organization(db)
    if organization is None or not hasattr(models, "OrganizationMember"):
        return None
    return (db.query(models.OrganizationMember).filter(
        models.OrganizationMember.organization_id == organization.id,
        models.OrganizationMember.user_id == int(user_id),
    ).first())


def _count_active_admins(db) -> int:
    return db.query(models.User).filter(models.User.role == "admin").count()


def _ensure_contributor_profile(db, user):
    """Create a workforce profile row lazily for an existing account."""
    if not hasattr(models, "ContributorProfile"):
        return None
    profile = (
        db.query(models.ContributorProfile)
        .filter(models.ContributorProfile.user_id == int(user.id))
        .first()
    )
    if profile is None:
        profile = models.ContributorProfile(
            user_id=int(user.id),
            display_name="",
            country="India",
            onboarding_status="pending",
        )
        db.add(profile)
        db.flush()
    return profile


def _ensure_workforce_detail_row(db, user_id: int):
    row = db.execute(
        workforce_profile_details.select().where(
            workforce_profile_details.c.user_id == int(user_id)
        )
    ).first()
    if row is None:
        now_iso = _utc_now().isoformat()
        db.execute(
            workforce_profile_details.insert().values(
                user_id=int(user_id),
                availability_status="unspecified",
                worker_tier="general",
                qualification_status="pending",
                updated_at=now_iso,
            )
        )
        db.flush()
        row = db.execute(
            workforce_profile_details.select().where(
                workforce_profile_details.c.user_id == int(user_id)
            )
        ).first()
    return row


def _split_csv(value):
    return [
        item.strip()
        for item in str(value or "").replace("\n", ",").split(",")
        if item.strip()
    ]


def _join_csv(items):
    cleaned = []
    seen = set()
    for item in items or []:
        value = str(item or "").strip()
        key = value.casefold()
        if value and key not in seen:
            seen.add(key)
            cleaned.append(value)
    return ", ".join(cleaned)


def _normalize_preference_values(values, max_items=50):
    if values is None:
        return []
    if not isinstance(values, list):
        values = [values]
    cleaned = []
    seen = set()
    for value in values:
        value = str(value or "").strip()
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(value)
        if len(cleaned) >= max_items:
            break
    return cleaned


def _get_preference_catalog_payload(db):
    rows = db.execute(
        contributor_work_interest_catalog.select()
        .where(contributor_work_interest_catalog.c.status == "active")
        .order_by(contributor_work_interest_catalog.c.sort_order.asc())
    ).mappings().all()
    return {
        "interests": [
            {"slug": str(row["slug"]), "name": str(row["name"]), "description": str(row.get("description") or "")}
            for row in rows
        ],
        "languages": [{"code": code, "name": name} for code, name in CONTRIBUTOR_LANGUAGE_CATALOG],
    }


def _get_contributor_preferences_payload(db, user_id: int):
    interest_rows = db.execute(
        contributor_work_interest_catalog.select()
        .join(contributor_work_interests, contributor_work_interest_catalog.c.id == contributor_work_interests.c.interest_id)
        .where(contributor_work_interests.c.user_id == int(user_id))
        .order_by(contributor_work_interest_catalog.c.sort_order.asc())
    ).mappings().all()
    language_rows = db.execute(
        contributor_interest_languages.select()
        .where(contributor_interest_languages.c.user_id == int(user_id))
        .order_by(contributor_interest_languages.c.language_name.asc())
    ).mappings().all()
    return {
        "interests": [
            {"slug": str(row["slug"]), "name": str(row["name"]), "description": str(row.get("description") or "")}
            for row in interest_rows
        ],
        "languages": [
            {"code": str(row["language_code"]), "name": str(row["language_name"])}
            for row in language_rows
        ],
    }


PAYMENT_METHODS = {"upi", "bank_account"}
BANK_ACCOUNT_TYPES = {"savings", "current"}


def _payment_is_complete(detail_map):
    method = str(detail_map.get("payment_method") or "").strip().lower()
    if method == "upi":
        return bool(str(detail_map.get("upi_id") or "").strip())
    if method == "bank_account":
        required = (
            "bank_account_name", "bank_account_number", "bank_ifsc",
            "bank_name", "bank_branch", "bank_account_type",
        )
        return all(bool(str(detail_map.get(field) or "").strip()) for field in required) and str(detail_map.get("bank_account_type") or "").strip().lower() in BANK_ACCOUNT_TYPES
    return False


def _mask_payment_account(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    if len(raw) <= 4:
        return "*" * len(raw)
    return "*" * max(0, len(raw) - 4) + raw[-4:]


def _validate_payment_payload(detail_data):
    method = str(detail_data.get("payment_method") or "").strip().lower()
    if method not in PAYMENT_METHODS:
        raise HTTPException(status_code=400, detail="Select either UPI or Bank Account as the payment method.")
    normalized = {k: (str(v).strip() if v is not None else "") for k, v in detail_data.items()}
    normalized["payment_method"] = method
    if method == "upi":
        if not normalized.get("upi_id"):
            raise HTTPException(status_code=400, detail="UPI ID is required for UPI payments.")
        normalized.update({
            "bank_account_name": "", "bank_account_number": "", "bank_ifsc": "",
            "bank_name": "", "bank_branch": "", "bank_account_type": "",
        })
    else:
        required = {
            "bank_account_name": "Bank account holder name",
            "bank_account_number": "Bank account number",
            "bank_ifsc": "IFSC",
            "bank_name": "Bank name",
            "bank_branch": "Bank branch",
            "bank_account_type": "Account type",
        }
        for key, label in required.items():
            if not normalized.get(key):
                raise HTTPException(status_code=400, detail=f"{label} is required for bank account payments.")
        if normalized["bank_account_type"].lower() not in BANK_ACCOUNT_TYPES:
            raise HTTPException(status_code=400, detail="Bank account type must be Savings or Current.")
        normalized["upi_id"] = ""
    return normalized


def _snapshot_workforce_record(db, user_id, version_type, changed_by_user_id=None, reason=""):
    detail = _ensure_workforce_detail_row(db, int(user_id))
    detail_map = dict(detail._mapping) if detail is not None else {}
    user = db.query(models.User).filter(models.User.id == int(user_id)).first()
    profile = _ensure_contributor_profile(db, user) if user is not None else None
    payload = {
        "profile": {
            "display_name": str(getattr(profile, "display_name", "") or ""),
            "country": str(getattr(profile, "country", "") or ""),
            "region": str(getattr(profile, "region", "") or ""),
            "city": str(getattr(profile, "city", "") or ""),
            "timezone": str(getattr(profile, "timezone", "") or ""),
        },
        "details": {
            key: detail_map.get(key) for key in (
                "workforce_type", "dialects", "domain_expertise", "experience_summary",
                "availability_hours_per_week", "availability_status", "preferred_shift",
                "payment_method", "upi_id", "bank_account_name", "bank_account_number",
                "bank_ifsc", "bank_name", "bank_branch", "bank_account_type",
            )
        },
        "skills": [], "languages": [],
    }
    if hasattr(models, "ContributorSkill") and hasattr(models, "Skill"):
        payload["skills"] = [str(skill.name or "") for _, skill in db.query(models.ContributorSkill, models.Skill).join(models.Skill, models.Skill.id == models.ContributorSkill.skill_id).filter(models.ContributorSkill.user_id == int(user_id)).all()]
    if hasattr(models, "ContributorLanguage") and hasattr(models, "Language"):
        payload["languages"] = [str(language.name or "") for _, language in db.query(models.ContributorLanguage, models.Language).join(models.Language, models.Language.id == models.ContributorLanguage.language_id).filter(models.ContributorLanguage.user_id == int(user_id)).all()]
    last_version = db.execute(workforce_profile_versions.select().where(workforce_profile_versions.c.user_id == int(user_id), workforce_profile_versions.c.version_type == str(version_type)).order_by(workforce_profile_versions.c.version_number.desc())).first()
    next_version = (int(last_version._mapping["version_number"]) + 1) if last_version else 1
    db.execute(workforce_profile_versions.insert().values(
        user_id=int(user_id), version_type=str(version_type), version_number=next_version,
        snapshot=json.dumps(payload, ensure_ascii=False, default=str),
        changed_by_user_id=(int(changed_by_user_id) if changed_by_user_id else None),
        reason=str(reason or ""), created_at=_utc_now().isoformat(),
    ))
    return next_version


REQUIRED_CONTRIBUTOR_PROFILE_FIELDS = {
    "display_name": "Full name", "country": "Country", "region": "State / region",
    "city": "City", "timezone": "Timezone",
    "availability_status": "Availability status", "availability_hours_per_week": "Hours per week",
    "experience_summary": "Experience summary", "payment_method": "Payment method",
    "payment_details": "Payment details",
}


def _get_contributor_profile_completion(db, user, skills_override=None, languages_override=None):
    # Always flush pending ORM writes before evaluating completion. The previous
    # implementation could validate against the pre-write taxonomy state and
    # incorrectly reject a valid first submission. That rejection rolled back
    # the entire request, which is why payment values appeared to disappear.
    db.flush()

    profile = _ensure_contributor_profile(db, user)
    detail = _ensure_workforce_detail_row(db, int(user.id))
    detail_map = dict(detail._mapping) if detail is not None else {}
    languages = []
    skills = []
    if hasattr(models, "ContributorLanguage") and hasattr(models, "Language"):
        rows = db.query(models.ContributorLanguage, models.Language).join(models.Language, models.Language.id == models.ContributorLanguage.language_id).filter(models.ContributorLanguage.user_id == int(user.id)).all()
        languages = [str(language.name or "").strip() for _, language in rows if str(language.name or "").strip()]
    if hasattr(models, "ContributorSkill") and hasattr(models, "Skill"):
        rows = db.query(models.ContributorSkill, models.Skill).join(models.Skill, models.Skill.id == models.ContributorSkill.skill_id).filter(models.ContributorSkill.user_id == int(user.id)).all()
        skills = [str(skill.name or "").strip() for _, skill in rows if str(skill.name or "").strip()]

    # During an active first-time submission, the submitted taxonomy strings are
    # already validated by the request layer and are the authoritative values
    # for this completion check. We still persist them into the normalized
    # ContributorSkill/ContributorLanguage tables before commit.
    if skills_override is not None:
        skills = [str(x).strip() for x in skills_override if str(x).strip()]
    if languages_override is not None:
        languages = [str(x).strip() for x in languages_override if str(x).strip()]
    hours = detail_map.get("availability_hours_per_week")
    try:
        hours_valid = hours is not None and 0 < float(hours) <= 168
    except (TypeError, ValueError):
        hours_valid = False
    method = str(detail_map.get("payment_method") or "").strip().lower()
    checks = {
        "display_name": bool(str(getattr(profile, "display_name", "") or "").strip()),
        "country": bool(str(getattr(profile, "country", "") or "").strip()),
        "region": bool(str(getattr(profile, "region", "") or "").strip()),
        "city": bool(str(getattr(profile, "city", "") or "").strip()),
        "timezone": bool(str(getattr(profile, "timezone", "") or "").strip()),
        "availability_status": str(detail_map.get("availability_status") or "").strip().lower() in {"available", "part-time", "full-time", "on-demand", "weekends", "evenings", "flexible"},
        "availability_hours_per_week": hours_valid,
        "experience_summary": bool(str(detail_map.get("experience_summary") or "").strip()),
        "payment_method": method in PAYMENT_METHODS,
        "payment_details": _payment_is_complete(detail_map),
    }
    missing = [label for key, label in REQUIRED_CONTRIBUTOR_PROFILE_FIELDS.items() if not checks.get(key, False)]
    return {
        "complete": not missing, "missing": missing,
        "completed_fields": sum(1 for value in checks.values() if value),
        "required_fields": len(checks),
        "profile_locked": bool(detail_map.get("profile_locked")),
        "payment_locked": bool(detail_map.get("payment_locked")),
        "payment_method": method or None, "payment_complete": checks["payment_details"],
    }


def _write_audit_log(db, request, action, resource_type, resource_id, details=None):
    try:
        if not hasattr(models, "AuditLog"):
            return
        organization = _get_kelyvo_organization(db)
        db.add(
            models.AuditLog(
                organization_id=(organization.id if organization is not None else None),
                user_id=_get_authenticated_user_id(request),
                action=str(action),
                resource_type=str(resource_type) if resource_type else None,
                resource_id=str(resource_id) if resource_id is not None else None,
                ip_address=(request.client.host if request.client else None),
                user_agent=request.headers.get("user-agent"),
                details=json.dumps(details or {}, ensure_ascii=False, default=str),
            )
        )
    except Exception:
        pass



# ============================================================
# DATA COLLECTOR PROFILE
# ============================================================

DATA_COLLECTOR_AVAILABILITY_OPTIONS = {
    "available",
    "part-time",
    "full-time",
    "on-demand",
    "weekends",
    "evenings",
    "flexible",
    "unspecified",
}


DATA_COLLECTOR_AVAILABILITY_OPTIONS = {
    "available",
    "part-time",
    "full-time",
    "on-demand",
    "weekends",
    "evenings",
    "flexible",
    "unspecified",
}

DATA_COLLECTOR_AREA_TYPES = {
    "urban",
    "semi-urban",
    "rural",
}

DATA_COLLECTOR_PAYMENT_METHODS = {
    "upi",
    "bank_account",
}

DATA_COLLECTOR_ACCOUNT_TYPES = {
    "savings",
    "current",
}

DATA_COLLECTOR_CAPABILITIES = {
    "voice_recording",
    "image_collection",
    "video_collection",
    "speech_collection",
    "text_collection",
    "face_data_collection",
    "environment_data_collection",
    "local_language_recording",
}

DATA_COLLECTOR_DEVICES = {
    "android_smartphone",
    "iphone",
    "dslr_camera",
    "mirrorless_camera",
    "laptop_desktop",
    "external_microphone",
}

DATA_COLLECTOR_ENVIRONMENTS = {
    "home",
    "outdoor",
    "studio_office",
    "field_location",
}

DATA_COLLECTOR_CONSENT_VERSION = "v1.0"


def _get_data_collector_profile(db: Session, user_id: int):
    if not hasattr(models, "DataCollectorProfile"):
        return None

    profile = (
        db.query(models.DataCollectorProfile)
        .filter(models.DataCollectorProfile.user_id == int(user_id))
        .first()
    )

    if profile is None:
        profile = models.DataCollectorProfile(
            user_id=int(user_id),
            display_name="",
            country="India",
            onboarding_status="pending",
        )
        db.add(profile)
        db.flush()

    return profile


def _validate_data_collector_profile(profile):
    capabilities = _split_csv(profile.collection_capabilities)
    devices = _split_csv(profile.device_availability)
    environments = _split_csv(profile.collection_environment)
    pin_code = str(profile.pin_code or "").strip()

    required_checks = {
        "display_name": bool(str(profile.display_name or "").strip()),
        "country": bool(str(profile.country or "").strip()),
        "region": bool(str(profile.region or "").strip()),
        "city": bool(str(profile.city or "").strip()),
        "pin_code": bool(re.fullmatch(r"\d{6}", pin_code)),
        "area_type": str(profile.area_type or "").strip().lower() in DATA_COLLECTOR_AREA_TYPES,
        "timezone": bool(str(profile.timezone or "").strip()),
        "languages": bool(_split_csv(profile.languages)),
        "collection_capabilities": bool(capabilities) and all(item in DATA_COLLECTOR_CAPABILITIES for item in capabilities),
        "device_availability": bool(devices) and all(item in DATA_COLLECTOR_DEVICES for item in devices),
        "collection_environment": bool(environments) and all(item in DATA_COLLECTOR_ENVIRONMENTS for item in environments),
        "availability_status": str(profile.availability_status or "").strip().lower() in DATA_COLLECTOR_AVAILABILITY_OPTIONS - {"unspecified"},
        "availability_hours_per_week": profile.availability_hours_per_week is not None and 0 < float(profile.availability_hours_per_week) <= 168,
        "experience_summary": bool(str(profile.experience_summary or "").strip()),
        "payment_method": str(profile.payment_method or "").strip().lower() in DATA_COLLECTOR_PAYMENT_METHODS,
        "payment_details": False,
        "consent_accepted": bool(profile.consent_accepted),
    }

    payment_method = str(profile.payment_method or "").strip().lower()
    if payment_method == "upi":
        required_checks["payment_details"] = bool(str(profile.upi_id or "").strip())
    elif payment_method == "bank_account":
        required_checks["payment_details"] = all([
            str(profile.bank_account_name or "").strip(),
            str(profile.bank_account_number or "").strip(),
            str(profile.bank_ifsc or "").strip(),
            str(profile.bank_name or "").strip(),
            str(profile.bank_branch or "").strip(),
            str(profile.bank_account_type or "").strip().lower() in DATA_COLLECTOR_ACCOUNT_TYPES,
        ])

    missing = [key for key, valid in required_checks.items() if not valid]
    return missing


def _data_collector_profile_payload(profile):
    return {
        "id": getattr(profile, "id", None),
        "display_name": str(profile.display_name or ""),
        "country": str(profile.country or ""),
        "region": str(profile.region or ""),
        "city": str(profile.city or ""),
        "pin_code": str(profile.pin_code or ""),
        "area_type": str(profile.area_type or ""),
        "timezone": str(profile.timezone or ""),
        "languages": _split_csv(profile.languages),
        "collection_capabilities": _split_csv(profile.collection_capabilities),
        "device_availability": _split_csv(profile.device_availability),
        "collection_environment": _split_csv(profile.collection_environment),
        "experience_summary": str(profile.experience_summary or ""),
        "availability_status": str(profile.availability_status or "unspecified"),
        "availability_hours_per_week": profile.availability_hours_per_week,
        "payment_method": str(profile.payment_method or ""),
        "upi_id": str(profile.upi_id or ""),
        "bank_account_name": str(profile.bank_account_name or ""),
        "bank_account_number": str(profile.bank_account_number or ""),
        "bank_ifsc": str(profile.bank_ifsc or ""),
        "bank_name": str(profile.bank_name or ""),
        "bank_branch": str(profile.bank_branch or ""),
        "bank_account_type": str(profile.bank_account_type or ""),
        "consent_accepted": bool(profile.consent_accepted),
        "consent_version": str(profile.consent_version or ""),
        "onboarding_status": str(profile.onboarding_status or "pending"),
        "profile_locked": bool(profile.profile_locked),
        "payment_locked": bool(profile.payment_locked),
        "profile_locked_at": profile.profile_locked_at.isoformat() if getattr(profile, "profile_locked_at", None) else None,
        "payment_locked_at": profile.payment_locked_at.isoformat() if getattr(profile, "payment_locked_at", None) else None,
        "submitted_at": profile.submitted_at.isoformat() if getattr(profile, "submitted_at", None) else None,
        "reviewed_at": profile.reviewed_at.isoformat() if getattr(profile, "reviewed_at", None) else None,
        "rejection_reason": str(profile.rejection_reason or ""),
    }


@app.get("/api/data-collector/profile")
def get_data_collector_profile(
    request: Request,
    db: Session = Depends(get_db),
):
    require_data_collector_session(request)
    user_id = _get_authenticated_user_id(request)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Data Collector session is not authenticated.")

    profile = _get_data_collector_profile(db, int(user_id))
    if profile is None:
        raise HTTPException(status_code=500, detail="Data Collector profile is unavailable.")

    return {
        "status": "success",
        "profile": _data_collector_profile_payload(profile),
    }


@app.patch("/api/data-collector/profile")
async def update_data_collector_profile(
    request: Request,
    db: Session = Depends(get_db),
):
    require_data_collector_session(request)
    user_id = _get_authenticated_user_id(request)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Data Collector session is not authenticated.")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    profile = _get_data_collector_profile(db, int(user_id))
    if profile is None:
        raise HTTPException(status_code=500, detail="Data Collector profile is unavailable.")

    if bool(profile.profile_locked) or bool(profile.payment_locked):
        raise HTTPException(
            status_code=423,
            detail="Your Data Collector profile is locked after submission. Ask a KELYVO Admin to unlock it before making changes.",
        )

    profile_data = payload.get("profile") or {}

    string_fields = {
        "display_name",
        "country",
        "region",
        "city",
        "pin_code",
        "timezone",
        "experience_summary",
        "payment_method",
        "upi_id",
        "bank_account_name",
        "bank_account_number",
        "bank_ifsc",
        "bank_name",
        "bank_branch",
        "bank_account_type",
    }

    for field in string_fields:
        if field in profile_data:
            value = str(profile_data.get(field) or "").strip()
            if field == "bank_ifsc":
                value = value.upper()
            if field == "payment_method":
                value = value.lower()
            if field == "bank_account_type":
                value = value.lower()
            setattr(profile, field, value)

    list_fields = {
        "languages": "languages",
        "collection_capabilities": "collection_capabilities",
        "device_availability": "device_availability",
        "collection_environment": "collection_environment",
    }

    for payload_key, model_field in list_fields.items():
        if payload_key in profile_data:
            raw_values = profile_data.get(payload_key)
            if not isinstance(raw_values, list):
                raw_values = _split_csv(raw_values)
            values = [str(item).strip() for item in raw_values if str(item).strip()]
            allowed = {
                "collection_capabilities": DATA_COLLECTOR_CAPABILITIES,
                "device_availability": DATA_COLLECTOR_DEVICES,
                "collection_environment": DATA_COLLECTOR_ENVIRONMENTS,
            }.get(model_field)
            if allowed is not None and any(item not in allowed for item in values):
                raise HTTPException(status_code=400, detail=f"Invalid value supplied for {model_field.replace('_', ' ')}.")
            setattr(profile, model_field, _join_csv(values))

    if "area_type" in profile_data:
        area_type = str(profile_data.get("area_type") or "").strip().lower()
        if area_type not in DATA_COLLECTOR_AREA_TYPES:
            raise HTTPException(status_code=400, detail="Invalid area type.")
        profile.area_type = area_type

    if "availability_status" in profile_data:
        availability_status = str(profile_data.get("availability_status") or "unspecified").strip().lower()
        if availability_status not in DATA_COLLECTOR_AVAILABILITY_OPTIONS:
            raise HTTPException(status_code=400, detail="Invalid availability status.")
        profile.availability_status = availability_status

    if "availability_hours_per_week" in profile_data:
        raw_hours = profile_data.get("availability_hours_per_week")
        if raw_hours in (None, ""):
            profile.availability_hours_per_week = None
        else:
            try:
                hours = float(raw_hours)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="Hours per week must be numeric.")
            if hours <= 0 or hours > 168:
                raise HTTPException(status_code=400, detail="Hours per week must be between 0 and 168.")
            profile.availability_hours_per_week = hours

    consent_value = profile_data.get("consent_accepted")
    if consent_value is True:
        profile.consent_accepted = True
        profile.consent_version = DATA_COLLECTOR_CONSENT_VERSION
        profile.consent_accepted_at = _utc_now()

    missing = _validate_data_collector_profile(profile)

    if missing:
        profile.onboarding_status = "pending"
        profile.updated_at = _utc_now()
        db.commit()
        db.refresh(profile)
        return {
            "status": "success",
            "profile": _data_collector_profile_payload(profile),
            "complete": False,
            "missing": missing,
        }

    # A complete first submission is immutable until KELYVO Admin unlocks it.
    now = _utc_now()
    profile.onboarding_status = "submitted"
    profile.profile_locked = True
    profile.payment_locked = True
    profile.profile_locked_at = now
    profile.payment_locked_at = now
    profile.submitted_at = now
    profile.updated_at = now
    profile.rejection_reason = None
    db.commit()
    db.refresh(profile)

    return {
        "status": "success",
        "profile": _data_collector_profile_payload(profile),
        "complete": True,
        "submitted": True,
        "message": "Your Data Collector profile has been submitted and locked. KELYVO will review it before collection work is assigned.",
        "missing": [],
    }


@app.get("/api/data-collector/summary")
def get_data_collector_summary(
    request: Request,
    db: Session = Depends(get_db),
):
    require_data_collector_session(request)
    user_id = _get_authenticated_user_id(request)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Data Collector session is not authenticated.")

    profile = _get_data_collector_profile(db, int(user_id))
    pending_count = (
        db.query(models.DataCollectionSubmission)
        .filter(
            models.DataCollectionSubmission.collector_id == int(user_id),
            models.DataCollectionSubmission.status == "pending_qa",
        )
        .count()
    )
    approved_count = (
        db.query(models.DataCollectionSubmission)
        .filter(
            models.DataCollectionSubmission.collector_id == int(user_id),
            models.DataCollectionSubmission.status == "approved",
        )
        .count()
    )
    open_opportunity_count = (
        db.query(models.DataCollectionOpportunity)
        .filter(models.DataCollectionOpportunity.status == "open")
        .count()
    )
    active_assignment_count = (
        db.query(models.DataCollectionOpportunityClaim)
        .filter(
            models.DataCollectionOpportunityClaim.collector_id == int(user_id),
            models.DataCollectionOpportunityClaim.status == "accepted",
        )
        .count()
    )

    return {
        "status": "success",
        "profile_status": str(profile.onboarding_status or "pending") if profile is not None else "pending",
        "opportunities": open_opportunity_count if profile is not None and str(profile.onboarding_status or "pending").lower() == "approved" else 0,
        "active_assignments": active_assignment_count,
        "submissions_pending": pending_count,
        "submissions_approved": approved_count,
    }


@app.get("/api/admin/data-collectors")
def list_admin_data_collectors(
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_session(request)
    users = (
        db.query(models.User)
        .filter(models.User.role == "data_collector")
        .order_by(models.User.id.desc())
        .all()
    )
    payload = []
    for user in users:
        profile = _get_data_collector_profile(db, int(user.id))
        payload.append({
            "id": int(user.id),
            "email": user.email,
            "status": str(profile.onboarding_status or "pending") if profile else "pending",
            "profile_locked": bool(profile.profile_locked) if profile else False,
            "payment_locked": bool(profile.payment_locked) if profile else False,
            "display_name": str(profile.display_name or "") if profile else "",
            "country": str(profile.country or "") if profile else "",
            "region": str(profile.region or "") if profile else "",
            "city": str(profile.city or "") if profile else "",
            "pin_code": str(profile.pin_code or "") if profile else "",
            "area_type": str(profile.area_type or "") if profile else "",
            "timezone": str(profile.timezone or "") if profile else "",
            "languages": _split_csv(profile.languages) if profile else [],
            "capabilities": _split_csv(profile.collection_capabilities) if profile else [],
            "devices": _split_csv(profile.device_availability) if profile else [],
            "environments": _split_csv(profile.collection_environment) if profile else [],
            "availability_status": str(profile.availability_status or "") if profile else "",
            "availability_hours_per_week": profile.availability_hours_per_week if profile else None,
            "experience_summary": str(profile.experience_summary or "") if profile else "",
            "payment_method": str(profile.payment_method or "") if profile else "",
            "payment_verified": bool(profile.payment_locked) if profile else False,
            "consent_accepted": bool(profile.consent_accepted) if profile else False,
            "submitted_at": profile.submitted_at.isoformat() if profile and profile.submitted_at else None,
            "reviewed_at": profile.reviewed_at.isoformat() if profile and profile.reviewed_at else None,
            "reviewed_by_user_id": profile.reviewed_by_user_id if profile else None,
            "review_reason": str(profile.rejection_reason or "") if profile else "",
        })
    return {"status": "success", "data_collectors": payload}


@app.patch("/api/admin/data-collectors/{user_id}/review")
async def review_admin_data_collector(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    admin_session = require_admin_session(request)
    user = db.query(models.User).filter(models.User.id == int(user_id), models.User.role == "data_collector").first()
    if user is None:
        raise HTTPException(status_code=404, detail="Data Collector account not found.")
    profile = _get_data_collector_profile(db, int(user.id))
    if profile is None:
        raise HTTPException(status_code=500, detail="Data Collector profile is unavailable.")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    action = str(payload.get("action") or "").strip().lower()
    reason = str(payload.get("reason") or "").strip()
    reviewer_id = _get_authenticated_user_id(request) or admin_session.get("user_id")

    if action == "approve":
        missing = _validate_data_collector_profile(profile)
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"This profile is not complete. Missing: {', '.join(missing)}.",
            )
        profile.onboarding_status = "approved"
        profile.reviewed_at = _utc_now()
        profile.reviewed_by_user_id = int(reviewer_id) if reviewer_id is not None else None
        profile.rejection_reason = None
        # Approval keeps the submitted profile and payment details locked.
        profile.profile_locked = True
        profile.payment_locked = True
    elif action in {"reject", "request_changes"}:
        if not reason:
            label = "rejection reason" if action == "reject" else "change request reason"
            raise HTTPException(status_code=400, detail=f"A {label} is required.")
        profile.onboarding_status = "rejected" if action == "reject" else "changes_requested"
        profile.reviewed_at = _utc_now()
        profile.reviewed_by_user_id = int(reviewer_id) if reviewer_id is not None else None
        profile.rejection_reason = reason
        # Both actions return the collector to an editable state; the status
        # tells the collector whether the result was a rejection or a change request.
        profile.profile_locked = False
        profile.payment_locked = False
        profile.profile_locked_at = None
        profile.payment_locked_at = None
        profile.profile_unlock_reason = reason
        profile.payment_unlock_reason = reason
    else:
        raise HTTPException(status_code=400, detail="Action must be approve, reject, or request_changes.")

    profile.updated_at = _utc_now()
    db.commit()
    db.refresh(profile)
    return {"status": "success", "profile": _data_collector_profile_payload(profile)}


@app.patch("/api/admin/data-collectors/{user_id}/unlock")
async def unlock_admin_data_collector(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_session(request)
    user = db.query(models.User).filter(models.User.id == int(user_id), models.User.role == "data_collector").first()
    if user is None:
        raise HTTPException(status_code=404, detail="Data Collector account not found.")
    profile = _get_data_collector_profile(db, int(user.id))
    if profile is None:
        raise HTTPException(status_code=500, detail="Data Collector profile is unavailable.")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    section = str(payload.get("section") or "profile").strip().lower()
    reason = str(payload.get("reason") or "").strip()
    if not reason:
        raise HTTPException(status_code=400, detail="An unlock reason is required.")

    if section == "profile":
        profile.profile_locked = False
        profile.profile_locked_at = None
        profile.profile_unlock_reason = reason
    elif section == "payment":
        profile.payment_locked = False
        profile.payment_locked_at = None
        profile.payment_unlock_reason = reason
    elif section == "all":
        profile.profile_locked = False
        profile.payment_locked = False
        profile.profile_locked_at = None
        profile.payment_locked_at = None
        profile.profile_unlock_reason = reason
        profile.payment_unlock_reason = reason
    else:
        raise HTTPException(status_code=400, detail="Section must be profile, payment, or all.")

    profile.onboarding_status = "pending"
    profile.updated_at = _utc_now()
    db.commit()
    db.refresh(profile)
    return {"status": "success", "profile": _data_collector_profile_payload(profile)}


# ============================================================
# DATA COLLECTION SUBMISSION PIPELINE
# ============================================================

DATA_COLLECTION_SUBMISSION_STATUSES = {
    "pending_qa",
    "approved",
    "rejected",
}


def _data_collection_submission_asset_payload(asset):
    return {
        "id": str(asset.id),
        "submission_id": str(asset.submission_id),
        "original_filename": str(asset.original_filename or ""),
        "stored_filename": str(asset.stored_filename or ""),
        "storage_provider": str(asset.storage_provider or ""),
        "storage_reference": str(asset.storage_reference or ""),
        "mime_type": asset.mime_type,
        "size_bytes": int(asset.size_bytes) if asset.size_bytes is not None else None,
        "checksum_sha256": asset.checksum_sha256,
        "status": str(asset.status or "active"),
        "uploaded_by_user_id": int(asset.uploaded_by_user_id),
        "created_at": asset.created_at.isoformat() if asset.created_at else None,
    }


def _data_collection_submission_payload(submission, collector_email=None):
    opportunity_id = (
        str(submission.project_id).strip()
        if submission.project_id is not None and str(submission.project_id).strip()
        else None
    )
    return {
        "id": str(submission.id),
        "collector_id": int(submission.collector_id),
        "collector_email": collector_email,
        "project_id": opportunity_id,
        "opportunity_id": opportunity_id,
        "submission_type": str(submission.submission_type or "unclassified"),
        "file_reference": submission.file_reference,
        "status": str(submission.status or "pending_qa"),
        "qa_status": str(submission.qa_status or "pending"),
        "qa_reviewer_id": int(submission.qa_reviewer_id) if submission.qa_reviewer_id is not None else None,
        "qa_notes": submission.qa_notes,
        "submitted_at": submission.submitted_at.isoformat() if submission.submitted_at else None,
        "reviewed_at": submission.reviewed_at.isoformat() if submission.reviewed_at else None,
        "created_at": submission.created_at.isoformat() if submission.created_at else None,
        "updated_at": submission.updated_at.isoformat() if submission.updated_at else None,
        "assets": [
            _data_collection_submission_asset_payload(asset)
            for asset in getattr(submission, "assets", [])
            if str(asset.status or "active").lower() != "deleted"
        ],
    }


@app.get("/api/data-collector/submissions")
def list_data_collector_submissions(
    request: Request,
    db: Session = Depends(get_db),
):
    require_data_collector_session(request)
    user_id = _get_authenticated_user_id(request)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Data Collector session is not authenticated.")

    rows = (
        db.query(models.DataCollectionSubmission)
        .filter(models.DataCollectionSubmission.collector_id == int(user_id))
        .order_by(models.DataCollectionSubmission.created_at.desc())
        .all()
    )
    return {
        "status": "success",
        "submissions": [
            _data_collection_submission_payload(row)
            for row in rows
        ],
    }


@app.post("/api/data-collector/submissions")
async def create_data_collector_submission(
    request: Request,
    db: Session = Depends(get_db),
):
    require_data_collector_session(request)
    user_id = _get_authenticated_user_id(request)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Data Collector session is not authenticated.")

    profile = _get_data_collector_profile(db, int(user_id))
    if profile is None:
        raise HTTPException(status_code=404, detail="Data Collector profile not found.")
    if str(profile.onboarding_status or "pending").lower() != "approved":
        raise HTTPException(status_code=403, detail="Your Data Collector profile must be approved before submitting collection work.")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    submission_type = str(payload.get("submission_type") or "").strip().lower()
    opportunity_id = str(payload.get("opportunity_id") or payload.get("project_id") or "").strip()
    file_reference = str(payload.get("file_reference") or "").strip()

    allowed_submission_types = {
        "audio",
        "image",
        "video",
        "text",
        "document",
        "other",
    }
    if submission_type not in allowed_submission_types:
        raise HTTPException(status_code=400, detail="Select a valid collection submission type.")
    if not opportunity_id:
        raise HTTPException(status_code=400, detail="Select an accepted collection opportunity.")
    if not file_reference:
        raise HTTPException(status_code=400, detail="Enter the collection data reference before submitting.")
    if len(file_reference) > 4000:
        raise HTTPException(status_code=400, detail="The collection data reference is too long.")

    opportunity = (
        db.query(models.DataCollectionOpportunity)
        .filter(models.DataCollectionOpportunity.id == opportunity_id)
        .first()
    )
    if opportunity is None:
        raise HTTPException(status_code=404, detail="Collection opportunity not found.")

    claim = (
        db.query(models.DataCollectionOpportunityClaim)
        .filter(
            models.DataCollectionOpportunityClaim.opportunity_id == opportunity_id,
            models.DataCollectionOpportunityClaim.collector_id == int(user_id),
            models.DataCollectionOpportunityClaim.status == "accepted",
        )
        .first()
    )
    if claim is None:
        raise HTTPException(status_code=403, detail="You must accept this collection opportunity before submitting work for it.")

    duplicate_pending = (
        db.query(models.DataCollectionSubmission)
        .filter(
            models.DataCollectionSubmission.collector_id == int(user_id),
            models.DataCollectionSubmission.project_id == opportunity_id,
            models.DataCollectionSubmission.status == "pending_qa",
        )
        .first()
    )
    if duplicate_pending is not None:
        raise HTTPException(status_code=409, detail="This opportunity already has a collection submission pending QA.")

    now = _utc_now()
    submission = models.DataCollectionSubmission(
        collector_id=int(user_id),
        project_id=opportunity_id,
        submission_type=submission_type,
        file_reference=file_reference,
        status="pending_qa",
        qa_status="pending",
        submitted_at=now,
        created_at=now,
        updated_at=now,
    )
    db.add(submission)
    db.commit()
    db.refresh(submission)

    return {
        "status": "success",
        "submission": _data_collection_submission_payload(submission),
        "message": "Collection submission created and placed in the QA queue.",
    }


@app.post("/api/data-collector/submissions/{submission_id}/assets")
async def upload_data_collector_submission_asset(
    submission_id: str,
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    require_data_collector_session(request)
    user_id = _get_authenticated_user_id(request)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Data Collector session is not authenticated.")

    profile = _get_data_collector_profile(db, int(user_id))
    if profile is None:
        raise HTTPException(status_code=404, detail="Data Collector profile not found.")
    if str(profile.onboarding_status or "pending").lower() != "approved":
        raise HTTPException(status_code=403, detail="Your Data Collector profile must be approved before uploading collection work.")

    submission = (
        db.query(models.DataCollectionSubmission)
        .filter(
            models.DataCollectionSubmission.id == str(submission_id),
            models.DataCollectionSubmission.collector_id == int(user_id),
        )
        .first()
    )
    if submission is None:
        raise HTTPException(status_code=404, detail="Collection submission not found.")

    if str(submission.status or "").lower() != "pending_qa":
        raise HTTPException(status_code=409, detail="Files can only be attached while the submission is pending QA.")

    if not file.filename:
        raise HTTPException(status_code=400, detail="A file is required.")

    try:
        stored = storage_provider.save_file(file, folder=f"submissions/{submission.id}")
        storage_path = str(stored.get("path") or "").strip()
        if not storage_path:
            raise RuntimeError("Storage provider returned no storage path.")

        size_bytes = os.path.getsize(storage_path)
        checksum = hashlib.sha256()
        with open(storage_path, "rb") as stored_file:
            for chunk in iter(lambda: stored_file.read(1024 * 1024), b""):
                checksum.update(chunk)

        now = _utc_now()
        asset = models.DataCollectionSubmissionAsset(
            submission_id=str(submission.id),
            original_filename=str(file.filename),
            stored_filename=str(stored.get("stored_name") or Path(storage_path).name),
            storage_provider=str(stored.get("storage") or "local"),
            storage_reference=storage_path,
            mime_type=str(file.content_type or "application/octet-stream"),
            size_bytes=int(size_bytes),
            checksum_sha256=checksum.hexdigest(),
            status="active",
            uploaded_by_user_id=int(user_id),
            created_at=now,
        )
        db.add(asset)
        db.commit()
        db.refresh(asset)

        return {
            "status": "success",
            "asset": _data_collection_submission_asset_payload(asset),
            "submission": _data_collection_submission_payload(submission),
            "message": "Collection asset attached to the submission.",
        }
    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        try:
            if 'storage_path' in locals() and storage_path:
                storage_provider.delete_file(storage_path)
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Could not store the collection file: {exc}")


@app.get("/api/qa/data-collection-submissions")
def list_qa_data_collection_submissions(
    request: Request,
    status: str = "all",
    db: Session = Depends(get_db),
):
    require_qa_session(request)
    normalized_status = str(status or "all").strip().lower()
    allowed_filters = {"all", "pending_qa", "approved", "rejected"}
    if normalized_status not in allowed_filters:
        raise HTTPException(status_code=400, detail="Invalid data collection QA status filter.")

    query = (
        db.query(models.DataCollectionSubmission, models.User.email)
        .join(models.User, models.User.id == models.DataCollectionSubmission.collector_id)
    )
    if normalized_status != "all":
        query = query.filter(models.DataCollectionSubmission.status == normalized_status)

    rows = query.order_by(models.DataCollectionSubmission.created_at.desc()).all()
    return {
        "status": "success",
        "submissions": [
            _data_collection_submission_payload(submission, collector_email=email)
            for submission, email in rows
        ],
    }


@app.patch("/api/qa/data-collection-submissions/{submission_id}/review")
async def review_qa_data_collection_submission(
    submission_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    reviewer_session = require_qa_session(request)
    reviewer_id = _get_authenticated_user_id(request)
    if reviewer_id is None:
        raise HTTPException(status_code=401, detail="QA session is not authenticated.")

    submission = (
        db.query(models.DataCollectionSubmission)
        .filter(models.DataCollectionSubmission.id == str(submission_id))
        .first()
    )
    if submission is None:
        raise HTTPException(status_code=404, detail="Data collection submission not found.")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    action = str(payload.get("action") or "").strip().lower()
    notes = str(payload.get("notes") or "").strip()
    if action not in {"approve", "reject"}:
        raise HTTPException(status_code=400, detail="Action must be approve or reject.")
    if action == "reject" and not notes:
        raise HTTPException(status_code=400, detail="A QA note is required when rejecting a collection submission.")

    now = _utc_now()
    submission.qa_reviewer_id = int(reviewer_id)
    submission.qa_notes = notes or None
    submission.reviewed_at = now
    submission.updated_at = now
    submission.qa_status = "approved" if action == "approve" else "rejected"
    submission.status = "approved" if action == "approve" else "rejected"

    db.commit()
    db.refresh(submission)

    collector = db.query(models.User).filter(models.User.id == int(submission.collector_id)).first()
    return {
        "status": "success",
        "submission": _data_collection_submission_payload(
            submission,
            collector_email=collector.email if collector else None,
        ),
        "message": "Collection submission approved." if action == "approve" else "Collection submission rejected.",
    }


# ============================================================
# DATA COLLECTION OPPORTUNITY MANAGEMENT
# ============================================================

DATA_COLLECTION_OPPORTUNITY_STATUSES = {"draft", "open", "closed"}


def _json_list_or_empty(value):
    if not value:
        return []
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except Exception:
        pass
    return _split_csv(value)


def _normalize_requirement_list(value, max_items=50):
    if isinstance(value, list):
        raw = value
    else:
        raw = str(value or "").split(",")
    result = []
    seen = set()
    for item in raw:
        text_value = str(item or "").strip()
        if not text_value:
            continue
        key = text_value.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(text_value)
        if len(result) >= max_items:
            break
    return result


def _data_collection_opportunity_payload(db: Session, opportunity, collector_id=None):
    claim_query = db.query(models.DataCollectionOpportunityClaim).filter(
        models.DataCollectionOpportunityClaim.opportunity_id == str(opportunity.id)
    )
    claims = claim_query.all()
    accepted_claims = [row for row in claims if str(row.status or "").lower() == "accepted"]
    collector_claim = None
    if collector_id is not None:
        collector_claim = next(
            (row for row in claims if int(row.collector_id) == int(collector_id)),
            None,
        )

    return {
        "id": str(opportunity.id),
        "title": str(opportunity.title or ""),
        "description": str(opportunity.description or ""),
        "required_languages": _json_list_or_empty(opportunity.required_languages),
        "required_capabilities": _json_list_or_empty(opportunity.required_capabilities),
        "required_devices": _json_list_or_empty(opportunity.required_devices),
        "required_environments": _json_list_or_empty(opportunity.required_environments),
        "collectors_needed": int(opportunity.collectors_needed or 1),
        "collectors_claimed": len(accepted_claims),
        "status": str(opportunity.status or "open"),
        "claimed_by_current_collector": bool(collector_claim and str(collector_claim.status or "").lower() == "accepted"),
        "current_claim_status": str(collector_claim.status) if collector_claim else None,
        "created_by_user_id": int(opportunity.created_by_user_id),
        "created_at": opportunity.created_at.isoformat() if opportunity.created_at else None,
        "updated_at": opportunity.updated_at.isoformat() if opportunity.updated_at else None,
    }


@app.get("/api/data-collector/opportunities")
def list_data_collector_opportunities(
    request: Request,
    db: Session = Depends(get_db),
):
    require_data_collector_session(request)
    collector_id = _get_authenticated_user_id(request)
    if collector_id is None:
        raise HTTPException(status_code=401, detail="Data Collector session is not authenticated.")

    profile = _get_data_collector_profile(db, int(collector_id))
    if profile is None or str(profile.onboarding_status or "pending").lower() != "approved":
        return {"status": "success", "opportunities": []}

    rows = (
        db.query(models.DataCollectionOpportunity)
        .filter(models.DataCollectionOpportunity.status == "open")
        .order_by(models.DataCollectionOpportunity.created_at.desc())
        .all()
    )
    opportunities = []
    for opportunity in rows:
        claimed = (
            db.query(models.DataCollectionOpportunityClaim)
            .filter(
                models.DataCollectionOpportunityClaim.opportunity_id == str(opportunity.id),
                models.DataCollectionOpportunityClaim.collector_id == int(collector_id),
                models.DataCollectionOpportunityClaim.status == "accepted",
            )
            .first()
        )
        accepted_count = (
            db.query(models.DataCollectionOpportunityClaim)
            .filter(
                models.DataCollectionOpportunityClaim.opportunity_id == str(opportunity.id),
                models.DataCollectionOpportunityClaim.status == "accepted",
            )
            .count()
        )
        if not claimed and accepted_count >= int(opportunity.collectors_needed or 1):
            continue
        opportunities.append(_data_collection_opportunity_payload(db, opportunity, collector_id=int(collector_id)))

    return {"status": "success", "opportunities": opportunities}


@app.post("/api/data-collector/opportunities/{opportunity_id}/accept")
def accept_data_collection_opportunity(
    opportunity_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    require_data_collector_session(request)
    collector_id = _get_authenticated_user_id(request)
    if collector_id is None:
        raise HTTPException(status_code=401, detail="Data Collector session is not authenticated.")

    profile = _get_data_collector_profile(db, int(collector_id))
    if profile is None or str(profile.onboarding_status or "pending").lower() != "approved":
        raise HTTPException(status_code=403, detail="Your Data Collector profile must be approved before accepting collection opportunities.")

    opportunity = (
        db.query(models.DataCollectionOpportunity)
        .filter(models.DataCollectionOpportunity.id == str(opportunity_id))
        .first()
    )
    if opportunity is None:
        raise HTTPException(status_code=404, detail="Collection opportunity not found.")
    if str(opportunity.status or "open").lower() != "open":
        raise HTTPException(status_code=400, detail="This collection opportunity is no longer open.")

    existing = (
        db.query(models.DataCollectionOpportunityClaim)
        .filter(
            models.DataCollectionOpportunityClaim.opportunity_id == str(opportunity.id),
            models.DataCollectionOpportunityClaim.collector_id == int(collector_id),
        )
        .first()
    )
    if existing is not None and str(existing.status or "").lower() == "accepted":
        return {"status": "success", "claim": {"id": str(existing.id), "status": "accepted"}, "message": "Opportunity already accepted."}

    accepted_count = (
        db.query(models.DataCollectionOpportunityClaim)
        .filter(
            models.DataCollectionOpportunityClaim.opportunity_id == str(opportunity.id),
            models.DataCollectionOpportunityClaim.status == "accepted",
        )
        .count()
    )
    if accepted_count >= int(opportunity.collectors_needed or 1):
        raise HTTPException(status_code=409, detail="This opportunity has already reached its collector capacity.")

    now = _utc_now()
    if existing is None:
        existing = models.DataCollectionOpportunityClaim(
            opportunity_id=str(opportunity.id),
            collector_id=int(collector_id),
            status="accepted",
            claimed_at=now,
            updated_at=now,
        )
        db.add(existing)
    else:
        existing.status = "accepted"
        existing.claimed_at = now
        existing.updated_at = now

    db.commit()
    db.refresh(existing)
    return {"status": "success", "claim": {"id": str(existing.id), "status": "accepted"}, "message": "Collection opportunity accepted."}


@app.get("/api/admin/data-collection-opportunities")
def list_admin_data_collection_opportunities(
    request: Request,
    status: str = "all",
    db: Session = Depends(get_db),
):
    require_admin_session(request)
    normalized_status = str(status or "all").strip().lower()
    if normalized_status not in {"all", *DATA_COLLECTION_OPPORTUNITY_STATUSES}:
        raise HTTPException(status_code=400, detail="Invalid collection opportunity status filter.")

    query = db.query(models.DataCollectionOpportunity)
    if normalized_status != "all":
        query = query.filter(models.DataCollectionOpportunity.status == normalized_status)
    rows = query.order_by(models.DataCollectionOpportunity.created_at.desc()).all()
    return {
        "status": "success",
        "opportunities": [_data_collection_opportunity_payload(db, row) for row in rows],
    }


@app.post("/api/admin/data-collection-opportunities")
async def create_admin_data_collection_opportunity(
    request: Request,
    db: Session = Depends(get_db),
):
    admin_session = require_admin_session(request)
    admin_id = _get_authenticated_user_id(request)
    if admin_id is None:
        raise HTTPException(status_code=401, detail="Admin session is not authenticated.")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    title = str(payload.get("title") or "").strip()
    description = str(payload.get("description") or "").strip()
    status = str(payload.get("status") or "open").strip().lower()
    if not title:
        raise HTTPException(status_code=400, detail="Opportunity title is required.")
    if status not in DATA_COLLECTION_OPPORTUNITY_STATUSES:
        raise HTTPException(status_code=400, detail="Invalid opportunity status.")

    try:
        collectors_needed = int(payload.get("collectors_needed") or 1)
    except (TypeError, ValueError):
        collectors_needed = 1
    if collectors_needed < 1 or collectors_needed > 100000:
        raise HTTPException(status_code=400, detail="Collectors needed must be between 1 and 100000.")

    requirements = {
        "required_languages": _normalize_requirement_list(payload.get("required_languages")),
        "required_capabilities": _normalize_requirement_list(payload.get("required_capabilities")),
        "required_devices": _normalize_requirement_list(payload.get("required_devices")),
        "required_environments": _normalize_requirement_list(payload.get("required_environments")),
    }

    now = _utc_now()
    opportunity = models.DataCollectionOpportunity(
        title=title,
        description=description or None,
        required_languages=json.dumps(requirements["required_languages"], ensure_ascii=False),
        required_capabilities=json.dumps(requirements["required_capabilities"], ensure_ascii=False),
        required_devices=json.dumps(requirements["required_devices"], ensure_ascii=False),
        required_environments=json.dumps(requirements["required_environments"], ensure_ascii=False),
        collectors_needed=collectors_needed,
        status=status,
        created_by_user_id=int(admin_id),
        created_at=now,
        updated_at=now,
    )
    db.add(opportunity)
    db.commit()
    db.refresh(opportunity)
    return {
        "status": "success",
        "opportunity": _data_collection_opportunity_payload(db, opportunity),
        "message": "Collection opportunity created.",
    }


@app.patch("/api/admin/data-collection-opportunities/{opportunity_id}")
async def update_admin_data_collection_opportunity(
    opportunity_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_session(request)
    opportunity = (
        db.query(models.DataCollectionOpportunity)
        .filter(models.DataCollectionOpportunity.id == str(opportunity_id))
        .first()
    )
    if opportunity is None:
        raise HTTPException(status_code=404, detail="Collection opportunity not found.")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    if "status" in payload:
        status = str(payload.get("status") or "").strip().lower()
        if status not in DATA_COLLECTION_OPPORTUNITY_STATUSES:
            raise HTTPException(status_code=400, detail="Invalid opportunity status.")
        opportunity.status = status

    if "title" in payload:
        title = str(payload.get("title") or "").strip()
        if not title:
            raise HTTPException(status_code=400, detail="Opportunity title cannot be empty.")
        opportunity.title = title

    if "description" in payload:
        opportunity.description = str(payload.get("description") or "").strip() or None

    if "collectors_needed" in payload:
        try:
            collectors_needed = int(payload.get("collectors_needed"))
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="Collectors needed must be a whole number.")
        if collectors_needed < 1 or collectors_needed > 100000:
            raise HTTPException(status_code=400, detail="Collectors needed must be between 1 and 100000.")
        opportunity.collectors_needed = collectors_needed

    requirement_columns = {
        "required_languages": "required_languages",
        "required_capabilities": "required_capabilities",
        "required_devices": "required_devices",
        "required_environments": "required_environments",
    }
    for payload_key, column_name in requirement_columns.items():
        if payload_key in payload:
            normalized = _normalize_requirement_list(payload.get(payload_key))
            setattr(opportunity, column_name, json.dumps(normalized, ensure_ascii=False))

    opportunity.updated_at = _utc_now()
    db.commit()
    db.refresh(opportunity)
    return {
        "status": "success",
        "opportunity": _data_collection_opportunity_payload(db, opportunity),
        "message": "Collection opportunity updated.",
    }


@app.get("/api/contributor/preferences")
def get_contributor_preferences(request: Request, db: Session = Depends(get_db)):
    require_contributor_session(request)
    user_id = _get_authenticated_user_id(request)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Contributor session is not authenticated.")
    selected = _get_contributor_preferences_payload(db, int(user_id))
    return {
        "status": "success",
        "selected": selected,
        "catalog": _get_preference_catalog_payload(db),
        "complete": bool(selected["interests"] and selected["languages"]),
    }


@app.patch("/api/contributor/preferences")
async def update_contributor_preferences(request: Request, db: Session = Depends(get_db)):
    require_contributor_session(request)
    user_id = _get_authenticated_user_id(request)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Contributor session is not authenticated.")
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    interests = _normalize_preference_values(payload.get("interests"), max_items=20)
    languages = _normalize_preference_values(payload.get("languages"), max_items=50)
    other_languages = _normalize_preference_values(payload.get("other_languages"), max_items=10)
    if not interests:
        raise HTTPException(status_code=400, detail="Select at least one work interest.")
    if not languages and not other_languages:
        raise HTTPException(status_code=400, detail="Select at least one language.")

    valid_interest_rows = db.execute(
        contributor_work_interest_catalog.select()
        .where(contributor_work_interest_catalog.c.status == "active")
        .where(contributor_work_interest_catalog.c.slug.in_(interests))
    ).mappings().all()
    valid_interest_slugs = {str(row["slug"]) for row in valid_interest_rows}
    if any(item not in valid_interest_slugs for item in interests):
        raise HTTPException(status_code=400, detail="One or more selected work interests are invalid. Refresh and try again.")

    language_catalog = {code.casefold(): (code, name) for code, name in CONTRIBUTOR_LANGUAGE_CATALOG}
    normalized_languages = []
    seen_codes = set()
    for item in languages + other_languages:
        key = str(item).strip().casefold()
        if not key:
            continue
        if key in language_catalog:
            code, name = language_catalog[key]
        else:
            slug = re.sub(r"[^a-z0-9]+", "-", key).strip("-")[:32]
            code = "custom-" + (slug or "language")
            name = str(item).strip()
        if code.casefold() in seen_codes:
            continue
        seen_codes.add(code.casefold())
        normalized_languages.append((code, name))

    now_iso = _utc_now().isoformat()
    db.execute(contributor_work_interests.delete().where(contributor_work_interests.c.user_id == int(user_id)))
    db.execute(
        contributor_work_interests.insert(),
        [{"user_id": int(user_id), "interest_id": int(row["id"]), "created_at": now_iso} for row in valid_interest_rows],
    )
    db.execute(contributor_interest_languages.delete().where(contributor_interest_languages.c.user_id == int(user_id)))
    if normalized_languages:
        db.execute(
            contributor_interest_languages.insert(),
            [{"user_id": int(user_id), "language_code": code, "language_name": name, "created_at": now_iso} for code, name in normalized_languages],
        )
    _write_audit_log(
        db,
        request,
        "contributor_work_preferences_updated",
        "contributor_preferences",
        int(user_id),
        {"interest_slugs": interests, "language_codes": [code for code, _ in normalized_languages]},
    )
    db.commit()
    selected = _get_contributor_preferences_payload(db, int(user_id))
    return {
        "status": "success",
        "message": "Work preferences saved.",
        "selected": selected,
        "catalog": _get_preference_catalog_payload(db),
        "complete": bool(selected["interests"] and selected["languages"]),
    }


@app.get("/api/contributor/profile")
def get_contributor_own_profile(request: Request, db: Session = Depends(get_db)):
    require_contributor_session(request)
    email = normalize_email(read_session_token(request).get("email", ""))
    user = db.query(models.User).filter(models.User.email == email).first()
    if user is None:
        raise HTTPException(status_code=404, detail="Contributor account not found.")
    payload = _get_workforce_profile_payload(db, user, include_payment=True)
    completion = _get_contributor_profile_completion(db, user)
    db.commit()
    return {"status": "success", "profile": payload["profile"], "details": payload["details"], "skills": payload["skills"], "languages": payload["languages"], "performance": payload["performance"], "completion": completion}


@app.patch("/api/contributor/profile")
async def update_contributor_own_profile(request: Request, db: Session = Depends(get_db)):
    require_contributor_session(request)
    session = read_session_token(request) or {}
    email = normalize_email(session.get("email", ""))
    user = db.query(models.User).filter(models.User.email == email).first()
    if user is None:
        raise HTTPException(status_code=404, detail="Contributor account not found.")

    try:
        payload = await request.json()
    except Exception:
        payload = {}
    profile_data = payload.get("profile") or {}
    detail_data = payload.get("details") or {}
    skills = payload.get("skills")
    languages = payload.get("languages")
    detail = _ensure_workforce_detail_row(db, int(user.id))
    detail_map = dict(detail._mapping) if detail is not None else {}
    profile_locked = bool(detail_map.get("profile_locked"))
    payment_locked = bool(detail_map.get("payment_locked"))

    # Skills and languages are now collected through Contributor Work Preferences.
    # Keep accepting these legacy fields from older/admin clients for backwards
    # compatibility, but they are no longer required for workforce-profile completion.
    submitted_skills = _split_csv(skills) if skills is not None else None
    submitted_languages = _split_csv(languages) if languages is not None else None

    profile_fields = ("display_name", "country", "region", "city", "timezone")
    profile_detail_fields = ("workforce_type", "dialects", "domain_expertise", "experience_summary", "availability_hours_per_week", "availability_status", "preferred_shift")
    payment_keys = {"payment_method", "upi_id", "bank_account_name", "bank_account_number", "bank_ifsc", "bank_name", "bank_branch", "bank_account_type"}

    if profile_locked and (any(key in profile_data for key in profile_fields) or skills is not None or languages is not None or any(key in detail_data for key in profile_detail_fields)):
        raise HTTPException(status_code=403, detail="Your workforce profile is locked. Ask a KELYVO Admin to unlock it before making changes.")
    if payment_locked and any(key in detail_data for key in payment_keys):
        raise HTTPException(status_code=403, detail="Your payment details are locked. Ask a KELYVO Admin to unlock them before making changes.")

    if not profile_locked:
        profile = _ensure_contributor_profile(db, user)
        if profile is not None:
            for field in profile_fields:
                if field in profile_data:
                    setattr(profile, field, str(profile_data.get(field) or "").strip())
        allowed_detail_fields = set(profile_detail_fields)
        values = {}
        for field in allowed_detail_fields:
            if field not in detail_data:
                continue
            value = detail_data.get(field)
            if field == "availability_hours_per_week" and value not in (None, ""):
                try:
                    value = float(value)
                except (TypeError, ValueError):
                    raise HTTPException(status_code=400, detail="Hours per week must be numeric.")
                if value <= 0 or value > 168:
                    raise HTTPException(status_code=400, detail="Hours per week must be between 0 and 168.")
            if field == "dialects":
                value = _join_csv(value if isinstance(value, list) else _split_csv(value))
            elif value is not None:
                value = str(value).strip()
            values[field] = value
        if values:
            values["updated_at"] = _utc_now().isoformat()
            db.execute(workforce_profile_details.update().where(workforce_profile_details.c.user_id == int(user.id)).values(**values))
        if submitted_skills is not None:
            _sync_named_taxonomy_links(db, int(user.id), "skills", submitted_skills, "skill")
        if submitted_languages is not None:
            _sync_named_taxonomy_links(db, int(user.id), "languages", submitted_languages, "language")
        db.flush()

    if not payment_locked and any(key in detail_data for key in payment_keys):
        merged_payment = {key: detail_map.get(key) for key in payment_keys}
        merged_payment.update({key: detail_data.get(key) for key in payment_keys if key in detail_data})
        normalized_payment = _validate_payment_payload(merged_payment)
        db.execute(workforce_profile_details.update().where(workforce_profile_details.c.user_id == int(user.id)).values(
            payment_method=normalized_payment["payment_method"], upi_id=normalized_payment["upi_id"],
            bank_account_name=normalized_payment["bank_account_name"], bank_account_number=normalized_payment["bank_account_number"],
            bank_ifsc=normalized_payment["bank_ifsc"], bank_name=normalized_payment["bank_name"], bank_branch=normalized_payment["bank_branch"],
            bank_account_type=normalized_payment["bank_account_type"], updated_at=_utc_now().isoformat()
        ))

    completion = _get_contributor_profile_completion(
        db,
        user,
        skills_override=submitted_skills if not profile_locked else None,
        languages_override=submitted_languages if not profile_locked else None,
    )
    if not completion["complete"]:
        raise HTTPException(status_code=400, detail="Complete all required workforce and payment fields before submitting. Missing: " + ", ".join(completion["missing"]))

    now_iso = _utc_now().isoformat()
    profile_lock_reason = str(detail_map.get("profile_unlock_reason") or "").strip()
    payment_lock_reason = str(detail_map.get("payment_unlock_reason") or "").strip()
    if not profile_locked:
        profile_history_reason = (
            "Contributor updated workforce profile after Admin unlock: "
            + profile_lock_reason
            if profile_lock_reason
            else "Contributor submitted workforce profile"
        )
        db.execute(workforce_profile_details.update().where(workforce_profile_details.c.user_id == int(user.id)).values(profile_locked=True, profile_locked_at=now_iso, profile_unlock_reason=None))
        _snapshot_workforce_record(db, int(user.id), "profile", int(user.id), profile_history_reason)
    if not payment_locked:
        payment_history_reason = (
            "Contributor updated payment details after Admin unlock: "
            + payment_lock_reason
            if payment_lock_reason
            else "Contributor submitted payment details"
        )
        db.execute(workforce_profile_details.update().where(workforce_profile_details.c.user_id == int(user.id)).values(payment_locked=True, payment_locked_at=now_iso, payment_unlock_reason=None))
        _snapshot_workforce_record(db, int(user.id), "payment", int(user.id), payment_history_reason)

    _write_audit_log(db, request, "contributor_profile_submitted_and_locked", "contributor_profile", user.id, {
        "profile_locked": True, "payment_locked": True,
        "previous_profile_locked": profile_locked, "previous_payment_locked": payment_locked,
    })
    db.commit(); db.refresh(user)
    profile_payload = _get_workforce_profile_payload(db, user, include_payment=True)
    completion = _get_contributor_profile_completion(db, user)
    db.commit()
    return {"status": "success", "message": "Profile submitted and locked.", "profile": profile_payload["profile"], "details": profile_payload["details"], "skills": profile_payload["skills"], "languages": profile_payload["languages"], "performance": profile_payload["performance"], "completion": completion}


def _get_workforce_profile_payload(db, user, include_payment=False):
    profile = _ensure_contributor_profile(db, user)
    detail = _ensure_workforce_detail_row(db, int(user.id))

    skills = []
    languages = []
    if hasattr(models, "ContributorSkill") and hasattr(models, "Skill"):
        rows = (
            db.query(models.ContributorSkill, models.Skill)
            .join(models.Skill, models.Skill.id == models.ContributorSkill.skill_id)
            .filter(models.ContributorSkill.user_id == int(user.id))
            .all()
        )
        for link, skill in rows:
            skills.append({
                "id": str(skill.id),
                "name": str(skill.name or ""),
                "category": str(skill.category or ""),
                "proficiency": str(link.proficiency or ""),
                "score": link.score,
                "verified": bool(link.verified),
            })
    if hasattr(models, "ContributorLanguage") and hasattr(models, "Language"):
        rows = (
            db.query(models.ContributorLanguage, models.Language)
            .join(models.Language, models.Language.id == models.ContributorLanguage.language_id)
            .filter(models.ContributorLanguage.user_id == int(user.id))
            .all()
        )
        for link, language in rows:
            languages.append({
                "id": str(language.id),
                "name": str(language.name or ""),
                "code": str(language.code or ""),
                "proficiency": str(link.proficiency or ""),
                "is_native": bool(link.is_native),
                "verified": bool(link.verified),
            })

    submissions = (
        db.query(models.TaskSubmission)
        .filter(models.TaskSubmission.user_id == int(user.id))
        .all()
    )
    total_submissions = len(submissions)
    passed = sum(1 for item in submissions if str(item.status or "").upper() == "PASSED")
    returned = sum(1 for item in submissions if str(item.status or "").upper() == "FAILED")
    qa_pass_rate = (passed / total_submissions * 100.0) if total_submissions else 0.0
    revision_rate = (returned / total_submissions * 100.0) if total_submissions else 0.0

    detail_map = dict(detail._mapping) if detail is not None else {}
    project_eligibility = _split_csv(detail_map.get("project_eligibility"))

    return {
        "user": {
            "id": int(user.id),
            "email": user.email,
            "role": str(user.role or "contributor").strip().lower(),
            "account_status": str(
                (_get_user_membership(db, int(user.id)).status
                 if _get_user_membership(db, int(user.id)) is not None
                 else "active")
            ).strip().lower(),
        },
        "profile": {
            "display_name": str(getattr(profile, "display_name", "") or ""),
            "country": str(getattr(profile, "country", "") or ""),
            "region": str(getattr(profile, "region", "") or ""),
            "city": str(getattr(profile, "city", "") or ""),
            "timezone": str(getattr(profile, "timezone", "") or ""),
            "status": str(getattr(profile, "status", "active") or "active"),
            "onboarding_status": str(getattr(profile, "onboarding_status", "pending") or "pending"),
            "overall_quality_score": float(getattr(profile, "overall_quality_score", 0.0) or 0.0),
        },
        "details": {
            "workforce_type": str(detail_map.get("workforce_type") or ""),
            "dialects": _split_csv(detail_map.get("dialects")),
            "domain_expertise": str(detail_map.get("domain_expertise") or ""),
            "experience_summary": str(detail_map.get("experience_summary") or ""),
            "availability_hours_per_week": detail_map.get("availability_hours_per_week"),
            "availability_status": str(detail_map.get("availability_status") or "unspecified"),
            "preferred_shift": str(detail_map.get("preferred_shift") or ""),
            "worker_tier": str(detail_map.get("worker_tier") or "general"),
            "qualification_status": str(detail_map.get("qualification_status") or "pending"),
            "qualification_score": detail_map.get("qualification_score"),
            "qualification_notes": str(detail_map.get("qualification_notes") or ""),
            "project_eligibility": project_eligibility,
            "test_scores": str(detail_map.get("test_scores") or ""),
            "profile_locked": bool(detail_map.get("profile_locked")),
            "payment_locked": bool(detail_map.get("payment_locked")),
            "profile_locked_at": str(detail_map.get("profile_locked_at") or ""),
            "payment_locked_at": str(detail_map.get("payment_locked_at") or ""),
            "payment_method": str(detail_map.get("payment_method") or "") if include_payment else "",
            "upi_id": str(detail_map.get("upi_id") or "") if include_payment else "",
            "bank_account_name": str(detail_map.get("bank_account_name") or "") if include_payment else "",
            "bank_account_number": str(detail_map.get("bank_account_number") or "") if include_payment else "",
            "bank_ifsc": str(detail_map.get("bank_ifsc") or "") if include_payment else "",
            "bank_name": str(detail_map.get("bank_name") or "") if include_payment else "",
            "bank_branch": str(detail_map.get("bank_branch") or "") if include_payment else "",
            "bank_account_type": str(detail_map.get("bank_account_type") or "") if include_payment else "",
            "bank_account_number_masked": _mask_payment_account(detail_map.get("bank_account_number")) if include_payment else "",
        },
        "skills": skills,
        "languages": languages,
        "performance": {
            "tasks_today": int(user.tasks_today or 0),
            "tasks_week": int(user.tasks_week or 0),
            "tasks_completed": total_submissions,
            "tasks_passed": passed,
            "tasks_returned": returned,
            "qa_pass_rate": round(qa_pass_rate, 2),
            "revision_rate": round(revision_rate, 2),
            "earnings": float(user.earnings or 0.0),
            "reliability_score": round(max(0.0, 100.0 - revision_rate), 2),
        },
    }


def _sync_named_taxonomy_links(db, user_id: int, field_name: str, names, role_filter_name: str):
    names = _split_csv(names) if not isinstance(names, list) else [str(x).strip() for x in names if str(x).strip()]
    names = _split_csv(_join_csv(names))
    if field_name == "skills" and hasattr(models, "ContributorSkill") and hasattr(models, "Skill"):
        db.query(models.ContributorSkill).filter(models.ContributorSkill.user_id == int(user_id)).delete(synchronize_session=False)
        db.flush()
        for name in names:
            skill = db.query(models.Skill).filter(models.Skill.name.ilike(name)).first()
            if skill is None:
                skill = models.Skill(name=name, category="workforce", status="active")
                db.add(skill)
                db.flush()
            db.add(models.ContributorSkill(user_id=int(user_id), skill_id=skill.id, proficiency="qualified", verified=False))
        db.flush()
        return names
    if field_name == "languages" and hasattr(models, "ContributorLanguage") and hasattr(models, "Language"):
        db.query(models.ContributorLanguage).filter(models.ContributorLanguage.user_id == int(user_id)).delete(synchronize_session=False)
        db.flush()
        for name in names:
            code = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:24] or "lang"
            language = db.query(models.Language).filter(models.Language.name.ilike(name)).first()
            if language is None:
                existing = db.query(models.Language).filter(models.Language.code == code).first()
                if existing is not None:
                    language = existing
                else:
                    language = models.Language(name=name, code=code, locale=None, language_family=None, status="active")
                    db.add(language)
                    db.flush()
            db.add(models.ContributorLanguage(user_id=int(user_id), language_id=language.id, proficiency="professional", is_native=False, verified=False))
        db.flush()
        return names
    return []


@app.get("/api/admin/workforce/interest-pools")
def get_admin_workforce_interest_pools(
    request: Request,
    interest: str = "all",
    language: str = "all",
    q: str = "",
    db: Session = Depends(get_db),
):
    require_admin_session(request)
    normalized_interest = str(interest or "all").strip().lower()
    normalized_language = str(language or "all").strip().lower()
    normalized_q = str(q or "").strip().lower()

    catalog = _get_preference_catalog_payload(db)
    all_contributors = db.query(models.User).filter(models.User.role == "contributor").order_by(models.User.id.desc()).all()
    all_user_ids = [int(user.id) for user in all_contributors]

    interest_by_user = {uid: [] for uid in all_user_ids}
    language_by_user = {uid: [] for uid in all_user_ids}

    if all_user_ids:
        interest_rows = db.execute(
            contributor_work_interest_catalog.select()
            .add_columns(contributor_work_interests.c.user_id)
            .join(contributor_work_interests, contributor_work_interest_catalog.c.id == contributor_work_interests.c.interest_id)
            .where(contributor_work_interests.c.user_id.in_(all_user_ids))
            .order_by(contributor_work_interest_catalog.c.sort_order.asc())
        ).mappings().all()
        for row in interest_rows:
            uid = int(row["user_id"]) if row.get("user_id") is not None else None
            if uid in interest_by_user:
                interest_by_user[uid].append({
                    "slug": str(row["slug"] or ""),
                    "name": str(row["name"] or ""),
                    "description": str(row.get("description") or ""),
                })

        language_rows = db.execute(
            contributor_interest_languages.select()
            .where(contributor_interest_languages.c.user_id.in_(all_user_ids))
            .order_by(contributor_interest_languages.c.language_name.asc())
        ).mappings().all()
        for row in language_rows:
            uid = int(row["user_id"]) if row.get("user_id") is not None else None
            if uid in language_by_user:
                language_by_user[uid].append({
                    "code": str(row["language_code"] or ""),
                    "name": str(row["language_name"] or ""),
                })

    matching = []
    contributors_with_preferences = 0
    interest_counts = {item["slug"]: 0 for item in catalog["interests"]}
    language_counts = {item["code"]: 0 for item in catalog["languages"]}

    for user in all_contributors:
        uid = int(user.id)
        interests = interest_by_user.get(uid, [])
        languages = language_by_user.get(uid, [])
        if interests and languages:
            contributors_with_preferences += 1
        for item in interests:
            interest_counts[item["slug"]] = interest_counts.get(item["slug"], 0) + 1
        for item in languages:
            language_counts[item["code"]] = language_counts.get(item["code"], 0) + 1

        interest_slugs = {str(item["slug"]).lower() for item in interests}
        language_codes = {str(item["code"]).lower() for item in languages}
        language_names = {str(item["name"]).lower() for item in languages}
        if normalized_interest not in {"", "all"} and normalized_interest not in interest_slugs:
            continue
        if normalized_language not in {"", "all"} and normalized_language not in language_codes and normalized_language not in language_names:
            continue
        if normalized_q and normalized_q not in str(user.email or "").lower():
            continue
        matching.append({"id": uid, "email": str(user.email or ""), "interests": interests, "languages": languages})

    return {
        "status": "success",
        "filters": {"interest": normalized_interest, "language": normalized_language, "q": normalized_q},
        "summary": {"contributors_with_preferences": contributors_with_preferences, "matching_contributors": len(matching)},
        "interest_counts": interest_counts,
        "language_counts": language_counts,
        "contributors": matching[:250],
        "catalog": catalog,
    }


@app.get("/api/admin/users")
def get_admin_workforce_users(request: Request, q: str = "", role: str = "all", db: Session = Depends(get_db)):
    require_admin_session(request)
    query = db.query(models.User)
    normalized_role = (role or "all").strip().lower()
    normalized_q = (q or "").strip().lower()
    if normalized_role in {"contributor", "data_collector", "qa", "admin"}:
        query = query.filter(models.User.role == normalized_role)
    if normalized_q:
        query = query.filter(models.User.email.ilike(f"%{normalized_q}%"))
    users = query.order_by(models.User.id.desc()).all()
    payload = []
    for user in users:
        membership = _get_user_membership(db, int(user.id))
        role_value = str(user.role or "contributor").strip().lower()
        profile = _ensure_contributor_profile(db, user) if role_value == "contributor" else None
        collector_profile = _get_data_collector_profile(db, int(user.id)) if role_value == "data_collector" else None
        detail = _ensure_workforce_detail_row(db, int(user.id))
        detail_map = dict(detail._mapping) if detail is not None else {}
        qualification_status = (
            str(getattr(profile, "onboarding_status", "pending")) if profile is not None else
            str(getattr(collector_profile, "onboarding_status", "pending")) if collector_profile is not None else
            str(detail_map.get("qualification_status") or "pending")
        )
        display_name = (
            str(getattr(profile, "display_name", "") or "") if profile is not None else
            str(getattr(collector_profile, "display_name", "") or "") if collector_profile is not None else
            ""
        )
        payload.append({
            "id": int(user.id),
            "email": user.email,
            "role": role_value,
            "status": str(membership.status or "active").strip().lower() if membership is not None else "active",
            "tasks_today": int(user.tasks_today or 0),
            "tasks_week": int(user.tasks_week or 0),
            "tasks_passed_qa": int(user.tasks_passed_qa or 0),
            "earnings": float(user.earnings or 0),
            "worker_tier": str(detail_map.get("worker_tier") or "general"),
            "qualification_status": qualification_status,
            "display_name": display_name,
        })
    db.commit()
    return {"status": "success", "users": payload}


@app.post("/api/admin/users")
async def create_admin_workforce_user(request: Request, db: Session = Depends(get_db)):
    require_admin_session(request)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    email = normalize_email(str(payload.get("email") or ""))
    password = str(payload.get("password") or "")
    role = str(payload.get("role") or "contributor").strip().lower()
    if role not in {"contributor", "data_collector", "qa", "admin"}:
        raise HTTPException(status_code=400, detail="Role must be contributor, data_collector, qa, or admin.")
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="A valid email address is required.")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Initial password must be at least 8 characters.")
    if db.query(models.User).filter(models.User.email == email).first() is not None:
        raise HTTPException(status_code=409, detail="Email already registered.")
    new_user = models.User(email=email, hashed_password=hash_password(password), role=role, tasks_today=0, tasks_week=0, tasks_passed_qa=0, earnings=0.0)
    db.add(new_user); db.flush()
    if role == "contributor" and hasattr(models, "ContributorProfile"):
        db.add(models.ContributorProfile(user_id=int(new_user.id), display_name="", country="India", onboarding_status="pending"))
    elif role == "data_collector" and hasattr(models, "DataCollectorProfile"):
        db.add(models.DataCollectorProfile(user_id=int(new_user.id), display_name="", country="India", onboarding_status="pending"))
    _ensure_workforce_detail_row(db, int(new_user.id))
    organization = _get_kelyvo_organization(db)
    if organization is not None and hasattr(models, "OrganizationMember"):
        db.add(models.OrganizationMember(organization_id=organization.id, user_id=int(new_user.id), role=role, status="active"))
    db.commit(); db.refresh(new_user)
    return {"status": "success", "id": int(new_user.id), "email": new_user.email, "role": str(new_user.role or role).strip().lower(), "message": "User created successfully."}


@app.patch("/api/admin/users/{user_id}")
async def update_admin_workforce_user(user_id: int, request: Request, db: Session = Depends(get_db)):
    require_admin_session(request)
    user = db.query(models.User).filter(models.User.id == int(user_id)).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found.")
    if int(user.id) == _get_authenticated_user_id(request):
        raise HTTPException(status_code=400, detail="Your own Admin account cannot be changed from this screen.")
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    requested_role = payload.get("role")
    requested_status = payload.get("status")
    if requested_role is None and requested_status is None:
        raise HTTPException(status_code=400, detail="Provide role or status to update.")
    if requested_role is not None:
        new_role = str(requested_role).strip().lower()
        if new_role not in {"contributor", "data_collector", "qa", "admin"}:
            raise HTTPException(status_code=400, detail="Role must be contributor, data_collector, qa, or admin.")
        old_role = str(user.role or "contributor").strip().lower()
        if old_role == "admin" and new_role != "admin" and _count_active_admins(db) <= 1:
            raise HTTPException(status_code=409, detail="KELYVO must retain at least one Admin account.")
        user.role = new_role
        membership = _get_user_membership(db, int(user.id))
        if membership is not None:
            membership.role = new_role
    if requested_status is not None:
        new_status = str(requested_status).strip().lower()
        if new_status not in {"active", "suspended"}:
            raise HTTPException(status_code=400, detail="Status must be active or suspended.")
        if new_status == "suspended" and str(user.role or "").strip().lower() == "admin" and _count_active_admins(db) <= 1:
            raise HTTPException(status_code=409, detail="The last Admin account cannot be suspended.")
        membership = _get_user_membership(db, int(user.id))
        if membership is None:
            organization = _get_kelyvo_organization(db)
            if organization is not None and hasattr(models, "OrganizationMember"):
                membership = models.OrganizationMember(organization_id=organization.id, user_id=int(user.id), role=str(user.role or "contributor").strip().lower(), status=new_status)
                db.add(membership)
        if membership is not None:
            membership.status = new_status
    db.commit(); db.refresh(user)
    membership = _get_user_membership(db, int(user.id))
    return {"status": "success", "id": int(user.id), "email": user.email, "role": str(user.role or "contributor").strip().lower(), "account_status": str(membership.status or "active").strip().lower() if membership is not None else "active"}


@app.get("/api/admin/workforce/{user_id}")
def get_admin_workforce_profile(user_id: int, request: Request, db: Session = Depends(get_db)):
    require_admin_session(request)
    user = db.query(models.User).filter(models.User.id == int(user_id)).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found.")
    payload = _get_workforce_profile_payload(db, user, include_payment=True)
    db.commit()
    return {"status": "success", **payload}


@app.patch("/api/admin/workforce/{user_id}")
async def update_admin_workforce_profile(user_id: int, request: Request, db: Session = Depends(get_db)):
    require_admin_session(request)
    user = db.query(models.User).filter(models.User.id == int(user_id)).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found.")
    if int(user.id) == _get_authenticated_user_id(request):
        raise HTTPException(status_code=400, detail="Your own Admin account cannot be edited as a workforce profile from this screen.")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    profile_data = payload.get("profile") or {}
    detail_data = payload.get("details") or {}
    skills = payload.get("skills")
    languages = payload.get("languages")

    profile = _ensure_contributor_profile(db, user)
    if profile is not None:
        for field in ("display_name", "country", "region", "city", "timezone", "status", "onboarding_status"):
            if field in profile_data:
                value = str(profile_data.get(field) or "").strip()
                setattr(profile, field, value)
        if "overall_quality_score" in profile_data:
            try:
                profile.overall_quality_score = float(profile_data.get("overall_quality_score") or 0)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="Overall quality score must be numeric.")

    _ensure_workforce_detail_row(db, int(user.id))
    allowed_detail_fields = {
        "workforce_type",
        "dialects", "domain_expertise", "experience_summary",
        "availability_hours_per_week", "availability_status",
        "preferred_shift", "worker_tier", "qualification_status",
        "qualification_score", "qualification_notes",
        "project_eligibility", "test_scores",
    }
    values = {}
    for field in allowed_detail_fields:
        if field not in detail_data:
            continue
        value = detail_data.get(field)
        if field in {"availability_hours_per_week", "qualification_score"} and value not in (None, ""):
            try:
                value = float(value)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail=f"{field} must be numeric.")
        if field in {"dialects", "project_eligibility"}:
            value = _join_csv(value if isinstance(value, list) else _split_csv(value))
        elif value is not None:
            value = str(value).strip()
        values[field] = value
    payment_keys = {"payment_method", "upi_id", "bank_account_name", "bank_account_number", "bank_ifsc", "bank_name", "bank_branch", "bank_account_type"}
    payment_updated = any(key in detail_data for key in payment_keys)
    if payment_updated:
        current_detail = dict(db.execute(workforce_profile_details.select().where(workforce_profile_details.c.user_id == int(user.id))).first()._mapping)
        merged_payment = {key: current_detail.get(key) for key in payment_keys}
        merged_payment.update({key: detail_data.get(key) for key in payment_keys if key in detail_data})
        normalized_payment = _validate_payment_payload(merged_payment)
        values.update({
            "payment_method": normalized_payment["payment_method"],
            "upi_id": normalized_payment["upi_id"],
            "bank_account_name": normalized_payment["bank_account_name"],
            "bank_account_number": normalized_payment["bank_account_number"],
            "bank_ifsc": normalized_payment["bank_ifsc"],
            "bank_name": normalized_payment["bank_name"],
            "bank_branch": normalized_payment["bank_branch"],
            "bank_account_type": normalized_payment["bank_account_type"],
            "payment_locked": True,
            "payment_locked_at": _utc_now().isoformat(),
        })
    if values:
        values["updated_at"] = _utc_now().isoformat()
        db.execute(
            workforce_profile_details.update()
            .where(workforce_profile_details.c.user_id == int(user.id))
            .values(**values)
        )

    if skills is not None:
        _sync_named_taxonomy_links(db, int(user.id), "skills", skills, "skill")
    if languages is not None:
        _sync_named_taxonomy_links(db, int(user.id), "languages", languages, "language")

    profile_updated = bool(profile_data) or skills is not None or languages is not None or bool(values)
    profile_change_reason = str(detail_map.get("profile_unlock_reason") or "").strip()
    payment_change_reason = str(detail_map.get("payment_unlock_reason") or "").strip()
    if profile_updated and not payment_updated:
        reason = (
            "Admin updated workforce profile after unlock: " + profile_change_reason
            if profile_change_reason
            else "Admin updated workforce profile"
        )
        _snapshot_workforce_record(db, int(user.id), "profile", _get_authenticated_user_id(request), reason)
    if payment_updated:
        reason = (
            "Admin updated payment details after unlock: " + payment_change_reason
            if payment_change_reason
            else "Admin updated payment details"
        )
        _snapshot_workforce_record(db, int(user.id), "payment", _get_authenticated_user_id(request), reason)
    _write_audit_log(db, request, "admin_updated_workforce_profile", "contributor_profile", user.id, {"payment_updated": payment_updated})

    db.commit()
    db.refresh(user)
    return {"status": "success", "message": "Workforce profile updated.", **_get_workforce_profile_payload(db, user, include_payment=True)}


@app.post("/api/admin/workforce/{user_id}/unlock-profile")
async def unlock_admin_workforce_profile(user_id: int, request: Request, db: Session = Depends(get_db)):
    require_admin_session(request)
    user = db.query(models.User).filter(models.User.id == int(user_id)).first()
    if user is None or str(user.role or "").strip().lower() != "contributor":
        raise HTTPException(status_code=404, detail="Contributor not found.")
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    reason = str(payload.get("reason") or "").strip()
    if not reason:
        raise HTTPException(status_code=400, detail="An unlock reason is required.")
    _ensure_workforce_detail_row(db, int(user.id))
    db.execute(workforce_profile_details.update().where(workforce_profile_details.c.user_id == int(user.id)).values(profile_locked=False, profile_unlock_reason=reason, updated_at=_utc_now().isoformat()))
    _write_audit_log(db, request, "admin_unlocked_workforce_profile", "contributor_profile", user.id, {"reason": reason, "scope": "profile"})
    db.commit()
    return {"status": "success", "message": "Workforce profile unlocked for contributor editing.", **_get_workforce_profile_payload(db, user, include_payment=True)}


@app.post("/api/admin/workforce/{user_id}/unlock-payment")
async def unlock_admin_workforce_payment(user_id: int, request: Request, db: Session = Depends(get_db)):
    require_admin_session(request)
    user = db.query(models.User).filter(models.User.id == int(user_id)).first()
    if user is None or str(user.role or "").strip().lower() != "contributor":
        raise HTTPException(status_code=404, detail="Contributor not found.")
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    reason = str(payload.get("reason") or "").strip()
    if not reason:
        raise HTTPException(status_code=400, detail="An unlock reason is required.")
    _ensure_workforce_detail_row(db, int(user.id))
    db.execute(workforce_profile_details.update().where(workforce_profile_details.c.user_id == int(user.id)).values(payment_locked=False, payment_unlock_reason=reason, updated_at=_utc_now().isoformat()))
    _write_audit_log(db, request, "admin_unlocked_payment_details", "contributor_payment", user.id, {"reason": reason, "scope": "payment"})
    db.commit()
    return {"status": "success", "message": "Payment details unlocked for contributor editing.", **_get_workforce_profile_payload(db, user, include_payment=True)}


@app.get("/api/admin/workforce/{user_id}/history")
def get_admin_workforce_history(user_id: int, request: Request, db: Session = Depends(get_db)):
    require_admin_session(request)
    user = db.query(models.User).filter(models.User.id == int(user_id)).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found.")
    rows = db.execute(workforce_profile_versions.select().where(workforce_profile_versions.c.user_id == int(user.id)).order_by(workforce_profile_versions.c.id.desc())).fetchall()
    versions = []
    for row in rows:
        m = dict(row._mapping)
        versions.append({"id": int(m["id"]), "version_type": m["version_type"], "version_number": int(m["version_number"]), "snapshot": json.loads(m["snapshot"]), "changed_by_user_id": m["changed_by_user_id"], "reason": m["reason"] or "", "created_at": m["created_at"]})
    return {"status": "success", "versions": versions}


@app.get("/api/admin/operations")
def get_admin_operations(
    request: Request,
    db: Session = Depends(get_db)
):
    """Return the high-level KELYVO Operations Control Center snapshot.

    This endpoint is deliberately admin-only. QA users retain access to the
    review queue, but they do not receive organization-wide operational data.
    """
    require_admin_session(request)
    now = _utc_now()

    # Keep all active reservations current before calculating the dashboard.
    _cleanup_expired_persistent_task_assignments()

    users = (
        db.query(models.User)
        .order_by(models.User.id.desc())
        .all()
    )

    role_counts = {
        "contributors": sum(
            1 for user in users
            if str(user.role or "").strip().lower() == "contributor"
        ),
        "qa": sum(
            1 for user in users
            if str(user.role or "").strip().lower() == "qa"
        ),
        "admins": sum(
            1 for user in users
            if str(user.role or "").strip().lower() == "admin"
        ),
    }

    task_rows = db.query(models.Task).all() if hasattr(models, "Task") else []
    task_counts = {
        "total": len(task_rows),
        "available": sum(
            1 for task in task_rows
            if str(task.status or "").strip().lower() == "available"
        ),
        "reserved": sum(
            1 for task in task_rows
            if str(task.status or "").strip().lower() == "reserved"
        ),
        "submitted": sum(
            1 for task in task_rows
            if str(task.status or "").strip().lower() == "submitted"
        ),
    }

    submission_rows = (
        db.query(models.TaskSubmission)
        .order_by(models.TaskSubmission.id.desc())
        .limit(20)
        .all()
    )
    submission_counts = {
        "pending_qa": db.query(models.TaskSubmission)
        .filter(models.TaskSubmission.status == "PENDING_QA")
        .count(),
        "returned": db.query(models.TaskSubmission)
        .filter(models.TaskSubmission.status == "FAILED")
        .count(),
        "passed": db.query(models.TaskSubmission)
        .filter(models.TaskSubmission.status == "PASSED")
        .count(),
    }

    active_assignments = (
        db.query(models.TaskAssignment)
        .options(joinedload(models.TaskAssignment.task), joinedload(models.TaskAssignment.user))
        .filter(models.TaskAssignment.status == "reserved")
        .order_by(models.TaskAssignment.assigned_at.desc())
        .all()
        if hasattr(models, "TaskAssignment")
        else []
    )

    reservation_data = []
    for assignment in active_assignments:
        task = assignment.task
        user = assignment.user
        expires_at = assignment.expires_at
        seconds_remaining = None
        if expires_at is not None:
            try:
                seconds_remaining = max(0, int((expires_at - now).total_seconds()))
            except Exception:
                seconds_remaining = None
        reservation_data.append({
            "task_id": int(task.external_task_id) if task and task.external_task_id is not None else None,
            "project_id": int(task.external_project_id) if task and task.external_project_id is not None else None,
            "task_title": task.title if task else "Unknown task",
            "task_type": task.task_type if task else "",
            "contributor_email": user.email if user else "Unknown",
            "assigned_at": assignment.assigned_at.isoformat() if assignment.assigned_at else None,
            "expires_at": expires_at.isoformat() if expires_at else None,
            "seconds_remaining": seconds_remaining,
        })

    project_rows = (
        db.query(models.Project)
        .order_by(models.Project.created_at.asc())
        .all()
        if hasattr(models, "Project")
        else []
    )
    projects = []
    for project in project_rows:
        project_task_count = (
            db.query(models.Task)
            .filter(models.Task.project_id == project.id)
            .count()
            if hasattr(models, "Task")
            else 0
        )
        projects.append({
            "id": project.id,
            "name": project.name,
            "code": project.project_code,
            "modality": project.modality,
            "status": project.status,
            "external_project_id": project.external_project_id,
            "task_count": project_task_count,
        })

    contributors = []
    for user in users:
        role = str(user.role or "").strip().lower()
        if role not in {"contributor", "qa", "admin"}:
            continue
        active = next(
            (item for item in reservation_data if item["contributor_email"] == user.email),
            None,
        )
        membership = _get_user_membership(db, int(user.id))
        contributors.append({
            "id": int(user.id),
            "email": user.email,
            "role": role,
            "status": str(membership.status or "active").strip().lower() if membership is not None else "active",
            "tasks_today": int(user.tasks_today or 0),
            "tasks_week": int(user.tasks_week or 0),
            "tasks_passed_qa": int(user.tasks_passed_qa or 0),
            "earnings": float(user.earnings or 0),
            "active_task": active,
        })

    recent_submissions = []
    for sub in submission_rows:
        recent_submissions.append({
            "id": int(sub.id),
            "contributor_email": sub.contributor.email if sub.contributor else "Unassigned",
            "task_type": sub.task_type,
            "task_title": sub.task_title,
            "status": str(sub.status or "").upper(),
        })

    return {
        "status": "success",
        "generated_at": now.isoformat(),
        "role_counts": role_counts,
        "task_counts": task_counts,
        "submission_counts": submission_counts,
        "active_reservations": reservation_data,
        "users": contributors,
        "projects": projects,
        "recent_submissions": recent_submissions,
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
        .order_by(
            models.TaskSubmission.id.desc()
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

    pending_qa_count = (
        db.query(
            models.TaskSubmission
        )
        .filter(
            models.TaskSubmission.user_id == user.id,
            models.TaskSubmission.status == "PENDING_QA",
        )
        .count()
    )

    return {
        "status":
            "success",
        "email":
            user.email,
        "submissions":
            result,
        "queue_count":
            pending_qa_count,
        "tasks_today":
            int(user.tasks_today or 0),
        "tasks_week":
            int(user.tasks_week or 0),
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

    # Always retrieve the real finalized annotations explicitly. QA reviews the
    # latest submitted attempt, not a historical annotation from an earlier
    # revision. This is especially important when a returned task has both the
    # original annotation and a newly promoted revision annotation.
    annotation_response = label_studio_request(
        "GET",
        f"/api/tasks/{int(task_id)}/annotations",
        params={"project": project_id},
    )

    if annotation_response.status_code < 400:
        try:
            annotation_payload = annotation_response.json()
        except ValueError:
            annotation_payload = []

        if isinstance(annotation_payload, dict):
            candidate_annotations = (
                annotation_payload.get("annotations")
                or annotation_payload.get("results")
                or annotation_payload.get("data")
                or []
            )
        else:
            candidate_annotations = annotation_payload

        if isinstance(candidate_annotations, list):
            annotations = candidate_annotations

    annotations = [
        item
        for item in annotations
        if (
            isinstance(item, dict)
            and isinstance(item.get("result"), list)
            and bool(item.get("result"))
            and not bool(item.get("was_cancelled"))
        )
    ]

    annotations.sort(
        key=lambda item: str(
            item.get("updated_at")
            or item.get("created_at")
            or item.get("submitted_at")
            or item.get("id")
            or ""
        )
    )

    # QA must see the current attempt only. Historical annotation versions stay
    # stored in Label Studio and KELYVO's enterprise annotation history.
    if annotations:
        annotations = [annotations[-1]]

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
        project_id,
        user_id=_get_authenticated_user_id(request),
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

    # Keep the canonical enterprise Task lifecycle synchronized with the QA
    # decision. This gives KELYVO one durable state even though Label Studio is
    # still the execution engine for the annotation surface.
    try:
        project_id_for_state = PROJECT_MAPPING.get(
            str(sub.task_type or "").strip().lower()
        )
        task_id_for_state = _extract_kelyvo_task_id(
            sub.task_title
        )
        if project_id_for_state and task_id_for_state is not None:
            canonical_task = (
                db.query(models.Task)
                .filter(
                    models.Task.external_project_id == int(project_id_for_state),
                    models.Task.external_task_id == int(task_id_for_state),
                )
                .first()
            )
            if canonical_task is not None:
                canonical_task.status = {
                    "PASSED": "passed",
                    "FAILED": "failed",
                    "PENDING_QA": "submitted",
                }.get(
                    decision,
                    canonical_task.status,
                )
                canonical_task.is_locked = False
    except Exception:
        pass

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
async def update_payout(request: Request, db: Session = Depends(get_db)):
    require_contributor_session(request)
    session = read_session_token(request) or {}
    email = normalize_email(session.get("email", ""))
    user = db.query(models.User).filter(models.User.email == email).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    details = payload.get("details") or {}
    method = str(payload.get("payment_method") or payload.get("payout_method") or "").strip().lower()
    if isinstance(details, str):
        details = {"upi_id": details}
    detail = _ensure_workforce_detail_row(db, int(user.id))
    current = dict(detail._mapping)
    if bool(current.get("payment_locked")):
        raise HTTPException(status_code=403, detail="Payment details are locked. Ask a KELYVO Admin to unlock them before making changes.")
    merged = {key: current.get(key) for key in ("payment_method", "upi_id", "bank_account_name", "bank_account_number", "bank_ifsc", "bank_name", "bank_branch", "bank_account_type")}
    merged["payment_method"] = method or merged.get("payment_method")
    merged.update({key: details.get(key) for key in details if key in merged})
    normalized = _validate_payment_payload(merged)
    now_iso = _utc_now().isoformat()
    db.execute(workforce_profile_details.update().where(workforce_profile_details.c.user_id == int(user.id)).values(
        payment_method=normalized["payment_method"], upi_id=normalized["upi_id"], bank_account_name=normalized["bank_account_name"],
        bank_account_number=normalized["bank_account_number"], bank_ifsc=normalized["bank_ifsc"], bank_name=normalized["bank_name"],
        bank_branch=normalized["bank_branch"], bank_account_type=normalized["bank_account_type"], payment_locked=True,
        payment_locked_at=now_iso, updated_at=now_iso
    ))
    _snapshot_workforce_record(db, int(user.id), "payment", int(user.id), "Contributor submitted payment details")
    _write_audit_log(db, request, "contributor_payment_submitted_and_locked", "contributor_payment", user.id, {"payment_method": normalized["payment_method"]})
    db.commit()
    return {"status": "success", "message": "Payment details submitted and locked."}


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
            project_id,
            user_id=_get_authenticated_user_id(request),
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
        [data-testid*="next-task" i],

        /* KELYVO owns the workflow transition. Label Studio's native Submit
           is hidden so contributors have one authoritative submit action. */
        button[aria-label="Submit" i],
        button[title="Submit" i],
        button[data-testid="submit" i] {
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

        function lockKelyvoWorkflowButtons() {
            lockTaskHistoryButtons();

            Array.from(document.querySelectorAll('button')).forEach(function (button) {
                const text = (button.innerText || button.textContent || '').trim();
                const aria = (button.getAttribute('aria-label') || '').trim();
                const title = (button.getAttribute('title') || '').trim();
                const isNativeSubmit =
                    /^submit$/i.test(text) ||
                    /^submit$/i.test(aria) ||
                    /^submit$/i.test(title);

                if (!isNativeSubmit) {
                    return;
                }

                button.style.setProperty('display', 'none', 'important');
                button.style.setProperty('visibility', 'hidden', 'important');
                button.style.setProperty('pointer-events', 'none', 'important');
            });
        }

        lockKelyvoWorkflowButtons();

        const observer = new MutationObserver(lockKelyvoWorkflowButtons);

        observer.observe(document.documentElement, {
            childList: true,
            subtree: true,
            attributes: true,
            attributeFilter: ['aria-label', 'title', 'data-testid']
        });

        // Defensive capture: if Label Studio renders its native Submit before
        // the observer hides it, stop Label Studio from advancing the task and
        // route the click into KELYVO's authoritative submission flow.
        document.addEventListener('click', function (event) {
            const target = event.target && event.target.closest
                ? event.target.closest('button')
                : null;
            if (!target) {
                return;
            }

            const text = (target.innerText || target.textContent || '').trim();
            const aria = (target.getAttribute('aria-label') || '').trim();
            const title = (target.getAttribute('title') || '').trim();
            const isNativeSubmit =
                /^submit$/i.test(text) ||
                /^submit$/i.test(aria) ||
                /^submit$/i.test(title);

            if (!isNativeSubmit) {
                return;
            }

            event.preventDefault();
            event.stopPropagation();
            event.stopImmediatePropagation();

            try {
                window.parent.postMessage({
                    type: 'kelyvo:labelstudio-submit'
                }, '*');
            } catch (error) {
                console.warn('KELYVO: native submit bridge failed:', error);
            }
        }, true);
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
        int(project_id),
        user_id=_get_authenticated_user_id(request),
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

    user_id = _get_authenticated_user_id(request)
    if user_id is None:
        raise HTTPException(
            status_code=401,
            detail="A valid contributor session is required."
        )

    db = SessionLocal()
    try:
        active_assignment = (
            db.query(models.TaskAssignment)
            .options(joinedload(models.TaskAssignment.task))
            .join(models.Task, models.Task.id == models.TaskAssignment.task_id)
            .filter(
                models.TaskAssignment.user_id == int(user_id),
                models.TaskAssignment.status == "reserved",
            )
            .order_by(models.TaskAssignment.assigned_at.desc())
            .first()
        )
        if active_assignment is not None:
            project_id = int(active_assignment.task.external_project_id)
            task_id = int(active_assignment.task.external_task_id)
    finally:
        db.close()

    if active_assignment is None:
        raise HTTPException(
            status_code=403,
            detail="No active contributor task."
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
    if contributor_next_task_action:
        locked_next_response = _kelyvo_assigned_next_task_response(
            project_id,
            task_id,
        )
        if locked_next_response is not None:
            locked_next_response.headers["X-KELYVO-Assigned-Task-Lock"] = "1"
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
        return JSONResponse(
            content={
                "submitted": False,
                "revision_updated": False,
                "task_id": int(request.query_params.get("task_id", 0) or 0),
            },
            status_code=200,
            headers={"Cache-Control": "no-store"},
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

    # Label Studio annotation writes are editor persistence only. They must
    # never create a KELYVO submission, advance the workflow, or trigger a
    # revision submission. The explicit KELYVO Submit to QA action is the only
    # authoritative workflow transition.


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
    # KELYVO must not install Label Studio's root-scoped service worker on the
    # portal origin. That worker can cache KELYVO API responses such as
    # /admin/submissions and make contributor/QA queues appear stale.
    # Label Studio remains fully usable in its embedded workspace without
    # exposing its service worker to the parent KELYVO application scope.
    return Response(
        content=(
            "self.addEventListener('install', event => self.skipWaiting());\n"
            "self.addEventListener('activate', event => event.waitUntil(self.clients.claim()));\n"
            "self.addEventListener('fetch', event => {});\n"
        ),
        media_type="application/javascript",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Service-Worker-Allowed": "/",
        },
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
        "/task-submission-ready",
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

    user_id = _get_authenticated_user_id(request)
    if user_id is None:
        raise HTTPException(
            status_code=401,
            detail="A valid contributor session is required."
        )

    db = SessionLocal()
    try:
        assignments = (
            db.query(models.TaskAssignment)
            .options(joinedload(models.TaskAssignment.task))
            .join(models.Task, models.Task.id == models.TaskAssignment.task_id)
            .filter(
                models.TaskAssignment.user_id == int(user_id),
                models.TaskAssignment.status == "reserved",
            )
            .order_by(models.TaskAssignment.assigned_at.desc())
            .all()
        )
    finally:
        db.close()

    if not assignments:
        requested_project_hint = None
        path_parts_hint = clean_path.strip("/").split("/")
        if (
            len(path_parts_hint) >= 3
            and path_parts_hint[0] == "api"
            and path_parts_hint[1] == "projects"
            and path_parts_hint[2].isdigit()
        ):
            requested_project_hint = int(path_parts_hint[2])

        recent_completed = get_recent_kelyvo_completed_submission(
            request,
            requested_project_hint,
        )
        if recent_completed is not None:
            return JSONResponse(
                content={
                    "status": "ok",
                    "submitted": True,
                    "task_id": int(recent_completed.get("task_id", 0) or 0),
                },
                status_code=200,
                headers={"Cache-Control": "no-store"},
            )

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
                int(assignment.task.external_task_id)
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
                int(assignment.task.external_project_id)
                == requested_project_id
            ):
                active_assignment = assignment
                break

    if active_assignment is None:
        active_assignment = max(
            assignments,
            key=lambda item: item.assigned_at or _utc_now()
        )

    if active_assignment is None:
        raise HTTPException(
            status_code=403,
            detail=(
                "This Label Studio resource is not available in "
                "contributor task mode."
            )
        )

    project_id = int(active_assignment.task.external_project_id)

    task_id = int(active_assignment.task.external_task_id)

    contributor_next_task_action = _is_contributor_next_task_action(
        request,
        clean_path,
        project_id,
    )

    # After native Submit, Label Studio immediately fires next_task.
    # Consume that one automatic navigation request and return success
    # with no replacement task. This keeps the contributor on the same
    # task until KELYVO's MARK TASK SUBMITTED action advances the queue.
    if contributor_next_task_action:
        locked_next_response = _kelyvo_assigned_next_task_response(
            project_id,
            task_id,
        )
        if locked_next_response is not None:
            locked_next_response.headers["X-KELYVO-Assigned-Task-Lock"] = "1"
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

    # Label Studio annotation writes only persist editor state. They never
    # trigger KELYVO submission or revision submission.

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



# ============================================================
# 24-HOUR PERSISTENT RESERVATION EXPIRATION WORKER
# ============================================================

def _normalize_existing_assignment_expirations():
    """Migrate existing reserved assignments to the fixed 24-hour window."""
    db = SessionLocal()
    try:
        assignments = (
            db.query(models.TaskAssignment)
            .filter(models.TaskAssignment.status == "reserved")
            .all()
        )
        changed = False
        for assignment in assignments:
            if assignment.assigned_at is None:
                continue
            desired_expiry = assignment.assigned_at + timedelta(seconds=TASK_ASSIGNMENT_TTL)
            if assignment.expires_at != desired_expiry:
                assignment.expires_at = desired_expiry
                changed = True
        if changed:
            db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


def _assignment_expiration_worker():
    """Release expired reservations in the background without requiring a request."""
    while True:
        try:
            _cleanup_expired_persistent_task_assignments()
        except Exception:
            pass
        time.sleep(60)


_normalize_existing_assignment_expirations()
threading.Thread(
    target=_assignment_expiration_worker,
    name="kelyvo-assignment-expiry",
    daemon=True,
).start()


# ============================================================
# SUPPORT CENTER
# ============================================================

@app.post("/api/support/tickets")
async def create_support_ticket(
    request: Request,
    message: str = Form(...),
    attachment: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db),
):
    user_id = _get_authenticated_user_id(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Authentication required.")

    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=401, detail="User not found.")

    normalized_message = (message or "").strip()
    if not normalized_message:
        raise HTTPException(status_code=400, detail="Support message cannot be empty.")

    ticket = models.SupportTicket(
        user_id=user.id,
        role=str(user.role or "contributor"),
        email_snapshot=str(user.email or ""),
        message=normalized_message,
        status="open",
    )
    db.add(ticket)
    db.commit()
    db.refresh(ticket)

    if attachment and attachment.filename:
        try:
            allowed_types = {
                "image/png",
                "image/jpeg",
                "image/webp",
                "image/gif",
            }

            if attachment.content_type not in allowed_types:
                db.delete(ticket)
                db.commit()
                raise HTTPException(
                    status_code=400,
                    detail="Support attachments must be PNG, JPG, WEBP or GIF images."
                )

            result = storage_provider.save_file(
                attachment,
                folder=f"support/{ticket.id}"
            )

            ticket.attachment_path = result.get("path")
            ticket.attachment_name = attachment.filename
            ticket.attachment_mime_type = attachment.content_type
            db.commit()
        finally:
            try:
                await attachment.close()
            except Exception:
                pass

    return {
        "status": "success",
        "ticket": {
            "id": ticket.id,
            "status": ticket.status,
            "created_at": ticket.created_at.isoformat() if ticket.created_at else None,
            "has_attachment": bool(ticket.attachment_path),
        },
    }


@app.get("/api/admin/support/tickets")
def list_admin_support_tickets(
    request: Request,
    status: str = "open",
    db: Session = Depends(get_db),
):
    require_admin_session(request)

    normalized_status = (status or "open").strip().lower()
    query = db.query(models.SupportTicket)

    if normalized_status in {"open", "resolved"}:
        query = query.filter(
            models.SupportTicket.status == normalized_status
        )

    rows = (
        query
        .order_by(
            models.SupportTicket.created_at.desc()
        )
        .limit(250)
        .all()
    )

    return {
        "status": "success",
        "tickets": [
            {
                "id": row.id,
                "email": row.email_snapshot,
                "role": row.role,
                "message": row.message,
                "status": row.status,
                "admin_notes": row.admin_notes,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "resolved_at": row.resolved_at.isoformat() if row.resolved_at else None,
                "attachment_name": row.attachment_name,
                "attachment_url": (
                    f"/api/admin/support/tickets/{row.id}/attachment"
                    if row.attachment_path
                    else None
                ),
            }
            for row in rows
        ],
    }


@app.patch("/api/admin/support/tickets/{ticket_id}")
async def update_admin_support_ticket(
    ticket_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_session(request)

    payload = await request.json()
    requested_status = str(payload.get("status") or "").strip().lower()
    admin_notes = str(payload.get("admin_notes") or "").strip()

    if requested_status not in {"open", "resolved"}:
        raise HTTPException(
            status_code=400,
            detail="Support ticket status must be open or resolved."
        )

    ticket = (
        db.query(models.SupportTicket)
        .filter(models.SupportTicket.id == ticket_id)
        .first()
    )

    if not ticket:
        raise HTTPException(status_code=404, detail="Support ticket not found.")

    ticket.status = requested_status
    ticket.admin_notes = admin_notes or ticket.admin_notes

    if requested_status == "resolved":
        ticket.resolved_at = datetime.now(timezone.utc)
    else:
        ticket.resolved_at = None

    db.commit()
    db.refresh(ticket)

    return {
        "status": "success",
        "ticket": {
            "id": ticket.id,
            "status": ticket.status,
            "admin_notes": ticket.admin_notes,
            "resolved_at": ticket.resolved_at.isoformat() if ticket.resolved_at else None,
        },
    }


@app.get("/api/admin/support/tickets/{ticket_id}/attachment")
def get_admin_support_attachment(
    ticket_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_session(request)

    ticket = (
        db.query(models.SupportTicket)
        .filter(models.SupportTicket.id == ticket_id)
        .first()
    )

    if not ticket or not ticket.attachment_path:
        raise HTTPException(status_code=404, detail="Attachment not found.")

    file_path = os.path.abspath(ticket.attachment_path)

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Attachment file is unavailable.")

    with open(file_path, "rb") as handle:
        content = handle.read()

    return Response(
        content=content,
        media_type=ticket.attachment_mime_type or "application/octet-stream",
        headers={
            "Content-Disposition": (
                f'inline; filename="{ticket.attachment_name or "attachment"}"'
            )
        },
    )
