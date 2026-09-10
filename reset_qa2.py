import os
import sys
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

from database import SessionLocal
import models


PROJECT_IDS = [2, 4, 5, 6]

LABEL_STUDIO_URL = os.getenv(
    "LABEL_STUDIO_URL",
    "http://localhost:8080",
).rstrip("/")

LABEL_STUDIO_TOKEN = os.getenv(
    "LABEL_STUDIO_API_TOKEN",
    "",
).strip()


def get_label_studio_headers():
    """
    Exchange the Label Studio Personal Access Token for a
    short-lived API access token, then return Bearer headers.
    """

    if not LABEL_STUDIO_TOKEN:
        raise RuntimeError(
            "LABEL_STUDIO_API_TOKEN is missing from the environment."
        )

    response = requests.post(
        LABEL_STUDIO_URL + "/api/token/refresh",
        json={
            "refresh": LABEL_STUDIO_TOKEN,
        },
        timeout=30,
    )

    response.raise_for_status()

    payload = response.json()

    access_token = payload.get("access")

    if not access_token:
        raise RuntimeError(
            "Label Studio did not return an access token."
        )

    return {
        "Authorization": "Bearer " + access_token,
        "Content-Type": "application/json",
    }


def delete_label_studio_annotations():
    headers = get_label_studio_headers()

    deleted = 0
    tasks_seen = 0

    for project_id in PROJECT_IDS:
        page = 1

        while True:
            response = requests.get(
                LABEL_STUDIO_URL + "/api/tasks",
                params={
                    "project": project_id,
                    "page": page,
                    "page_size": 100,
                },
                headers=headers,
                timeout=30,
            )

            response.raise_for_status()

            payload = response.json()

            if isinstance(payload, dict):
                tasks = payload.get(
                    "tasks",
                    payload.get("results", []),
                )
                total = payload.get("total", None)
            else:
                tasks = payload
                total = None

            if not isinstance(tasks, list) or not tasks:
                break

            for task in tasks:
                task_id = (
                    task.get("id")
                    if isinstance(task, dict)
                    else None
                )

                if not task_id:
                    continue

                tasks_seen += 1

                task_response = requests.get(
                    LABEL_STUDIO_URL
                    + "/api/tasks/"
                    + str(int(task_id)),
                    params={
                        "project": project_id,
                        "resolve_uri": "true",
                    },
                    headers=headers,
                    timeout=30,
                )

                task_response.raise_for_status()

                task_payload = task_response.json()

                annotations = (
                    task_payload.get("annotations", [])
                    if isinstance(task_payload, dict)
                    else []
                )

                if not isinstance(annotations, list):
                    annotations = []

                for annotation in annotations:
                    annotation_id = (
                        annotation.get("id")
                        if isinstance(annotation, dict)
                        else None
                    )

                    if not annotation_id:
                        continue

                    delete_response = requests.delete(
                        LABEL_STUDIO_URL
                        + "/api/annotations/"
                        + str(int(annotation_id)),
                        headers=headers,
                        timeout=30,
                    )

                    if delete_response.status_code not in (200, 204):
                        delete_response.raise_for_status()

                    deleted += 1

            if total is not None and page * 100 >= int(total):
                break

            if len(tasks) < 100:
                break

            page += 1

    return tasks_seen, deleted


def reset_database():
    db = SessionLocal()

    counts = {
        "qa_reviews": 0,
        "submissions": 0,
        "annotations": 0,
        "task_submissions": 0,
        "released_assignments": 0,
    }

    try:
        if hasattr(models, "QAReview"):
            rows = db.query(models.QAReview).all()

            counts["qa_reviews"] = len(rows)

            for row in rows:
                db.delete(row)

            db.flush()

        if hasattr(models, "Submission"):
            rows = db.query(models.Submission).all()

            counts["submissions"] = len(rows)

            for row in rows:
                db.delete(row)

            db.flush()

        if hasattr(models, "Annotation"):
            rows = db.query(models.Annotation).all()

            counts["annotations"] = len(rows)

            for row in rows:
                db.delete(row)

            db.flush()

        rows = db.query(models.TaskSubmission).all()

        counts["task_submissions"] = len(rows)

        for row in rows:
            db.delete(row)

        db.flush()

        if hasattr(models, "TaskAssignment"):
            assignments = db.query(models.TaskAssignment).all()

            for assignment in assignments:
                if assignment.status in {
                    "reserved",
                    "completed",
                }:
                    assignment.status = "released"

                    assignment.released_at = datetime.now(
                        timezone.utc
                    )

                    assignment.completed_at = None

                    counts["released_assignments"] += 1

                    if getattr(
                        assignment,
                        "task",
                        None,
                    ) is not None:
                        assignment.task.is_locked = False
                        assignment.task.status = "available"

        db.commit()

        return counts

    except Exception:
        db.rollback()
        raise

    finally:
        db.close()


def main():
    print("KELYVO QA2 FRESH RESET")

    print(
        "This preserves users, projects, tasks, "
        "payout ledger and audit logs."
    )

    print(
        "It deletes QA/submission records and "
        "Label Studio annotations in projects 2,4,5,6."
    )

    print()

    answer = input(
        "Type RESET_QA2 to continue: "
    ).strip()

    if answer != "RESET_QA2":
        print("Cancelled. Nothing was changed.")
        return 1

    counts = reset_database()

    tasks_seen, annotations_deleted = (
        delete_label_studio_annotations()
    )

    print()

    print("QA2 reset complete.")

    print(
        "Database:",
        counts,
    )

    print(
        "Label Studio tasks inspected:",
        tasks_seen,
    )

    print(
        "Label Studio annotations deleted:",
        annotations_deleted,
    )

    print("Fresh QA queue: 0")

    return 0


if __name__ == "__main__":
    sys.exit(main())