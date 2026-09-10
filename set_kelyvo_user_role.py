"""One-time/local utility to correct a KELYVO user's persisted portal role.

Usage:
    python3 set_kelyvo_user_role.py email@example.com qa
    python3 set_kelyvo_user_role.py email@example.com admin

The script only changes the User.role field. It does not modify the password,
submissions, assignments, annotations, or other account data.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow execution directly from the project root.
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database import SessionLocal  # type: ignore
import models  # type: ignore

VALID_ROLES = {"contributor", "qa", "admin"}


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: python3 set_kelyvo_user_role.py <email> <contributor|qa|admin>")
        return 2

    email = sys.argv[1].strip().lower()
    new_role = sys.argv[2].strip().lower()

    if not email:
        print("ERROR: email is required.")
        return 2

    if new_role not in VALID_ROLES:
        print("ERROR: role must be contributor, qa, or admin.")
        return 2

    db = SessionLocal()
    try:
        user = (
            db.query(models.User)
            .filter(models.User.email == email)
            .first()
        )

        if user is None:
            print(f"ERROR: no KELYVO user found for {email}")
            return 1

        old_role = str(user.role or "").strip().lower()
        user.role = new_role
        db.commit()

        print(f"UPDATED: {email}")
        print(f"Role: {old_role or '(empty)'} -> {new_role}")
        print("The user must sign out and sign in again so a fresh role-based session is issued.")
        return 0
    except Exception as exc:
        db.rollback()
        print(f"ERROR: could not update user role: {exc}")
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
