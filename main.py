import hashlib
import os
import random
import requests
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from database import SessionLocal, engine
import models

# Create database tables
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="KELYVO Portal API")

# Enable CORS for cross-origin frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Label Studio Configuration
LABEL_STUDIO_URL = os.getenv("LABEL_STUDIO_URL", "http://localhost:8080")
LABEL_STUDIO_API_KEY = "q912apgQg1bQrteHpLBGNuCWBnrV6GcKaXMdHATB0yM"

headers = {
    "Authorization": f"Token {LABEL_STUDIO_API_KEY}"
}

# Corrected Project Mapping matching your exact Label Studio IDs:
# Video=6, Text=5, Audio=4, Image=2
PROJECT_MAPPING = {
    "video": 6,
    "text": 5,
    "audio": 4,
    "image": 2
}

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/", response_class=HTMLResponse)
def serve_portal():
    html_path = os.path.join("templates", "index.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    return "Template not found"

@app.post("/register")
def register_user(email: str, password: str, role: str = "contributor", db: Session = Depends(get_db)):
    existing_user = db.query(models.User).filter(models.User.email == email).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    assigned_role = "admin" if role == "admin" else "contributor"
    hashed_pwd = hash_password(password)
    
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
    
    return {
        "status": "success",
        "email": new_user.email,
        "role": new_user.role,
        "tasks_today": new_user.tasks_today,
        "tasks_week": new_user.tasks_week,
        "tasks_passed_qa": new_user.tasks_passed_qa,
        "earnings": new_user.earnings,
        "payout_details": new_user.payout_details
    }

@app.post("/login")
def login_user(email: str, password: str, db: Session = Depends(get_db)):
    hashed_pwd = hash_password(password)
    user = db.query(models.User).filter(
        models.User.email == email, 
        models.User.hashed_password == hashed_pwd
    ).first()
    
    if not user:
        raise HTTPException(status_code=400, detail="Invalid email or password")
    
    return {
        "status": "success",
        "email": user.email,
        "role": user.role,
        "tasks_today": user.tasks_today,
        "tasks_week": user.tasks_week,
        "tasks_passed_qa": user.tasks_passed_qa,
        "earnings": user.earnings,
        "payout_details": user.payout_details
    }

@app.get("/api/start-task")
def start_task(modality: str = "text"):
    """
    Pulls an unassigned task from Label Studio and directs annotators 
    to Label Studio's official single-task labeling stream view.
    """
    clean_input = modality.strip().lower()
    
    if clean_input.isdigit():
        project_id = int(clean_input)
    else:
        project_id = PROJECT_MAPPING.get(clean_input, 5)
    
    try:
        response = requests.get(
            f"{LABEL_STUDIO_URL}/api/projects/{project_id}/tasks",
            headers=headers,
            timeout=5
        )
        
        if response.status_code != 200:
            return {
                "status": "success",
                "modality": clean_input,
                "project_id": project_id,
                "redirect_url": f"{LABEL_STUDIO_URL}/projects/{project_id}/data?labeling=true"
            }
        
        tasks_data = response.json()
        tasks = tasks_data.get("tasks", tasks_data) if isinstance(tasks_data, dict) else tasks_data
        
        available_tasks = [t for t in tasks if not t.get("is_annotated", False)]
        
        if not available_tasks:
            target_url = f"{LABEL_STUDIO_URL}/projects/{project_id}/data?labeling=true"
        else:
            selected_task = random.choice(available_tasks)
            task_id = selected_task.get("id")
            # Official Label Studio single-task stream URL format with labeling=true parameter
            target_url = f"{LABEL_STUDIO_URL}/projects/{project_id}/data?labeling=true&task={task_id}"
        
        return {
            "status": "success",
            "modality": clean_input,
            "project_id": project_id,
            "redirect_url": target_url
        }
        
    except Exception:
        return {
            "status": "success",
            "modality": clean_input,
            "project_id": project_id,
            "redirect_url": f"{LABEL_STUDIO_URL}/projects/{project_id}/data?labeling=true"
        }

@app.post("/submit-task")
def submit_task(email: str, task_type: str, task_title: str, db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.email == email).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
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
    
    return {
        "status": "success",
        "tasks_today": user.tasks_today,
        "tasks_week": user.tasks_week
    }

@app.get("/admin/submissions")
def get_admin_submissions(db: Session = Depends(get_db)):
    submissions = db.query(models.TaskSubmission).all()
    result = []
    for sub in submissions:
        result.append({
            "id": sub.id,
            "contributor_email": sub.contributor.email if sub.contributor else "Unassigned / Requeued",
            "task_type": sub.task_type,
            "task_title": sub.task_title,
            "status": sub.status,
            "reviewer_notes": sub.reviewer_notes
        })
    return result

@app.post("/admin/review-task")
def review_task(submission_id: int, decision: str, notes: str = "", db: Session = Depends(get_db)):
    sub = db.query(models.TaskSubmission).filter(models.TaskSubmission.id == submission_id).first()
    if not sub:
        raise HTTPException(status_code=404, detail="Submission not found")
    
    sub.status = decision
    sub.reviewer_notes = notes
    
    if decision == "PASSED" and sub.contributor:
        sub.contributor.tasks_passed_qa += 1
        sub.contributor.earnings += 500.0
    elif decision == "FAILED":
        sub.user_id = None
        
    db.commit()
    return {"status": "success", "new_status": decision}

@app.post("/update-payout")
def update_payout(email: str, payout_method: str, payout_details: str, db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.email == email).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if user.payout_details:
        raise HTTPException(status_code=400, detail="Payout details are locked for security and cannot be changed.")
    
    user.payout_method = payout_method
    user.payout_details = payout_details
    db.commit()
    
    return {
        "status": "success",
        "payout_method": user.payout_method,
        "payout_details": user.payout_details
    }

@app.get("/admin/test-label-studio")
def test_label_studio():
    try:
        response = requests.get(f"{LABEL_STUDIO_URL}/api/health")
        if response.status_code == 200:
            return {"status": "success", "message": "Successfully connected to Label Studio!", "details": response.json()}
        else:
            return {"status": "error", "message": f"Label Studio responded with status {response.status_code}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/pipeline/projects")
def get_label_studio_projects():
    """Fetches active labeling projects from Label Studio with a safe local fallback."""
    try:
        response = requests.get(f"{LABEL_STUDIO_URL}/api/projects", headers=headers)
        if response.status_code == 200:
            data = response.json()
            return {
                "status": "success",
                "source": "label_studio",
                "projects": data.get("results", []) if isinstance(data, dict) else data
            }
        else:
            return {
                "status": "success",
                "source": "fallback_mock",
                "projects": [
                    {
                        "id": 5,
                        "title": "Kelyvo Default Annotation Pipeline",
                        "description": "Active local development pipeline",
                        "task_count": 0
                    }
                ]
            }
    except Exception as e:
        return {
            "status": "success",
            "source": "fallback_mock",
            "projects": [
                {
                    "id": 5,
                    "title": "Kelyvo Default Annotation Pipeline",
                    "description": "Active local development pipeline",
                    "task_count": 0
                }
            ]
        }