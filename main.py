import hashlib
import os
import requests
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from database import SessionLocal, engine
import models

models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="KELYVO Portal API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Label Studio REST API Configuration
LABEL_STUDIO_URL = "http://localhost:8080"
LABEL_STUDIO_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6ODA5NTAyNzMwMywiaWF0IjoxNzg3ODI3MzAzLCJqdGkiOiIyNTY5NDg1NzVjNTI0YmRhYjc3NDI2MDUwNjJhOGZjZCIsInVzZXJfaWQiOiIxIn0.PMSw6QeN7cOegzyqD80S7LS61Dt5F2z36hYD1RidmaA"

headers = {
    "Authorization": f"Token {LABEL_STUDIO_API_KEY}"
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
            return {"status": "success", "message": "Successfully connected to local Label Studio!", "details": response.json()}
        else:
            return {"status": "error", "message": f"Label Studio responded with status {response.status_code}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}