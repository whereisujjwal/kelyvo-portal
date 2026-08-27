from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship
from database import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    role = Column(String, default="contributor")
    tasks_today = Column(Integer, default=0)
    tasks_week = Column(Integer, default=0)
    tasks_passed_qa = Column(Integer, default=0)
    earnings = Column(Float, default=0.0)
    payout_method = Column(String, nullable=True)
    payout_details = Column(String, nullable=True)

    submissions = relationship("TaskSubmission", back_populates="contributor")

class TaskSubmission(Base):
    __tablename__ = "task_submissions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    task_type = Column(String)
    task_title = Column(String)
    status = Column(String, default="PENDING_QA")
    reviewer_notes = Column(String, nullable=True)

    contributor = relationship("User", back_populates="submissions")