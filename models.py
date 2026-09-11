from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from database import Base


# ============================================================
# SHARED HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def generate_uuid():
    return str(uuid4())


# ============================================================
# EXISTING USER MODEL
# ============================================================

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

    submissions = relationship(
        "TaskSubmission",
        back_populates="contributor"
    )

    memberships = relationship(
        "OrganizationMember",
        back_populates="user",
        cascade="all, delete-orphan"
    )

    contributor_profile = relationship(
        "ContributorProfile",
        back_populates="user",
        uselist=False,
        cascade="all, delete-orphan"
    )

    data_collector_profile = relationship(
        "DataCollectorProfile",
        back_populates="user",
        uselist=False,
        cascade="all, delete-orphan"
    )

    contributor_skills = relationship(
        "ContributorSkill",
        back_populates="user",
        cascade="all, delete-orphan"
    )

    contributor_languages = relationship(
        "ContributorLanguage",
        back_populates="user",
        cascade="all, delete-orphan"
    )

    task_assignments = relationship(
        "TaskAssignment",
        back_populates="user"
    )

    annotations = relationship(
        "Annotation",
        back_populates="annotator"
    )

    submissions_v2 = relationship(
        "Submission",
        back_populates="contributor"
    )

    qa_reviews = relationship(
        "QAReview",
        back_populates="reviewer"
    )

    quality_scores = relationship(
        "QualityScore",
        back_populates="user"
    )

    payout_ledger_entries = relationship(
        "PayoutLedger",
        back_populates="user"
    )

    data_collection_submissions = relationship(
        "DataCollectionSubmission",
        foreign_keys="DataCollectionSubmission.collector_id",
        back_populates="collector",
        cascade="all, delete-orphan"
    )


# ============================================================
# EXISTING TASK SUBMISSION MODEL
# ============================================================

class TaskSubmission(Base):
    __tablename__ = "task_submissions"

    id = Column(Integer, primary_key=True, index=True)

    user_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=True
    )

    task_type = Column(String)
    task_title = Column(String)

    status = Column(
        String,
        default="PENDING_QA"
    )

    reviewer_notes = Column(
        String,
        nullable=True
    )

    contributor = relationship(
        "User",
        back_populates="submissions"
    )


# ============================================================
# ORGANIZATION
# ============================================================

class Organization(Base):
    """
    Top-level business/client boundary.

    One KELYVO deployment can support many organizations.
    """

    __tablename__ = "organizations"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    name = Column(
        String,
        nullable=False,
        index=True
    )

    slug = Column(
        String,
        nullable=False,
        unique=True,
        index=True
    )

    organization_type = Column(
        String,
        nullable=False,
        default="client"
    )

    status = Column(
        String,
        nullable=False,
        default="active",
        index=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now
    )

    members = relationship(
        "OrganizationMember",
        back_populates="organization",
        cascade="all, delete-orphan"
    )

    workspaces = relationship(
        "Workspace",
        back_populates="organization",
        cascade="all, delete-orphan"
    )

    audit_logs = relationship(
        "AuditLog",
        back_populates="organization"
    )

    __table_args__ = (
        Index(
            "ix_organizations_type_status",
            "organization_type",
            "status"
        ),
    )


# ============================================================
# ORGANIZATION MEMBER
# ============================================================

class OrganizationMember(Base):
    """
    Connects users to organizations.

    Foundation for multi-tenant permissions and RBAC.
    """

    __tablename__ = "organization_members"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    organization_id = Column(
        String,
        ForeignKey("organizations.id"),
        nullable=False,
        index=True
    )

    user_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
        index=True
    )

    role = Column(
        String,
        nullable=False,
        default="member"
    )

    status = Column(
        String,
        nullable=False,
        default="active",
        index=True
    )

    joined_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    organization = relationship(
        "Organization",
        back_populates="members"
    )

    user = relationship(
        "User",
        back_populates="memberships"
    )

    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "user_id",
            name="uq_organization_member"
        ),
        Index(
            "ix_organization_members_org_role",
            "organization_id",
            "role"
        ),
    )


# ============================================================
# WORKSPACE
# ============================================================

class Workspace(Base):
    """
    Operational area inside an organization.
    """

    __tablename__ = "workspaces"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    organization_id = Column(
        String,
        ForeignKey("organizations.id"),
        nullable=False,
        index=True
    )

    name = Column(
        String,
        nullable=False
    )

    slug = Column(
        String,
        nullable=False,
        index=True
    )

    description = Column(
        Text,
        nullable=True
    )

    status = Column(
        String,
        nullable=False,
        default="active",
        index=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now
    )

    organization = relationship(
        "Organization",
        back_populates="workspaces"
    )

    projects = relationship(
        "Project",
        back_populates="workspace",
        cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "slug",
            name="uq_workspace_org_slug"
        ),
    )


# ============================================================
# PROJECT
# ============================================================

class Project(Base):
    """
    KELYVO's real project abstraction.

    External annotation engines such as Label Studio become
    execution engines behind this entity.
    """

    __tablename__ = "projects"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    workspace_id = Column(
        String,
        ForeignKey("workspaces.id"),
        nullable=False,
        index=True
    )

    name = Column(
        String,
        nullable=False
    )

    project_code = Column(
        String,
        nullable=False,
        unique=True,
        index=True
    )

    description = Column(
        Text,
        nullable=True
    )

    modality = Column(
        String,
        nullable=False,
        index=True
    )

    status = Column(
        String,
        nullable=False,
        default="draft",
        index=True
    )

    external_engine = Column(
        String,
        nullable=True,
        default="label_studio"
    )

    external_project_id = Column(
        Integer,
        nullable=True,
        index=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now
    )

    workspace = relationship(
        "Workspace",
        back_populates="projects"
    )

    datasets = relationship(
        "Dataset",
        back_populates="project",
        cascade="all, delete-orphan"
    )

    tasks = relationship(
        "Task",
        back_populates="project",
        cascade="all, delete-orphan"
    )

    workflows = relationship(
        "Workflow",
        back_populates="project"
    )

    deliveries = relationship(
        "Delivery",
        back_populates="project"
    )

    __table_args__ = (
        Index(
            "ix_projects_workspace_status",
            "workspace_id",
            "status"
        ),
        Index(
            "ix_projects_modality_status",
            "modality",
            "status"
        ),
    )


# ============================================================
# DATASET
# ============================================================

class Dataset(Base):
    """
    Logical collection of data belonging to a project.
    """

    __tablename__ = "datasets"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    project_id = Column(
        String,
        ForeignKey("projects.id"),
        nullable=False,
        index=True
    )

    name = Column(
        String,
        nullable=False
    )

    dataset_code = Column(
        String,
        nullable=False,
        unique=True,
        index=True
    )

    description = Column(
        Text,
        nullable=True
    )

    status = Column(
        String,
        nullable=False,
        default="active",
        index=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now
    )

    project = relationship(
        "Project",
        back_populates="datasets"
    )

    versions = relationship(
        "DatasetVersion",
        back_populates="dataset",
        cascade="all, delete-orphan"
    )


# ============================================================
# DATASET VERSION
# ============================================================

class DatasetVersion(Base):
    """
    Version boundary for datasets.

    Allows KELYVO to identify exactly which data version
    was used for annotation, QA, evaluation or delivery.
    """

    __tablename__ = "dataset_versions"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    dataset_id = Column(
        String,
        ForeignKey("datasets.id"),
        nullable=False,
        index=True
    )

    version_number = Column(
        Integer,
        nullable=False
    )

    version_label = Column(
        String,
        nullable=True
    )

    status = Column(
        String,
        nullable=False,
        default="draft",
        index=True
    )

    task_count = Column(
        Integer,
        nullable=False,
        default=0
    )

    source_uri = Column(
        Text,
        nullable=True
    )

    checksum = Column(
        String,
        nullable=True,
        index=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    dataset = relationship(
        "Dataset",
        back_populates="versions"
    )

    tasks = relationship(
        "Task",
        back_populates="dataset_version"
    )

    __table_args__ = (
        UniqueConstraint(
            "dataset_id",
            "version_number",
            name="uq_dataset_version_number"
        ),
    )


# ============================================================
# TASK
# ============================================================

class Task(Base):
    """
    KELYVO's canonical task record.

    This eventually replaces deriving task identity from
    task titles and hardcoded project mappings.
    """

    __tablename__ = "tasks"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    project_id = Column(
        String,
        ForeignKey("projects.id"),
        nullable=False,
        index=True
    )

    dataset_version_id = Column(
        String,
        ForeignKey("dataset_versions.id"),
        nullable=True,
        index=True
    )

    task_number = Column(
        Integer,
        nullable=True,
        index=True
    )

    external_task_id = Column(
        Integer,
        nullable=True,
        index=True
    )

    external_engine = Column(
        String,
        nullable=True,
        default="label_studio"
    )

    external_project_id = Column(
        Integer,
        nullable=True,
        index=True
    )

    title = Column(
        String,
        nullable=True
    )

    task_type = Column(
        String,
        nullable=False,
        index=True
    )

    status = Column(
        String,
        nullable=False,
        default="available",
        index=True
    )

    priority = Column(
        Integer,
        nullable=False,
        default=0
    )

    is_locked = Column(
        Boolean,
        nullable=False,
        default=False,
        index=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now
    )

    project = relationship(
        "Project",
        back_populates="tasks"
    )

    dataset_version = relationship(
        "DatasetVersion",
        back_populates="tasks"
    )

    assignments = relationship(
        "TaskAssignment",
        back_populates="task",
        cascade="all, delete-orphan"
    )

    annotations = relationship(
        "Annotation",
        back_populates="task"
    )

    submissions = relationship(
        "Submission",
        back_populates="task"
    )

    qa_reviews = relationship(
        "QAReview",
        back_populates="task"
    )

    __table_args__ = (
        Index(
            "ix_tasks_project_status",
            "project_id",
            "status"
        ),
        Index(
            "ix_tasks_project_external",
            "project_id",
            "external_task_id"
        ),
        Index(
            "ix_tasks_type_status",
            "task_type",
            "status"
        ),
    )


# ============================================================
# TASK ASSIGNMENT
# ============================================================

class TaskAssignment(Base):
    """
    Persistent task assignment/reservation.

    Eventually replaces the in-memory assignment dictionary.
    """

    __tablename__ = "task_assignments"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    task_id = Column(
        String,
        ForeignKey("tasks.id"),
        nullable=False,
        index=True
    )

    user_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
        index=True
    )

    status = Column(
        String,
        nullable=False,
        default="reserved",
        index=True
    )

    assigned_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    expires_at = Column(
        DateTime(timezone=True),
        nullable=True,
        index=True
    )

    released_at = Column(
        DateTime(timezone=True),
        nullable=True
    )

    completed_at = Column(
        DateTime(timezone=True),
        nullable=True
    )

    task = relationship(
        "Task",
        back_populates="assignments"
    )

    user = relationship(
        "User",
        back_populates="task_assignments"
    )

    __table_args__ = (
        Index(
            "ix_task_assignments_user_status",
            "user_id",
            "status"
        ),
        Index(
            "ix_task_assignments_task_status",
            "task_id",
            "status"
        ),
    )


# ============================================================
# CONTRIBUTOR PROFILE
# ============================================================

class ContributorProfile(Base):
    """
    Extended workforce profile.

    Keeps workforce-specific information separate from
    authentication/account information.
    """

    __tablename__ = "contributor_profiles"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    user_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
        unique=True,
        index=True
    )

    display_name = Column(
        String,
        nullable=True
    )

    country = Column(
        String,
        nullable=True,
        index=True
    )

    region = Column(
        String,
        nullable=True,
        index=True
    )

    city = Column(
        String,
        nullable=True
    )

    timezone = Column(
        String,
        nullable=True
    )

    status = Column(
        String,
        nullable=False,
        default="active",
        index=True
    )

    onboarding_status = Column(
        String,
        nullable=False,
        default="pending",
        index=True
    )

    overall_quality_score = Column(
        Float,
        nullable=False,
        default=0.0
    )

    tasks_completed = Column(
        Integer,
        nullable=False,
        default=0
    )

    tasks_failed = Column(
        Integer,
        nullable=False,
        default=0
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now
    )

    user = relationship(
        "User",
        back_populates="contributor_profile"
    )


# ============================================================
# DATA COLLECTOR PROFILE
# ============================================================

class DataCollectorProfile(Base):
    """
    Dedicated profile for the KELYVO data-collection workforce.

    Data collectors are intentionally separate from annotators. Their
    recruitment, qualifications and future collection workflow can evolve
    independently without changing the existing ContributorProfile model.
    """

    __tablename__ = "data_collector_profiles"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    user_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
        unique=True,
        index=True
    )

    display_name = Column(String, nullable=True)
    country = Column(String, nullable=True, index=True)
    region = Column(String, nullable=True, index=True)
    city = Column(String, nullable=True)
    pin_code = Column(String, nullable=True)
    area_type = Column(String, nullable=True, index=True)
    timezone = Column(String, nullable=True)
    languages = Column(Text, nullable=True)
    collection_capabilities = Column(Text, nullable=True)
    device_availability = Column(Text, nullable=True)
    collection_environment = Column(Text, nullable=True)
    experience_summary = Column(Text, nullable=True)

    availability_status = Column(
        String,
        nullable=False,
        default="unspecified"
    )
    availability_hours_per_week = Column(Float, nullable=True)

    payment_method = Column(String, nullable=True)
    upi_id = Column(String, nullable=True)
    bank_account_name = Column(String, nullable=True)
    bank_account_number = Column(String, nullable=True)
    bank_ifsc = Column(String, nullable=True)
    bank_name = Column(String, nullable=True)
    bank_branch = Column(String, nullable=True)
    bank_account_type = Column(String, nullable=True)

    consent_accepted = Column(Boolean, nullable=False, default=False)
    consent_version = Column(String, nullable=True)
    consent_accepted_at = Column(DateTime(timezone=True), nullable=True)

    onboarding_status = Column(
        String,
        nullable=False,
        default="pending",
        index=True
    )

    profile_locked = Column(Boolean, nullable=False, default=False)
    payment_locked = Column(Boolean, nullable=False, default=False)
    profile_locked_at = Column(DateTime(timezone=True), nullable=True)
    payment_locked_at = Column(DateTime(timezone=True), nullable=True)
    profile_unlock_reason = Column(Text, nullable=True)
    payment_unlock_reason = Column(Text, nullable=True)

    submitted_at = Column(DateTime(timezone=True), nullable=True)
    reviewed_at = Column(DateTime(timezone=True), nullable=True)
    reviewed_by_user_id = Column(Integer, nullable=True)
    rejection_reason = Column(Text, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now
    )

    user = relationship(
        "User",
        back_populates="data_collector_profile"
    )


# ============================================================
# DATA COLLECTION SUBMISSION
# ============================================================

class DataCollectionSubmission(Base):
    """
    Generic data-collection submission record.

    This is intentionally storage-agnostic: file_reference remains nullable
    until a real collection project and storage provider are connected.
    """

    __tablename__ = "data_collection_submissions"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    collector_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
        index=True
    )

    project_id = Column(
        String,
        nullable=True,
        index=True
    )

    submission_type = Column(
        String,
        nullable=False,
        default="unclassified",
        index=True
    )

    file_reference = Column(
        Text,
        nullable=True
    )

    status = Column(
        String,
        nullable=False,
        default="pending_qa",
        index=True
    )

    qa_status = Column(
        String,
        nullable=False,
        default="pending",
        index=True
    )

    qa_reviewer_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=True,
        index=True
    )

    qa_notes = Column(
        Text,
        nullable=True
    )

    submitted_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    reviewed_at = Column(
        DateTime(timezone=True),
        nullable=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now
    )

    collector = relationship(
        "User",
        foreign_keys=[collector_id],
        back_populates="data_collection_submissions"
    )

    qa_reviewer = relationship(
        "User",
        foreign_keys=[qa_reviewer_id]
    )

    assets = relationship(
        "DataCollectionSubmissionAsset",
        back_populates="submission",
        cascade="all, delete-orphan",
        order_by="DataCollectionSubmissionAsset.created_at.asc()"
    )

    __table_args__ = (
        Index(
            "ix_data_collection_submissions_collector_status",
            "collector_id",
            "status"
        ),
        Index(
            "ix_data_collection_submissions_project_status",
            "project_id",
            "status"
        ),
    )


# ============================================================
# DATA COLLECTION SUBMISSION ASSET
# ============================================================

class DataCollectionSubmissionAsset(Base):
    """
    Storage-agnostic record for a file attached to a data-collection submission.

    This model stores metadata and a provider-specific storage reference. It does
    not assume Google Cloud Storage, S3, local disk, or any other provider.
    """

    __tablename__ = "data_collection_submission_assets"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    submission_id = Column(
        String,
        ForeignKey("data_collection_submissions.id"),
        nullable=False,
        index=True
    )

    original_filename = Column(
        String,
        nullable=False
    )

    stored_filename = Column(
        String,
        nullable=False
    )

    storage_provider = Column(
        String,
        nullable=False,
        default="local",
        index=True
    )

    storage_reference = Column(
        Text,
        nullable=False
    )

    mime_type = Column(
        String,
        nullable=True,
        index=True
    )

    size_bytes = Column(
        Integer,
        nullable=True
    )

    checksum_sha256 = Column(
        String,
        nullable=True,
        index=True
    )

    status = Column(
        String,
        nullable=False,
        default="active",
        index=True
    )

    uploaded_by_user_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
        index=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        index=True
    )

    submission = relationship(
        "DataCollectionSubmission",
        back_populates="assets"
    )

    __table_args__ = (
        Index(
            "ix_data_collection_submission_assets_submission_status",
            "submission_id",
            "status"
        ),
        Index(
            "ix_data_collection_submission_assets_uploader_created",
            "uploaded_by_user_id",
            "created_at"
        ),
    )


# ============================================================
# DATA COLLECTION OPPORTUNITY
# ============================================================

class DataCollectionOpportunity(Base):
    """
    Generic collection opportunity definition.

    This deliberately contains requirements rather than a fixed collection
    template. Real project-specific upload/collection instructions can be
    attached later without changing the workforce layer.
    """

    __tablename__ = "data_collection_opportunities"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    title = Column(
        String,
        nullable=False,
        index=True
    )

    description = Column(
        Text,
        nullable=True
    )

    required_languages = Column(
        Text,
        nullable=True
    )

    required_capabilities = Column(
        Text,
        nullable=True
    )

    required_devices = Column(
        Text,
        nullable=True
    )

    required_environments = Column(
        Text,
        nullable=True
    )

    collectors_needed = Column(
        Integer,
        nullable=False,
        default=1
    )

    status = Column(
        String,
        nullable=False,
        default="open",
        index=True
    )

    created_by_user_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
        index=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now
    )

    created_by_user = relationship(
        "User",
        foreign_keys=[created_by_user_id]
    )

    claims = relationship(
        "DataCollectionOpportunityClaim",
        back_populates="opportunity",
        cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index(
            "ix_data_collection_opportunities_status_created",
            "status",
            "created_at"
        ),
    )


# ============================================================
# DATA COLLECTION OPPORTUNITY CLAIM
# ============================================================

class DataCollectionOpportunityClaim(Base):
    """Collector acceptance of an available collection opportunity."""

    __tablename__ = "data_collection_opportunity_claims"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    opportunity_id = Column(
        String,
        ForeignKey("data_collection_opportunities.id"),
        nullable=False,
        index=True
    )

    collector_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
        index=True
    )

    status = Column(
        String,
        nullable=False,
        default="accepted",
        index=True
    )

    claimed_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now
    )

    opportunity = relationship(
        "DataCollectionOpportunity",
        back_populates="claims"
    )

    collector = relationship(
        "User",
        foreign_keys=[collector_id]
    )

    __table_args__ = (
        UniqueConstraint(
            "opportunity_id",
            "collector_id",
            name="uq_data_collection_opportunity_claim"
        ),
        Index(
            "ix_data_collection_opportunity_claims_collector_status",
            "collector_id",
            "status"
        ),
    )


# ============================================================
# SKILL
# ============================================================

class Skill(Base):
    """
    Reusable workforce/project skill.
    """

    __tablename__ = "skills"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    name = Column(
        String,
        nullable=False,
        unique=True,
        index=True
    )

    category = Column(
        String,
        nullable=True,
        index=True
    )

    description = Column(
        Text,
        nullable=True
    )

    status = Column(
        String,
        nullable=False,
        default="active",
        index=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    contributor_skills = relationship(
        "ContributorSkill",
        back_populates="skill",
        cascade="all, delete-orphan"
    )


# ============================================================
# LANGUAGE
# ============================================================

class Language(Base):
    """
    Language/locale supported by KELYVO's workforce and projects.
    """

    __tablename__ = "languages"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    name = Column(
        String,
        nullable=False
    )

    code = Column(
        String,
        nullable=False,
        unique=True,
        index=True
    )

    locale = Column(
        String,
        nullable=True,
        index=True
    )

    language_family = Column(
        String,
        nullable=True
    )

    status = Column(
        String,
        nullable=False,
        default="active",
        index=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    contributor_languages = relationship(
        "ContributorLanguage",
        back_populates="language",
        cascade="all, delete-orphan"
    )


# ============================================================
# CONTRIBUTOR SKILL
# ============================================================

class ContributorSkill(Base):
    """
    Many-to-many relationship between contributors and skills.
    """

    __tablename__ = "contributor_skills"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    user_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
        index=True
    )

    skill_id = Column(
        String,
        ForeignKey("skills.id"),
        nullable=False,
        index=True
    )

    proficiency = Column(
        String,
        nullable=True
    )

    score = Column(
        Float,
        nullable=True
    )

    verified = Column(
        Boolean,
        nullable=False,
        default=False
    )

    verified_at = Column(
        DateTime(timezone=True),
        nullable=True
    )

    user = relationship(
        "User",
        back_populates="contributor_skills"
    )

    skill = relationship(
        "Skill",
        back_populates="contributor_skills"
    )

    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "skill_id",
            name="uq_contributor_skill"
        ),
    )


# ============================================================
# CONTRIBUTOR LANGUAGE
# ============================================================

class ContributorLanguage(Base):
    """
    Many-to-many relationship between contributors and languages.
    """

    __tablename__ = "contributor_languages"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    user_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
        index=True
    )

    language_id = Column(
        String,
        ForeignKey("languages.id"),
        nullable=False,
        index=True
    )

    proficiency = Column(
        String,
        nullable=True
    )

    is_native = Column(
        Boolean,
        nullable=False,
        default=False
    )

    verified = Column(
        Boolean,
        nullable=False,
        default=False
    )

    user = relationship(
        "User",
        back_populates="contributor_languages"
    )

    language = relationship(
        "Language",
        back_populates="contributor_languages"
    )

    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "language_id",
            name="uq_contributor_language"
        ),
    )


# ============================================================
# ANNOTATION
# ============================================================

class Annotation(Base):
    """
    Canonical KELYVO annotation record.

    Label Studio can remain the annotation engine while KELYVO
    maintains its own business-level annotation history.
    """

    __tablename__ = "annotations"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    task_id = Column(
        String,
        ForeignKey("tasks.id"),
        nullable=False,
        index=True
    )

    annotator_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
        index=True
    )

    external_annotation_id = Column(
        Integer,
        nullable=True,
        index=True
    )

    version = Column(
        Integer,
        nullable=False,
        default=1
    )

    status = Column(
        String,
        nullable=False,
        default="draft",
        index=True
    )

    annotation_data = Column(
        Text,
        nullable=True
    )

    submitted_at = Column(
        DateTime(timezone=True),
        nullable=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now
    )

    task = relationship(
        "Task",
        back_populates="annotations"
    )

    annotator = relationship(
        "User",
        back_populates="annotations"
    )

    submissions = relationship(
        "Submission",
        back_populates="annotation"
    )

    __table_args__ = (
        Index(
            "ix_annotations_task_status",
            "task_id",
            "status"
        ),
        Index(
            "ix_annotations_annotator_status",
            "annotator_id",
            "status"
        ),
    )


# ============================================================
# SUBMISSION
# ============================================================

class Submission(Base):
    """
    New canonical submission entity.

    Existing TaskSubmission remains untouched for compatibility
    with the current portal while the new workflow is introduced.
    """

    __tablename__ = "submissions"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    task_id = Column(
        String,
        ForeignKey("tasks.id"),
        nullable=False,
        index=True
    )

    annotation_id = Column(
        String,
        ForeignKey("annotations.id"),
        nullable=True,
        index=True
    )

    contributor_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
        index=True
    )

    status = Column(
        String,
        nullable=False,
        default="pending_qa",
        index=True
    )

    attempt_number = Column(
        Integer,
        nullable=False,
        default=1
    )

    submitted_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now
    )

    contributor = relationship(
        "User",
        back_populates="submissions_v2"
    )

    task = relationship(
        "Task",
        back_populates="submissions"
    )

    annotation = relationship(
        "Annotation",
        back_populates="submissions"
    )

    qa_reviews = relationship(
        "QAReview",
        back_populates="submission",
        cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index(
            "ix_submissions_task_status",
            "task_id",
            "status"
        ),
        Index(
            "ix_submissions_contributor_status",
            "contributor_id",
            "status"
        ),
    )


# ============================================================
# QA REVIEW
# ============================================================

class QAReview(Base):
    """
    Formal quality-control review.

    Supports multiple QA passes and reviewers.
    """

    __tablename__ = "qa_reviews"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    submission_id = Column(
        String,
        ForeignKey("submissions.id"),
        nullable=False,
        index=True
    )

    task_id = Column(
        String,
        ForeignKey("tasks.id"),
        nullable=False,
        index=True
    )

    reviewer_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
        index=True
    )

    review_round = Column(
        Integer,
        nullable=False,
        default=1
    )

    decision = Column(
        String,
        nullable=False,
        default="pending",
        index=True
    )

    score = Column(
        Float,
        nullable=True
    )

    reviewer_notes = Column(
        Text,
        nullable=True
    )

    reviewed_at = Column(
        DateTime(timezone=True),
        nullable=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    submission = relationship(
        "Submission",
        back_populates="qa_reviews"
    )

    task = relationship(
        "Task",
        back_populates="qa_reviews"
    )

    reviewer = relationship(
        "User",
        back_populates="qa_reviews"
    )

    __table_args__ = (
        Index(
            "ix_qa_reviews_submission_decision",
            "submission_id",
            "decision"
        ),
        Index(
            "ix_qa_reviews_reviewer_created",
            "reviewer_id",
            "created_at"
        ),
    )


# ============================================================
# QUALITY SCORE
# ============================================================

class QualityScore(Base):
    """
    Historical quality measurements for contributors.

    Can later support contributor ranking, routing and
    quality-based task assignment.
    """

    __tablename__ = "quality_scores"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    user_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
        index=True
    )

    project_id = Column(
        String,
        ForeignKey("projects.id"),
        nullable=True,
        index=True
    )

    task_id = Column(
        String,
        ForeignKey("tasks.id"),
        nullable=True,
        index=True
    )

    metric = Column(
        String,
        nullable=False,
        index=True
    )

    score = Column(
        Float,
        nullable=False
    )

    source = Column(
        String,
        nullable=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        index=True
    )

    user = relationship(
        "User",
        back_populates="quality_scores"
    )


# ============================================================
# WORKFLOW
# ============================================================

class Workflow(Base):
    """
    Defines the lifecycle of work inside a project.

    Example:

        Collection
          ↓
        Annotation
          ↓
        QA
          ↓
        Revision
          ↓
        Final approval
          ↓
        Delivery
    """

    __tablename__ = "workflows"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    project_id = Column(
        String,
        ForeignKey("projects.id"),
        nullable=False,
        index=True
    )

    name = Column(
        String,
        nullable=False
    )

    description = Column(
        Text,
        nullable=True
    )

    version = Column(
        Integer,
        nullable=False,
        default=1
    )

    status = Column(
        String,
        nullable=False,
        default="draft",
        index=True
    )

    is_default = Column(
        Boolean,
        nullable=False,
        default=False
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now
    )

    project = relationship(
        "Project",
        back_populates="workflows"
    )

    steps = relationship(
        "WorkflowStep",
        back_populates="workflow",
        cascade="all, delete-orphan",
        order_by="WorkflowStep.step_order"
    )


# ============================================================
# WORKFLOW STEP
# ============================================================

class WorkflowStep(Base):
    """
    Individual stage inside a workflow.
    """

    __tablename__ = "workflow_steps"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    workflow_id = Column(
        String,
        ForeignKey("workflows.id"),
        nullable=False,
        index=True
    )

    step_order = Column(
        Integer,
        nullable=False
    )

    name = Column(
        String,
        nullable=False
    )

    step_type = Column(
        String,
        nullable=False,
        index=True
    )

    description = Column(
        Text,
        nullable=True
    )

    status = Column(
        String,
        nullable=False,
        default="active"
    )

    configuration = Column(
        Text,
        nullable=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    workflow = relationship(
        "Workflow",
        back_populates="steps"
    )

    __table_args__ = (
        UniqueConstraint(
            "workflow_id",
            "step_order",
            name="uq_workflow_step_order"
        ),
    )


# ============================================================
# DELIVERY
# ============================================================

class Delivery(Base):
    """
    Records delivery of completed work to a client/project.
    """

    __tablename__ = "deliveries"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    project_id = Column(
        String,
        ForeignKey("projects.id"),
        nullable=False,
        index=True
    )

    dataset_version_id = Column(
        String,
        ForeignKey("dataset_versions.id"),
        nullable=True,
        index=True
    )

    delivery_code = Column(
        String,
        nullable=False,
        unique=True,
        index=True
    )

    status = Column(
        String,
        nullable=False,
        default="pending",
        index=True
    )

    format = Column(
        String,
        nullable=True
    )

    destination_uri = Column(
        Text,
        nullable=True
    )

    task_count = Column(
        Integer,
        nullable=False,
        default=0
    )

    checksum = Column(
        String,
        nullable=True
    )

    delivered_at = Column(
        DateTime(timezone=True),
        nullable=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now
    )

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now
    )

    project = relationship(
        "Project",
        back_populates="deliveries"
    )


# ============================================================
# PAYOUT LEDGER
# ============================================================

class PayoutLedger(Base):
    """
    Financial transaction ledger for contributor earnings.

    This is deliberately separate from User.earnings so that
    future financial history is auditable.
    """

    __tablename__ = "payout_ledger"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    user_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
        index=True
    )

    project_id = Column(
        String,
        ForeignKey("projects.id"),
        nullable=True,
        index=True
    )

    task_id = Column(
        String,
        ForeignKey("tasks.id"),
        nullable=True,
        index=True
    )

    transaction_type = Column(
        String,
        nullable=False,
        index=True
    )

    amount = Column(
        Float,
        nullable=False
    )

    currency = Column(
        String,
        nullable=False,
        default="INR"
    )

    status = Column(
        String,
        nullable=False,
        default="pending",
        index=True
    )

    reference = Column(
        String,
        nullable=True,
        index=True
    )

    description = Column(
        Text,
        nullable=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        index=True
    )

    processed_at = Column(
        DateTime(timezone=True),
        nullable=True
    )

    user = relationship(
        "User",
        back_populates="payout_ledger_entries"
    )

    __table_args__ = (
        Index(
            "ix_payout_ledger_user_created",
            "user_id",
            "created_at"
        ),
    )


# ============================================================
# AUDIT LOG
# ============================================================

class AuditLog(Base):
    """
    Permanent record of important system actions.

    Foundation for enterprise security, investigations,
    compliance and accountability.
    """

    __tablename__ = "audit_logs"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    organization_id = Column(
        String,
        ForeignKey("organizations.id"),
        nullable=True,
        index=True
    )

    user_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=True,
        index=True
    )

    action = Column(
        String,
        nullable=False,
        index=True
    )

    resource_type = Column(
        String,
        nullable=True,
        index=True
    )

    resource_id = Column(
        String,
        nullable=True,
        index=True
    )

    ip_address = Column(
        String,
        nullable=True
    )

    user_agent = Column(
        Text,
        nullable=True
    )

    details = Column(
        Text,
        nullable=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        index=True
    )

    organization = relationship(
        "Organization",
        back_populates="audit_logs"
    )

    __table_args__ = (
        Index(
            "ix_audit_logs_org_created",
            "organization_id",
            "created_at"
        ),
        Index(
            "ix_audit_logs_user_created",
            "user_id",
            "created_at"
        ),
    )

# ============================================================
# SUPPORT TICKETS
# ============================================================

class SupportTicket(Base):
    """
    User support requests routed directly into the KELYVO admin
    operations workspace. Attachments remain storage-provider
    agnostic and use the existing storage service.
    """
    __tablename__ = "support_tickets"

    id = Column(
        String,
        primary_key=True,
        default=generate_uuid
    )

    user_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
        index=True
    )

    role = Column(
        String,
        nullable=False,
        index=True
    )

    email_snapshot = Column(
        String,
        nullable=False
    )

    message = Column(
        Text,
        nullable=False
    )

    attachment_path = Column(
        Text,
        nullable=True
    )

    attachment_name = Column(
        String,
        nullable=True
    )

    attachment_mime_type = Column(
        String,
        nullable=True
    )

    status = Column(
        String,
        nullable=False,
        default="open",
        index=True
    )

    admin_notes = Column(
        Text,
        nullable=True
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        index=True
    )

    resolved_at = Column(
        DateTime(timezone=True),
        nullable=True
    )

    __table_args__ = (
        Index(
            "ix_support_tickets_status_created",
            "status",
            "created_at"
        ),
        Index(
            "ix_support_tickets_user_created",
            "user_id",
            "created_at"
        ),
    )
