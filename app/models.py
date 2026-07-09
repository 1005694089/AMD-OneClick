"""
Data models for AMD OneClick Notebook Manager
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, EmailStr


class NotebookRequest(BaseModel):
    """Request model for creating a notebook instance"""
    email: Optional[EmailStr] = None
    image: Optional[str] = None
    instance_type: str = "jupyter"
    gpu_count: int = 1
    resource_profile: Optional[str] = "auto"
    disk_size_gb: Optional[int] = None
    pod_type: Optional[str] = None
    # None = server default (durable). true = force durable PVC-backed storage. false = ephemeral
    # local-SSD-only (no durable NFS hydrate/flush; data does not survive a pod restart).
    use_pvc: Optional[bool] = None


class ImageRequest(BaseModel):
    """Request model for managing image catalog entries"""
    name: str
    image: Optional[str] = None
    description: Optional[str] = ""
    enabled: bool = True
    source_type: Optional[str] = None
    source_ref: Optional[str] = None
    github_url: Optional[str] = None


class CreditGrantRequest(BaseModel):
    """Request model for manually granting credits to a user"""
    amount: int
    reason: Optional[str] = "manual admin grant"


class EditorGrantRequest(BaseModel):
    """Request model for granting/revoking editor (template publishing) permission"""
    is_editor: bool


class InstanceBulkDestroyRequest(BaseModel):
    """Request model for bulk destroying instances by email matcher"""
    matcher: str


class CouponRedeemRequest(BaseModel):
    """Request model for redeeming encrypted credit coupons"""
    coupon: str


class NotebookTemplateRequest(BaseModel):
    """Request model for managing notebook templates"""
    title: str
    slug: Optional[str] = ""
    description: Optional[str] = ""
    category: Optional[str] = ""
    tags: Optional[list[str] | str] = ""
    image: str
    repo_url: Optional[str] = ""
    branch: str = "main"
    notebook_path: Optional[str] = ""
    cover_url: Optional[str] = ""
    enabled: bool = True
    sort_order: int = 0
    instance_type: Optional[str] = ""
    start_command: Optional[str] = ""
    app_port: Optional[int] = None
    model_source: Optional[str] = ""
    ssh_enabled: bool = False
    use_pvc: Optional[bool] = False


class SshPublicKeyRequest(BaseModel):
    """Request model for saving the user's SSH public key (Profile)."""
    ssh_public_key: str = ""


class TemplateLaunchRequest(BaseModel):
    """Request model for launching an instance from a notebook template"""
    gpu_count: int = 1


class HuggingFaceNotebookLaunchRequest(BaseModel):
    """Request model for launching a GitHub notebook from the Hugging Face demo API"""
    user_name: str
    notebook_path: Optional[str] = None
    gpu_count: int = 1
    image: Optional[str] = None
    pod_type: Optional[str] = None
    # When true, the demo user is marked unlimited: their credit balance is frozen (never
    # decremented by the billing loop) at whatever it is when this flag is applied. Does not
    # exempt the instance from the API idle reaper (still destroyed after 8h idle).
    unlimited_credits: bool = False
    # Optional GitHub token for cloning a PRIVATE repo. Only honored for a `.git` workshop launch
    # (pod_type='workshop') whose resolved clone URL is https://; rejected otherwise. The
    # authenticated clone runs in a dedicated init container (never the user's notebook container),
    # with the token consumed by git via GIT_ASKPASS — never on the git argv nor in .git/config.
    git_token: Optional[str] = None
    # None = server default (durable). true = force durable PVC-backed storage. false = ephemeral
    # local-SSD-only (no durable NFS hydrate/flush; data does not survive a pod restart).
    use_pvc: Optional[bool] = None
    # Optional subdirectory of a cloned `.git` repo to open as /workspace instead of the repo root.
    # Only honored for a `.git` workshop launch (pod_type='workshop'); rejected otherwise. Must not
    # contain '..' path-traversal components.
    repo_sub_path: Optional[str] = None


class CustomImageBuildRequest(BaseModel):
    """Request model for enqueuing a custom image build"""
    name: str
    dockerfile: Optional[str] = None
    source_type: str = "dockerfile"
    github_url: Optional[str] = None


class BuildClaimRequest(BaseModel):
    """Build-agent request to lease the next pending build"""
    agent_id: str


class BuildLogRequest(BaseModel):
    """Build-agent log chunk"""
    agent_id: str
    log: str


class BuildResultRequest(BaseModel):
    """Build-agent terminal result for a build"""
    agent_id: str
    status: str  # ready | failed


class BuildEvictRequest(BaseModel):
    """Build-agent notification that node-local image content was reclaimed."""
    agent_id: str
    image_ids: list[int] = []


class ImageJobClaimRequest(BaseModel):
    """Image-service request to lease the next pending job, optionally filtered by kind."""
    agent_id: str
    kinds: Optional[list[str]] = None


class ImageJobLogRequest(BaseModel):
    """Image-service log chunk for a job"""
    agent_id: str
    log: str


class ImageJobHeartbeatRequest(BaseModel):
    """Image-service liveness ping for a long-running job (keeps the lease fresh)."""
    agent_id: str


class ImageJobResultRequest(BaseModel):
    """Image-service terminal result for a job"""
    agent_id: str
    status: str  # succeeded | failed
    result: Optional[dict] = None


class ImageNodeStatusRequest(BaseModel):
    """Image-service report of a node's status for an image ref (importing | quarantined | loaded)."""
    agent_id: str
    node: str
    ref: str
    status: str
    quarantine_seconds: Optional[int] = None


class GitHubNotebookInfo(BaseModel):
    """GitHub notebook information"""
    org: str
    repo: str
    branch: str
    path: str
    raw_url: str


class NotebookInstance(BaseModel):
    """Model representing a notebook instance"""
    id: str
    email: str
    pod_name: str
    service_name: str
    image: str
    url: str
    status: str  # pending, creating, running, terminating, failed
    created_at: datetime
    last_activity: Optional[datetime] = None
    node_port: Optional[int] = None
    github_info: Optional[GitHubNotebookInfo] = None


class NotebookStatus(BaseModel):
    """Status response for notebook creation"""
    status: str  # allocating, loading, initializing, ready, failed
    message: str
    url: Optional[str] = None
    opencode_url: Optional[str] = None
    opencode_username: Optional[str] = None
    opencode_password: Optional[str] = None
    email: Optional[str] = None
    instance_id: Optional[str] = None
    phase: Optional[str] = None
    reason: Optional[str] = None
    detail: Optional[str] = None
    ready: bool = False
    instance_type: Optional[str] = None
    app_port: Optional[int] = None
    api_base_url: Optional[str] = None
    api_key: Optional[str] = None
    api_model: Optional[str] = None
    # /spaces/<id>/8501/ once a hackathon user's Streamlit app is live; None otherwise
    # (never set for non-hackathon pod_type, even if something is listening on 8501).
    streamlit_url: Optional[str] = None
    ssh_host: Optional[str] = None
    ssh_port: Optional[int] = None
    ssh_username: Optional[str] = None
    ssh_command: Optional[str] = None


class NotebookListItem(BaseModel):
    """Item in the notebook list for admin view"""
    id: str
    email: str
    pod_name: str
    url: str
    status: str
    created_at: str
    last_activity: Optional[str] = None
    uptime_minutes: int
    instance_type: str = "jupyter"
    gpu_count: int = 1
    github_org: Optional[str] = None
    github_repo: Optional[str] = None
    github_path: Optional[str] = None


class AdminListResponse(BaseModel):
    """Response for admin list endpoint"""
    instances: list[NotebookListItem]
    total_count: int


class DestroyResponse(BaseModel):
    """Response for destroy operations"""
    success: bool
    message: str
    destroyed_count: int = 0
