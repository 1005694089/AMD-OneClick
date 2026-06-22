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


class ImageRequest(BaseModel):
    """Request model for managing image catalog entries"""
    name: str
    image: str
    description: Optional[str] = ""
    enabled: bool = True


class CreditGrantRequest(BaseModel):
    """Request model for manually granting credits to a user"""
    amount: int
    reason: Optional[str] = "manual admin grant"


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


class TemplateLaunchRequest(BaseModel):
    """Request model for launching an instance from a notebook template"""
    gpu_count: int = 1


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
    email: Optional[str] = None
    instance_id: Optional[str] = None
    phase: Optional[str] = None
    reason: Optional[str] = None
    detail: Optional[str] = None
    ready: bool = False


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
