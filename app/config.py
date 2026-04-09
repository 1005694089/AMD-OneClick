"""
Configuration settings for AMD OneClick Notebook Manager
"""
import os
from typing import Any, Optional


class Settings:
    # K8s Configuration
    K8S_NAMESPACE: str = os.getenv("K8S_NAMESPACE", "default")
    
    # Default Notebook Image
    DEFAULT_IMAGE: str = os.getenv(
        "DEFAULT_IMAGE", 
        "crpi-07r6ldyx2gp3ntwb.cn-shanghai.personal.cr.aliyuncs.com/amd_docker_mirage/rocm-vllm-dev:rocm7.1.1_navi_ubuntu24.04_py3.12_pytorch_2.8_vllm_0.10.2rc1"
    )

    # PaddleOCR-VL image (full notebook + OCR environment)
    PADDLEOCR_VL_IMAGE: str = os.getenv(
        "PADDLEOCR_VL_IMAGE",
        "crpi-07r6ldyx2gp3ntwb.cn-shanghai.personal.cr.aliyuncs.com/amd_docker_mirage/paddleocr-vl:all-in-one-notebook-20260409",
    )
    
    # Available Images (can be extended)
    AVAILABLE_IMAGES: list = [
        DEFAULT_IMAGE,
        PADDLEOCR_VL_IMAGE,
    ]

    # Public instance cards shown on the launch page.
    # Dict order controls tile placement.
    INSTANCE_TYPES: dict[str, dict[str, Any]] = {
        "jupyter": {
            "name": "Jupyter Notebook",
            "description": "GPU-powered Jupyter Lab for data science and AI development",
            "icon": "📓",
            "enabled": True,
            "image": DEFAULT_IMAGE,
            "max_lifetime_hours": None,
            "idle_timeout_minutes": None,
        },
        "opencode": {
            "name": "OpenCode",
            "description": "AI coding agent in terminal - launch opencode from Jupyter",
            "icon": "🤖",
            "enabled": True,
            "image": DEFAULT_IMAGE,
            "max_lifetime_hours": 2160,
            "idle_timeout_minutes": 0,
        },
        "openclaw": {
            "name": "OpenCLAW",
            "description": "Coming soon",
            "icon": "🔬",
            "enabled": False,
            "image": DEFAULT_IMAGE,
            "max_lifetime_hours": None,
            "idle_timeout_minutes": None,
        },
        "paddleocr_vl": {
            "name": "PaddleOCR-VL",
            "description": "Complete OCR-VL notebook with full Paddle base environment",
            "icon": "🧾",
            "enabled": True,
            "image": PADDLEOCR_VL_IMAGE,
            "max_lifetime_hours": None,
            "idle_timeout_minutes": None,
        },
    }

    # Optional per-instance resource overrides.
    INSTANCE_TYPE_RESOURCES: dict[str, dict[str, str]] = {
        "paddleocr_vl": {
            "cpu_limit": os.getenv("PADDLEOCR_CPU_LIMIT", "128"),
            "memory_limit": os.getenv("PADDLEOCR_MEMORY_LIMIT", "256Gi"),
            "cpu_request": os.getenv("PADDLEOCR_CPU_REQUEST", "64"),
            "memory_request": os.getenv("PADDLEOCR_MEMORY_REQUEST", "128Gi"),
            "gpu_limit": os.getenv("PADDLEOCR_GPU_LIMIT", "1"),
            "gpu_memory_utilization": os.getenv("PADDLEOCR_GPU_MEMORY_UTILIZATION", "0.85"),
        }
    }
    
    # Notebook Configuration
    NOTEBOOK_TOKEN: str = os.getenv("NOTEBOOK_TOKEN", "amd-oneclick")
    NOTEBOOK_PORT: int = 8888
    NOTEBOOK_LABEL_PREFIX: str = "amd-oneclick"
    
    # Resource Limits
    CPU_LIMIT: str = os.getenv("CPU_LIMIT", "128")
    MEMORY_LIMIT: str = os.getenv("MEMORY_LIMIT", "256Gi")
    GPU_LIMIT: str = os.getenv("GPU_LIMIT", "1")
    CPU_REQUEST: str = os.getenv("CPU_REQUEST", "40")
    MEMORY_REQUEST: str = os.getenv("MEMORY_REQUEST", "48Gi")
    
    # Cleanup Configuration
    IDLE_TIMEOUT_MINUTES: int = int(os.getenv("IDLE_TIMEOUT_MINUTES", "10"))
    MAX_LIFETIME_HOURS: int = int(os.getenv("MAX_LIFETIME_HOURS", "6"))
    
    # Email Configuration (optional)
    SMTP_HOST: Optional[str] = os.getenv("SMTP_HOST")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USER: Optional[str] = os.getenv("SMTP_USER")
    SMTP_PASSWORD: Optional[str] = os.getenv("SMTP_PASSWORD")
    SMTP_FROM: str = os.getenv("SMTP_FROM", "noreply@amd-oneclick.local")
    
    # Service Configuration
    SERVICE_HOST: str = os.getenv("SERVICE_HOST", "localhost")
    NOTEBOOK_PROXY_HOST: Optional[str] = os.getenv("NOTEBOOK_PROXY_HOST")
    NODE_PORT_BASE: int = int(os.getenv("NODE_PORT_BASE", "30000"))
    NOTEBOOK_IMAGE_PULL_SECRET: Optional[str] = os.getenv("NOTEBOOK_IMAGE_PULL_SECRET")
    
    # PyPI Mirror for China
    PYPI_MIRROR: str = "https://pypi.tuna.tsinghua.edu.cn/simple"
    PYPI_HOST: str = "pypi.tuna.tsinghua.edu.cn"
    PYPI_HOST_IP: str = "101.6.15.130"
    
    # Admin Configuration
    ADMIN_PASSWORD: str = os.getenv("ADMIN_PASSWORD", "admin123")

    DEFAULT_INSTANCE_TYPE: str = "jupyter"

    def get_instance_type(self, instance_type: Optional[str]) -> Optional[dict[str, Any]]:
        """Fetch instance type config by key."""
        if not instance_type:
            return self.INSTANCE_TYPES.get(self.DEFAULT_INSTANCE_TYPE)
        return self.INSTANCE_TYPES.get(instance_type)

    def get_public_instance_types(self) -> dict[str, dict[str, Any]]:
        """Return public instance type metadata for UI rendering."""
        public_types: dict[str, dict[str, Any]] = {}
        for key, value in self.INSTANCE_TYPES.items():
            public_types[key] = {
                "name": value["name"],
                "description": value["description"],
                "icon": value["icon"],
                "enabled": value["enabled"],
                "max_lifetime_hours": value.get("max_lifetime_hours"),
                "idle_timeout_minutes": value.get("idle_timeout_minutes"),
            }
        return public_types

    def get_image_for_instance_type(self, instance_type: str, requested_image: Optional[str]) -> str:
        """Resolve image based on instance type mapping, with request fallback."""
        type_config = self.get_instance_type(instance_type) or {}
        mapped_image = type_config.get("image")
        return mapped_image or requested_image or self.DEFAULT_IMAGE


settings = Settings()
