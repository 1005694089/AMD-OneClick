"""
OSS workspace backup helpers.

All Aliyun SDK imports are lazy so the manager can boot with OSS disabled even
when the optional packages or runtime secrets are not installed yet.
"""
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional

from kubernetes.client.rest import ApiException

from .config import settings

logger = logging.getLogger(__name__)


def _configured_value(value: str) -> bool:
    raw = (value or "").strip()
    return bool(raw and not (raw.startswith("<") and raw.endswith(">")))


def oss_runtime_configured() -> bool:
    return bool(
        settings.OSS_ENABLED
        and _configured_value(settings.OSS_BUCKET)
        and _configured_value(settings.OSS_ENDPOINT)
        and _configured_value(settings.OSS_REGION)
        and _configured_value(settings.OSS_STS_ROLE_ARN)
        and settings.OSS_STS_ROLE_ARN.startswith("acs:ram::")
        and _configured_value(settings.OSS_RAM_ACCESS_KEY_ID)
        and _configured_value(settings.OSS_RAM_ACCESS_KEY_SECRET)
    )


def oss_instance_enabled(user_id: Optional[int]) -> bool:
    return bool(settings.OSS_ENABLED and user_id and settings.OSS_BUCKET and settings.OSS_ENDPOINT)


def user_prefix(user_id: int) -> str:
    prefix = settings.OSS_BACKUP_PREFIX.strip("/")
    return f"{prefix}/{int(user_id)}"


def workspace_prefix(user_id: int) -> str:
    return f"{user_prefix(user_id)}/workspace/"


def sts_secret_name(instance_id: str, launch_id: Optional[str] = None) -> str:
    safe = re.sub(r"[^a-z0-9.-]", "-", instance_id.lower()).strip("-")
    suffix = ""
    if launch_id:
        safe_launch = re.sub(r"[^a-z0-9.-]", "-", str(launch_id).lower()).strip("-")
        if safe_launch:
            suffix = f"-{safe_launch}"
    safe = safe[: max(1, 63 - len("oss-sts-") - len(suffix))]
    return f"oss-sts-{safe}{suffix}"[:63].rstrip("-")


def sts_policy_for_user(user_id: int) -> dict:
    prefix = user_prefix(user_id)
    bucket = settings.OSS_BUCKET
    return {
        "Version": "1",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["oss:ListObjects", "oss:GetBucketInfo"],
                "Resource": [f"acs:oss:*:*:{bucket}"],
                "Condition": {
                    "StringLike": {
                        "oss:Prefix": [f"{prefix}/", f"{prefix}/*"]
                    }
                },
            },
            {
                "Effect": "Allow",
                "Action": ["oss:GetObject", "oss:PutObject", "oss:DeleteObject", "oss:AbortMultipartUpload"],
                "Resource": [f"acs:oss:*:*:{bucket}/{prefix}/*"],
            },
        ],
    }


def mint_sts_for_user(user_id: int) -> dict:
    if not oss_runtime_configured():
        raise RuntimeError("OSS is enabled but OSS/STS runtime configuration is incomplete")

    try:
        from aliyunsdkcore.client import AcsClient
        from aliyunsdksts.request.v20150401.AssumeRoleRequest import AssumeRoleRequest
    except Exception as e:
        raise RuntimeError(f"Aliyun STS SDK is not installed: {e}") from e

    client = AcsClient(settings.OSS_RAM_ACCESS_KEY_ID, settings.OSS_RAM_ACCESS_KEY_SECRET, settings.OSS_REGION)
    request = AssumeRoleRequest()
    request.set_accept_format("json")
    request.set_RoleArn(settings.OSS_STS_ROLE_ARN)
    request.set_RoleSessionName(f"oneclick-u{int(user_id)}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}")
    request.set_DurationSeconds(max(900, settings.OSS_STS_DURATION_SECONDS))
    request.set_Policy(json.dumps(sts_policy_for_user(user_id), separators=(",", ":")))

    last_error = None
    for attempt in range(1, 4):
        try:
            response = client.do_action_with_exception(request)
            break
        except Exception as e:
            last_error = e
            if attempt == 3:
                raise
            logger.warning("STS AssumeRole failed for user %s on attempt %s: %s", user_id, attempt, e)
            time.sleep(attempt)
    else:
        raise RuntimeError(f"STS AssumeRole failed: {last_error}")

    payload = json.loads(response.decode("utf-8") if isinstance(response, bytes) else response)
    creds = payload["Credentials"]
    return {
        "access_key_id": creds["AccessKeyId"],
        "access_key_secret": creds["AccessKeySecret"],
        "security_token": creds["SecurityToken"],
        "expiration": creds["Expiration"],
    }


def _secret_body(instance_id: str, user_id: int, creds: dict, secret_name: Optional[str] = None, launch_id: Optional[str] = None) -> dict:
    name = secret_name or sts_secret_name(instance_id, launch_id)
    labels = {
        "app": "amd-oneclick-oss-sts",
        "instance-id": instance_id,
        "user-id": str(int(user_id)),
    }
    if launch_id:
        labels["launch-id"] = str(launch_id)
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name,
            "labels": labels,
        },
        "type": "Opaque",
        "stringData": {
            "access_key_id": creds["access_key_id"],
            "access_key_secret": creds["access_key_secret"],
            "security_token": creds["security_token"],
            "expiration": creds.get("expiration", ""),
        },
    }


def ensure_instance_secret(core_v1, namespace: str, instance_id: str, user_id: int, secret_name: Optional[str] = None, launch_id: Optional[str] = None) -> str:
    creds = mint_sts_for_user(user_id)
    name = secret_name or sts_secret_name(instance_id, launch_id)
    body = _secret_body(instance_id, user_id, creds, secret_name=name, launch_id=launch_id)
    try:
        core_v1.create_namespaced_secret(namespace=namespace, body=body)
        logger.info("Created OSS STS Secret %s for instance %s", name, instance_id)
    except ApiException as e:
        if e.status != 409:
            raise
        core_v1.patch_namespaced_secret(
            name=name,
            namespace=namespace,
            body={
                "metadata": {
                    "labels": body["metadata"]["labels"],
                },
                "stringData": body["stringData"],
                "type": body["type"],
            },
        )
        logger.info("Refreshed OSS STS Secret %s for instance %s", name, instance_id)
    return name


def delete_instance_secret(core_v1, namespace: str, instance_id: str, secret_name: Optional[str] = None) -> bool:
    if not secret_name:
        return delete_instance_secrets(core_v1, namespace, instance_id)
    name = secret_name
    try:
        core_v1.delete_namespaced_secret(name=name, namespace=namespace)
        logger.info("Deleted OSS STS Secret %s", name)
        return True
    except ApiException as e:
        if e.status != 404:
            logger.warning("Failed to delete OSS STS Secret %s: %s", name, e)
        return False


def delete_instance_secrets(core_v1, namespace: str, instance_id: str) -> bool:
    label_selector = f"app=amd-oneclick-oss-sts,instance-id={instance_id}"
    try:
        secrets = core_v1.list_namespaced_secret(namespace=namespace, label_selector=label_selector)
    except ApiException as e:
        logger.warning("Failed to list OSS STS Secrets for %s: %s", instance_id, e)
        return False

    deleted = False
    for secret in secrets.items:
        try:
            core_v1.delete_namespaced_secret(name=secret.metadata.name, namespace=namespace)
            logger.info("Deleted OSS STS Secret %s", secret.metadata.name)
            deleted = True
        except ApiException as e:
            if e.status != 404:
                logger.warning("Failed to delete OSS STS Secret %s: %s", secret.metadata.name, e)
    return deleted


def refresh_instance_secrets(core_v1, namespace: str, active_records: list[dict]) -> dict:
    if not oss_runtime_configured():
        return {"refreshed": 0, "failed": [], "skipped": len(active_records)}

    refreshed = 0
    failed = []
    for record in active_records:
        user_id = record.get("user_id")
        instance_id = record.get("instance_id") or record.get("id")
        secret_name = record.get("oss_secret_name")
        if not user_id or not instance_id:
            continue
        try:
            ensure_instance_secret(core_v1, namespace, str(instance_id), int(user_id), secret_name=secret_name)
            refreshed += 1
        except Exception as e:
            logger.warning("Failed to refresh OSS STS Secret for %s: %s", instance_id, e)
            failed.append({"instance_id": instance_id, "error": str(e)})
    return {"refreshed": refreshed, "failed": failed, "skipped": 0}


def _oss_bucket_for_user(user_id: int):
    try:
        import oss2
    except Exception as e:
        raise RuntimeError(f"oss2 SDK is not installed: {e}") from e

    creds = mint_sts_for_user(user_id)
    endpoint = settings.OSS_ENDPOINT
    if not endpoint.startswith(("http://", "https://")):
        endpoint = f"https://{endpoint}"
    auth = oss2.StsAuth(creds["access_key_id"], creds["access_key_secret"], creds["security_token"])
    return oss2.Bucket(auth, endpoint, settings.OSS_BUCKET), oss2


def get_user_backup_status(user_id: int) -> dict:
    if not settings.OSS_ENABLED:
        return {"enabled": False}
    if not oss_runtime_configured():
        return {"enabled": True, "configured": False, "error": "OSS/STS runtime configuration is incomplete"}

    try:
        bucket, oss2 = _oss_bucket_for_user(user_id)
        total_bytes = 0
        object_count = 0
        latest_modified = None
        for obj in oss2.ObjectIterator(bucket, prefix=workspace_prefix(user_id)):
            object_count += 1
            total_bytes += int(getattr(obj, "size", 0) or 0)
            last_modified = getattr(obj, "last_modified", None)
            if last_modified and (latest_modified is None or last_modified > latest_modified):
                latest_modified = last_modified
        latest_iso = None
        if latest_modified:
            latest_iso = datetime.fromtimestamp(latest_modified, timezone.utc).isoformat()
        quota_bytes = settings.OSS_INSTANCE_QUOTA_GB * 1024 * 1024 * 1024
        return {
            "enabled": True,
            "configured": True,
            "prefix": workspace_prefix(user_id),
            "used_bytes": total_bytes,
            "quota_bytes": quota_bytes,
            "object_count": object_count,
            "last_backup_at": latest_iso,
        }
    except Exception as e:
        logger.warning("Failed to read OSS backup status for user %s: %s", user_id, e)
        return {"enabled": True, "configured": oss_runtime_configured(), "error": str(e)}


def delete_user_prefix(user_id: int, dry_run: bool = True) -> dict:
    if not oss_runtime_configured():
        return {"user_id": user_id, "deleted": 0, "dry_run": dry_run, "skipped": True}

    bucket, oss2 = _oss_bucket_for_user(user_id)
    prefix = f"{user_prefix(user_id)}/"
    keys = [obj.key for obj in oss2.ObjectIterator(bucket, prefix=prefix)]
    if not dry_run:
        for start in range(0, len(keys), 1000):
            bucket.batch_delete_objects(keys[start:start + 1000])
    return {"user_id": user_id, "deleted": len(keys), "dry_run": dry_run, "prefix": prefix}
