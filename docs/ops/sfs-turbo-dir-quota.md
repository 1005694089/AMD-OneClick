# Huawei SFS Turbo 目录配额设置说明

本文记录 `new_cluster_prod.yaml` 中华为云 SFS Turbo NFS 盘的挂载方式和目录 quota 设置方法。不要在本文或任何提交中记录 AK/SK。

## 关键信息

- Kubeconfig：`new_cluster_prod.yaml`
- SFS Turbo API endpoint：`https://sfs-turbo.cidc-rp-12.joint.cmecloud.cn`
- Region：`CIDC-RP-12`
- Project ID：`f7878b039c0a4f92a10edf20e323200c`
- 凭证文件：本地 `env.sh`，已加入 `.gitignore`，权限应为 `600`

`new_cluster.yaml` 当前没有 StorageClass。华为云 SFS Turbo StorageClass 在 `new_cluster_prod.yaml`：

| StorageClass | Server / share |
|---|---|
| `managed-nfs-storage-2` | `38cb5453-92e5-4f00-a4e8-a71dec09b855.sfsturbo.internal:/` |
| `managed-nfs-storage-3` | `8b8ff68f-e2cf-4ad5-a800-687332ba0145.sfsturbo.internal:/` |
| `managed-nfs-storage-4` | `8e37933a-f1cd-432d-ad83-d5bad06de76f.sfsturbo.internal:/` |
| `managed-nfs-storage-5` | `ac8711bc-6819-4c71-a28c-6befd494ca81.sfsturbo.internal:/` |

历史 PV 中也出现过 `managed-nfs-storage-1`，server 为 `712f4074-688c-4143-8130-6e4181a31813.sfsturbo.internal:/`。

## PVC 与目录 quota 的关系

`nfs.csi.k8s.io` 动态创建 PVC 时，会在对应 SFS Turbo 文件系统根目录下创建一个 `pvc-<uuid>` 子目录。例如：

```text
Filesystem:
38cb5453-92e5-4f00-a4e8-a71dec09b855.sfsturbo.internal:/pvc-5df...
```

Kubernetes PVC 的 `resources.requests.storage` 只会成为 PV 元数据，不会自动限制 SFS Turbo 目录容量。未设置目录 quota 时，Pod 内 `df` 会看到整个文件系统容量（约 101T）。

要设置目录 quota，需要从 PV 读取：

```bash
PV=$(kubectl -n <namespace> get pvc <pvc-name> -o jsonpath='{.spec.volumeName}')
kubectl get pv "$PV" -o jsonpath='{.spec.csi.volumeAttributes.server}{"\n"}{.spec.csi.volumeAttributes.subdir}{"\n"}'
```

映射关系：

- `share_id`：`server` 中 `.sfsturbo.internal` 前面的 UUID
- `path`：`/<subdir>`

## 本地环境变量

`env.sh` 示例，实际 AK/SK 只放本地，不提交：

```bash
export HUAWEICLOUD_SDK_AK='<AK>'
export HUAWEICLOUD_SDK_SK='<SK>'
export HUAWEICLOUD_REGION='CIDC-RP-12'
export HUAWEICLOUD_PROJECT_ID='f7878b039c0a4f92a10edf20e323200c'
export HUAWEICLOUD_SFSTURBO_ENDPOINT='https://sfs-turbo.cidc-rp-12.joint.cmecloud.cn'
```

加载：

```bash
source ./env.sh
```

## 设置目录 quota

安装 SDK：

```bash
python3 -m pip install --user huaweicloudsdkcore huaweicloudsdksfsturbo
```

设置 `capacity` 单位是 MB。示例设置 10Gi：

```python
import os
from huaweicloudsdkcore.auth.credentials import BasicCredentials
from huaweicloudsdkcore.region.region import Region
from huaweicloudsdksfsturbo.v1.sfsturbo_client import SFSTurboClient
from huaweicloudsdksfsturbo.v1.model.create_fs_dir_quota_request import CreateFsDirQuotaRequest
from huaweicloudsdksfsturbo.v1.model.create_fs_dir_quota_request_body import CreateFsDirQuotaRequestBody

share_id = "38cb5453-92e5-4f00-a4e8-a71dec09b855"
path = "/pvc-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

client = SFSTurboClient.new_builder() \
    .with_credentials(BasicCredentials(
        os.environ["HUAWEICLOUD_SDK_AK"],
        os.environ["HUAWEICLOUD_SDK_SK"],
        os.environ["HUAWEICLOUD_PROJECT_ID"],
    )) \
    .with_region(Region(
        os.environ["HUAWEICLOUD_REGION"],
        os.environ["HUAWEICLOUD_SFSTURBO_ENDPOINT"],
    )) \
    .build()

resp = client.create_fs_dir_quota(CreateFsDirQuotaRequest(
    share_id=share_id,
    body=CreateFsDirQuotaRequestBody(
        path=path,
        capacity=10240,
        inode=1000000,
    ),
))
print(resp)
```

如果 quota 已存在，用 `UpdateFsDirQuotaRequest` / `UpdateFsDirQuotaRequestBody` 更新。

## 验证方法

在 Pod 内查看：

```bash
df -hT /workspace
```

设置 quota 后，`df` 应显示 quota 容量，而不是整个 SFS Turbo 文件系统容量。

写入超过限制的数据：

```bash
dd if=/dev/zero of=/workspace/quota-test.bin bs=1M count=<超过剩余容量的大小> conv=fsync
```

预期失败信息：

```text
Disk quota exceeded
```

## 清理注意事项

SFS Turbo 不允许删除非空目录上的目录 quota。清理顺序：

1. 先在挂载 Pod 中删除测试文件并 `sync`。
2. 调用 `DeleteFsDirQuota` 删除目录 quota。
3. 删除测试 Pod / PVC / PV。

本次 2026-07-05 验证结果：

- 测试 PVC 未设置 quota 前，`df` 显示约 `101T`，写入 `64MiB` 成功。
- 对目录设置 `128MB` quota 后，Pod 内 `df` 变为 `128M`。
- 再写入 `80MiB` 时，`dd` 返回 `Disk quota exceeded`，退出码 `1`。
- 删除测试文件后，目录 quota、测试 Pod、namespace、PV 均已清理。
