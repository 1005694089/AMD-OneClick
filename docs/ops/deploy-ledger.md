# Deploy Ledger (台账)

Authoritative, committed record of every manager deployment. This file MUST be
committed & pushed as part of every deploy (see
`.cursor/rules/deploy-ledger-commit.mdc`).

Raw manifests (`*.local.yaml`, `cluster.yaml`) and `local-deploy-history/` are
**git-ignored on purpose** — some embed raw Secrets (e.g. `GITHUB_CLIENT_SECRET`,
private keys). Do NOT commit them. Instead, each entry below pins the exact local
yaml version by `sha256` so it stays auditable/tamper-evident without leaking
secrets.

## Entry schema (every deploy adds one row per service)

| Field | Meaning |
|-------|---------|
| Date | Local time of the deploy (UTC+8) |
| Service / Port | e.g. `v2 prod` / 30088, `v2 test` / 30288, `pr1-zijun` / 30392 |
| Code commit | Git SHA the deployed image was built from (must be pushed) |
| Image | Full image ref `:tag` |
| Image digest | Registry manifest digest `sha256:…` |
| Image ID | Local image ID `sha256:…` |
| Local yaml | Manifest file applied + its `sha256` |
| Snapshot | Path under `local-deploy-history/` (git-ignored) |
| Notes | What changed / verification result |

---

## 2026-07-13 (1700) — v2 PROD: GeeTest v4 CAPTCHA + re-enable email login

**Code commit:** `8fb073862247f6ef88c7ff22c95e9495c980af8b` (branch
`feature/oauth-credit-manager`, pushed) — same validated commit as the 1545 test
deploy. Promotes GeeTest v4 CAPTCHA on `/auth/email/request-code` to the main site
and re-enables email OTP login (which had been disabled by the abuse stopgap).
User confirmed the GeeTest widget loads and logs in fine on radeon-test from a
mainland China network before this promotion.

Manifest-first: `kubectl diff` showed only image + `EMAIL_LOGIN_ENABLED` false→true
+ new `CAPTCHA_ENABLED=true` + `GEETEST_CAPTCHA_ID`. No ConfigMap/Postgres/edge
changes. `GEETEST_CAPTCHA_KEY` added to Secret `amd-oneclick-secrets-v2` (value not
recorded), injected via existing `envFrom` secretRef. Image pre-imported on
`wx-ms-w7900d-0005`.

| Service / Port | Image (`:tag`) | Digest / ID | Local yaml + sha256 | Snapshot |
|----------------|----------------|-------------|---------------------|----------|
| v2 prod / 30088 | `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick:v2-email-geetest-20260713-1700` | digest `sha256:ec2d015deb99744ef85f1177c608bb03b08cf9ca46cee76b7d3f9f5c007913b5` / id `sha256:7cb3d46dec9fee10f104d7b39c98fc27881021b6f2c3dbc25bb9b3bfe74de03c` | `k8s-manager-v2-manager-only.local.yaml` sha256 `ad14301d9bd7481165a2710b7bad77f7e2edeb5a4ed65f741bb953a39916ca45` | `local-deploy-history/v2-manager-only/2026-07-13-1700-v2-prod-email-geetest.local.yaml` |

**Rollback:** `kubectl -n default rollout undo deployment/amd-oneclick-manager-v2`
(prev image `v2-email-otp-20260710-1300`); or fast-disable via
`kubectl -n default set env deploy/amd-oneclick-manager-v2 CAPTCHA_ENABLED-`
(drops CAPTCHA) or `EMAIL_LOGIN_ENABLED=false` (re-arms the abuse stopgap).

**Verified:** rollout `1/1 ready`, new pod `amd-oneclick-manager-v2-55c9c78749-bmqcs`;
pod env `CAPTCHA_ENABLED=true`, `GEETEST_CAPTCHA_ID` set, `GEETEST_CAPTCHA_KEY` len 32,
`EMAIL_LOGIN_ENABLED=true`; `/health` 200; `request-code` no-captcha → 400 and
forged-params → 400 (fail-closed); served `/` injects `gt4.js` + `captchaEnabled=true`
+ captcha id. Positive end-to-end (widget solve → 200 → login) already validated on
radeon-test with the identical image bits.

## 2026-07-13 (1545) — v2 TEST: GeeTest v4 CAPTCHA on email request-code

**Code commit:** `8fb073862247f6ef88c7ff22c95e9495c980af8b` (branch
`feature/oauth-credit-manager`, pushed). Adds a GeeTest v4 (行为验4) CAPTCHA gate on
`/auth/email/request-code` to block automated mass registration. Replaces an
earlier Cloudflare Turnstile attempt (`af9bb87`) that could not load behind the
GFW — GeeTest's `static.geetest.com` / `gcaptcha4.geetest.com` are China-hosted and
load reliably. Frontend loads `gt4.js` + `initGeetest4({captchaId})` in the email
login modal; backend re-verifies the result server-side against
`gcaptcha4.geetest.com/validate` with an HMAC-SHA256(`lot_number`, key) `sign_token`.
Fail-closed on rejected/forged params (`8fb0738`); fail-open only on GeeTest
transport outage (`GEETEST_FAIL_OPEN`, backstopped by rate limits).

Deployed via `kubectl set env` + `set image` (test iteration, no manifest snapshot).
Env set on `amd-oneclick-manager-v2-test`: `CAPTCHA_ENABLED=true`,
`GEETEST_CAPTCHA_ID=57d37973395d92ae75f3a4b38c32cb64`, `EMAIL_LOGIN_ENABLED=true`,
removed stale `TURNSTILE_SITE_KEY`. Secret `amd-oneclick-secrets-v2-test`:
`GEETEST_CAPTCHA_KEY` added (value not recorded), stale `TURNSTILE_SECRET_KEY` removed.

| Service / Port | Image (`:tag`) | Digest | Rollback |
|----------------|----------------|--------|----------|
| v2 test / 30288 | `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick:v2-test-geetest-20260713-1545` | `sha256:ec2d015deb99744ef85f1177c608bb03b08cf9ca46cee76b7d3f9f5c007913b5` | `kubectl -n default set env deploy/amd-oneclick-manager-v2-test CAPTCHA_ENABLED-` (disable), or `rollout undo` |

**Verified:** pod env correct (`CAPTCHA_ENABLED=true`, id set, key len 32,
`EMAIL_LOGIN_ENABLED=true`, `TURNSTILE_SITE_KEY` empty); `request-code` with no
captcha → 400, with forged complete params → 400 (fail-closed); cluster reaches
`static.geetest.com` (gt4.js 200) and `gcaptcha4.geetest.com/validate` (200);
browser: email modal renders the GeeTest v4 widget (`initGeetest4` fn present,
`geetest_captcha`/`geetest_holder` DOM mounted). Not yet promoted to prod —
awaiting user confirmation that the widget loads from a mainland China Telecom
network before prod rollout. Prod email login remains disabled from the prior
abuse stopgap.

## 2026-07-10 (1748) — PR1 Zijun manager sync to online branch (token factory)

**Code commit:** `3241b24aa863a362c53ce0a49bbeaaeb40209b10`
("Merge pull request #10 from AMD-AIM/feature/token-factory"), branch
`sync/v2-base-with-pr1-account-20260626` (pushed). Advances the prior deploy
`832930f` by PR #10: `994aa91` "Add Token Factory model provider management" and
`8f53b5d` "Remove default preview model seeding".

| Service / Port | Image (`:tag`) | Digest / ID | Local yaml + sha256 | Snapshot |
|----------------|----------------|-------------|---------------------|----------|
| pr1-zijun / 30392 | `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick:pr1-zijun-sync-20260710-1748` | digest `sha256:8aec20ad8e1b1cbd9d3e27ae08edd00ef36e43dfe28ed84defe33f82e5fbc00b` / id `sha256:48faf258f8199808a877ea8c020e36e7b3be9d89caca228b2015a70ab67e9bbe` | `k8s-manager-pr1-zijun.local.yaml` sha256 `d6473ff112b221862475f996990c3fff1a8459609f3beaa55bf6eb2433bc15e3` | `local-deploy-history/pr1-zijun/2026-07-10-1748-manager-sync-3241b24.local.yaml` |

**Changes:** Pull the online `/radeon/` branch to new HEAD `3241b24` and roll the
PR1 manager onto an image built from it. Manager-only image + `STATIC_ASSET_VERSION`
bump; environment unchanged from live (SSO config, inline `SSO_ENABLED=false`,
`PUBLIC_BASE_URL`, `RUN_SCHEDULER=false`).

**Process:** built from a clean detached worktree at the pushed commit; image
pushed to ACR, pre-imported on `wx-ms-w7900d-0005`, manager-only manifest snapshot
`kubectl diff`'d (only image + `STATIC_ASSET_VERSION`), then `kubectl apply` +
bounded rollout. Did NOT touch ConfigMap/Secret/Postgres or the PR1 edge.

**Verification:** rollout `1/1 ready`, new pod `amd-oneclick-manager-pr1-zijun-68b487c8fc-w5f2c`;
`http://36.150.116.200:30392/health` 200; `/radeon/` 200 title `Radeon Cloud`, no
`/radeon/radeon`; `/radeon/static/app-path.js` 200; `/` → 307; pod env
`SSO_ENABLED=false`, `PUBLIC_BASE_URL=https://developer.amd.com.cn/radeon`,
`RUN_SCHEDULER=false`, `STATIC_ASSET_VERSION=pr1-zijun-sync-20260710-1748`; no
errors in recent logs.

**Rollback:** `kubectl -n default rollout undo deployment/amd-oneclick-manager-pr1-zijun`
(previous image `pr1-zijun-sync-20260710-1009`).

---

## 2026-07-13 — Incident stopgap: disable email OTP login (abuse)

**Scope:** env-only, manager-only. No image/code change (image stays
`v2-email-otp-20260710-1300`). Prod `amd-oneclick-manager-v2` and test
`amd-oneclick-manager-v2-test` (which share the same Postgres via
`amd-oneclick-postgres`).

**Incident:** the 2026-07-10 passwordless email OTP login was being mass-abused
for automated registration — 958 `provider=email` users in 24h, ~200/hour,
using catch-all/disposable domains (e.g. `rumahwebku.my.id`, `actionvspot.com`,
`gardianwaves.org`, `airfryersbg.com`) where any local part receives the code,
so the per-email cooldown/cap are ineffective (unlimited unique addresses). The
operator had already set free signup credits to 0; remaining risk was DB growth
and SMTP abuse.

**Action:** `kubectl -n default set env deployment/amd-oneclick-manager-v2 EMAIL_LOGIN_ENABLED=false`
and the same on `amd-oneclick-manager-v2-test`. The feature is env-gated, so this
instantly makes `/auth/email/request-code` and `/auth/email/verify` return 404 —
no more DB inserts or outbound OTP email from this vector.

**Verification:** both deployments rolled out `1/1`; pod `EMAIL_LOGIN_ENABLED=false`;
`request-code` → 404 on both `radeon.anruicloud.com` and `radeon-test.anruicloud.com`;
`provider=email` signups in the last 1 min = 0 after rollout (total flat at ~969).
Postgres healthy (74 MB, ~14 connections). Did NOT touch image/ConfigMap/Postgres.

**Re-enable:** `kubectl -n default set env deployment/amd-oneclick-manager-v2 EMAIL_LOGIN_ENABLED=true`
— but only AFTER anti-abuse hardening (CAPTCHA/Turnstile on request-code, global
hourly registration cap, disposable-domain handling, keep signup credits gated).
The ~969 abusive `provider=email` accounts currently have 0 credits (cannot
launch); cleanup is optional (DB size is small).

---

## 2026-07-10 — Passwordless email OTP login (radeon-test then radeon prod)

**Code commit:** `0dec34734327bd319e391225a0d4bb384d6a8af8`
("Add passwordless email OTP login"), branch `feature/oauth-credit-manager`
(pushed). Feature is env-gated by `EMAIL_LOGIN_ENABLED` (default off), so the
shared branch/image is inert anywhere the flag is not set.

| Service / Port | Image (`:tag`) | Digest / ID | Local yaml + sha256 | Snapshot |
|----------------|----------------|-------------|---------------------|----------|
| v2 test / 30288 | `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick:v2-test-email-otp-20260710-1300` | digest `sha256:a9f4525d29ea622b3430f71d540b518c2fdbe85d15be6d4f5f5d05326db4ec29` / id `sha256:6031b7959fd9ba58d89a4fe4f1d719ecf363c47cff6c66e58f11c12e52891b66` | `kubectl set env` + `set image` (no manifest snapshot for the test iteration) | N/A |
| v2 prod / 30088 | `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick:v2-email-otp-20260710-1300` | digest `sha256:a9f4525d29ea622b3430f71d540b518c2fdbe85d15be6d4f5f5d05326db4ec29` / id `sha256:6031b7959fd9ba58d89a4fe4f1d719ecf363c47cff6c66e58f11c12e52891b66` | `k8s-manager-v2-manager-only.local.yaml` sha256 `c4a37bb679409c72f31e749bddfa0d679d1a1d45e2da3ca354ca43058b4a56cd` | `local-deploy-history/v2-manager-only/2026-07-10-1345-v2-prod-email-otp.local.yaml` |

The prod tag is a retag of the exact validated test image (identical digest
`sha256:a9f4525d…`).

**Feature:** New passwordless email login (OTP). `POST /auth/email/request-code`
sends a 6-digit code over SMTP (Office365, STARTTLS:587); `POST /auth/email/verify`
checks it and establishes a session, auto-registering via the `email` provider
(inherits `SIGNUP_BONUS_CREDITS`, currently 2). Codes are stored in Redis as an
HMAC(SESSION_SECRET) with TTL `EMAIL_OTP_TTL_SECONDS` (600s), per-email resend
cooldown (60s), per-email hourly cap (5), and max 5 verify attempts; fails closed
without Redis. Header login dropdown gains a two-step "Login with Email" modal.
Open registration (any email), relying on the existing per-IP/day signup quota.

**Env / secrets applied (both services):**
- `EMAIL_LOGIN_ENABLED=true`, `SMTP_HOST=smtp.office365.com`, `SMTP_PORT=587`,
  `SMTP_USER=noreply_radeoncloud@mail.developer.amd.com.cn`,
  `SMTP_FROM=AMD Radeon Cloud <noreply_radeoncloud@mail.developer.amd.com.cn>`.
- `SMTP_PASSWORD` added to Secret `amd-oneclick-secrets-v2` (prod, injected via
  envFrom) and `amd-oneclick-secrets-v2-test` (test); raw value not recorded here.
- Redis: prod uses existing `redis://10.233.140.78:6379/0`; test was given
  `redis://10.233.140.78:6379/1` (dedicated DB index) since it had none.

**Process:** code committed/pushed first; image built from that commit and
validated on radeon-test (`kubectl set env`+`set image`); after the user confirmed
functionality, the SAME image was retagged for prod, prod Secret updated, a
manager-only manifest snapshot was `kubectl diff`'d (only image + the 5 new env),
pre-imported on `wx-ms-w7900d-0005`, then `kubectl apply` + bounded rollout. Did
NOT touch ConfigMap/Postgres or the PR1/edge.

**Verification:**
- radeon-test: env present, `redis_ok=True`; `request-code` (self-send to the
  noreply mailbox) → 200 (`Sent login verification code`), proving Office365 creds
  work; `verify` wrong→401 / right→200 auto-creating a `provider=email`,
  `credits=2` user; served page exposes the flow; browser: Login dropdown shows
  "Login with Email".
- radeon prod: rollout `1/1 ready`; `https://radeon.anruicloud.com/health` 200 and
  page exposes email login; env present, `redis_ok=True`; `request-code` self-send
  → 200; `verify` wrong→401 / right→200 (user created, credits=2); test users and
  Redis keys cleaned up after each check.

**Rollback (prod):** `kubectl -n default rollout undo deployment/amd-oneclick-manager-v2`
(previous image `v2-proxy-recover-ws-nfs-20260704-1224`), or simply disable the
feature with `kubectl -n default set env deployment/amd-oneclick-manager-v2 EMAIL_LOGIN_ENABLED-`
(and the SMTP_* env) — the code default is off.

---

## 2026-07-10 — PR1 Zijun manager sync to online branch (shared model API)

**Code commit:** `832930fbd3f56b0d36959f558c4e5ba36318a515`
("Merge pull request #9 from AMD-AIM/feature/shared-model-api"), branch
`sync/v2-base-with-pr1-account-20260626` (pushed to `origin`). Advances the prior
deploy `15e4a4a` by PR #9: `0724cf4` "Shared Model API: OpenAI-compatible /v1
proxy + Model APIs catalog UI".

| Service / Port | Image (`:tag`) | Digest / ID | Local yaml + sha256 | Snapshot |
|----------------|----------------|-------------|---------------------|----------|
| pr1-zijun / 30392 | `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick:pr1-zijun-sync-20260710-1009` | digest `sha256:0b3e86b56e66401033080e5e58cdc8e8c9cc2f44c1a713663f4b0b646ab92b8c` / id `sha256:f7c21884ac6560c8a743319f70bd7acf692f6454d3482219979fc0edf2468ae1` | `k8s-manager-pr1-zijun.local.yaml` sha256 `54f3fd28b5c8169e0c205596d2004e14f763e9d6cfc1ee6d3cb661cdc8818b0c` | `local-deploy-history/pr1-zijun/2026-07-10-1009-manager-sync-832930f.local.yaml` |

**Changes:** Pull the online `/radeon/` branch to its new HEAD `832930f` and roll
the PR1 manager onto an image built from it. Manager-only image + `STATIC_ASSET_VERSION`
bump; all environment unchanged from live (SSO config, inline `SSO_ENABLED=false`,
`PUBLIC_BASE_URL`, `RUN_SCHEDULER=false`).

**Process:** Built from a clean detached worktree at the pushed commit (the dirty
`AMD-OneClick-pr1-zijun` worktree was NOT used). Image pushed to ACR, pre-imported
into `wx-ms-w7900d-0005` containerd `k8s.io` via a temporary privileged helper pod,
manager-only manifest snapshot `kubectl diff`'d (only image + `STATIC_ASSET_VERSION`
changed), then `kubectl apply` + bounded rollout. Did NOT touch ConfigMap
`amd-oneclick-config-pr1-zijun`, Secret, Postgres, or the PR1 edge.

**Verification:** rollout `1/1 ready`, new pod `amd-oneclick-manager-pr1-zijun-54bcb789b9-m85mv`;
`http://36.150.116.200:30392/health` 200; `/radeon/` 200 title `Radeon Cloud`, no
`/radeon/radeon`; `/radeon/static/app-path.js` 200; `/` → 307; pod env
`SSO_ENABLED=false`, `PUBLIC_BASE_URL=https://developer.amd.com.cn/radeon`,
`RUN_SCHEDULER=false`, `STATIC_ASSET_VERSION=pr1-zijun-sync-20260710-1009`; no
errors/tracebacks in recent logs.

**Rollback:** `kubectl -n default rollout undo deployment/amd-oneclick-manager-pr1-zijun`
(previous image `pr1-zijun-sync-20260708-1343`).

---

## 2026-07-08 — PR1 Zijun manager sync to online branch

**Code commit:** `15e4a4a40419add8d93ab5a4dd11897e8e8476d1`
("----更新 amdai 用户不在显示提示以及 amdai 用户不允许进行绑定操作"),
branch `sync/v2-base-with-pr1-account-20260626` (pushed to `origin`).

| Service / Port | Image (`:tag`) | Digest / ID | Local yaml + sha256 | Snapshot |
|----------------|----------------|-------------|---------------------|----------|
| pr1-zijun / 30392 | `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick:pr1-zijun-sync-20260708-1343` | digest `sha256:ba480dbf3aea6546adb18d69862b90b76147eff65b60a8c739a09ecdadabf078` / id `sha256:4ebc6e814aeb3a06763b003798668e09b31d274971dc99bc68f996f01ee68777` | `k8s-manager-pr1-zijun.local.yaml` sha256 `9aa9b7be401fb224a05a3b706f554ae6520ae5cdb0ced3777be6755f9472a03f` | `local-deploy-history/pr1-zijun/2026-07-08-1355-manager-sync-15e4a4a.local.yaml` |

**Changes:** Pull the online `/radeon/` branch `sync/v2-base-with-pr1-account-20260626`
(HEAD `15e4a4a`, latest commits update the login prompt wording and stop showing
the account-binding notice / disallow binding for `amdai` users) and roll the
PR1 manager onto an image built from it. Manager-only image + `STATIC_ASSET_VERSION`
bump; all environment (SSO config, `SSO_ENABLED=false` inline override,
`PUBLIC_BASE_URL`, `RUN_SCHEDULER=false`) is unchanged from the live Deployment.

**Process:** Built from a clean detached worktree at the pushed commit (no
uncommitted tree changes; the separate `AMD-OneClick-pr1-zijun` worktree's local
edits were NOT used). Image pushed to ACR, pre-imported into `wx-ms-w7900d-0005`
containerd `k8s.io` via a temporary privileged helper pod, then a manager-only
manifest snapshot was `kubectl diff`'d (only image + `STATIC_ASSET_VERSION`
change confirmed) and `kubectl apply`'d with bounded rollout. Did NOT touch
ConfigMap `amd-oneclick-config-pr1-zijun`, Secret, Postgres, or the PR1 edge.

**Verification:** rollout `1/1 ready`, new pod `amd-oneclick-manager-pr1-zijun-76f967b5d5-6nx5t`
on the new image; `http://36.150.116.200:30392/health` 200; `/radeon/` 200 with
title `Radeon Cloud` and no `/radeon/radeon`; `/radeon/static/app-path.js` 200;
`/` → 307 redirect; running pod env `SSO_ENABLED=false`,
`PUBLIC_BASE_URL=https://developer.amd.com.cn/radeon`,
`STATIC_ASSET_VERSION=pr1-zijun-sync-20260708-1343`, `RUN_SCHEDULER=false`; no
errors/tracebacks in recent manager logs.

**Rollback:** `kubectl -n default rollout undo deployment/amd-oneclick-manager-pr1-zijun`
(previous image `pr1-zijun-sync-20260630-0928`).

---

## 2026-07-04 — v2 proxy recovery, websocket token fix, NFS workspace rollout

**Code commit:** `4f80868881283b9f293cbed3b04cd56d34384e2f`
("Match template metadata before reusing pods"), built on top of:
`d3b0c7c` ("Isolate template repos in persistent workspaces") and
`1ee5dce` ("Fix instance proxy recovery and websocket auth").

| Service / Port | Image (`:tag`) | Digest / ID | Local yaml + sha256 | Snapshot |
|----------------|----------------|-------------|---------------------|----------|
| v2 test / 30288 | `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick:v2-proxy-recover-ws-nfs-20260704-1224` | digest `sha256:752c0b288f1e6e3afbab7259738ecc773d8f04893176b9dcac7197d8fd491158` / id `sha256:1807fd60da326835d6d178977e751d316ee307f7209b5bad8ad611ee260332c3` | `k8s-manager-v2-test.local.yaml` sha256 `b87d845af37494be494b39d697642413d2bd93e8a4fbd9f2c822692a24d88535` | `local-deploy-history/v2-test/2026-07-04-1224-v2-test-proxy-recover-ws-nfs.local.yaml` |
| v2 prod / 30088 | `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick:v2-proxy-recover-ws-nfs-20260704-1224` | digest `sha256:752c0b288f1e6e3afbab7259738ecc773d8f04893176b9dcac7197d8fd491158` / id `sha256:1807fd60da326835d6d178977e751d316ee307f7209b5bad8ad611ee260332c3` | `k8s-manager-v2-manager-only.local.yaml` sha256 `554c1d082f2142490d0bbae4faea8fd380287399040ce29a35264ef148ade06d` | `local-deploy-history/v2-manager-only/2026-07-04-1231-v2-prod-proxy-recover-ws-nfs.local.yaml` |

**Changes:**
- Jupyter websocket proxy now appends `NOTEBOOK_TOKEN` to upstream websocket
  URLs when the browser request lacks a token, preventing Jupyter WS 403s after
  the HTML/API load succeeds.
- Shared `httpx.AsyncClient` for `/instances` and `/spaces` now logs
  `PoolTimeout` explicitly, replaces the shared client, retries once, and closes
  upstream streams in `finally` so one exhausted pool does not stall all instance
  traffic.
- NFS-backed persistent `/workspace` no longer reuses a single fixed
  `/workspace/repo`: template repos are isolated under
  `/workspace/template-repos/template-<id>/repo`, and a missing notebook/path
  mismatch forces a re-clone.
- Existing-pod reuse now compares image, instance type, template id, repo URL,
  branch, and notebook path. A different template with the same image/type is no
  longer silently reused during launch/delete races.
- Production `amd-oneclick-manager-v2` now has per-user workspace NFS enabled:
  `WORKSPACE_NFS_ENABLED=true`, prefix `oneclick-newprod-nfs-`, count `4`, size
  `10Gi`, access mode `ReadWriteMany`, PVC prefix `oneclick-ws`.
- The temporary production `main.py` ConfigMap hotfix mount
  `amd-oneclick-v2-ws-token-hotfix` was removed because the WS token fix is now
  part of the image.

**Process note:** `v2 test` was first validated with the same final image. The
`v2 prod` rollout was then patched live with the final image/NFS env/hotfix
mount removal after test validation; the user later confirmed prod functionality
was normal and requested no re-release. This entry and the snapshots above
normalize the paper trail after that non-standard patch. The rule
`.cursor/rules/deploy-ledger-commit.mdc` was tightened so future formal
production releases must be manifest-backed before applying, except for explicit
emergencies.

**Verification:**
- `radeon-test`: rollout `1/1 ready`; `https://radeon-test.anruicloud.com/health`
  200; existing instance APIs for `u-1-258c504b` and `u-1292-add8c9f2`
  returned 200; generated startup scripts for templates 257 and 320 use
  `/workspace/template-repos/template-<id>/repo` and no longer contain fixed
  `/workspace/repo`.
- Proxy stress on `radeon-test` before the final NFS repo-isolation commit:
  70 concurrent slow requests intentionally filled the 64-connection pool,
  produced `Proxy pool timeout`, logged
  `Replaced shared proxy client old_generation=1 new_generation=2`, and a real
  Jupyter API request still returned 200 in ~0.72s. A 240-request / 40-concurrent
  real Jupyter API run had 0 failures (p99 ~1.13s).
- `radeon`: after rollout, `https://radeon.anruicloud.com/health` 200 and the
  homepage 200; deployment `amd-oneclick-manager-v2` `1/1 ready`; new image and
  NFS env present; `ws-token-hotfix` volume/mount absent. Recent proxy logs had
  no `Traceback`, proxy 500, or pool timeout. Existing template preview 404s for
  templates 878/942 are unrelated upstream template-source errors.

**Rollback:**
- Test: `kubectl -n default rollout undo deployment/amd-oneclick-manager-v2-test`.
- Prod: `kubectl -n default rollout undo deployment/amd-oneclick-manager-v2`.
  This restores the previous Deployment template, including the old image and
  prior hotfix mount/env state. If rolling back only the NFS enablement, unset
  the six `WORKSPACE_NFS_*` env vars instead of rolling back the image.

---

## 2026-07-03 — new-prod NFS per-user REAL hard quota (route A: project quota + root_squash)

**Scope:** storage layer only (NFS servers in new-prod cluster
`new_cluster_prod.yaml`, ns `amd-oneclick-storage`). No manager image change.
These NFS servers back per-user workspaces for radeon / radeon-test via the
cross-cluster StorageClasses `oneclick-newprod-nfs-1..4`.

**Goal:** enforce a real ~10Gi per-user hard cap (previously the NFS-subdir
provisioner set no quota; PVC size was advisory only).

| Component | Image (`:tag`) | Digest / ID | Local yaml + sha256 |
|-----------|----------------|-------------|---------------------|
| NFS server (nfs-cpu-1/2) | `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/nfs-server-crossmnt:rootsquash-20260703` | digest `sha256:2cd44e5ad1bbaa3f5be6538015392bc710b9f981e7ff25acadb518456009ffb2` / id `sha256:93adc95efc07bf8ffb63387afa125801bd2b0f32f5ba08a1032e42c148bac809` | `nfs-quota.local.yaml` sha256 `c89080112178cd7833d8a252a34f9caa40134d044018cbdcbc77957a1b945816` |
| quota tooling (init + enforcer sidecar) | `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/nfs-quota-tools:20260703` | digest `sha256:980b60e7a874d25d2aadf16af6eaf9b525774a29e1ea3f95ff35cd941b2c13b9` / id `sha256:6d4d9037aef2e95f19d74663f439ccefa70c3ca7e31c8d2881c26f4e6b388095` | (same manifest) |

Dockerfiles: `nfs-quota-tools/Dockerfile` sha256
`48613e2ea4baa1af137e72908788de8b969b680ef25bffab4ef231ce84e47618`;
`nfs-server-crossmnt/Dockerfile.rootsquash` sha256
`3bc54a967f70486d906654aadfa9e5613be0823cc9e112e9c854846aa1af8c94`.

**Key design findings (why it works now):**
- `tune2fs -O quota,project` on an EXISTING ext4 does NOT yield working quota
  (kernel mounts `Quota mode: none`, `prjquota` dropped). Must `mkfs.ext4 -O
  quota,project`. XFS needs no reformat (project quota is enabled at first
  mount via `-o prjquota`).
- Nodes need the `quota_v2` kernel module (`CONFIG_QFMT_V2=m`); init runs
  `modprobe quota_v2`.
- **root bypasses project quota** (`CAP_SYS_RESOURCE`). Since Jupyter runs
  `--allow-root`, the export MUST be `root_squash` (root→nobody) or quota is
  a no-op. Hence the `rootsquash-*` server image.

**What was done:**
- Built `nfs-quota-tools` (Ubuntu + xfsprogs/quota/e2fsprogs) for the init
  (mount) and a `quota-enforcer` sidecar. Kept the alpine nfs-server image
  (its EOL apk repos can't install quota pkgs) and only flipped the export
  template to `root_squash`.
- Reformatted the 3 ext4 disks `mkfs.ext4 -O quota,project`
  (nfs-cpu-1 disk0+disk1, nfs-cpu-2 disk1); nfs-cpu-2 disk0 (xfs) kept, first
  re-mounted `-o prjquota`. All 4 disks now mount with `prjquota`.
  - Data wiped was operator test data / stale junk only (incl. a live but
    self-owned test instance `u-1-258c504b`, a 208G `opencompass.tar`, and
    stale containerd/buildkit dirs on nfs-cpu-2 disk1). xfs disk0 data
    (vllm test artifacts, 69G) preserved.
- `quota-enforcer` scans `/exports/diskN/pvc-*` every 30s and stamps each PVC
  subdir with a unique project id (= its inode) + `WORKSPACE_QUOTA_HARD=10G`
  hard block limit (xfs via `xfs_quota`, ext4 via `chattr +P` + `setquota`).

**Verification (end-to-end, real NFS client, running as root → squashed to
nobody):** on BOTH ext4 (nfs-cpu-1, 10.5.10.15) and xfs (nfs-cpu-2,
10.5.10.19): writing 50M into a 20M-limited dir → `du` capped at 20M and
`dd` (conv=fsync) exits rc=1 (EDQUOT surfaced); O_DIRECT write blocks
immediately at the limit. Enforcer confirmed auto-stamping 10G on `pvc-*`
dirs (`repquota`/`xfs_quota report`). Both NFS pods 2/2 Running.

**Gotcha for future restarts:** the lablab DaemonSet
`oneclick-workspace-localssd-prep` (ns `amd-oneclick-lablab`) bind-mounts host
`/` and captures stale copies of these disk mounts in its mount namespace,
which can block a re-mount/mkfs. The init only does a host-ns umount; the
one-time transition here required killing the holder pids. Steady state (and
node reboots, where the NFS init mounts the disks first) is fine; if you ever
need to re-enable xfs quota after it reverts to `noquota`, fully release the
device (kill holders) before re-mounting `-o prjquota`.

---

## 2026-06-24 — image prepull sync fix (IfNotPresent + ref normalization)

**Code commit:** `d8f0cea8ee9f830dda1e243eb1f0eb62198d47af`
("Fix image prepull sync: use IfNotPresent and normalize image refs")
built on top of `705fda2` (proxy-pool fix).

| Service / Port | Image (`:tag`) | Digest / ID | Local yaml + sha256 | Snapshot |
|----------------|----------------|-------------|---------------------|----------|
| v2 test / 30288 | `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick:v2-test-imgsync-fix-20260624-1500` | digest `sha256:de2ce10383d2611ee5509ad2b3fb5f19140c6889b33348d61003fe6b59377357` / id `sha256:73aa935bbe8a69c8984ca3bc87744803568ebd4c017e5f722a4b52ca2067d8ad` | `k8s-manager-v2-test.local.yaml` sha256 `913d2b91b94aa75447679fe527de6799a178ded137e44ff90dc50f6636500683` | `local-deploy-history/v2-test/2026-06-24-1500-v2-test-imgsync-prepull-fix.local.yaml` |
| v2 prod / 30088 | `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick:v2-imgsync-prepull-fix-20260624-1535` | digest `sha256:de2ce10383d2611ee5509ad2b3fb5f19140c6889b33348d61003fe6b59377357` / id `sha256:73aa935bbe8a69c8984ca3bc87744803568ebd4c017e5f722a4b52ca2067d8ad` | `k8s-manager-v2-manager-only.local.yaml` sha256 `1d279c528691f1362154ff290d55a45bcd6186ef8b74d052c6d807d05fe0769d` | `local-deploy-history/v2-manager-only/2026-06-24-1538-v2-manager-imgsync-prepull-fix.local.yaml` |

**Process:** built from working tree → pushed to ACR → pre-imported on
`wx-ms-w7900d-0005` → `kubectl diff` confirmed image-only change vs live →
`kubectl apply` → bounded rollout (old pod kept serving until new ready).
Prod and test share the same image digest (prod tag is a retag of the validated
test image).

**Verification:** `/health=200` on both; image sync for catalog 52/53/54
(`rocm/atom-dev`, `vllm/vllm-openai-rocm`, `rocm/pytorch`) went `0/22` → `ready
21/22` (DaemonSet 25/27).

**Side task:** the two private images were node-to-node synced (ctr export/import
over cluster net) to the 5 missing nodes `0004/0005/0006/0008/0042`; `0008` used
a locally-cached base image with `imagePullPolicy: Never` (it cannot pull busybox
from ACR).
