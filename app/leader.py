"""Lease-based leader election for the manager's scheduled jobs.

When the manager runs more than one replica, the background scheduler's jobs (billing, reapers, the
image-presence reconciler) must run on exactly one replica or they double-run. APScheduler is
per-process and has no cross-replica coordination, so we gate each job body on holding a
coordination.k8s.io/v1 Lease.

Design:
  - A single daemon thread continually tries to acquire/renew the Lease.
  - is_leader() is a cheap, lock-free read other code (the scheduler jobs) calls before doing work.
  - When leader election is disabled (default) or k8s is unreachable (dev/tests), is_leader() returns
    True so single-process behaviour is unchanged.

This is intentionally small and dependency-free (uses the kubernetes client already vendored). It is
not a general-purpose election library; it is good enough for "run these cron jobs once."
"""
import logging
import os
import socket
import threading
import time
from datetime import datetime, timezone

from .config import settings

logger = logging.getLogger(__name__)


def _now():
    return datetime.now(timezone.utc)


class LeaderElector:
    def __init__(self):
        self._identity = f"{socket.gethostname()}-{os.getpid()}"
        self._is_leader = False
        self._stop = threading.Event()
        self._thread = None
        self._coord = None
        self._namespace = settings.K8S_NAMESPACE
        # Timestamp of our last successful acquire/renew. On a TRANSIENT renew error we keep
        # leadership as long as our own lease has not yet expired, instead of dropping it on the
        # first blip (which would briefly leave zero leaders). Only a confirmed loss (a peer holds
        # a fresh lease) flips us to follower.
        self._last_renew_ok = None
        # Monotonic counterpart of _last_renew_ok, used for the is_leader() time fence. Wall
        # clock (datetime.now) can be stepped backward by NTP/VM-resume, which would DEFEAT a
        # wall-clock fence (age computes too small -> a stale leader keeps acting). monotonic()
        # never goes backward, so the fence holds regardless of clock discipline.
        self._last_renew_ok_mono = None
        # True only on the start() fail-safe (k8s client uninitializable): act as sole leader
        # without a lease. Explicit flag so the fence's "no renew timestamp" case can mean
        # "NOT leader" everywhere else, never an accidental unconditional bypass.
        self._fail_safe_leader = False

    @property
    def identity(self) -> str:
        return self._identity

    def is_leader(self) -> bool:
        # Election disabled -> always act as leader (single-replica / dev / tests).
        if not settings.LEADER_ELECTION_ENABLED:
            return True
        if not self._is_leader:
            return False
        # Explicit fail-safe (k8s client could not be initialized in start()): no lease exists to
        # contend, so act as the sole leader. Intended single-process behaviour.
        if self._fail_safe_leader:
            return True
        # Time fence: the cached _is_leader flag is set once by the renew daemon thread and only
        # cleared when that thread RUNS and observes a loss. Under CPU starvation (exactly what a
        # load spike causes) the thread may not run for many seconds, leaving _is_leader stale-True
        # while a peer has already taken over the expired lease. Independently cap how long we act:
        # refuse once our last successful renew is older than the fencing deadline (strictly < lease
        # duration, so we stop acting before any peer is entitled to take over). MONOTONIC clock so a
        # wall-clock step-back cannot hold the fence open.
        last_mono = self._last_renew_ok_mono
        if last_mono is None:
            # _is_leader True, no successful renew recorded, and not the fail-safe path: a torn/unknown
            # state (e.g. observed mid-transition). Fail safe toward NOT leader; self-heals next renew.
            return False
        age = time.monotonic() - last_mono
        if age > float(settings.LEADER_LEASE_RENEW_DEADLINE_SECONDS):
            logger.warning(
                "Leader fence: last renew %.1fs ago exceeds deadline %.1fs; NOT acting as leader",
                age, float(settings.LEADER_LEASE_RENEW_DEADLINE_SECONDS),
            )
            return False
        return True

    def _mark_leader(self, now):
        # Record renew timestamps BEFORE flipping the flag: a reader that observes _is_leader True is
        # then guaranteed (CPython atomic attribute writes + GIL program order) to also see a fresh
        # renew timestamp, so is_leader() never sees a torn True/None window.
        self._last_renew_ok = now
        self._last_renew_ok_mono = time.monotonic()
        self._is_leader = True

    def start(self):
        if not settings.LEADER_ELECTION_ENABLED:
            logger.info("Leader election disabled; this process acts as leader")
            return
        try:
            from kubernetes import client, config as kube_config

            try:
                kube_config.load_incluster_config()
            except Exception:
                kube_config.load_kube_config()
            self._coord = client.CoordinationV1Api()
        except Exception as exc:
            # Can't reach k8s — fail SAFE toward single-leader behaviour rather than zero leaders.
            logger.warning("Leader election init failed (%s); acting as leader", exc)
            self._is_leader = True
            self._fail_safe_leader = True
            return
        self._thread = threading.Thread(target=self._run, name="leader-elector", daemon=True)
        self._thread.start()
        logger.info("Leader elector started as %s for lease %s", self._identity, settings.LEADER_LEASE_NAME)

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        # Best-effort: if we hold the lease, release it so a peer takes over promptly.
        if self._is_leader and self._coord is not None:
            try:
                self._renew_or_acquire(release=True)
            except Exception:
                pass

    def _run(self):
        renew = max(1.0, float(settings.LEADER_LEASE_RENEW_SECONDS))
        # Claim promptly on startup rather than waiting one full interval first.
        try:
            self._renew_or_acquire()
        except Exception as exc:
            logger.warning("Leader election initial acquire error: %s", exc)
        while not self._stop.wait(renew):
            try:
                self._renew_or_acquire()
            except Exception as exc:
                # Transient apiserver error: do NOT immediately abdicate. Keep leadership while our
                # own last successful renew is still within the lease duration (fail-safe toward one
                # leader). Only a confirmed loss inside _renew_or_acquire flips us to follower.
                duration = float(settings.LEADER_LEASE_DURATION_SECONDS)
                if (
                    self._is_leader
                    and self._last_renew_ok_mono is not None
                    and (time.monotonic() - self._last_renew_ok_mono) < duration
                ):
                    logger.warning("Leader renew transient error (holding lease): %s", exc)
                else:
                    logger.warning("Leader renew error past lease window; dropping leadership: %s", exc)
                    self._is_leader = False

    def _renew_or_acquire(self, release: bool = False):
        from kubernetes import client
        from kubernetes.client.rest import ApiException

        name = settings.LEADER_LEASE_NAME
        duration = int(settings.LEADER_LEASE_DURATION_SECONDS)
        now = _now()

        try:
            lease = self._coord.read_namespaced_lease(name, self._namespace)
        except ApiException as e:
            if e.status == 404:
                lease = None
            else:
                raise

        if lease is None:
            # Create and claim. If a peer created it first we get 409 — re-read and evaluate the
            # now-existing lease instead of treating it as a generic loop error.
            body = client.V1Lease(
                metadata=client.V1ObjectMeta(name=name, namespace=self._namespace),
                spec=client.V1LeaseSpec(
                    holder_identity=self._identity,
                    lease_duration_seconds=duration,
                    acquire_time=now,
                    renew_time=now,
                ),
            )
            try:
                self._coord.create_namespaced_lease(self._namespace, body)
                self._mark_leader(now)
                return
            except ApiException as e:
                if e.status == 409:
                    lease = self._coord.read_namespaced_lease(name, self._namespace)
                else:
                    raise

        spec = lease.spec
        holder = spec.holder_identity
        renew_time = spec.renew_time
        expired = True
        if renew_time is not None:
            age = (now - renew_time).total_seconds()
            expired = age > (spec.lease_duration_seconds or duration)

        if release and holder == self._identity:
            spec.holder_identity = None
            spec.renew_time = now
            self._coord.replace_namespaced_lease(name, self._namespace, lease)
            self._is_leader = False
            return

        if holder == self._identity:
            # We hold it: renew.
            spec.renew_time = now
            spec.lease_duration_seconds = duration
            self._coord.replace_namespaced_lease(name, self._namespace, lease)
            self._mark_leader(now)
        elif holder is None or expired:
            # Vacant or stale: take over.
            spec.holder_identity = self._identity
            spec.acquire_time = now
            spec.renew_time = now
            spec.lease_duration_seconds = duration
            self._coord.replace_namespaced_lease(name, self._namespace, lease)
            self._mark_leader(now)
        else:
            # Someone else holds a fresh lease: confirmed loss.
            self._is_leader = False


# Module-level singleton used by the scheduler job bodies.
elector = LeaderElector()


def is_leader() -> bool:
    return elector.is_leader()
