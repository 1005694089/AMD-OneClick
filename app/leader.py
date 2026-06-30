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

    @property
    def identity(self) -> str:
        return self._identity

    def is_leader(self) -> bool:
        # Election disabled -> always act as leader (single-replica / dev / tests).
        if not settings.LEADER_ELECTION_ENABLED:
            return True
        return self._is_leader

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
                    and self._last_renew_ok is not None
                    and (_now() - self._last_renew_ok).total_seconds() < duration
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
                self._is_leader = True
                self._last_renew_ok = now
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
            self._is_leader = True
            self._last_renew_ok = now
        elif holder is None or expired:
            # Vacant or stale: take over.
            spec.holder_identity = self._identity
            spec.acquire_time = now
            spec.renew_time = now
            spec.lease_duration_seconds = duration
            self._coord.replace_namespaced_lease(name, self._namespace, lease)
            self._is_leader = True
            self._last_renew_ok = now
        else:
            # Someone else holds a fresh lease: confirmed loss.
            self._is_leader = False


# Module-level singleton used by the scheduler job bodies.
elector = LeaderElector()


def is_leader() -> bool:
    return elector.is_leader()
