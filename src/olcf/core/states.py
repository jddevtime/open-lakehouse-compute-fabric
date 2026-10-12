# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Job lifecycle state machine (spec section 33)."""

from __future__ import annotations

from enum import StrEnum

from olcf.core.errors import Conflict


class JobState(StrEnum):
    """States of a job from accepted to a terminal state (succeeded, failed or cancelled); ``TRANSITIONS`` lists the moves that are allowed.
    """
    ACCEPTED = "accepted"
    VALIDATING = "validating"
    QUEUED = "queued"
    PROVISIONING = "provisioning"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCEL_REQUESTED = "cancel_requested"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"

    @property
    def terminal(self) -> bool:
        return self in TERMINAL_STATES


TERMINAL_STATES = frozenset({JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED, JobState.TIMED_OUT})

_S = JobState
#: Allowed transitions. Anything else raises :class:`InvalidTransition`.
TRANSITIONS: dict[JobState, frozenset[JobState]] = {
    _S.ACCEPTED: frozenset({_S.VALIDATING, _S.FAILED, _S.CANCEL_REQUESTED, _S.CANCELLED}),
    _S.VALIDATING: frozenset({_S.QUEUED, _S.FAILED, _S.CANCEL_REQUESTED, _S.CANCELLED}),
    _S.QUEUED: frozenset({_S.PROVISIONING, _S.FAILED, _S.CANCEL_REQUESTED, _S.CANCELLED, _S.TIMED_OUT}),
    _S.PROVISIONING: frozenset({_S.RUNNING, _S.FAILED, _S.CANCEL_REQUESTED, _S.CANCELLED, _S.TIMED_OUT, _S.QUEUED}),
    # RUNNING -> QUEUED is the automatic retry of an *idempotent* job after infrastructure loss (compute/jobs.py enforces the rule).
    _S.RUNNING: frozenset({_S.SUCCEEDED, _S.FAILED, _S.CANCEL_REQUESTED, _S.TIMED_OUT, _S.QUEUED}),
    _S.CANCEL_REQUESTED: frozenset({_S.CANCELLED, _S.FAILED, _S.SUCCEEDED, _S.TIMED_OUT}),
    _S.SUCCEEDED: frozenset(),
    _S.FAILED: frozenset(),
    _S.CANCELLED: frozenset(),
    _S.TIMED_OUT: frozenset(),
}


class InvalidTransition(Conflict):
    """A job was asked to move between two states that the state machine does not allow (code ``invalid_transition``)."""
    code = "invalid_transition"


def check_transition(current: JobState | str, new: JobState | str) -> None:
    cur, nxt = JobState(current), JobState(new)
    if nxt not in TRANSITIONS[cur]:
        raise InvalidTransition(f"job cannot move from {cur.value} to {nxt.value}", details={"from": cur.value, "to": nxt.value})


class SessionState(StrEnum):
    """States of an interactive session: starting, idle, busy, dead and stopped."""
    STARTING = "starting"
    IDLE = "idle"
    BUSY = "busy"
    DEAD = "dead"
    STOPPED = "stopped"


class WorkerState(StrEnum):
    """Lifecycle states of a worker: pending (enrolled, never connected), online, degraded, offline and revoked."""
    PENDING = "pending"  # enrolled, never connected
    ONLINE = "online"
    DEGRADED = "degraded"
    OFFLINE = "offline"
    REVOKED = "revoked"


class SchedulingState(StrEnum):
    """Whether a worker accepts new work: active, cordoned (no new allocations) or draining (cordoned and waiting for running work to finish).
    """
    ACTIVE = "active"
    CORDONED = "cordoned"  # no new allocations
    DRAINING = "draining"  # cordoned and waiting for running work to finish
