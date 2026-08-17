"""Author a study, inspect what it will do, and run it — from one import.

    from hedloom import operation, flow, plan, file, shell, lsf, Site, study

Everything here already existed in three units; what was missing was the seam
that let one file be a whole study. `hedloom_flow` still owns authoring and the
Plan, `hedloom_exec` still owns one attempt's durable record and imports neither
this package nor Dask, and `hedloom_run` still owns binding and readiness. This
package composes them and adds the one thing none of them could own alone: a
`submit` that runs what was authored, because it holds both halves.

The operation decorator here is `hedloom_flow`'s, wrapped so the body it already
kept is remembered as something callable. That is the whole difference. Before
it, `@operation` bodies were dead code and every study needed a second file to
supply real implementations — for the OTA reference, six hundred lines whose
only job was to agree with the first file.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping

from hedloom_flow import (  # noqa: F401 - the authoring surface, re-exported
    ArtifactContract,
    Parameter,
    Plan,
    Policy,
    ResourceContract,
    address,
    artifact,
    artifacts,
    codec,
    flow,
    input_artifact,
    local,
    materialization,
    named_policy,
    parameter,
    plan,
    planned,
)
from hedloom_flow import operation as _operation
from hedloom_flow.authoring import file, returned, stdout, sweep  # noqa: F401
from hedloom_run.site import Site, SiteError  # noqa: F401

from hedloom.binding import BoundTransport, Shell, Workspace, shell  # noqa: F401
from hedloom.session import Session, session  # noqa: F401
from hedloom.study import Study, StudyRun, submit  # noqa: F401

__all__ = [
    "ArtifactContract",
    "Parameter",
    "Plan",
    "Policy",
    "ResourceContract",
    "Shell",
    "Session",
    "Site",
    "SiteError",
    "Study",
    "StudyBuilder",
    "StudyRun",
    "Workspace",
    "address",
    "artifact",
    "artifacts",
    "codec",
    "file",
    "flow",
    "implementations",
    "input_artifact",
    "local",
    "lsf",
    "materialization",
    "named_policy",
    "operation",
    "parameter",
    "plan",
    "planned",
    "pooled",
    "returned",
    "session",
    "shell",
    "stdout",
    "study",
    "submit",
    "sweep",
]

_IMPLEMENTATIONS: dict[str, Callable[..., Any]] = {}


def operation(
    function: Callable[..., Any] | None = None, **declaration: Any
) -> Any:
    """Declare an operation whose body is what actually runs.

    Identical to `hedloom_flow.operation` except that the decorated function is
    remembered here, keyed by the operation identity the Plan records. The Plan
    already carries that identity and a fingerprint of this source, so the
    registry resolves a name the document names — it does not smuggle in a
    second, unrecorded notion of what an operation is.
    """

    inner = _operation(**declaration)

    def decorate(body: Callable[..., Any]) -> Any:
        declared = inner(body)
        _IMPLEMENTATIONS[declared.identity.name] = body
        return declared

    # `@operation` bare, for a body that declares nothing but its signature.
    return decorate(function) if function is not None else decorate


def implementations() -> Mapping[str, Callable[..., Any]]:
    """Every operation body declared in this process."""

    return dict(_IMPLEMENTATIONS)


def lsf(**options: Any) -> Policy:
    """Place this work on its own LSF job, with its own resource request.

    Options travel to the job that needs them: `queue`, `cores`, `memory_mb`,
    `walltime`, a raw `resources` string, and `licences={"name": n}`, which
    becomes a `rusage` term so the scheduler that owns the licence count is the
    one that arbitrates it.
    """

    return named_policy("lsf")(**options)


def pooled(**options: Any) -> Policy:
    """Place this work on a shared pool of reusable LSF workers, not its own job.

    The trade, stated plainly. A pool pays queue dispatch once per *worker*
    rather than once per invocation, and holds no `bsub` client process on the
    submit host per corner in flight — which is the ceiling that actually binds
    a wide sweep. What it gives up is everything that needs a corner to *be* a
    job: per-corner resource requests, per-corner `bkill`, per-corner
    accounting, and per-corner licence arbitration. The farm sees the pool's
    workers, never your corners, so the watcher can no longer tell you that one
    particular corner is queued.

    Worth it when an operation's median queue wait is a significant fraction of
    its median runtime — roughly a third, as a starting rule — and its corners
    are uniform enough to share one worker shape. Below that, `lsf()` is the
    better deal. It is a per-operation judgement, which is why it is authored
    here and not on the study.

    Names the placement `pool`, as `lsf()` names `lsf`. A site that offers
    several pools of different shapes — the usual case, since one pool has one
    worker shape — names them itself, and `named_policy("pool_bigmem")()`
    reaches any of them.
    """

    return named_policy("pool")(**options)


@dataclass(frozen=True, slots=True)
class StudyBuilder:
    """What ``@study`` leaves behind: call it to build one study.

    A family rather than a study, because the decorated function takes the
    arguments that distinguish one member from another. Calling it plans, which
    costs nothing but Python; `submit` is still the only thing that spends.
    """

    build: Callable[..., Plan]
    declared: Mapping[str, Any] | None = None

    def __call__(self, *args: Any, **kwargs: Any) -> Study:
        return Study(self.build(*args, **kwargs), self.declared or _IMPLEMENTATIONS)

    def __getattr__(self, name: str) -> Any:
        # The mistake this API invites: `sweep.submit(...)` for `sweep().submit(...)`.
        if name in {"submit", "plan", "document", "summary", "implementations"}:
            called = getattr(self.build, "__name__", "this")
            raise AttributeError(
                f"{called!r} is a family of studies, not one: call it first, "
                f"as {called}(...).{name}"
            )
        raise AttributeError(name)


def study(
    subject: Plan | Callable[..., Any] | None = None,
    *,
    default_policy: Policy | None = None,
    implementations: Mapping[str, Any] | None = None,
) -> Any:
    """Pair the bodies that implement a plan with the plan itself.

    The ordinary form is a decorator, and the decorated function *is* the study:
    author inside it, return what the study produces, and call it to get one::

        @study
        def sweep(name):
            return corners.named(name)(POINTS)

        sweep("north").submit(site)

    Nothing runs inside the body — an operation call records itself and hands
    back a handle — so what comes back is a plan you can inspect before
    spending anything.

    `default_policy` is where every call in the study runs unless a call says
    otherwise: `local()` for work that runs in this process, `lsf(...)` for work
    that wants its own job.

    A finished `Plan` is also accepted, for the `plan()` escape hatch.
    """

    if subject is None:
        # `@study(default_policy=local())`, the decorator with arguments.
        def decorate(function: Callable[..., Any]) -> StudyBuilder:
            return study(
                function,
                default_policy=default_policy,
                implementations=implementations,
            )

        return decorate
    if isinstance(subject, Plan):
        if default_policy is not None:
            raise TypeError(
                "a finished Plan already carries its policies; pass "
                "default_policy where the plan is authored"
            )
        return Study(subject, implementations or _IMPLEMENTATIONS)
    if callable(subject):
        return StudyBuilder(
            planned(subject, default_policy=default_policy), implementations
        )
    raise TypeError(
        "study() takes a strategy to plan or a finished Plan, "
        f"not {type(subject).__name__}"
    )
