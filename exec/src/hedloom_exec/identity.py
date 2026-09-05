"""Attempt identities chosen before submission.

An attempt identity names one content-addressed record.  A try name adds the
record-local try number and must exist *before* a transport is asked to accept
work, so that a submission whose receipt is lost can still be discovered.
Neither name depends on a transport handle, a process, or a wall-clock reading.

    A record is selected by what the work declares it computes, and by
    nothing else.

The requester is not part of that.  A study name, an authored key, a Plan ID,
a placement and a try number all describe *who asked* or *where it ran*, and
two requesters that declare the same computation are asking for the same
record.  Folding any of them into the rendering would silently turn one shared
computation into several, which is the defect this module exists to prevent.

What the identity therefore promises is exactly what the declaration says:
equal declared computational dependencies, under the author's existing
responsibility to declare them faithfully.  It does not prove semantic
equivalence, source immutability, or determinism.  An intentional independent
repetition must declare a computational distinction -- a seed, a repetition
index -- because merely naming a second invocation differently does not request
a second execution.

The rendered form is deliberately restricted to characters that survive use as
a batch job name, a filesystem directory, and an environment value.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import blake2b
import re

__all__ = [
    "AttemptIdentity",
    "IdentityError",
    "attempt_identity",
    "parse_try_name",
    "try_name",
]

_SAFE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_SEPARATOR = "\x1f"
_DIGEST_BYTES = 10
_RENDERED = re.compile(r"\Ahedloom-[0-9a-f]{20}\Z")
_TRY_NAME = re.compile(r"\A(hedloom-[0-9a-f]{20})-(0|[1-9][0-9]*)\Z")


class IdentityError(ValueError):
    """An attempt identity cannot be derived from the given planning facts."""


@dataclass(frozen=True, slots=True)
class AttemptIdentity:
    """One content-addressed record for one declared computation.

    Equality is the record: two identities compare equal exactly when they
    name the same record.  No requester metadata is carried here, so no
    requester can make one shared record compare as two identities.
    """

    computation_digest: str
    rendered: str

    def __str__(self) -> str:
        return self.rendered


def _require_digest(value: object) -> str:
    """The digest must be unambiguous, not printable-safe.

    Only the *rendered* identity is used as a job name and directory, and that
    is a generated hash.  The declared digest merely has to hash unambiguously,
    so an ordinary hex digest is welcome and so is any other unambiguous
    declaration a caller derives.
    """

    if not isinstance(value, str) or not value:
        raise IdentityError(
            "computation_digest must be a non-empty string: a record is "
            "selected by the computation it declares, and an absent digest "
            "would collapse every declaration onto one record"
        )
    if _SEPARATOR in value:
        raise IdentityError("computation_digest must not contain the field separator")
    if any(character.isspace() and character != " " for character in value):
        raise IdentityError("computation_digest must not contain control whitespace")
    return value


def attempt_identity(*, computation_digest: str) -> AttemptIdentity:
    """Derive the stable record identity of one declared computation.

    The identity is a pure function of its argument: the same declaration
    always renders the same value, in this process or a later one, in this
    study or another one.  That is what lets a restarted controller ask a
    transport whether *this* record was already accepted, and what lets a
    second study that declares the same work reuse the first study's evidence
    instead of recomputing it.

    ``computation_digest`` is the digest of the declared computational
    dependencies -- normally :func:`hedloom_exec.reuse.input_digest` of the
    execution bundle.  Because nothing else participates, changed declarations
    produce a different record rather than colliding with an existing result,
    and reuse cannot be stale by construction: finding a manifest at this
    identity means the work was done under exactly this declaration.

    There is no fallback for a missing digest.  A request that cannot say what
    it computes cannot select a record, and is refused rather than being given
    a requester-derived name that would look content-addressed and not be.
    """

    digest = _require_digest(computation_digest)
    material = _SEPARATOR.join(("computation", digest)).encode()
    rendered = f"hedloom-{blake2b(material, digest_size=_DIGEST_BYTES).hexdigest()}"
    assert _SAFE.match(rendered)  # the generated form is what must be safe
    return AttemptIdentity(computation_digest=digest, rendered=rendered)


def try_name(identity: str, try_number: int) -> str:
    """Return the workspace and batch-job name for one record-local try."""

    if not isinstance(identity, str) or _RENDERED.fullmatch(identity) is None:
        raise IdentityError(
            "identity must be a rendered record identity "
            "('hedloom-' followed by exactly 20 lowercase hex digits)"
        )
    if (
        not isinstance(try_number, int)
        or isinstance(try_number, bool)
        or try_number < 0
    ):
        raise IdentityError("try_number must be a non-negative integer")
    return f"{identity}-{try_number}"


def parse_try_name(name: str) -> tuple[str, int]:
    """Parse a strict try name into its rendered record identity and number."""

    if not isinstance(name, str):
        raise IdentityError("try name must be a string")
    matched = _TRY_NAME.fullmatch(name)
    if matched is None:
        raise IdentityError(
            "try name must be '<rendered-record-identity>-<un-padded-number>'"
        )
    return matched.group(1), int(matched.group(2))
