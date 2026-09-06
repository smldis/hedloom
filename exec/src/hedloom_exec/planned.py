"""Prepare symbolic invocations and finalize their identities from selected artifacts.

Exec reads the schema-4 document independently of Flow. Graph shape is fixed by
that document; an invocation's identity is fixed before its own execution, once
Run supplies its selected input artifacts. Producer identity remains economical;
explicit content/declared identity can stop conservative invalidation upstream.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Mapping, Sequence

from hedloom_exec.reuse import input_digest

__all__ = [
    "PlanDerivationError",
    "PlannedInvocation",
    "plan_bundles",
    "prepare_invocations",
    "finalize_invocation",
    "source_references",
]

SUPPORTED_SCHEMA = frozenset({4})


class PlanDerivationError(ValueError):
    """The Plan document cannot be turned into execution bundles."""


@dataclass(frozen=True, slots=True)
class PlannedInvocation:
    """A specification with symbolic references, or its finalized execution bundle."""

    invocation_id: str
    operation: str
    authored_key: str | None
    depends_on: tuple[str, ...]
    input_digest: str | None
    bundle: Mapping[str, Any]
    execution: str = "reuse"
    output_names: tuple[str, ...] = ()
    policy: Mapping[str, Any] = field(default_factory=dict)
    """The Plan's resolved placement for this invocation.

    Carried alongside the bundle rather than inside it: placement decides where
    work runs, never what it produces, so it must not reach the input digest.
    """


def _source_identity(
    source: Mapping[str, Any], fingerprint: str | None = None
) -> str:
    """Identify a source by what it declares, and by what is there.

    Source IDs are authored-order and can renumber when earlier work is
    inserted; the declared address and artifact contract are what actually
    determine the data. Using the declaration means adding an unrelated source
    does not invalidate anything.

    The declaration alone is not enough for reuse to be honest, though: editing
    an input file in place leaves every declared fact unchanged, so without
    ``fingerprint`` a rerun reuses results computed from a file that no longer
    exists in that form. This unit resolves no addresses and should not start,
    so the fingerprint is supplied by the run, which knows what an address
    space means on this machine. Absent one, behaviour is as before —
    declaration-only, and stale on an in-place edit.
    """

    return input_digest(
        {
            "operation": "source",
            "inputs": {
                "artifact": source.get("artifact"),
                "address": source.get("address"),
                "fingerprint": fingerprint,
            },
        }
    )


def _reference_identity(
    reference: Mapping[str, Any],
    sources: Mapping[str, Mapping[str, Any]],
    invocation_ids: set[str],
    fingerprints: Mapping[str, str] | None = None,
) -> str | tuple[str, str]:
    kind = reference.get("type")
    if kind == "source":
        source_id = reference.get("source_id")
        source = sources.get(source_id)
        if source is None:
            raise PlanDerivationError(f"input names unknown source {source_id!r}")
        fingerprint = (fingerprints or {}).get(source_id)
        return f"source:{_source_identity(source, fingerprint)}"
    if kind == "output":
        producer = reference.get("invocation_id")
        if producer not in invocation_ids:
            raise PlanDerivationError(
                f"input depends on unknown invocation {producer!r}"
            )
        return (producer, reference.get("output_name"))
    raise PlanDerivationError(f"unknown reference type {kind!r}")


def _dependencies(invocation: Mapping[str, Any]) -> tuple[str, ...]:
    found: list[str] = []
    for binding in invocation.get("inputs", []):
        references = (
            [binding["reference"]]
            if "reference" in binding
            else binding.get("references", [])
        )
        for reference in references:
            if reference.get("type") == "output":
                found.append(reference["invocation_id"])
    return tuple(dict.fromkeys(found))


def _ordered(invocations: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Authored order, adjusted so producers precede consumers.

    Deterministic: ties keep authored order, so the same plan always yields the
    same sequence and identical digests across runs.
    """

    remaining = list(invocations)
    emitted: set[str] = set()
    ordered: list[Mapping[str, Any]] = []

    while remaining:
        progressed = False
        for invocation in list(remaining):
            if all(dep in emitted for dep in _dependencies(invocation)):
                ordered.append(invocation)
                emitted.add(invocation["id"])
                remaining.remove(invocation)
                progressed = True
        if not progressed:
            unresolved = ", ".join(sorted(item["id"] for item in remaining))
            raise PlanDerivationError(
                f"plan has a dependency cycle or dangling edge among: {unresolved}"
            )
    return ordered


def source_references(
    document: Mapping[str, Any],
    source_fingerprints: Mapping[str, str] | None = None,
) -> dict[str, Mapping[str, Any]]:
    """Name each declared source the way an input binding already names it.

    An input bound to an operation output carries ``output:<digest>:<name>``;
    one bound to a source carries ``source:<digest>``. Identity has always
    treated a source as something produced before it is used — `_source_identity`
    digests it as an invocation named ``source`` — but nothing ever delivered
    it, so a body declaring a source as an input was called with nothing.

    This returns the string an input binding will carry, paired with the
    source's own declaration. The address inside is still unresolved: locating
    one is the run's authority, and this unit does not acquire it by handing
    the declaration back.

    ``source_fingerprints`` must be the same mapping given to ``plan_bundles``.
    The fingerprint is part of the digest, so a different one names a different
    string and matches nothing.
    """

    return {
        f"source:{_source_identity(source, (source_fingerprints or {}).get(source['id']))}":
            source
        for source in document.get("sources", [])
    }


def prepare_invocations(
    document: Mapping[str, Any],
    *,
    commands: Mapping[str, Sequence[str]] | None = None,
    identity_env: Mapping[str, str] | None = None,
    outputs: Mapping[str, Mapping[str, Any]] | None = None,
    source_fingerprints: Mapping[str, str] | None = None,
) -> tuple[PlannedInvocation, ...]:
    """Turn a validated Plan document into symbolic invocation specifications.

    ``commands`` maps an operation name to the command line that runs it, for
    invocations destined for an external substrate. Operations absent from it
    produce bundles carrying declared arguments only, suitable for an
    in-process transport.

    ``identity_env`` names environment values that genuinely change results —
    a toolchain root, a shared model library — and folds them into every digest.

    ``source_fingerprints`` identifies each declared source by its content, so
    that editing an input in place invalidates the work that read it. It is
    supplied by the caller because identifying a source means resolving its
    address, which this unit does not do. Omitting it reuses on declaration
    alone, which is stale after an in-place edit.
    """

    schema = document.get("schema_version")
    if schema not in SUPPORTED_SCHEMA:
        raise PlanDerivationError(
            f"unsupported Plan schema {schema!r}; this unit reads schema "
            f"{', '.join(str(item) for item in sorted(SUPPORTED_SCHEMA))}"
        )

    sources = {source["id"]: source for source in document.get("sources", [])}
    definitions = {
        definition["identity"]["name"]: definition
        for definition in document.get("operations", [])
    }
    outputs_by_operation = {
        name: tuple(item["name"] for item in definition.get("outputs", []))
        for name, definition in definitions.items()
    }
    # The Plan declares where each output lands and what implements the
    # operation. Both are authored facts, so a run no longer supplies them and
    # a Plan can finally say what it will compute.
    bindings_by_operation = {
        name: {
            item["name"]: {**dict(item.get("binding") or {"value": True}), "artifact": item.get("artifact", {"kind": "value"})}
            for item in definition.get("outputs", [])
        }
        for name, definition in definitions.items()
    }
    implementations = {
        name: definition.get("implementation")
        for name, definition in definitions.items()
        if definition.get("implementation")
    }
    invocation_ids = {item["id"] for item in document.get("invocations", [])}
    source_declarations = source_references(document, source_fingerprints)
    planned: list[PlannedInvocation] = []

    for invocation in _ordered(document.get("invocations", [])):
        operation = invocation["operation"]["name"]
        version = invocation["operation"].get("version")

        resolved: dict[str, Any] = {}
        for binding in invocation.get("inputs", []):
            name = binding["name"]
            if "reference" in binding:
                resolved[name] = _reference_identity(
                    binding["reference"], sources, invocation_ids, source_fingerprints
                )
            else:
                resolved[name] = [
                    _reference_identity(
                        reference, sources, invocation_ids, source_fingerprints
                    )
                    for reference in binding.get("references", [])
                ]

        arguments = {
            item["name"]: item["value"] for item in invocation.get("config", [])
        }

        bundle: dict[str, Any] = {
            "operation": operation,
            "operation_version": version,
            "arguments": arguments,
            "inputs": resolved,
        }
        bundle["source_declarations"] = source_declarations
        declared_bindings = (outputs or {}).get(operation, bindings_by_operation.get(operation))
        if declared_bindings:
            bundle["outputs"] = dict(declared_bindings)
        implementation = implementations.get(operation)
        if implementation:
            # Identity-bearing: an edited body must invalidate what it produced,
            # rather than resting on an author remembering to bump a version.
            bundle["implementation"] = {
                "entry_point": implementation.get("entry_point"),
                "fingerprint": implementation.get("fingerprint"),
            }
        if commands and operation in commands:
            bundle["command"] = list(commands[operation])
        if identity_env:
            bundle["identity_env"] = dict(identity_env)

        planned.append(
            PlannedInvocation(
                invocation_id=invocation["id"],
                operation=operation,
                execution=definitions.get(operation, {}).get("execution", "reuse"),
                authored_key=invocation.get("authored_key"),
                depends_on=_dependencies(invocation),
                input_digest=None,
                bundle=bundle,
                output_names=outputs_by_operation.get(operation, ()),
                policy=dict(invocation.get("policy") or {}),
            )
        )

    return tuple(planned)


def finalize_invocation(item, resolved_artifacts, observation_id=None):
    """Fix identity before this invocation enters Exec; never locate payloads."""
    def identity(reference):
        if isinstance(reference, list):
            return [identity(member) for member in reference]
        if isinstance(reference, str) and reference.startswith("source:"):
            return reference
        try:
            return resolved_artifacts[reference]["identity"]
        except KeyError as error:
            raise PlanDerivationError(f"unresolved artifact {reference!r}") from error

    bundle = dict(item.bundle)
    bundle["input_references"] = dict(item.bundle["inputs"])
    bundle["inputs"] = {name: identity(ref) for name, ref in item.bundle["inputs"].items()}
    if item.execution == "each_submission":
        if not observation_id:
            raise PlanDerivationError("fresh execution requires a durable observation identifier")
        bundle["observation_id"] = observation_id
    return replace(item, bundle=bundle, input_digest=input_digest(bundle))


def plan_bundles(document, **options):
    """Derive producer-only identities without execution; runtime identities refuse."""
    produced = {}
    finalized = []
    for spec in prepare_invocations(document, **options):
        item = finalize_invocation(spec, produced)
        finalized.append(item)
        for name, declaration in item.bundle.get("outputs", {}).items():
            if declaration.get("identity", "producer") != "producer":
                raise PlanDerivationError("runtime output identity requires execution")
            produced[(item.invocation_id, name)] = {
                "identity": f"output:{item.input_digest}:{name}"}
    return tuple(finalized)
