"""Outputs on a filesystem both sides can already see.

On a shared store, materializing an output does not mean moving bytes. A tool
writes where it writes; the next invocation opens the same path. What has to be
durable is the *address* and enough about the artifact to tell whether it is
still the one that was produced.

Four kinds of output are supported, because real commands produce all four:

* ``{"path": "result.dat"}`` — a file the command wrote itself, relative to
  its working directory. This is the ordinary case for a batch tool.
* ``{"path": "results", "filesystem_kind": "directory"}`` — a directory
  tree the command wrote inside its working directory.
* ``{"stream": "stdout"}`` — the captured stream, for tools whose result really
  is what they printed.
* ``{"value": True}`` — the return value of an in-process implementation.

Standard output is always captured to a file regardless, but as *diagnostics*.
A command printing progress while writing its real answer to disk is the norm,
so stdout is never the result unless an operation says it is.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping
import os
import json
from hashlib import blake2b


def canonical_identity(value):
    """Copy finite JSON identity data without coercing object keys or live objects."""
    def check(item):
        if isinstance(item, dict):
            if any(type(key) is not str for key in item):
                raise ValueError("identity object keys must be strings")
            for member in item.values():
                check(member)
        elif isinstance(item, list):
            for member in item:
                check(member)
        elif item is not None and type(item) not in (str, int, float, bool):
            raise ValueError("identity must be finite JSON data")
    if value is None:
        raise ValueError("an explicit identity is required")
    check(value)
    return json.loads(json.dumps(value, sort_keys=True, allow_nan=False))


def fingerprint_content(path):
    """Hash every file byte with bounded memory, independent of size and mtime."""
    digest = blake2b(digest_size=32)
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return "blake2b-256:" + digest.hexdigest()


def validate_borrowed(artifacts):
    """Accessibility is required for reuse; it does not verify declared identity."""
    for ref in artifacts:
        if ref.get("ownership") == "borrowed":
            path = Path(ref["address"])
            if not artifact_accessible(ref):
                raise MissingOutput(f"borrowed output is no longer accessible: {path}")


def artifact_accessible(ref):
    """Present access, independently of the historical identity claim."""
    if ref is None:
        return False
    if ref.get("address") is None:
        return True
    path = Path(ref["address"])
    return (path.is_dir() and os.access(path, os.R_OK | os.X_OK) if ref["kind"] == "directory"
            else path.is_file() and os.access(path, os.R_OK))

__all__ = [
    "ArtifactRef",
    "MissingOutput",
    "OutputDeclarationError",
    "capture_outputs",
    "artifact_accessible",
    "fingerprint_content",
    "workspace_for",
    "workspace_path",
]


class MissingOutput(RuntimeError):
    """A declared output is not there after the work reported success.

    Treated as a failure of the invocation rather than ignored: an operation
    that promises an artifact and does not produce one has not done its job,
    and publishing a manifest without it would let downstream work resolve an
    address to nothing.
    """


class OutputDeclarationError(ValueError):
    """An output declaration is not one of the supported kinds."""


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    """Where an output is, and enough to notice if it changed underneath us."""

    name: str
    kind: str
    address: str | None = None
    size: int | None = None
    modified_ns: int | None = None
    value: Any = None
    identity: Any = None
    ownership: str | None = None

    def as_data(self) -> dict[str, Any]:
        data: dict[str, Any] = {"name": self.name, "kind": self.kind}
        if self.identity is not None:
            data["identity"] = self.identity
        if self.ownership is not None:
            data["ownership"] = self.ownership
        if self.address is not None:
            data["address"] = self.address
        if self.size is not None:
            data["size"] = self.size
        if self.modified_ns is not None:
            data["modified_ns"] = self.modified_ns
        if self.value is not None:
            data["value"] = self.value
        return data


def workspace_path(root: str | os.PathLike[str], name: str) -> Path:
    """Where a workspace is, without creating or inspecting it."""

    return Path(root) / name


def workspace_for(root: str | os.PathLike[str], identity: str) -> Path:
    """The directory one try runs in.

    Per attempt rather than per invocation: a rerun after a failure must not
    write over the evidence of what the previous attempt produced.
    """

    directory = workspace_path(root, identity)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _directory_metadata(candidate: Path) -> tuple[int, int]:
    """Return contained entry bytes and the latest tree modification time."""

    root_stat = candidate.stat()
    size = 0
    modified_ns = root_stat.st_mtime_ns
    for entry in candidate.rglob("*"):
        stat = entry.stat()
        modified_ns = max(modified_ns, stat.st_mtime_ns)
        if not entry.is_dir():
            size += stat.st_size
    return size, modified_ns


def _file_reference(
    name: str,
    workdir: Path,
    relative: str,
    *,
    filesystem_kind: str = "file",
) -> ArtifactRef:
    candidate = (workdir / relative).resolve()
    try:
        candidate.relative_to(workdir.resolve())
    except ValueError as error:
        raise OutputDeclarationError(
            f"output {name!r} points outside its working directory: {relative!r}"
        ) from error
    if not candidate.exists():
        raise MissingOutput(
            f"declared output {name!r} was not produced at {candidate}"
        )
    if filesystem_kind == "file":
        if not candidate.is_file():
            raise MissingOutput(
                f"declared file output {name!r} was not produced as a file at "
                f"{candidate}"
            )
        stat = candidate.stat()
        size = stat.st_size
        modified_ns = stat.st_mtime_ns
    elif filesystem_kind == "directory":
        if not candidate.is_dir():
            raise MissingOutput(
                f"declared directory output {name!r} was not produced as a "
                f"directory at {candidate}"
            )
        size, modified_ns = _directory_metadata(candidate)
    else:
        raise OutputDeclarationError(
            f"output {name!r} names unknown filesystem kind {filesystem_kind!r}"
        )
    return ArtifactRef(
        name=name,
        kind=filesystem_kind,
        address=str(candidate),
        size=size,
        modified_ns=modified_ns,
    )


def capture_outputs(
    declarations: Mapping[str, Mapping[str, Any]] | None,
    *,
    workdir: Path | None,
    stdout: str = "",
    stderr: str = "",
    value: Any = None,
    producer_digest: str | None = None,
) -> tuple[ArtifactRef, ...]:
    """Record each declared output after the work reported success.

    Deliberately not a search: only what an operation declared is recorded.
    Whatever else the command scattered in its working directory stays there as
    evidence, unnamed and unpromised.
    """

    if not declarations:
        return ()

    captured: list[ArtifactRef] = []
    for name, declaration in sorted(declarations.items()):
        if not isinstance(declaration, Mapping):
            raise OutputDeclarationError(
                f"output {name!r} must be a mapping such as {{'path': 'sim.raw'}}"
            )
        mode = declaration.get("identity", "producer")
        supplied = value.get(name) if isinstance(value, Mapping) else None
        if declaration.get("external"):
            if mode != "declared" or not isinstance(supplied, Mapping) or "location" not in supplied:
                raise MissingOutput(f"output {name!r} requires a named located value")
            if not isinstance(supplied["location"], str):
                raise MissingOutput(f"output {name!r} location must be an absolute path string")
            candidate = Path(supplied["location"])
            shape = declaration.get("filesystem_kind", "file")
            if not candidate.is_absolute() or not (candidate.is_dir() if shape == "directory" else candidate.is_file()):
                raise MissingOutput(f"output {name!r} is not an accessible {shape}: {candidate}")
            captured.append(ArtifactRef(name=name, kind=shape, address=str(candidate), ownership="borrowed"))
            if not artifact_accessible(captured[-1].as_data()):
                raise MissingOutput(f"borrowed output {name!r} is inaccessible: {candidate}")
        elif "path" in declaration:
            if workdir is None:
                raise OutputDeclarationError(
                    f"output {name!r} is on the filesystem but no workspace "
                    "was provided"
                )
            captured.append(
                _file_reference(
                    name,
                    workdir,
                    declaration["path"],
                    filesystem_kind=declaration.get("filesystem_kind", "file"),
                )
            )
        elif "stream" in declaration:
            stream = declaration["stream"]
            if stream not in ("stdout", "stderr"):
                raise OutputDeclarationError(
                    f"output {name!r} names unknown stream {stream!r}"
                )
            captured.append(
                ArtifactRef(
                    name=name,
                    kind="stream",
                    value=stdout if stream == "stdout" else stderr,
                )
            )
        elif declaration.get("value"):
            if not isinstance(value, Mapping) or name not in value:
                raise MissingOutput(f"missing named return {name!r}")
            captured.append(ArtifactRef(name=name, kind="value", value=value[name]))
        else:
            raise OutputDeclarationError(
                f"output {name!r} declares none of 'path', 'stream', or 'value'"
            )
        ref = captured[-1]
        if ref.address is not None and ref.ownership is None:
            ref = replace(ref, ownership="workspace")
        if mode == "declared":
            if not isinstance(supplied, Mapping) or supplied.get("identity") is None:
                raise MissingOutput(f"output {name!r} requires an explicit identity")
            if ref.address != supplied.get("location"):
                raise MissingOutput(f"output {name!r} location differs from its declaration")
            try:
                canonical = canonical_identity(supplied["identity"])
            except (TypeError, ValueError) as error:
                raise MissingOutput(f"output {name!r} identity must be finite JSON data") from error
            ref = replace(ref, identity={"mode": mode, "artifact": declaration.get("artifact"),
                                        "representation": ref.kind, "value": canonical})
        elif mode == "content":
            if ref.kind != "file" or ref.ownership != "workspace":
                raise OutputDeclarationError("content identity requires an owned file")
            ref = replace(ref, identity={"mode": mode, "artifact": declaration.get("artifact"),
                                        "representation": ref.kind, "value": fingerprint_content(ref.address)})
        elif mode != "producer":
            raise OutputDeclarationError(f"unsupported output identity {mode!r}")
        elif producer_digest is not None:
            ref = replace(ref, identity=f"output:{producer_digest}:{name}")
        captured[-1] = ref
    return tuple(captured)


def write_diagnostics(workdir: Path | None, stdout: str, stderr: str) -> None:
    """Keep the streams as evidence, separately from any declared result."""

    if workdir is None:
        return
    if stdout:
        (workdir / "stdout.log").write_text(stdout, encoding="utf-8")
    if stderr:
        (workdir / "stderr.log").write_text(stderr, encoding="utf-8")
