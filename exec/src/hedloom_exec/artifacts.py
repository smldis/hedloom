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

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
import os

__all__ = [
    "ArtifactRef",
    "MissingOutput",
    "OutputDeclarationError",
    "capture_outputs",
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

    def as_data(self) -> dict[str, Any]:
        data: dict[str, Any] = {"name": self.name, "kind": self.kind}
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
    """The directory one attempt runs in.

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
        if "path" in declaration:
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
            captured.append(ArtifactRef(name=name, kind="value", value=value))
        else:
            raise OutputDeclarationError(
                f"output {name!r} declares none of 'path', 'stream', or 'value'"
            )
    return tuple(captured)


def write_diagnostics(workdir: Path | None, stdout: str, stderr: str) -> None:
    """Keep the streams as evidence, separately from any declared result."""

    if workdir is None:
        return
    if stdout:
        (workdir / "stdout.log").write_text(stdout, encoding="utf-8")
    if stderr:
        (workdir / "stderr.log").write_text(stderr, encoding="utf-8")
