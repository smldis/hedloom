"""Stable, derived names for the outputs a run currently resolves to."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

__all__ = ["ALIAS_DIR", "alias_path", "alias_root", "aliases_into", "point_alias"]


ALIAS_DIR = "latest"


def alias_root(root: str | os.PathLike[str]) -> Path:
    """Return ``<root>/latest`` without creating or inspecting it."""

    return Path(root) / ALIAS_DIR


def _component(value: str) -> str:
    """Keep authored spelling visible while making one filesystem component."""

    if not isinstance(value, str) or not value:
        raise ValueError("alias components must be non-empty strings")
    return quote(value, safe="._-:@+")


def alias_path(
    root: str | os.PathLike[str],
    *,
    plan_id: str,
    authored_key: str,
    output: str,
) -> Path:
    """Return ``<root>/latest/<plan>/<authored-key>/<output>`` without I/O."""

    return (
        alias_root(root)
        / _component(plan_id)
        / _component(authored_key)
        / _component(output)
    )


def point_alias(
    root: str | os.PathLike[str],
    *,
    plan_id: str,
    authored_key: str,
    output: str,
    target: str | os.PathLike[str],
) -> Path:
    """Create or atomically repoint one alias to a possibly absent target."""

    published = alias_path(
        root, plan_id=plan_id, authored_key=authored_key, output=output
    )
    published.parent.mkdir(parents=True, exist_ok=True)
    temporary = published.with_name(f".{published.name}.{uuid4().hex}.partial")
    relative = os.path.relpath(Path(target), start=published.parent)
    try:
        os.symlink(relative, temporary)
        os.replace(temporary, published)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return published


def aliases_into(
    root: str | os.PathLike[str], workspace: Path
) -> tuple[Path, ...]:
    """Return every alias whose target is inside ``workspace``."""

    base = alias_root(root)
    if not base.is_dir():
        return ()
    destination = workspace.resolve(strict=False)
    found: list[Path] = []
    for directory, directories, filenames in os.walk(base, followlinks=False):
        # A symlink is an alias leaf even when its target is a directory. Do not
        # let os.walk follow it as another branch of the alias tree.
        directory_path = Path(directory)
        leaves = list(filenames)
        for name in list(directories):
            candidate = directory_path / name
            if candidate.is_symlink():
                leaves.append(name)
                directories.remove(name)
        for name in leaves:
            candidate = directory_path / name
            if not candidate.is_symlink():
                continue
            target = candidate.resolve(strict=False)
            if target == destination or destination in target.parents:
                found.append(candidate)
    return tuple(sorted(found))
