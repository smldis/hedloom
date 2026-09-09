"""Stage the documentation declared by unit.toml for a standalone Sphinx build.

Keep repository-relative paths so links between units and example downloads
resolve without depending on the parent workspace's composition tooling.
"""

from pathlib import Path
import shutil
import tomllib


ROOT = Path(__file__).resolve().parents[1]
STAGE = ROOT / "build" / "docs-source"


def stage_unit(root: Path) -> list[str]:
    manifest = tomllib.loads((root / "unit.toml").read_text())
    docs = manifest["docs"]
    relative = root.relative_to(ROOT)
    for resource in [docs["source"], *docs.get("resources", [])]:
        source = root / resource
        target = STAGE / relative / resource
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            shutil.copytree(
                source, target,
                ignore=shutil.ignore_patterns("_runs", "_build", "__pycache__", "states"),
            )
        else:
            shutil.copy2(source, target)
    pages = [(relative / docs["source"] / docs["index"]).with_suffix("").as_posix()]
    for child in manifest["unit"].get("children", []):
        pages.extend(stage_unit(root / child))
    return pages


def main() -> None:
    if STAGE.exists():
        shutil.rmtree(STAGE)
    pages = stage_unit(ROOT)
    shutil.copy2(ROOT / "docs" / "conf.py", STAGE / "conf.py")
    (STAGE / "index.md").write_text(
        "# Hedloom\n\nAuthor a study, inspect its plan, and run it.\n\n"
        "```{toctree}\n:maxdepth: 2\n\n" + "\n".join(pages) + "\n```\n"
    )
    print(f"Staged documentation in {STAGE}")


if __name__ == "__main__":
    main()
