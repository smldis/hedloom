"""Real IPython reloads refresh builders and preserve selected implementations."""

import importlib
import os
import sys
from threading import Event
from uuid import uuid4

from runtime_helpers import run_study
import pytest

from hedloom import Reproducibility, Runtime, Site, runtime, study

DISABLED = Reproducibility(enabled=False)


@pytest.fixture
def bench_module(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    module_name = f"interactive_bench_{uuid4().hex}"
    source = tmp_path / f"{module_name}.py"
    generation = 0
    template = '''from hedloom import operation, parameter, returned, study, flow, named_policy
def helper():
    return {offset}
@operation(config={{"n": parameter(int)}}, outputs={{"value": returned()}})
def compute(*, n):
    return {{"value": n * {factor} + helper()}}
@flow
def group():
    return compute.named("inner")(n={inner})
@study(name={name!r}, default_policy=named_policy({policy!r})())
def my_study(n={direct}):
    return {{"group": group.named("group")().value,
             "direct": compute.named("direct")(n=n).value}}
'''

    def edit(*, factor=1, inner=2, direct=3, offset=5, name=None, policy="local"):
        nonlocal generation
        previous = source.stat().st_mtime if source.exists() else 0
        generation += 1
        source.write_text("\n" * generation + template.format(
            factor=factor, inner=inner, direct=direct, offset=offset,
            name=name or module_name, policy=policy,
        ))
        # Avoid a timing-dependent sleep and stale same-second bytecode.
        modified = max(source.stat().st_mtime, previous + 2)
        os.utime(source, (modified, modified))
        importlib.invalidate_caches()

    edit()
    try:
        yield module_name, edit
    finally:
        sys.modules.pop(module_name, None)


@pytest.fixture
def interactive_bench(bench_module, tmp_path, monkeypatch):
    monkeypatch.setenv("IPYTHONDIR", str(tmp_path / "ipython"))
    shell = pytest.importorskip("IPython.core.interactiveshell").InteractiveShell()
    module_name, edit = bench_module

    def cell(code):
        result = shell.run_cell(code)
        assert result.success, result.error_before_exec or result.error_in_exec

    shell.run_line_magic("load_ext", "autoreload")
    try:
        yield shell, module_name, edit, cell
    finally:
        shell.run_line_magic("autoreload", "0")


def test_module_reload_refreshes_retained_study_builder(bench_module):
    module_name, edit = bench_module
    module = importlib.import_module(module_name)
    imported_builder = module.my_study
    first = imported_builder()
    edit(direct=30, inner=20, policy="elsewhere")
    importlib.reload(module)

    updated = imported_builder()
    assert updated.document == module.my_study().document
    assert all(item["policy"]["name"] == "elsewhere"
               for item in updated.document["invocations"])
    assert all(item["policy"]["name"] == "local"
               for item in first.document["invocations"])
    direct = next(item for item in updated.document["invocations"]
                  if item["authored_key"] == "direct")
    assert direct["config"] == [{"name": "n", "value": 30}]


@pytest.mark.parametrize("mode", ["2", "3"])
def test_autoreload_from_import_refreshes_study_and_preserves_old_plans(
    interactive_bench, tmp_path, mode,
):
    shell, module_name, edit, cell = interactive_bench
    shell.run_line_magic("autoreload", mode)
    cell(f"from {module_name} import my_study\nfirst = my_study()")
    imported_builder = shell.user_ns["my_study"]
    first = shell.user_ns["first"]

    edit(factor=10, inner=20, direct=30, name=f"{module_name}-edited")
    cell("second = my_study()")
    second = shell.user_ns["second"]
    # Exercise the original object, even if an IPython version rebinds names.
    assert imported_builder().document == second.document
    assert imported_builder().name == f"{module_name}-edited"
    assert second.document == sys.modules[module_name].my_study().document
    assert first.plan.operations != second.plan.operations
    assert first.name == module_name

    rebuilt = study(first.plan, name=f"{module_name}-rebuilt")
    site = Site(records_dir=str(tmp_path / "records"), runs_dir=str(tmp_path / "history"))
    with runtime(site) as live:
        after = live.submit(second, name="after", reproducibility=DISABLED).result(15)
        before = live.submit(first, name="before", reproducibility=DISABLED).result(15)
        restored = live.submit(rebuilt, name="restored", reproducibility=DISABLED).result(15)
        reused = live.submit(imported_builder(), name="reused", reproducibility=DISABLED).result(15)
    assert {key: value.value for key, value in after.outputs.items()} == {
        "group": 205, "direct": 305,
    }
    assert {key: value.value for key, value in before.outputs.items()} == {
        "group": 7, "direct": 8,
    }
    assert all(row.reused for row in restored.report.outcomes)
    assert all(row.reused for row in reused.report.outcomes)


def test_autoreload_does_not_replace_queued_run_code_or_helpers(
    interactive_bench, tmp_path, monkeypatch,
):
    shell, module_name, edit, cell = interactive_bench
    shell.run_line_magic("autoreload", "2")
    cell(f"from {module_name} import my_study\nfirst = my_study()")
    entered, release = Event(), Event()
    original = Runtime._prepare

    def prepare(self, *args):
        entered.set()
        assert release.wait(15)
        return original(self, *args)

    monkeypatch.setattr(Runtime, "_prepare", prepare)
    site = Site(records_dir=str(tmp_path / "records"), runs_dir=str(tmp_path / "history"))
    with runtime(site) as live:
        try:
            before = live.submit(shell.user_ns["first"], name="queued", reproducibility=DISABLED)
            assert entered.wait(10)
            edit(factor=10, inner=20, direct=30, offset=50)
            cell("second = my_study()")
            after = live.submit(shell.user_ns["second"], name="edited", reproducibility=DISABLED)
        finally:
            release.set()
        before_result = before.result(15)
        after_result = after.result(15)
    assert {key: value.value for key, value in before_result.outputs.items()} == {
        "group": 7, "direct": 8,
    }
    assert {key: value.value for key, value in after_result.outputs.items()} == {
        "group": 250, "direct": 350,
    }


def test_imported_builder_does_not_follow_an_unrelated_definition(bench_module):
    module_name, edit = bench_module
    module = importlib.import_module(module_name)
    original = module.my_study

    @study
    def unrelated():
        return {}

    module.my_study = unrelated
    assert original().name == module_name
    assert len(original().plan.invocations) == 2


def test_rerun_edited_script_retains_old_plan_body(tmp_path):
    shell = pytest.importorskip("IPython.core.interactiveshell").InteractiveShell()
    source = tmp_path / "reload_study.py"
    template = '''from hedloom import operation, returned, study
@operation(outputs={"value": returned()})
def reload_value():
    return {"value": %r}
@study(name="interactive-reload-regression")
def reload_study():
    return {"value": reload_value().value}
subject = reload_study()
'''
    source.write_text(template % "before")
    shell.run_line_magic("run", str(source))
    first = shell.user_ns["subject"]
    # Shift declaration lines as well as editing the body, as in ordinary edits.
    source.write_text("\n\n" + template % "after")
    shell.run_line_magic("run", str(source))
    second = shell.user_ns["subject"]
    rebuilt = study(first.plan, name="old-rebuilt")
    site = Site(records_dir=str(tmp_path / "records"), runs_dir=str(tmp_path / "history"))
    for subject, expected in ((second, "after"), (first, "before"), (rebuilt, "before")):
        run = run_study(subject, site=site, name="reload")
        assert run.succeeded
        assert run.outputs["value"].value == expected
