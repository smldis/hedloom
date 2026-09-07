"""Reloaded source keeps old plans bound to their authored implementation."""
import pytest

from hedloom import Site, study


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
    site = Site(root=str(tmp_path / "records"), history_root=str(tmp_path / "history"))
    for subject, expected in ((second, "after"), (first, "before"), (rebuilt, "before")):
        run = subject.submit(site=site, name="reload", sequential=True)
        assert run.succeeded
        assert run.outputs["value"].value == expected
