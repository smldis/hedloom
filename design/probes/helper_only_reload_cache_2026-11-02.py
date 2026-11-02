import importlib
import os
from pathlib import Path
import sys
import tempfile

from hedloom import Reproducibility, Site, runtime

root = Path(tempfile.mkdtemp(prefix="hedloom-helper-reload-"))
sys.path.insert(0, str(root))
source = root / "helper_bench.py"
template = '''from hedloom import operation, returned, study
def helper():
    return %r
@operation(outputs={"value": returned()})
def compute():
    return {"value": helper()}
@study
def my_study():
    return compute()
'''
source.write_text(template % "before")
module = importlib.import_module("helper_bench")
first = module.my_study()
site = Site(records_dir=str(root / "records"), runs_dir=str(root / "runs"))
with runtime(site) as live:
    before = live.submit(first, name="before", reproducibility=Reproducibility(enabled=False)).result(15)
    modified = source.stat().st_mtime + 2
    source.write_text(template % "after-helper-only-edit")
    os.utime(source, (modified, modified))
    importlib.invalidate_caches()
    importlib.reload(module)
    second = module.my_study()
    after = live.submit(second, name="after", reproducibility=Reproducibility(enabled=False)).result(15)
    print("current helper:", module.helper())
    print("same operation fingerprint:", first.plan.operations[0].implementation == second.plan.operations[0].implementation)
    print("first result:", before.outputs["output"].value)
    print("result after helper edit:", after.outputs["output"].value)
    print("reused:", after.report.outcomes[0].reused)
