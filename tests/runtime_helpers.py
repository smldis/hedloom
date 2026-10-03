"""Run semantic regression subjects through the real async Runtime."""

from hedloom import runtime


def run_study(subject, *, site, name, override=None, locally=False,
              watch=False, _watch_reader=None, environment=None,
              require_success=False, **options):
    runtime_options = {"override": override, "locally": locally}
    if watch:
        runtime_options["watch"] = True
    if _watch_reader is not None:
        runtime_options["_watch_reader"] = _watch_reader
    if environment is not None:
        options["environment"] = environment
    with runtime(site, **runtime_options) as live:
        live.ready()
        receipt = live.submit(subject, name=name, **options)
        return receipt.result() if require_success else receipt.wait()
