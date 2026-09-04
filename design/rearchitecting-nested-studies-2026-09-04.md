# Worth evaluating: should nested studies be re-architected? (2026-09-04)

**Status: a question, not a proposal.** Nothing here is designed. This exists
so the question is not rediscovered from scratch a third time, and so the next
person to reach for a nested submit finds the evidence already gathered.

`nested-submission-and-capacity-2026-08-30.md` is the measurement behind it and
should be read first: it records why the fetch cannot be an ordinary operation,
what `secede()`, `worker_client()`, donation and configuration each cost, and
why the refusal was the honest thing to ship. This note does not revisit any of
that. It asks the question that note deliberately did not: **is nesting the
right primitive at all, or is it a workaround we have made safe?**

## Why ask now

Nesting exists to solve one real problem — freshness and reuse cannot come from
the same node, because every invocation's identity is fixed before any body
runs, so a node that must rerun every time invalidates everything beneath it
whether or not the fetched bytes moved. Two stages solve that honestly.
`examples/live_source.py` demonstrates it end to end.

But the shape has accumulated costs that are each individually defensible and
collectively worth a second look:

* **A waiting invocation holds a unit of its placement.** The safety net is a
  refusal, so the failure mode is now loud rather than a 45-second hang — but
  the user still has to over-declare capacity by hand, and the example needs a
  comment explaining why its `placements` has headroom it never uses.
* **The refusal is graph-kernel only.** The sequential driver simply recurses.
  Two kernels disagree about whether a plan is runnable.
* **A body reaching a live `Session` needs a module-level indirection**
  (`examples/live_source_state.py`) purely to defeat cloudpickle's by-value
  capture of `__main__` globals. Fourteen lines of file whose entire purpose is
  to change a pickling mode.
* **The pattern was available and not chosen.** The study this example was
  written for pulled before `submit()` instead — no nonce, no inner plan, a
  symlink at a stable locator under an address space whose *directory* is not
  identity-bearing. That is materially simpler and gets the same invalidation.
  One data point, not a verdict, but it is the data point that prompted this.

## What a re-architecture might mean

Unexamined, listed to be argued with rather than implemented:

* **A first-class stage in the plan.** If "author a plan, then submit it" is a
  real phase, the kernel could know about it instead of discovering it as a
  blocked thread. Capacity accounting stops being process-global bookkeeping.
* **A source that is re-read per submission by declaration.** The staged
  pattern's whole payload is "fingerprint this again, now". If a source could
  declare that, the outer stage disappears and with it the nonce, the shared
  session and the capacity problem.
* **Accept nesting and pay for it properly.** Donation is designed and
  demonstrated in the 08-30 note; what is missing is counting and a decision.
* **Decide nesting is not supported.** Document pull-before-submit as the
  answer, keep the refusal as the guard rail, and delete the rest.

## What to do before deciding

* Ask whether any real study still needs the staged shape now that
  pull-before-submit is known to work. If none does, most of this is moot.
* Do not treat `examples/live_source.py` as evidence of demand. It was written
  to explain the mechanism, and the study that prompted it went the other way.

Per this directory's rule: if this is answered, the answer belongs in `docs/`,
and this file stops being edited.
