"""Task-local evidence for pooled assignment loss and owned restart.

A missing Dask assignment does not prove that the old command stopped. Keep
unknown losses sticky for the lifetime of that TaskState. An owned restart is
only pending until its nanny acknowledgement certifies the exact old worker
and task identities; a timeout cannot turn missing bookkeeping into safety.
"""
from distributed import Scheduler

LOSS = "_hedloom_assignment_loss"
FENCE = "_hedloom_interrupt_owner"
RESTART = "_hedloom_owned_restart"
ADMISSION = "_hedloom_force_admission"


def loss_record(task):
    if getattr(task, "metadata", None) is None:
        task.metadata = {}
    return task.metadata.setdefault(LOSS, {"unknown": False, "pending": None})


def assignment_loss(task, worker):
    """Record before Dask clears processing_on, including expected removal."""
    record = loss_record(task)
    token = worker.extra.get(RESTART)
    if token is None or record["pending"] is not None:
        # A second loss cannot overwrite evidence about an older live copy.
        record["unknown"] = True
    else:
        record["pending"] = {"token": token, "worker": worker.address,
                             "identity": id(worker)}


def reconsider(task, scheduler):
    """Revisit an admission hold after its exact owner releases it."""
    if task.state == "no-worker":
        scheduler.transitions({task.key: "processing"}, stimulus_id="hedloom-restart-confirmed")


class OwnedPoolScheduler(Scheduler):
    def valid_workers(self, task):
        metadata = task.metadata or {}
        pending = metadata.get(LOSS, {}).get("pending")
        if pending is not None or metadata.get(ADMISSION):
            # Command tasks have an explicit whole-worker resource restriction.
            # Keep them unrunnable during an unconfirmed owned restart, and
            # while a force waiter is withdrawing their scheduling interest.
            # Otherwise a replacement may enter queued work before that waiter
            # can freeze admission. No resource accounting is altered.
            return set()
        return super().valid_workers(task)

    async def remove_worker(self, address, **kwargs):
        worker = self.workers.get(self.coerce_address(address))
        if worker is not None:
            for task in tuple(worker.processing):
                assignment_loss(task, worker)
        return await super().remove_worker(address, **kwargs)

    def handle_worker_status_change(self, status, worker, stimulus_id):
        state = self.workers.get(worker) if isinstance(worker, str) else worker
        if getattr(status, "name", status) == "running" and state is not None and state.extra.get(FENCE):
            # A batched old running message must not undo a newer RPC fence.
            # Matching-owner cleanup clears FENCE before explicitly resuming.
            return
        return super().handle_worker_status_change(status, worker, stimulus_id)
