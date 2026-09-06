"""An in-process Session reference that survives operation serialization.

A body defined in ``__main__`` is serialized by value, including globals it
uses. A live Session contains locks and cannot travel that way. An imported
module travels by reference, so ``state.SESSION`` reaches the existing object
on the Session's in-process workers. This is example wiring for one Session;
it does not provide access to that Session from another process or host.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hedloom import Session

SESSION: "Session | None" = None
