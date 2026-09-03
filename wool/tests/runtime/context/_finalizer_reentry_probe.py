"""Out-of-process probe for the token registry's finalizer re-entrancy.

NOT a test module — no ``test_`` prefix — so pytest does not collect it. It is
run in a subprocess by the re-entrancy regression in ``test_token.py``, whose
docstring owns the scenario. It lives out of process because a regression hangs
while holding the token registry's lock, which would strand every later test in
the interpreter rather than failing one.
"""

import sys
import uuid

import wool
from wool.runtime.context.token import dead_token_ids
from wool.runtime.context.var import ContextVar

#: Holds the token whose release must fire inside the critical section, so the
#: profile hook can drop its last reference from there.
_victim: list = []


def _fail(message: str) -> None:
    """Exit non-zero with *message*.

    Used in place of ``assert``, which the interpreter strips under ``-O`` —
    inherited from the parent environment, that would let this probe report
    success without checking anything.
    """
    raise SystemExit(message)


def _drop_victim_inside_critical_section(frame, event, arg) -> None:
    """Drop the victim's last reference from inside the registry's lock.

    Anchored on the registry's drain, whose documented precondition is that
    callers hold the registry lock, so landing inside the critical section is
    structural rather than incidental to whichever C call happens to run there.

    Naming a private function is a deliberate, reviewed waiver of the test
    guide's rule against referencing private symbols. No public anchor exists:
    the critical section runs no user-controllable callback, garbage-collection
    stress never reproduced the interleaving across thousands of rounds, and an
    ungated ``gc.callbacks`` drop always fired before the lock was taken.
    """
    if event == "call" and frame.f_code.co_name == "_drain_released":
        sys.setprofile(None)
        _victim.clear()


def main() -> None:
    """Register a token while another token's release fires under the lock."""
    victim_var = ContextVar(f"probe_victim_{uuid.uuid4().hex}")
    trigger_var = ContextVar(f"probe_trigger_{uuid.uuid4().hex}")
    key = (victim_var.namespace, victim_var.name)

    _victim.append(victim_var.set("victim"))
    manifest = wool.__chain__.get().to_manifest()
    victim_id = next(iter(manifest.unspent_tokens[key]))

    sys.setprofile(_drop_victim_inside_critical_section)
    try:
        trigger_var.set("trigger")
    finally:
        sys.setprofile(None)

    if _victim:
        _fail("the probe never reached the token registry's critical section")
    if dead_token_ids({victim_id}) != frozenset({victim_id}):
        _fail("the release fired under the lock but its decrement was lost")
    print("OK")


if __name__ == "__main__":
    main()
