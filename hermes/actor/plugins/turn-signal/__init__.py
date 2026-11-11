"""turn-signal - tells the gateway when this agent has actually finished.

WHY THIS EXISTS
---------------
The gateway decides when to suspend an idle actor, and its only evidence that an
agent is busy is an open HTTP request. That request times out after five minutes.
So an agent asked to do forty minutes of work is abandoned at minute five,
reported to its user as failed, and then suspended mid-task a minute later.
There is no way for the gateway to tell "finished" from "still working, we
stopped listening" - both look like a closed socket. This closes that gap from
the only place that knows the answer: inside the agent.

WHAT "FINISHED" MEANS
---------------------
Not "the model stopped talking". A turn can end while work it started keeps
running: a backgrounded terminal command, a code_execution process, a subagent
if the delegation toolset is ever enabled. Reporting the turn done then lets the
gateway suspend the actor, and because suspension is a gVisor checkpoint of the
whole sandbox, that work is not killed - it is FROZEN, and makes no progress
until someone next messages the agent. A twenty-minute build silently takes
hours.

So the signal is sent when the turn ends AND nothing the turn spawned is still
alive. If something is, this keeps watching it and sends the signal once it
drains. The gateway is unchanged: it still just receives "this actor is done".

HOW IT ATTACHES
---------------
Hermes is not modified. This is an ordinary user plugin, dropped into
$HERMES_HOME/plugins and enabled in config, registering two shipped hooks:

  pre_llm_call    fired once per turn BEFORE the tool-calling loop. Marks the
                  turn as started and snapshots which child processes were
                  already alive, so we can tell what THIS turn spawned.
  on_session_end  fired canonically at each turn finalisation. Unlike
                  transform_llm_output (which we used before) it also fires when
                  the turn was interrupted or produced no text, which otherwise
                  left the actor held awake until the gateway's hour-long ceiling
                  released it.

Both are observers: they return None and cannot alter the turn.

DETECTING SPAWNED WORK
----------------------
By walking /proc for processes descended from this one, not by asking Hermes
what it thinks it is running. That deliberately does not depend on any internal
structure: it sees backgrounded shells, code execution, and subagents alike, and
it cannot quietly go stale when upstream refactors. If /proc is unreadable the
answer is "something may be running" - never "nothing is".

SECURITY
--------
The signal carries no identity and needs none. The gateway resolves which actor
sent it from the connection itself - source address, to worker pod, to the actor
Substrate reports RUNNING there - exactly as it does for Google calls. An agent
therefore cannot signal on behalf of another: "who finished" is not something the
request gets to say. A hostile agent can at most end its own turn early, which
costs it its own remaining work and affects nobody else.

Failure is silent by design. If the gateway is unreachable the agent's work is
unaffected; the gateway falls back to its existing timeout, which is the
behaviour we had before this plugin.
"""
from __future__ import annotations

import logging
import os
import threading
import urllib.request

log = logging.getLogger("turn-signal")

GATEWAY = os.environ.get(
    "GATEWAY_URL", "http://hermes-gateway.hermes.svc.cluster.local")
ENDPOINT = f"{GATEWAY}/agent/turn-ended"
# Withholding "done" is not enough on its own. The gateway only holds an actor
# awake if something told it to, and the only other thing that does is the turn
# HTTP request timing out. A turn that answers quickly and leaves work running
# is otherwise suspended on the ordinary idle timeout - observed live: replied
# in 9s, suspended 63s later, background process frozen mid-flight. So say so.
WORKING_ENDPOINT = f"{GATEWAY}/agent/turn-working"
TIMEOUT_SECONDS = 5

# "working" must land or the actor is suspended mid-task, so it retries with
# backoff. "done" failing only costs a held worker until the gateway's ceiling,
# so one attempt is enough and a slow retry would delay the turn's return.
WORKING_ATTEMPTS = int(os.environ.get("TURN_SIGNAL_WORKING_ATTEMPTS", "4"))
DONE_ATTEMPTS = int(os.environ.get("TURN_SIGNAL_DONE_ATTEMPTS", "1"))
RETRY_DELAY_SECONDS = float(os.environ.get("TURN_SIGNAL_RETRY_DELAY", "0.5"))

# How often to re-check work the turn left running. 30s is well inside the
# gateway's idle timeouts, so a drained process is reported long before the
# ceiling that would have released the actor anyway.
POLL_SECONDS = float(os.environ.get("TURN_SIGNAL_POLL_SECONDS", "30"))

# Stop watching eventually. A process that genuinely never exits (a daemon the
# agent started, a hung command) must not keep a monitor thread alive for the
# life of the actor. When this expires we simply stop watching and send nothing:
# the gateway's own MAX_WORKING_SECONDS ceiling then decides, which is the
# behaviour we would have had without this plugin. Sending "done" here instead
# would be asserting something we know to be false.
MAX_WATCH_SECONDS = float(os.environ.get("TURN_SIGNAL_MAX_WATCH_SECONDS", "3300"))

_lock = threading.Lock()

# Turns in flight, turn_id -> the processes that were already alive when it
# started. Keyed per turn rather than held as one global snapshot because an
# actor can run concurrent conversations: with a single latch, turn A finishing
# would clear the hold while turn B was still working, and with a single epoch
# counter a newer turn would cancel an older turn's watch. Both are the same bug
# from opposite ends, and both are fixed by knowing which turn is which.
#
# turn_id arrives in both hook payloads. If it ever does not, _turn_key falls
# back to a single shared key, which degrades to the old single-turn behaviour
# rather than breaking.
_active: "dict[str, set[int]]" = {}

# Turns whose work outlived them and is still being watched. An actor is idle
# only when BOTH are empty: a turn that spawned nothing must not report done
# while another turn's background work is still draining, or it releases a latch
# it never set. Found by a test, not by reading the code.
_watching: "dict[str, set[int]]" = {}


def _actor_is_idle() -> bool:
    """No turn in flight and no spawned work still being watched."""
    with _lock:
        return not _active and not _watching


def _turn_key(kwargs) -> str:
    return str(kwargs.get("turn_id") or kwargs.get("session_id") or "_single")


# --------------------------------------------------------------------------
# process inspection
# --------------------------------------------------------------------------

def _ppid_from_stat(data: bytes) -> "int | None":
    """Parse ppid out of one /proc/<pid>/stat line.

    Split from the reading so it can be tested without a Linux /proc. comm is
    field 2 and may itself contain spaces and parentheses - `sh (my job)` is a
    legal process name - so the only safe anchor is the LAST ')'. Splitting on
    whitespace from the left, which is the obvious implementation, mis-parses
    exactly the processes an agent is most likely to spawn.
    """
    close = data.rfind(b")")
    if close == -1:
        return None
    fields = data[close + 2:].split()
    if len(fields) < 2:
        return None
    try:
        return int(fields[1])
    except ValueError:
        return None


def _descendants_of(parent: "dict[int, int]", root: int) -> "set[int]":
    """Every pid whose parent chain reaches `root`. Pure; total."""
    out: "set[int]" = set()
    for pid in parent:
        walker, hops = pid, 0
        # Bounded: a corrupt or cyclic parent chain must not hang the turn.
        while walker not in (0, 1) and hops < 64:
            walker = parent.get(walker, 0)
            hops += 1
            if walker == root:
                out.add(pid)
                break
    return out


def _descendants(root: int) -> "set[int] | None":
    """PIDs descended from `root`, or None if /proc could not be read.

    None is deliberately distinct from the empty set: "I could not tell" must
    never be mistaken for "nothing is running", because the whole point of this
    plugin is to not suspend an actor that is still working.
    """
    try:
        entries = os.listdir("/proc")
    except OSError as exc:
        log.debug("turn-signal cannot read /proc (%s) - assuming work may run", exc)
        return None
    parent: "dict[int, int]" = {}
    for entry in entries:
        if not entry.isdigit():
            continue
        pid = int(entry)
        try:
            with open(f"/proc/{pid}/stat", "rb") as fh:
                data = fh.read()
        except OSError:
            continue            # exited between listdir and open - normal
        ppid = _ppid_from_stat(data)
        if ppid is not None:
            parent[pid] = ppid
    return _descendants_of(parent, root)


def _spawned_still_running(turn: str) -> "set[int] | None":
    """What THIS turn started that is still alive. None if it cannot be told."""
    live = _descendants(os.getpid())
    if live is None:
        return None
    with _lock:
        started_before = set(_active.get(turn, ()))
    return live - started_before


# --------------------------------------------------------------------------
# the signal
# --------------------------------------------------------------------------

def _post(url: str, attempts: int) -> bool:
    """POST once per attempt. Returns whether the gateway accepted it.

    Retries are ASYMMETRIC on purpose, because the two signals fail in opposite
    directions. Losing "done" holds a worker until the ceiling: wasteful, safe.
    Losing "working" means the actor is never latched and gets suspended with
    work still running: the exact thing this plugin exists to prevent. So
    "working" retries and "done" does not need to.

    It is worth retrying at all because the gateway can legitimately refuse us
    for a moment: caller identity is resolved from the connection, that lookup
    can fail during a resume race, and a negative result is cached briefly - so
    a single attempt can lose to a window that would have closed on its own.
    """
    delay = RETRY_DELAY_SECONDS
    for attempt in range(1, attempts + 1):
        try:
            req = urllib.request.Request(url, data=b"{}", method="POST",
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
                resp.read()
            return True
        except Exception as exc:
            # Never raise: this runs on the agent's turn-finalisation path, and
            # a notifier that can break a turn is worse than no notifier.
            if attempt == attempts:
                log.warning("turn-signal gave up on %s after %d attempt(s): %s",
                            url, attempts, exc)
                return False
            log.debug("turn-signal %s attempt %d/%d failed (%s), retrying in %.1fs",
                      url, attempt, attempts, exc, delay)
            if _stop.wait(delay):
                return False
            delay *= 2
    return False


def _notify() -> None:
    _post(ENDPOINT, DONE_ATTEMPTS)


def _notify_working() -> bool:
    return _post(WORKING_ENDPOINT, WORKING_ATTEMPTS)


_stop = threading.Event()


def _watch_then_notify(pids: "set[int]", turn: str) -> None:
    """Wait for one turn's leftover processes to drain, then report done.

    Runs on a daemon thread so it can never hold the process open. Three ways it
    ends without signalling, all deliberate:

      * ANOTHER turn is still in flight - that turn owns the signal now, and
        reporting done would clear a latch its work still needs;
      * this turn started again (same id, new run) - superseded;
      * the watch window expired - the work is genuinely long-lived, and the
        honest thing is to stay silent and let the gateway's ceiling decide
        rather than claim it finished.
    """
    waited = 0.0
    try:
        _watch_loop(pids, turn)
    finally:
        with _lock:
            _watching.pop(turn, None)


def _watch_loop(pids: "set[int]", turn: str) -> None:
    waited = 0.0
    while waited < MAX_WATCH_SECONDS:
        if _stop.wait(POLL_SECONDS):
            return
        waited += POLL_SECONDS
        alive = {p for p in pids if os.path.exists(f"/proc/{p}")}
        if alive:
            pids = alive
            log.debug("turn-signal: %d spawned process(es) still running", len(alive))
            continue
        with _lock:
            if turn in _active:
                log.debug("turn-signal: %s started again - dropping this watch", turn)
                return
            _watching.pop(turn, None)
            busy = len(_active) + len(_watching)
        if busy:
            log.info("turn-signal: work from %s finished, but %d other turn(s) or "
                     "watch(es) are still outstanding - leaving the actor held",
                     turn, busy)
            return
        log.info("turn-signal: work the turn spawned has finished (%.0fs) - "
                 "reporting the turn done", waited)
        _notify()
        return
    log.warning("turn-signal: spawned work still running after %.0fs - stopping the "
                "watch without signalling; the gateway's own ceiling decides", waited)


# ---------------------------------------------------------------------------
# hooks
# ---------------------------------------------------------------------------

def _on_turn_start(**kwargs):
    """pre_llm_call - once per turn, before the tool-calling loop."""
    turn = _turn_key(kwargs)
    log.info("turn-signal: turn %s starting", turn)
    live = _descendants(os.getpid())
    with _lock:
        # Anything already running is not this turn's work. An unreadable /proc
        # records an empty baseline, which errs towards calling things "spawned"
        # and so towards holding the actor - the safe direction.
        _active[turn] = set(live or ())
    return None


def _on_turn_end(**kwargs):
    """on_session_end - fired at each turn finalisation, interrupted or not."""
    turn = _turn_key(kwargs)
    spawned = _spawned_still_running(turn)
    with _lock:
        _active.pop(turn, None)

    if spawned is None:
        # Could not inspect. Say nothing and let the gateway's ceiling decide;
        # claiming "done" on a guess is the one outcome that loses work.
        log.warning("turn-signal: cannot inspect processes - not signalling")
        return None

    if spawned:
        log.info("turn-signal: turn %s ended but %d spawned process(es) are still "
                 "running - telling the gateway to hold this actor awake",
                 turn, len(spawned))
        # Assert it, do not merely withhold "done": the gateway has no other way
        # to learn that a fast turn left work behind.
        with _lock:
            _watching[turn] = set(spawned)
        _notify_working()
        threading.Thread(target=_watch_then_notify, args=(spawned, turn),
                         name="turn-signal-watch", daemon=True).start()
        return None

    if not _actor_is_idle():
        # Nothing left from this turn, but the actor is not idle: another
        # conversation is mid-turn, or another turn's background work is still
        # draining. Either way the latch is not ours to clear.
        #
        # INFO, not debug, and it names what is holding it. This branch decides
        # whether an actor is ever suspended on its own signal, and at debug it
        # is indistinguishable from the hook never firing at all: both look like
        # silence from outside. A stale entry here, which a checkpoint can carry
        # across a restore, makes the plugin permanently silent.
        with _lock:
            held = sorted(_active) + [f"watch:{k}" for k in sorted(_watching)]
        log.info("turn-signal: %s done, but the actor is still busy, held by %s",
                 turn, held or "nothing (this is a bug)")
        return None

    log.info("turn-signal: %s done and nothing else is running, signalling", turn)
    _notify()
    return None


def register(ctx) -> None:
    ctx.register_hook("pre_llm_call", _on_turn_start)
    ctx.register_hook("on_session_end", _on_turn_end)
    log.info("turn-signal registered; will notify %s when a turn ends and the "
             "work it spawned has drained", ENDPOINT)
