#!/usr/bin/env python3
"""T1 soak - the spine of pass 1. Five tests read out of this one run.

Plan §5.0: T1 is the long pole, and while it runs it is already generating the
evidence for four other tests. So this driver does not just cycle the fleet, it
records what those four need, separately and without mixing clocks:

  T1   cycles per agent, state-corruption events, stranded actors
  T15  first-response latency, split by wake type and never averaged across it
  T3   cron fires observed against agents this driver is putting to sleep
  T6   restore checks sampled on resume - credential, clock, scheduled work
  T9   is the soak's first cycle taken from cold, and is *not* run from here

Three things it deliberately does not do.

**It does not recover anything.** `actor_watchdog.py` owns recovery, and this
driver refuses to start unless the watchdog is alive (§5.0: risk 7 is unfixed at
the pinned build). A wedged agent is parked and recorded here, not cleared. Two
processes racing to suspend the same actor would make both their logs useless.

**It never restarts the gateway.** T10 does that, and it runs last, after the
soak, because a restart mid-soak invalidates the cycle count.

**It never invokes `debug-flush-redis`.** Same reason the watchdog does not: it
flushes every actor in the space, so run against a ten-agent soak it destroys
the run it was called to save.

## What counts as a cycle

Only a full lifecycle: SUSPENDED -> (our message wakes it) -> RUNNING -> settles
back to SUSPENDED. A turn taken against an agent that was already RUNNING is a
warm turn - it is recorded, it feeds T15's warm distribution, and it does not
count toward T1. This distinction is the whole T2 lesson from 27 Aug: standin-06
answered its question perfectly and had never suspended, so the answer came from
a live process and evidenced nothing about restore. A driver that counts turns
instead of transitions reports a number that means something else.

Cycles are also subtracted when the watchdog took the agent's worker away under
it - rung 4 restarts a pod and everything else on it loses its sandbox. The
watchdog enumerates the collateral before acting; `report` reads its event log
and excludes any cycle overlapping one.

## Provider errors are a third outcome

Hermes returns a failed model call as HTTP 200 with the provider's error as the
assistant message (§5.2). Folded into success it makes a latency table wrong by
about forty-five seconds a sample; folded into failure it reports a platform
fault that did not happen. Classified here, quarantined from every mean, and
counted on its own line. The classifier is shared with T2 rather than
reimplemented, so the two runs cannot drift apart in how they judge it.

## Usage

    kubectl --context <ctx> -n hermes port-forward deploy/hermes-gateway 18080:8080
    ./actor_watchdog.py                       # in its own terminal, first
    ./t1_soak.py run    --run 260828 --cycles 100
    ./t1_soak.py report --run 260828

Attended. Ctrl-C stops cleanly at the end of each agent's current cycle and the
artefacts are complete either way - every cycle is appended to JSONL as it
happens, so a killed run loses at most the cycle in flight.
"""
from __future__ import annotations

import argparse
import json
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

# Shared with T2 rather than copied. The AGENTS table, the actor-name
# derivation and the provider-error classifier all have to agree between the two
# runs or the results cannot be read against each other.
from t2_overnight import (
    AGENTS, ATESPACE, CONTEXT, FLEET_PROVENANCE, IDLE_THREE, OUTDIR,
    actor_docs, actor_name, ate, check_registry, now, provider_error, turn,
)

WATCHDOG_EVENTS = OUTDIR / "t1-watchdog-events.jsonl"

# Idle tiers as configured in the fleet file, and the budget this driver allows
# for a suspend at each. The budget is the tier plus slack for the snapshot
# itself; overrunning it is recorded as a finding, not a crash.
#
# `always-on` is None rather than a large number, and that is the point: the
# tier means the actor is never supposed to suspend, so there is no budget to
# wait out. The pass 1 stand-in fleet had no always-on agent, so this lookup was
# total; the real fleet has one, and `TIER_SUSPEND_BUDGET_S[tier]` on it was a
# KeyError on that agent's first cycle.
TIER_IDLE_S = {"short": 60, "medium": 900, "long": 3600, "always-on": None}
TIER_SUSPEND_BUDGET_S = {"short": 240, "medium": 1200, "long": 4200,
                         "always-on": None}

# The canary is soak-local and distinct from T2's cold facts, which must not be
# disturbed - a soak that keeps re-surfacing the graded fact would hand T2 a
# pass it did not earn. Same WORD-4digits shape so the substring grading is
# identical.
CANARY_WORDS = (
    "BASALT", "CATKIN", "DAMSON", "ELDRITCH", "FULMAR", "GORSE", "HALYARD",
    "ISOBAR", "JACINTH", "KESTREL", "LIMPET", "MARLIN", "NACRE", "ORPIMENT",
    "PLINTH", "QUILLON",
)

CANARY_PLANT = (
    "One thing to keep hold of for the rest of today, and I will keep asking: "
    "the word is {canary}. Just acknowledge it, nothing else needed."
)
CANARY_ASK = (
    "Without me repeating it: what was the word I asked you to keep hold of?"
)
# Deliberately cheap. The soak is measuring the lifecycle, not the model - a
# long generation adds seconds to every T15 sample and buys nothing.
CYCLE_PROMPTS = (
    "Check-in {n}. Nothing new from me - just confirm you are still with me, briefly.",
    "Check-in {n}. In one line: what is the {noun} I gave you?",
    "Check-in {n}. Still here? One short sentence back is fine.",
)
T6_CLOCK_ASK = (
    "Two quick things, one line each: what is today's date, and roughly how "
    "long has it been since we last spoke?"
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def canary_for(run: str, agent_id: str) -> str:
    """Deterministic from (run, agent), so a resumed or re-read run derives the
    same token without needing the plant artefact on disk."""
    h = sha256(f"{run}:{agent_id}".encode()).hexdigest()
    return f"{CANARY_WORDS[int(h[:2], 16) % len(CANARY_WORDS)]}-{int(h[2:6], 16) % 10000:04d}"


def path_for(run: str, what: str, ext: str = "jsonl") -> Path:
    return OUTDIR / f"t1-soak-{what}-{run}.{ext}"


def pct(vals: list[float], p: float) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    return round(s[min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1))))], 2)


def summarise(vals: list[float]) -> dict:
    """n first, because a p99 of four samples is a max wearing a hat."""
    return {"n": len(vals), "p50": pct(vals, 50), "p90": pct(vals, 90),
            "p99": pct(vals, 99),
            "min": round(min(vals), 2) if vals else None,
            "max": round(max(vals), 2) if vals else None}


class Writer:
    """One lock, one append-only file. Every record is durable before the next
    turn starts, so a killed soak still has everything up to the last cycle."""

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        self._fh = path.open("a", buffering=1)

    def write(self, rec: dict) -> None:
        with self._lock:
            self._fh.write(json.dumps(rec) + "\n")

    def close(self) -> None:
        with self._lock:
            self._fh.close()


class StateCache:
    """One control-plane read per interval, shared by every worker.

    Ten threads each polling `get actors` would put more load on the API server
    than the soak itself does, and the samples would not line up, which is what
    the transition trace needs them to do.
    """

    def __init__(self, sample_s: float, writer: Writer, stop: threading.Event):
        self.sample_s = sample_s
        self.writer = writer
        self.stop = stop
        self._lock = threading.Lock()
        self._states: dict[str, str] = {}
        self._at = 0.0
        self.inflight: set[str] = set()
        self.unattributed: list[dict] = []
        self.read_failures = 0

    def refresh(self) -> None:
        try:
            docs = actor_docs()
        except Exception as e:
            self.read_failures += 1
            self.writer.write({"event": "state_read_failed", "at": now(),
                               "error": repr(e)[:300]})
            return
        fresh = {n: (d.get("status") or {}).get("state", "ABSENT")
                 for n, d in docs.items()}
        with self._lock:
            prev, self._states, self._at = self._states, fresh, time.monotonic()
            inflight = set(self.inflight)
        moves = []
        for name, st in fresh.items():
            was = prev.get(name)
            if was is None or was == st:
                continue
            moves.append({"actor": name, "from": was, "to": st})
            # T3: a wake nobody asked for is the scheduler doing its job. The
            # gateway owns the cron clock and its log is the authority; this is
            # a second, independent observation of the same event, recorded as
            # inferred rather than presented as the fire log itself.
            if (st == "ACTOR_STATE_RUNNING"
                    and was == "ACTOR_STATE_SUSPENDED"
                    and name not in inflight):
                ev = {"event": "unattributed_wake", "at": now(), "actor": name}
                self.unattributed.append(ev)
                self.writer.write(ev)
        self.writer.write({"event": "sample", "at": now(), "states": fresh,
                           "transitions": moves})

    def run(self) -> None:
        while not self.stop.is_set():
            self.refresh()
            self.stop.wait(self.sample_s)

    def get(self, name: str) -> str:
        with self._lock:
            return self._states.get(name, "ABSENT")

    def fresh(self, name: str) -> str:
        """One actor, read now, cache bypassed. Use where staleness changes the
        verdict rather than the schedule.

        `get` is up to `sample_s` old, which is the right trade for deciding
        when to suspend and the wrong one for deciding whether a turn was cold.
        A turn sent seconds after the previous turn woke the actor reads the
        pre-wake sample, is filed as cold, and lands in the restore latency as a
        fast one. On the 7 Sep pass-2 run that was 325 of 850 counted cycles:
        the control plane recorded ~555 restores for the window and the soak
        claimed 850, and the mislabelled turns sat at a 3.37s median against
        6.77s for the confirmed ones. It made the fleet look busier and quicker
        at the same time, and nothing in the run looked wrong.

        Falls back to the cached value rather than raising: a read that fails is
        not a reason to drop a cycle, and the record carries `state_fresh` so a
        reader can tell which turns were classified on a stale sample.
        """
        p = ate("get", "actor", name, "-a", ATESPACE, "-o", "json")
        if p.returncode != 0:
            with self._lock:
                self.read_failures += 1
            return self.get(name)
        try:
            doc = json.loads(p.stdout or "{}")
            doc = (doc.get("actors") or [doc])[0]
            st = (doc.get("status") or {}).get("state", "ABSENT")
        except Exception:
            return self.get(name)
        with self._lock:
            self._states[name] = st
        return st

    def mark(self, name: str, busy: bool) -> None:
        with self._lock:
            self.inflight.add(name) if busy else self.inflight.discard(name)

    def wait_for(self, name: str, want: str, timeout: float) -> tuple[str, float]:
        """Returns (state reached or last seen, seconds waited). A timeout is a
        result the caller records, not an exception - an agent that will not
        suspend is exactly the kind of thing this run exists to find."""
        t0 = time.monotonic()
        while True:
            st = self.get(name)
            if st == want:
                # Confirm before returning. A stale sample that already says
                # what the caller is waiting for returns instantly and the
                # caller proceeds as though it had waited, which is how a cycle
                # skips its own suspend and then reports the warm turn that
                # follows as a restore.
                st = self.fresh(name)
                if st == want:
                    return st, round(time.monotonic() - t0, 1)
            if time.monotonic() - t0 >= timeout or self.stop.is_set():
                return st, round(time.monotonic() - t0, 1)
            self.stop.wait(min(5.0, self.sample_s))


def preflight(a: argparse.Namespace) -> None:
    """Everything that is cheaper to find out now than at hour six."""
    check_registry(a.gateway)

    # The watchdog. Without it a single wedge at hour six is a lost night, and
    # the plan makes it a precondition rather than a nicety.
    fresh = None
    if WATCHDOG_EVENTS.exists():
        for line in WATCHDOG_EVENTS.read_text().splitlines()[-400:]:
            try:
                r = json.loads(line)
            except Exception:
                continue
            # The watchdog stamps `time`, not `at`. Read both: getting this
            # wrong makes a healthy watchdog look absent, which is a preflight
            # that refuses to start the run it exists to protect.
            if r.get("event") in ("heartbeat", "watchdog_start"):
                fresh = r.get("time") or r.get("at") or fresh
    age = None
    if fresh:
        age = (utcnow() - datetime.fromisoformat(fresh)).total_seconds()
    if age is None or age > 180:
        msg = (f"watchdog heartbeat is {'absent' if age is None else f'{age:.0f}s old'} "
               f"in {WATCHDOG_EVENTS} - start ./actor_watchdog.py first")
        if not a.no_watchdog:
            raise SystemExit(msg + "\n  (--no-watchdog to soak without it, which "
                                   "means an unrecovered wedge stops that agent "
                                   "for the rest of the run)")
        print(f"  WARNING: {msg}")
    else:
        print(f"  watchdog alive, last heartbeat {age:.0f}s ago")

    # O-15. The egress gateway serves a certificate it will not reload, and the
    # CronJob that restarts it was itself broken for its first nine hours. A
    # soak that outlives the served leaf sees every turn fail with the model
    # provider's name on it, which is the one failure this run must not
    # misattribute.
    p = subprocess.run(
        ["kubectl", "--context", CONTEXT, "-n", "ate-system", "get",
         "cronjob", "egress-cert-restarter", "-o",
         "jsonpath={.status.lastSuccessfulTime}"],
        capture_output=True, text=True, timeout=60)
    last = (p.stdout or "").strip()
    if p.returncode != 0 or not last:
        print("  WARNING: no successful run of egress-cert-restarter found. "
              "Check the served leaf before starting:\n"
              "    kubectl -n ate-system exec deploy/atenet-egress -- \\\n"
              "      openssl x509 -noout -dates -in <cert path>")
    else:
        hrs = (utcnow() - datetime.fromisoformat(last.replace("Z", "+00:00"))
               ).total_seconds() / 3600.0
        flag = "WARNING: " if hrs > 13 else ""
        print(f"  {flag}egress-cert-restarter last succeeded {hrs:.1f}h ago "
              f"(schedule is every 12h)")


def cycle_agent(a: argparse.Namespace, agent_id: str, idx: int,
                cache: StateCache, cycles: Writer, gate: threading.Semaphore,
                stop: threading.Event, counters: dict) -> None:
    session, tier, noun, _value, _q, _cold = AGENTS[agent_id]
    actor = actor_name(agent_id)
    canary = canary_for(a.run, agent_id)
    budget = TIER_SUSPEND_BUDGET_S[tier]  # None for always-on, guarded below

    # Forcing a suspend is a real change to what the cycle evidences, so it is
    # decided once, per tier, and recorded on every cycle rather than inferred
    # from the timings afterwards. The medium and long tiers idle for 15 and 60
    # minutes: waiting them out is 100 cycles in 25 hours and 100 hours
    # respectively, which does not fit pass 1 at all. The wake stays
    # message-driven either way, which is what T1 specifies; what changes is
    # whether the sleep was the agent's own idle timer or ours.
    mode = a.suspend_mode
    if tier == "always-on":
        # Never suspended by us, on any --suspend-mode. Forcing one would be
        # measuring the opposite of what the tier asks for, and the interesting
        # question about an always-on agent is not how fast it wakes, it is
        # whether it ever went to sleep on its own. That is checked per cycle
        # below and recorded as a finding, not corrected.
        mode = "never"
    elif mode == "auto":
        mode = "idle" if tier == "short" else "forced"

    def emit(rec: dict) -> None:
        cycles.write({"event": "cycle", "run": a.run, "at": now(),
                      "agent_id": agent_id, "actor": actor, "tier": tier,
                      "held_idle": agent_id in IDLE_THREE,
                      "suspend_mode": mode, **rec})

    def say(text: str, kind: str) -> dict:
        with gate:
            cache.mark(actor, True)
            try:
                r = turn(a.gateway, session, text, a.timeout)
            finally:
                cache.mark(actor, False)
        r["kind"] = kind
        r["provider_error"] = provider_error(r.get("reply", ""))
        return r

    # Stagger the starts. Ten agents beginning together is a ten-way concurrent
    # burst against a per-minute provider limit, which is T9's job and not this
    # one's - it cost two of ten turns on the T2 plant.
    stop.wait(idx * a.stagger)
    if stop.is_set():
        return

    plant = say(CANARY_PLANT.format(canary=canary), "canary-plant")
    emit({"cycle": 0, "phase": "canary-plant", "canary": canary,
          "first_response_s": plant["elapsed_s"], "status": plant["status"],
          "ok": plant["ok"], "provider_error": plant["provider_error"],
          "reply_head": (plant.get("reply") or "")[:200],
          "counted": False,
          "exclude_reason": "setup turn, not a lifecycle cycle"})
    if not plant["ok"] or plant["provider_error"]:
        print(f"  {agent_id}: canary plant did not land - corruption checks for "
              f"this agent are unevidenced, cycling anyway")

    n = 0
    while not stop.is_set() and n < a.cycles:
        n += 1

        # 1. Get it asleep. This is where a cycle begins, not where it ends:
        #    the state immediately before our message is what decides whether
        #    the wake evidences a restore.
        pre_wait = 0.0
        if mode == "never":
            # No suspend, no wait. Read where it actually is: an always-on actor
            # found SUSPENDED is the platform not honouring the tier, which is a
            # result this soak exists to catch, so it is emitted and the cycle
            # continues rather than being repaired.
            if cache.get(actor) == "ACTOR_STATE_SUSPENDED":
                emit({"cycle": n, "phase": "always-on-suspended", "counted": False,
                      "exclude_reason": "finding: always-on actor was found suspended"})
                counters[agent_id]["always_on_suspended"] += 1
        elif mode == "forced":
            # Ask unless the actor is already observed asleep, and ask again if
            # the wait overruns. Gating this on `== RUNNING` looks equivalent and
            # is not: the cached state is up to --sample seconds old, so a sample
            # landing mid-RESUMING skips the suspend entirely and the cycle
            # degrades to waiting out an idle timer. For the three held-idle
            # agents no idle timer ever fires, so the agent produces nothing for
            # the rest of the run - which is what happened to standin-07 on the
            # first 260827 attempt while its two identical peers reached 18.
            for attempt in (1, 2):
                # Fresh: the cached SUSPENDED may predate the wake the previous
                # cycle caused, and believing it skips this cycle's suspend
                # entirely. That is where 325 of the 850 counted cycles on the
                # 7 Sep run came from.
                if cache.fresh(actor) == "ACTOR_STATE_SUSPENDED":
                    break
                if cache.get(actor) == "ACTOR_STATE_RESUMING":
                    # Suspending mid-resume is a race against the control plane.
                    # Let it land first; a cycle late is better than a cycle lost.
                    cache.wait_for(actor, "ACTOR_STATE_RUNNING", 120)
                p = ate("suspend", "actor", actor, "-a", ATESPACE)
                if p.returncode != 0:
                    emit({"cycle": n, "phase": "suspend-failed", "attempt": attempt,
                          "rc": p.returncode, "stderr": p.stderr.strip()[:300],
                          "counted": False, "exclude_reason": "forced suspend failed"})
                _, waited = cache.wait_for(actor, "ACTOR_STATE_SUSPENDED", budget)
                pre_wait += waited
                if cache.get(actor) == "ACTOR_STATE_SUSPENDED":
                    break
        else:
            _, pre_wait = cache.wait_for(actor, "ACTOR_STATE_SUSPENDED", budget)
        # Fresh, not cached. Everything above is scheduling and a stale sample
        # only costs a few seconds there; this read is the cold/warm verdict and
        # a stale sample corrupts the result. See StateCache.fresh.
        before = cache.fresh(actor)

        if before not in ("ACTOR_STATE_SUSPENDED", "ACTOR_STATE_RUNNING"):
            # RESUMING, DELETING, CRASHED - the watchdog's problem. Park, do not
            # race it, and record the park as its own event so the cost of risk
            # 7 to the cycle count is visible rather than showing up as an agent
            # that mysteriously fell behind.
            emit({"cycle": n, "phase": "parked", "state": before,
                  "waited_s": pre_wait, "counted": False,
                  "exclude_reason": f"not settled ({before}) - left to the watchdog"})
            counters[agent_id]["parked"] += 1
            n -= 1
            # budget is None on always-on, which has no suspend budget to
            # divide; the park backoff is a fixed minute there.
            stop.wait(min(120.0, budget / 4) if budget else 60.0)
            continue

        wake = "cold" if before == "ACTOR_STATE_SUSPENDED" else "warm"

        # 2. Wake it with a message. The prompt rotates so a cached-completion
        #    path, if one ever appears, cannot make the fleet look fast.
        if n % a.canary_every == 0:
            text, kind = CANARY_ASK, "canary-ask"
        elif n % a.t6_every == 0:
            text, kind = T6_CLOCK_ASK, "t6-clock"
        else:
            text = CYCLE_PROMPTS[n % len(CYCLE_PROMPTS)].format(n=n, noun=noun)
            kind = "cycle"
        r = say(text, kind)
        reply = r.get("reply") or ""

        # 3. Grade only what this turn can evidence.
        rec = {"cycle": n, "phase": "turn", "kind": kind, "wake": wake,
               "state_before": before, "suspend_wait_s": pre_wait,
               "first_response_s": r["elapsed_s"], "status": r["status"],
               "ok": r["ok"], "provider_error": r["provider_error"],
               "reply_head": reply[:240]}

        if r["provider_error"]:
            # Third outcome. Not a failed cycle - the actor woke, the platform
            # did its half, and the model call is what did not land. Excluded
            # from the latency distribution because a 429 after three retries is
            # about forty-five seconds of retry, not of restore.
            rec.update(counted=False, exclude_reason="provider error")
            counters[agent_id]["provider_errors"] += 1
        elif not r["ok"]:
            rec.update(counted=False, exclude_reason=f"turn failed: {r['status']}")
            counters[agent_id]["failed"] += 1
        elif wake != "cold":
            # T15 still wants it, T1 does not.
            rec.update(counted=False, exclude_reason="warm turn - no restore in it")
            counters[agent_id]["warm"] += 1
        else:
            rec.update(counted=True, exclude_reason=None)
            counters[agent_id]["counted"] += 1

        if kind == "canary-ask" and r["ok"] and not r["provider_error"]:
            # T1's state-corruption check. Substring, not model-graded, for the
            # same reason T2 graded that way: a grader that can be argued with
            # is not evidence. `report` withholds a miss that follows a
            # non-state-preserving watchdog recovery - the actor was deleted, so
            # the conversation was expected to be gone and its absence says
            # nothing about corruption.
            rec["canary"] = canary
            rec["canary_ok"] = canary.lower() in reply.lower()
            if not rec["canary_ok"]:
                counters[agent_id]["canary_miss"] += 1
                print(f"  {agent_id}: canary MISS at cycle {n} - {reply[:120]!r}")
        if kind == "t6-clock" and r["ok"] and not r["provider_error"]:
            # T6, the clock half. Recorded, deliberately not graded: the plan
            # says this half still needs a spike, and a substring match on
            # today's date would let the agent pass by guessing the only date it
            # has ever seen. The truth is written alongside so the spike, when
            # it happens, has something to check against.
            rec["t6_clock_probe"] = {
                "true_utc": now(), "slept_s": pre_wait,
                "date_string_present": utcnow().strftime("%Y-%m-%d") in reply
                                       or utcnow().strftime("%d %B") in reply,
                "graded": False,
                "why": "clock reconciliation needs the spike; this is evidence, not a verdict"}
        if wake == "cold" and r["ok"] and not r["provider_error"]:
            # T6, the credential half. A real answer means the model credential
            # survived the restore, because the actor could not have produced
            # one without it. That is the whole check, and it is worth stating
            # that it is free rather than pretending it was a separate test.
            #
            # Cold wakes only. A warm turn also proves the credential works, and
            # proves nothing about restore - counting those would inflate the
            # number of restores this test claims to have checked.
            rec["t6_credential_ok"] = True

        emit(rec)

        # 4. Let it go back to sleep on its own if that is this tier's mode; the
        #    idle path is the production one and forcing it everywhere would
        #    leave the run with no evidence that it works at all.
        if mode == "idle":
            settled, waited = cache.wait_for(actor, "ACTOR_STATE_SUSPENDED", budget)
            if settled != "ACTOR_STATE_SUSPENDED":
                emit({"cycle": n, "phase": "did-not-suspend", "state": settled,
                      "waited_s": waited, "budget_s": budget, "counted": False,
                      "exclude_reason": "idle suspend overran its budget"})
        stop.wait(a.pause)

    # 5. Give the worker back. An agent that has finished its cycles otherwise
    #    sits RUNNING and pins a worker until its own idle timeout expires,
    #    which for the long tier is an hour. With a pool sized to the fleet that
    #    starves whichever agents are still going, and the wait for a free
    #    worker is recorded as `first_response_s` - so it arrives in the results
    #    dressed as a slow restore. On the 7 Sep run six agents finished within
    #    three minutes of each other and the agents still cycling went from a
    #    16s worst case to 70s, with one turn refused outright as `no free
    #    workers available`. The p50 never moved, which is exactly what makes it
    #    easy to miss.
    #
    #    Not done for always-on, which is the one tier where being awake is the
    #    behaviour under test.
    if mode != "never":
        p = ate("suspend", "actor", actor, "-a", ATESPACE)
        emit({"cycle": n, "phase": "released", "counted": False,
              "rc": p.returncode,
              "exclude_reason": "worker released at end of run, not a cycle"})

    print(f"  {agent_id}: finished at {counters[agent_id]['counted']} counted cycles")


def cmd_run(a: argparse.Namespace) -> int:
    OUTDIR.mkdir(parents=True, exist_ok=True)
    cyc_path, st_path = path_for(a.run, "cycles"), path_for(a.run, "states")
    if cyc_path.exists() and not a.append:
        raise SystemExit(f"{cyc_path} exists - use --append to add to this run's "
                         f"cycle count, or pick another --run id. Overwriting it "
                         f"would silently discard cycles already earned.")

    print(f"T1 soak, run {a.run} - preflight")
    preflight(a)

    stop = threading.Event()
    cycles, states = Writer(cyc_path), Writer(st_path)
    cache = StateCache(a.sample, states, stop)
    cache.refresh()

    counters = {k: {"counted": 0, "warm": 0, "provider_errors": 0, "failed": 0,
                    "parked": 0, "canary_miss": 0, "always_on_suspended": 0}
                for k in AGENTS}

    def on_sigint(_s, _f):
        if not stop.is_set():
            print("\nstopping at the end of each agent's current cycle - "
                  "artefacts are already on disk")
            stop.set()

    signal.signal(signal.SIGINT, on_sigint)

    header = {"event": "soak_start", "at": now(), "run": a.run, "test": "T1",
              "atespace": ATESPACE, "gateway": a.gateway,
              "target_cycles": a.cycles, "concurrency": a.concurrency,
              "suspend_mode": a.suspend_mode,
              "provenance": FLEET_PROVENANCE,
              "counts_only": ("a cycle counts only if the agent was SUSPENDED "
                              "when our message arrived; warm turns feed T15 and "
                              "not T1"),
              "recovery": "owned by actor_watchdog.py; this driver never suspends "
                          "a wedged actor, never restarts the gateway, and never "
                          "calls debug-flush-redis"}
    cycles.write(header)
    print(f"  target {a.cycles} cycles/agent, {a.concurrency} turns in flight, "
          f"mode {a.suspend_mode}")

    sampler = threading.Thread(target=cache.run, daemon=True, name="sampler")
    sampler.start()
    gate = threading.Semaphore(a.concurrency)
    workers = [
        threading.Thread(target=cycle_agent, name=aid,
                         args=(a, aid, i, cache, cycles, gate, stop, counters))
        for i, aid in enumerate(sorted(AGENTS))
    ]
    t0 = time.monotonic()
    for w in workers:
        w.start()

    deadline = t0 + a.max_hours * 3600 if a.max_hours else None
    try:
        while any(w.is_alive() for w in workers):
            time.sleep(20)
            if deadline and time.monotonic() > deadline and not stop.is_set():
                print(f"\n--max-hours {a.max_hours} reached - stopping")
                stop.set()
    finally:
        stop.set()
        for w in workers:
            w.join(timeout=300)
        cycles.write({"event": "soak_end", "at": now(),
                      "elapsed_h": round((time.monotonic() - t0) / 3600.0, 2),
                      "counters": counters,
                      "state_read_failures": cache.read_failures})
        cycles.close()
        states.close()

    print(f"\nwrote {cyc_path}\n      {st_path}")
    print(f"now:  ./t1_soak.py report --run {a.run}")
    return 0


def collateral_windows() -> list[dict]:
    """Watchdog recoveries that took a worker away, and everything that was on
    it. Plan §5.0: a T1 cycle count must subtract them."""
    out = []
    if not WATCHDOG_EVENTS.exists():
        return out
    for line in WATCHDOG_EVENTS.read_text().splitlines():
        try:
            r = json.loads(line)
        except Exception:
            continue
        if r.get("event") != "recovery":
            continue
        # The recovery record is flat - clear()'s dict is splatted into the
        # event, not nested under a key.
        victims = set(r.get("collateral_actors") or [])
        # Also harvest per-rung. A LADDER_EXHAUSTED record has no top-level
        # collateral even when it reached rung 4 and took a worker down, so
        # reading only the summary loses exactly the worst case.
        for g in r.get("rungs") or []:
            victims.update(g.get("collateral_actors") or [])
        if r.get("actor"):
            victims.add(r["actor"])
        out.append({"at": r.get("time") or r.get("at"), "actors": sorted(victims),
                    "at_rung": r.get("at_rung"), "outcome": r.get("outcome"),
                    "state_preserved": bool(r.get("state_preserved"))})
    return out


SHIFT_WINDOW = 3   # turns either side of a miss to look for the canary


def _shifted_replies(mine: list[dict], asks: list[dict]) -> set[int]:
    """Canary misses where a nearby reply proves the agent still had the value.

    Returns the `id()`s of the ask records to withhold. Called by the T1 count,
    where the distinction that matters is "the state is gone" against "this one
    reply was not the answer to this one question".
    """
    order = {id(r): i for i, r in enumerate(mine)}
    out = set()
    for ask in asks:
        if ask["canary_ok"]:
            continue
        want = (ask.get("canary") or "").lower()
        i = order[id(ask)]
        near = mine[max(0, i - SHIFT_WINDOW):i + SHIFT_WINDOW + 1]
        if any(n is not ask and want and want in (n.get("reply_head") or "").lower()
               for n in near):
            ask["canary_shifted"] = True
            out.add(id(ask))
    return out


def cmd_report(a: argparse.Namespace) -> int:
    recs = []
    for line in path_for(a.run, "cycles").read_text().splitlines():
        try:
            recs.append(json.loads(line))
        except Exception:
            continue
    head = next((r for r in recs if r.get("event") == "soak_start"), {})
    tail = next((r for r in recs if r.get("event") == "soak_end"), {})
    turns = [r for r in recs if r.get("event") == "cycle" and r.get("phase") == "turn"]
    if not turns:
        raise SystemExit(f"no turns in {path_for(a.run, 'cycles')}")

    windows = collateral_windows()
    lost = {}
    for w in windows:
        for act in w["actors"]:
            lost.setdefault(act, []).append(w)

    # Subtract cycles that overlap a worker replacement. Coarse by design: the
    # watchdog records the moment it acted, and the actor was already broken for
    # up to `--wedge-after` before that, so the window opens early. Over-
    # subtracting a cycle is the safe direction - it understates T1.
    for r in turns:
        hits = [w for w in lost.get(r["actor"], [])
                if w["at"] and abs((datetime.fromisoformat(r["at"])
                                    - datetime.fromisoformat(w["at"])).total_seconds()) < 600]
        if hits and r.get("counted"):
            r["counted"] = False
            r["exclude_reason"] = f"watchdog took its worker ({hits[0]['at_rung']})"
        r["_wiped"] = any(h for h in hits if not h["state_preserved"])

    by_agent = {}
    for aid in sorted(AGENTS):
        mine = [r for r in turns if r["agent_id"] == aid]
        counted = [r for r in mine if r.get("counted")]
        cold = [r["first_response_s"] for r in mine
                if r.get("wake") == "cold" and r.get("ok") and not r.get("provider_error")]
        warm = [r["first_response_s"] for r in mine
                if r.get("wake") == "warm" and r.get("ok") and not r.get("provider_error")]
        asks = [r for r in mine if r.get("kind") == "canary-ask" and "canary_ok" in r]
        # A miss after the actor was deleted is not corruption; it is the
        # documented cost of the third and fourth recovery rungs.
        #
        # Nor is a miss where the agent demonstrably still holds the canary. The
        # substring check reads one reply, so it cannot tell a forgotten value
        # from a reply that arrived one turn late - and a turn does arrive late
        # after a provider error swallows the one before it. A later successful
        # recall distinguishes a delayed response from lost state.
        #
        # The test is direct evidence rather than a guess at intent: if a reply
        # near the miss contains the canary, the state was there. Only
        # `reply_head` is stored, so a canary buried deep in a long reply is not
        # seen and the miss stands, which is the safe direction to be wrong in.
        shifted = _shifted_replies(mine, asks)
        misses = [r for r in asks
                  if not r["canary_ok"] and not r.get("_wiped") and id(r) not in shifted]
        by_agent[aid] = {
            "actor": actor_name(aid), "tier": AGENTS[aid][1],
            "held_idle": aid in IDLE_THREE,
            "suspend_mode": mine[0].get("suspend_mode") if mine else None,
            "counted_cycles": len(counted),
            "turns": len(mine),
            "warm_turns": sum(1 for r in mine if r.get("wake") == "warm"),
            "provider_errors": sum(1 for r in mine if r.get("provider_error")),
            "failed_turns": sum(1 for r in mine if not r.get("ok")),
            "excluded_collateral": sum(
                1 for r in mine if (r.get("exclude_reason") or "").startswith("watchdog took")),
            "canary_asks": len(asks), "canary_misses": len(misses),
            "t15_cold_s": summarise(cold), "t15_warm_s": summarise(warm),
        }

    unattributed = 0
    st_path = path_for(a.run, "states")
    if st_path.exists():
        unattributed = sum(1 for line in st_path.read_text().splitlines()
                           if '"unattributed_wake"' in line)

    t6 = [r for r in turns if "t6_clock_probe" in r]
    summary = {
        "test": "T1 (with T3, T6, T15 riding on it)", "run": a.run,
        "started": head.get("at"), "ended": tail.get("at"),
        "elapsed_h": tail.get("elapsed_h"),
        "provenance": head.get("provenance"),
        "target_cycles": head.get("target_cycles"),
        "counted_cycles_total": sum(v["counted_cycles"] for v in by_agent.values()),
        # The always-on agent is the control and never suspends, so it cannot
        # produce a counted cycle by definition. Left in the minimum it reports
        # 0 for every run and reads as the fleet's worst agent missing the
        # target by 100, which is the first line of the T1 result.
        "min_counted_cycles_any_agent": min(
            [v["counted_cycles"] for v in by_agent.values()
             if v["suspend_mode"] != "never"] or [0]),
        "agents_excluded_from_minimum": [aid for aid, v in by_agent.items()
                                         if v["suspend_mode"] == "never"],
        "state_corruption_events": sum(v["canary_misses"] for v in by_agent.values()),
        "canary_misses_shifted_reply": sum(1 for r in turns if r.get("canary_shifted")),
        "provider_errors": sum(v["provider_errors"] for v in by_agent.values()),
        "failed_turns": sum(v["failed_turns"] for v in by_agent.values()),
        "watchdog_recoveries": len(windows),
        "cycles_subtracted_as_collateral": sum(v["excluded_collateral"] for v in by_agent.values()),
        "t3_unattributed_wakes": unattributed,
        "t6_credential_checks": sum(1 for r in turns
                                    if r.get("t6_credential_ok") and r.get("wake") == "cold"),
        "t6_clock_probes": len(t6),
        "t15_cold_all_s": summarise([r["first_response_s"] for r in turns
                                     if r.get("wake") == "cold" and r.get("ok")
                                     and not r.get("provider_error")]),
        "t15_warm_all_s": summarise([r["first_response_s"] for r in turns
                                     if r.get("wake") == "warm" and r.get("ok")
                                     and not r.get("provider_error")]),
        "agents": by_agent, "watchdog_windows": windows,
    }
    out_json = path_for(a.run, "summary", "json")
    out_json.write_text(json.dumps(summary, indent=2))

    L = [f"# T1 soak - run {a.run}", "",
         f"Started {summary['started']}, ended {summary['ended']} "
         f"({summary['elapsed_h']}h). Ten stand-in agents - **{head.get('provenance')}**.", "",
         "A cycle counts only where the agent was SUSPENDED when the message arrived. "
         "Warm turns are recorded and feed T15; they evidence no restore and are not counted.", "",
         "| Agent | Tier | Mode | Counted | Warm | Provider err | Failed | "
         "Collateral | Canary miss/ask | Cold p50/p90 s |", "|---|---|---|---|---|---|---|---|---|---|"]
    for aid, v in by_agent.items():
        c, w = v["t15_cold_s"], v["t15_warm_s"]
        L.append(f"| {aid} | {v['tier']} | {v['suspend_mode']} | **{v['counted_cycles']}** | "
                 f"{v['warm_turns']} | {v['provider_errors']} | {v['failed_turns']} | "
                 f"{v['excluded_collateral']} | {v['canary_misses']}/{v['canary_asks']} | "
                 f"{c['p50']}/{c['p90']} |")
    skipped = summary["agents_excluded_from_minimum"]
    shifted = summary["canary_misses_shifted_reply"]
    L += ["",
          f"**T1** - {summary['counted_cycles_total']} counted cycles, "
          f"lowest agent {summary['min_counted_cycles_any_agent']} against a target of "
          f"{summary['target_cycles']}"
          + (f" (excluding {', '.join(skipped)}, which never suspends and so "
             f"cannot produce a counted cycle)" if skipped else "")
          + ". State-corruption events: "
          f"**{summary['state_corruption_events']}**. Watchdog recoveries: "
          f"{summary['watchdog_recoveries']}, subtracting "
          f"{summary['cycles_subtracted_as_collateral']} cycles as collateral."
          + (f" {shifted} further canary miss held back: a reply within "
             f"{SHIFT_WINDOW} turns carried the canary, so the state was there "
             f"and the reply was late, not wrong." if shifted else ""),
          "",
          f"**T15** - cold wake p50 {summary['t15_cold_all_s']['p50']}s / "
          f"p90 {summary['t15_cold_all_s']['p90']}s over "
          f"{summary['t15_cold_all_s']['n']} samples; warm p50 "
          f"{summary['t15_warm_all_s']['p50']}s over {summary['t15_warm_all_s']['n']}. "
          f"Gateway wall clock, one clock, never summed with control-plane resume "
          f"timings. Provider errors ({summary['provider_errors']}) are excluded from both.",
          "",
          f"**T3** - {summary['t3_unattributed_wakes']} wakes observed that this driver did "
          f"not cause. Inferred from state transitions; the gateway's own cron log is the "
          f"authority and this is a second observation of the same events.",
          "",
          f"**T6** - credential half: {summary['t6_credential_checks']} restores followed by a "
          f"real model answer, which the actor could not produce without its credential. "
          f"Clock half: {summary['t6_clock_probes']} probes recorded and **not graded** - that "
          f"half still needs the spike. Scheduled-work half is the T3 line above.", ""]
    out_md = path_for(a.run, "report", "md")
    out_md.write_text("\n".join(L))
    print("\n".join(L))
    print(f"\nwrote {out_json}\n      {out_md}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="phase", required=True)

    r = sub.add_parser("run")
    r.add_argument("--run", required=True, help="run id, e.g. 260828")
    r.add_argument("--gateway", default="http://127.0.0.1:18080")
    r.add_argument("--cycles", type=int, default=100)
    r.add_argument("--concurrency", type=int, default=4,
                   help="turns in flight across the whole fleet. Ten is a burst "
                        "against a per-minute provider limit; that is T9's job")
    r.add_argument("--suspend-mode", choices=("auto", "idle", "forced"), default="auto",
                   help="auto: idle timer for the short tier, control-plane suspend "
                        "for medium and long, whose 15- and 60-minute timers do not "
                        "fit the pass. Recorded per cycle either way")
    r.add_argument("--stagger", type=float, default=15.0)
    r.add_argument("--pause", type=float, default=5.0, help="between cycles, per agent")
    r.add_argument("--sample", type=float, default=15.0, help="control-plane poll interval")
    r.add_argument("--timeout", type=float, default=300.0)
    r.add_argument("--canary-every", type=int, default=5)
    r.add_argument("--t6-every", type=int, default=11,
                   help="coprime with --canary-every so the two probes do not collide")
    r.add_argument("--max-hours", type=float, default=0.0, help="0 = until the target")
    r.add_argument("--append", action="store_true")
    r.add_argument("--no-watchdog", action="store_true",
                   help="soak without recovery. An unrecovered wedge then stops "
                        "that agent for the rest of the run")
    r.set_defaults(fn=cmd_run)

    p = sub.add_parser("report")
    p.add_argument("--run", required=True)
    p.set_defaults(fn=cmd_report)

    a = ap.parse_args()
    OUTDIR.mkdir(parents=True, exist_ok=True)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
