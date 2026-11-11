# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Self-test for the gateway's pure logic. No cluster, no network, no aiohttp.

    python3 tests/selftest.py

Covers the parts where being wrong is expensive and the failure is invisible in a
demo: cron semantics, the agent registry's config validation, per-agent idle policy,
presence during a slow wake, and the scheduler's catch-up behaviour after an outage.

Deliberately dependency-free and deliberately not a pytest suite - it runs anywhere
Python 3.11+ runs, including inside the gateway image, so "is this build sane?" is
one command with nothing to install.

`aiohttp` is stubbed rather than imported: everything under test here is
transport-agnostic, and requiring the dependency would defeat the point.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import types
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "gateway"))

# --- aiohttp stub, so ingress imports without the dependency ----------------
_aiohttp = types.ModuleType("aiohttp")
_web = types.ModuleType("aiohttp.web")
for _n in ("Request", "Application", "RouteTableDef"):
    setattr(_web, _n, type(_n, (), {}))
# Response keeps what it was given, so the proxy can be exercised offline and a
# test can assert on what would have gone over the wire.
_web.Response = type("Response", (), {
    "__init__": lambda self, **k: self.__dict__.update(k)})
_web.json_response = lambda *a, **k: None
_web.post = _web.get = lambda *a, **k: None
_web.run_app = lambda *a, **k: None
_aiohttp.web = _web
_aiohttp.ClientSession = type("ClientSession", (), {})
_aiohttp.ClientTimeout = type("ClientTimeout", (), {"__init__": lambda self, **k: None})
sys.modules.setdefault("aiohttp", _aiohttp)
sys.modules.setdefault("aiohttp.web", _web)

import agents  # noqa: E402
import ingress  # noqa: E402
import presence  # noqa: E402
import scheduler  # noqa: E402
from actor_router import actor_name_for_agent, actor_name_for_conversation  # noqa: E402
from cron import CronError, CronSpec  # noqa: E402
from idle_suspender import IdleSuspender  # noqa: E402

PASSED = FAILED = 0


def check(label: str, cond: bool, extra: str = "") -> None:
    global PASSED, FAILED
    if cond:
        PASSED += 1
        print(f"  ok    {label}" + (f"  ({extra})" if extra else ""))
    else:
        FAILED += 1
        print(f"  FAIL  {label}  {extra}")


def section(name: str) -> None:
    print(f"\n== {name} ==")


UTC = timezone.utc


# ---------------------------------------------------------------------------
# cron
# ---------------------------------------------------------------------------

def test_cron() -> None:
    section("cron parser")
    base = datetime(2026, 7, 31, 10, 30, tzinfo=UTC)  # a Friday

    check("every minute",
          CronSpec.parse("* * * * *").next_after(base) == datetime(2026, 7, 31, 10, 31, tzinfo=UTC))

    # Friday 10:30 -> the next weekday 08:00 is Monday, not Saturday.
    nxt = CronSpec.parse("0 8 * * 1-5").next_after(base)
    check("weekday range rolls Fri -> Mon",
          nxt == datetime(2026, 8, 3, 8, 0, tzinfo=UTC), nxt.strftime("%a %d %H:%M"))

    step = CronSpec.parse("*/15 * * * *")
    got = [step.next_after(datetime(2026, 7, 31, h, m, tzinfo=UTC)).strftime("%H:%M")
           for h, m in ((10, 5), (10, 14), (10, 20), (10, 50))]
    check("*/15 steps", got == ["10:15", "10:15", "10:30", "11:00"], str(got))

    # The rule people get wrong: both day fields restricted means OR, not AND.
    either = CronSpec.parse("0 0 1 * 1")
    fires = either.between(datetime(2026, 6, 30, tzinfo=UTC), datetime(2026, 7, 14, tzinfo=UTC))
    labels = [f.strftime("%m-%d %a") for f in fires[:3]]
    check("dom+dow is OR, not AND",
          labels == ["07-01 Wed", "07-06 Mon", "07-13 Mon"], str(labels))

    check("dom alone stays AND-free",
          CronSpec.parse("0 0 15 * *").next_after(base).day == 15)
    check("weekday 7 == 0 (Sunday)",
          CronSpec.parse("0 0 * * 0").weekday == CronSpec.parse("0 0 * * 7").weekday)
    check("weekday range wraps (5-0 = Fri..Sun)",
          CronSpec.parse("0 0 * * 5-0").weekday == frozenset({5, 6, 0}))

    la = CronSpec.parse("0 8 * * *", tz="America/Los_Angeles").next_after(base)
    check("local time converts to UTC", la.hour == 15 and la.tzinfo == UTC, la.isoformat())

    missed = CronSpec.parse("0 * * * *").between(
        datetime(2026, 7, 31, 4, 30, tzinfo=UTC), base)
    check("between() finds what we slept through", len(missed) == 6, f"{len(missed)} hours")

    try:
        CronSpec.parse("0 0 30 2 *").next_after(base)   # February 30th
        check("impossible date terminates", False)
    except CronError:
        check("impossible date terminates", True)

    for bad in ("* * * *", "60 * * * *", "x * * * *", "* * * * 9", "*/0 * * * *", "5-1 * * * *"):
        try:
            CronSpec.parse(bad)
            check(f"rejects {bad!r}", False)
        except CronError:
            check(f"rejects {bad!r}", True)


# ---------------------------------------------------------------------------
# registry
# ---------------------------------------------------------------------------

CONFIG = {
    "defaults": {"idle_tier": "short", "timezone": "UTC"},
    "agents": [
        {
            "id": "research-agent",
            "idle_tier": "medium",
            "timezone": "America/Los_Angeles",
            "channels": ["telegram:100200300", "whatsapp:111:4477000"],
            "jobs": [{"name": "morning-brief", "cron": "45 8 * * 1-5",
                      "prompt": "Brief me.", "deliver_to": "telegram:100200300"}],
        },
        {
            "id": "oncall-agent",
            "idle_tier": "always-on",
            "channels": ["telegram:999"],
            "jobs": [{"name": "heartbeat", "cron": "*/5 * * * *",
                      "prompt": "ping", "deliver_to": "telegram:999"}],
        },
        {"id": "batch-agent"},
    ],
}


def test_registry() -> None:
    section("agent registry")
    reg = agents.AgentRegistry.from_dict(CONFIG)
    maya = reg.for_channel("telegram:100200300")

    check("channel resolves to its agent", maya.agent_id == "research-agent")
    check("two channels, one agent",
          reg.for_channel("whatsapp:111:4477000").actor == maya.actor, maya.actor)
    check("actor is keyed by agent",
          maya.actor == actor_name_for_agent("research-agent"))
    check("agent keying differs from the old conversation keying",
          maya.actor != actor_name_for_conversation("telegram:100200300"))

    check("named tier resolves", maya.idle_timeout == 900.0)
    check("always-on means never suspend",
          reg.by_id("oncall-agent").idle_timeout is None)
    check("no tier inherits the default", reg.by_id("batch-agent").idle_timeout == 60.0)
    check("suspender can look policy up by actor name",
          reg.idle_timeout_for_actor(maya.actor) == 900.0)
    check("unknown actor gets the default, never None",
          reg.idle_timeout_for_actor("agent-nonexistent") == 60.0)
    check("jobs are collected fleet-wide", len(reg.all_jobs()) == 2)

    # An agent id is a routing key too. Addressing an agent by the name it is
    # registered under has to reach the registered agent, tier and jobs and all,
    # not mint a look-alike ad-hoc one on the default tier.
    by_own_id = reg.for_channel("oncall-agent")
    check("an agent id resolves to its own agent", by_own_id.registered)
    check("...on its declared tier, not the default", by_own_id.idle_timeout is None)
    check("...to the same actor its channel resolves to",
          by_own_id.actor == reg.for_channel("telegram:999").actor)

    adhoc = reg.for_channel("telegram:12345")
    check("unregistered identity still works", not adhoc.registered)
    check("...and is stable across calls", reg.for_channel("telegram:12345").actor == adhoc.actor)

    bad_configs = [
        ({"agents": [{"id": "a", "idle_tier": "sometimes"}]}, "unknown tier"),
        ({"agents": [{"id": "a"}, {"id": "a"}]}, "duplicate agent id"),
        ({"agents": [{"id": "a", "channels": ["c"]},
                     {"id": "b", "channels": ["c"]}]}, "channel bound twice"),
        ({"agents": [{"id": "a", "jobs": [{"name": "j", "cron": "* * * *", "prompt": "p",
                                           "deliver_to": "telegram:1"}]}]}, "malformed cron"),
        ({"agents": [{"id": "a", "jobs": [{"name": "j", "cron": "* * * * *",
                                           "prompt": "p"}]}]}, "job with nowhere to deliver"),
    ]
    for doc, why in bad_configs:
        try:
            agents.AgentRegistry.from_dict(doc)
            check(f"rejects {why}", False)
        except agents.AgentConfigError:
            check(f"rejects {why}", True)

    # Manifests written before tiers existed must keep working.
    saved = {k: os.environ.get(k) for k in
             ("IDLE_TIMEOUT_SECONDS", "IDLE_TIER_DEFAULT", "AGENTS_CONFIG")}
    try:
        os.environ["IDLE_TIMEOUT_SECONDS"] = "30"
        os.environ.pop("IDLE_TIER_DEFAULT", None)
        os.environ.pop("AGENTS_CONFIG", None)
        check("legacy IDLE_TIMEOUT_SECONDS is honoured",
              agents.AgentRegistry.from_env().default_timeout == 30.0)
        os.environ["IDLE_TIER_DEFAULT"] = "long"
        check("an explicit tier wins over the legacy variable",
              agents.AgentRegistry.from_env().default_timeout == 3600.0)
    finally:
        for k, v in saved.items():
            os.environ.pop(k, None)
            if v is not None:
                os.environ[k] = v


# ---------------------------------------------------------------------------
# idle policy
# ---------------------------------------------------------------------------

class _FakeAte:
    def __init__(self):
        self.suspended: list[str] = []

    async def suspend_actor(self, _space: str, name: str) -> None:
        self.suspended.append(name)


async def test_wake_timeout_never_duplicates() -> None:
    """A 504 must not re-send a turn the actor is already running.

    Observed live: a turn that legitimately ran long returned 504 from an
    upstream hop, and the retry ladder re-sent the SAME message five more times.
    Each copy is a full run of the work. The 503 path already reasons this way
    ("the retry was the cause, not the cure"); the 504 path did not.
    """
    import ingress

    section("a 504 does not run the turn twice")

    class _Resp:
        def __init__(self, status, body):
            self.status, self._b = status, body
            self.headers = {"Content-Type": "application/json"}
        async def read(self): return self._b
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    class _Client:
        def __init__(self): self.posts = 0
        def post(self, *a, **kw):
            self.posts += 1
            return _Resp(504, b"upstream request timeout")

    class _Ate:
        """Stands in for the control plane, one actor, given its raw `status`.

        It answers through the same normaliser the real client uses, so the
        `status` handed in here is a verbatim control-plane document field -
        either spelling. That is the point: this guard read the field directly
        once, the control plane renamed the states underneath it, and a RUNNING
        actor started being re-sent its turn five more times.
        """
        def __init__(self, status): self.status = status
        async def actor_status(self, _space, _name):
            from ateapi_client import actor_state
            return actor_state({"status": self.status}) or None
        async def wait_running(self, *a, **kw): return False

    # The actor is RUNNING: it has almost certainly taken the turn. Asserted for
    # both document shapes - the acceptance cluster speaks the nested one, the
    # older PoC install the flat one, and this must hold on both.
    for label, status in (("", "STATUS_RUNNING"),
                          (" (nested document shape)",
                           {"state": "ACTOR_STATE_RUNNING",
                            "workerAssignment": {"workerPodUid": "uid-1"}})):
        c = _Client()
        await ingress._post_turn(c, "http://x", "{}", {}, "agent-x",
                                 ate=_Ate(status))
        check(f"a RUNNING actor is asked exactly once, never re-sent{label}",
              c.posts == 1, f"{c.posts} posts")

    # Still resuming: it cannot have taken the turn, so retrying is safe and is
    # the cold-start case the retry exists for.
    c = _Client()
    await ingress._post_turn(c, "http://x", "{}", {}, "agent-x",
                             ate=_Ate({"state": "ACTOR_STATE_RESUMING"}))
    check("a resuming actor is retried", c.posts > 1, f"{c.posts} posts")
    check("retries are bounded", c.posts <= ingress.WAKE_TIMEOUT_RETRIES + 1,
          f"{c.posts} posts")


async def test_working_signal() -> None:
    """A long task must not be suspended just because we stopped waiting.

    The suspender's only evidence of work is an open HTTP request, and that times
    out after five minutes. Before this, a forty-minute task was abandoned at
    minute five and suspended a minute later, mid-work.
    """
    from idle_suspender import IdleSuspender, MAX_WORKING_SECONDS

    section("long tasks survive our impatience")

    calls = []

    class _Ate:
        async def suspend_actor(self, space, name):
            calls.append(name)

    s_ = IdleSuspender("hermes", _Ate(), idle_timeout_seconds=0.05, poll_seconds=0.02)
    s_.begin("agent-x"); s_.end("agent-x")          # a turn that returned
    s_.still_working("agent-y")                      # one we gave up waiting on
    s_.start()
    await asyncio.sleep(0.25)
    s_.stop()

    check("a finished actor is suspended", "agent-x" in calls)
    check("one still working is NOT suspended", "agent-y" not in calls)

    was = s_.signal_done("agent-y")
    check("the completion signal is recognised", was is True)
    check("signalling twice is harmless", s_.signal_done("agent-y") is False)

    calls.clear()
    s_.start(); await asyncio.sleep(0.25); s_.stop()
    check("and then it becomes suspendable", "agent-y" in calls)

    # A lost signal must not pin a worker forever.
    check("there is a ceiling on claimed work", MAX_WORKING_SECONDS <= 3600 * 4,
          f"{MAX_WORKING_SECONDS/60:.0f} min")


async def test_idle() -> None:
    section("per-agent idle policy")
    client = _FakeAte()
    tiers = {"agent-fast": 0.2, "agent-slow": 60.0, "agent-always": None}
    sus = IdleSuspender("ns", client, 0.2, poll_seconds=0.05,
                        timeout_for=lambda a: tiers[a] if a in tiers else 0.2)
    for actor in tiers:
        sus.touch(actor)
    sus.begin("agent-busy")
    sus.start()
    await asyncio.sleep(0.6)
    check("short tier is reclaimed", "agent-fast" in client.suspended)
    check("long tier is left warm", "agent-slow" not in client.suspended)
    check("always-on is never suspended", "agent-always" not in client.suspended)
    check("never suspends mid-turn", "agent-busy" not in client.suspended)
    sus.end("agent-busy")
    await asyncio.sleep(0.5)
    check("suspends once the turn ends", "agent-busy" in client.suspended)
    sus.stop()


# ---------------------------------------------------------------------------
# presence
# ---------------------------------------------------------------------------

async def test_presence() -> None:
    section("presence during a slow wake")
    ticks: list[int] = []
    notices: list[str] = []

    async def tick():
        ticks.append(1)

    async def notify(text):
        notices.append(text)

    slow = presence.PresenceTicker(tick=tick, notify=notify, interval=0.1,
                                   slow_after=0.25, label="test")
    async with slow:
        await asyncio.sleep(0.65)
    check("indicator is re-asserted through the wait", len(ticks) >= 5, f"{len(ticks)} ticks")
    check("slow-wake notice is sent exactly once", len(notices) == 1)
    check("turn is flagged slow", slow.slow)

    ticks.clear(); notices.clear()
    fast = presence.PresenceTicker(tick=tick, notify=notify, interval=0.1, slow_after=5.0)
    async with fast:
        await asyncio.sleep(0.05)
    check("a warm turn ticks once and says nothing", len(ticks) == 1 and not notices)

    async def broken():
        raise RuntimeError("channel down")

    async with presence.PresenceTicker(tick=broken, interval=0.05, slow_after=None):
        await asyncio.sleep(0.3)
    check("a dead channel never breaks the turn", True)

    class Sentinel(Exception):
        pass

    try:
        async with presence.PresenceTicker(tick=tick, interval=0.05):
            raise Sentinel()
    except Sentinel:
        check("the turn's own exception still propagates", True)
    else:
        check("the turn's own exception still propagates", False)


# ---------------------------------------------------------------------------
# scheduler
# ---------------------------------------------------------------------------

async def test_scheduler() -> None:
    section("cron scheduler")
    state = os.path.join(tempfile.mkdtemp(prefix="hermes-cron-"), "state.json")
    heartbeat = [j for j in agents.AgentRegistry.from_dict(CONFIG).all_jobs()
                 if j.name == "heartbeat"][0]
    runs: list[tuple] = []
    sent: list[tuple] = []

    async def run_turn(job, prompt, session):
        runs.append((job.key, prompt, session))
        return f"result of {job.name}"

    async def deliver(address, text):
        sent.append((address, text))

    sched = scheduler.CronScheduler([heartbeat], run_turn, deliver, state_path=state,
                                    tick_seconds=0.05, catchup_seconds=3600)
    sched.start()
    await asyncio.sleep(0.1)
    check("a fresh deployment does not replay history", not runs)

    sched._cursor[heartbeat.key] = datetime.now(UTC) - timedelta(minutes=32)
    sched._tick()
    await asyncio.sleep(0.2)
    check("six missed fires coalesce into one run", len(runs) == 1, f"{len(runs)} runs")
    check("the run knows it is late", "delivered late" in runs[0][1])
    check("result goes to the job's address", sent and sent[0][0] == "telegram:999")
    check("the run shares the delivery thread's session", runs[0][2] == "telegram:999")

    runs.clear(); sent.clear()
    # A long outage of a frequent job produces more occurrences than between() will
    # enumerate. The recent, still-relevant one must not be crowded out by ancient ones.
    sched._cursor[heartbeat.key] = datetime.now(UTC) - timedelta(days=2)
    sched._tick()
    await asyncio.sleep(0.2)
    check("a long outage still fires the recent occurrence", len(runs) == 1, f"{len(runs)} runs")
    sched.stop()

    runs.clear()
    nightly = agents.AgentRegistry.from_dict({"agents": [{"id": "a", "jobs": [
        {"name": "nightly", "cron": "0 3 * * *", "prompt": "p",
         "deliver_to": "telegram:999"}]}]}).all_jobs()[0]
    stale = scheduler.CronScheduler([nightly], run_turn, deliver, state_path=state,
                                    tick_seconds=0.05, catchup_seconds=600)
    stale._cursor[nightly.key] = datetime.now(UTC) - timedelta(days=2)
    stale._tick()
    await asyncio.sleep(0.2)
    check("fires older than the catch-up window are dropped, not run", not runs)

    check("cursor is persisted", os.path.exists(state))
    check("cursor is keyed per job", heartbeat.key in json.load(open(state))["cursor"])
    resumed = scheduler.CronScheduler([heartbeat], run_turn, deliver, state_path=state)
    resumed._load()
    check("cursor survives a restart", heartbeat.key in resumed._cursor)

    sent.clear()

    async def never_wakes(job, prompt, session):
        raise RuntimeError("actor never woke")

    failing = scheduler.CronScheduler([heartbeat], never_wakes, deliver, state_path=state,
                                      tick_seconds=0.05, catchup_seconds=3600)
    failing._cursor[heartbeat.key] = datetime.now(UTC) - timedelta(minutes=6)
    failing._tick()
    await asyncio.sleep(0.2)
    check("a failed scheduled job tells the human",
          bool(sent) and "failed to run" in sent[0][1])


# ---------------------------------------------------------------------------
# failure feedback
# ---------------------------------------------------------------------------

def test_systemic_failure_message() -> None:
    """After a run of failures, stop calling it transient.

    The certificate outage lasted 21 hours and every single failed turn told the
    user "this is usually transient, please send that again". The mechanism
    worked perfectly and the advice was wrong for a day.
    """
    import time as _t
    from ingress import (SYSTEMIC_AFTER, FailureStreak, _how_long,
                         _reply_or_error, _systemic_message)

    section("failure message stops lying during an outage")

    st = FailureStreak()
    who = "http:test-session"
    body = b"upstream connect error or disconnect/reset before headers"

    first = _reply_or_error(503, body, who, st)
    check("a first failure is still treated as a blip", "transient" in first)

    for _ in range(SYSTEMIC_AFTER - 2):
        _reply_or_error(503, body, who, st)
    nth = _reply_or_error(503, body, who, st)
    check("a run of failures is called what it is", "not a temporary" in nth)
    check("and tells them NOT to resend", "won't help" in nth or "not help" in nth)
    check("and asks for an operator", "operator" in nth)
    check("and says how long it has been failing", "minute" in nth or "hour" in nth)

    # A success must clear it, or one bad patch poisons every later message.
    ok_body = b'{"choices":[{"message":{"content":"hi"}}]}'
    check("a success returns the reply", _reply_or_error(200, ok_body, who, st) == "hi")
    again = _reply_or_error(503, body, who, st)
    check("and resets the streak", "transient" in again)

    check("durations read naturally", _how_long(_t.time() - 7200).startswith("2 hour"))
    check("the systemic text names the count", "17" in _systemic_message(17, _t.time()))

    # No tracker (cron, tests) must not crash.
    check("works without a tracker", "transient" in _reply_or_error(503, body, who))


def test_failure_feedback() -> None:
    section("failed-wake feedback")
    cases = [
        (503, b"no free workers available", "busy"),
        (503, b"upstream connect error ... before headers", "waking up"),
        (401, b"unauthorized", "authenticate"),
        (404, b"not found", "doesn't exist"),
        (504, b"gateway timeout", "wake up in time"),
        (500, b'error resuming actor "agent-abc"', "restored from its saved state"),
        (500, b"boom", "internal error"),
        (418, b"teapot", "HTTP 418"),
    ]
    for status, raw, needle in cases:
        check(f"HTTP {status} is explained, not generic",
              needle in ingress._failure_message(status, raw))
    check("no two conditions produce the same sentence",
          len({ingress._failure_message(s, r) for s, r, _ in cases}) == len(cases))

    # Retry classification: a failed restore is retryable (the actor never ran);
    # a 500 from Hermes itself is not.
    check("a failed restore is recognised as retryable",
          ingress._resume_failed(500, b'error resuming actor "agent-abc"'))
    check("an ordinary 500 is not treated as a failed restore",
          not ingress._resume_failed(500, b"internal server error"))
    check("worker starvation is not confused with a failed restore",
          not ingress._resume_failed(503, b"no free workers available"))

    good = json.dumps({"choices": [{"message": {"content": "hello"}}]}).encode()
    check("happy path returns the assistant's text",
          ingress._reply_or_error(200, good, "t") == "hello")
    check("a 200 with no content is not silently empty",
          "empty" in ingress._reply_or_error(200, b'{"choices":[]}', "t"))


async def test_delivery() -> None:
    section("channel address routing")
    for address in ("slack:C123", "telegram:1"):
        try:
            await ingress._deliver_to_address({}, address, "hi")
            check(f"{address} rejected when unroutable", False)
        except ValueError:
            check(f"{address} rejected when unroutable", True)



# ---------------------------------------------------------------------------
# Wake timeout classification.

def test_wake_timeout() -> None:
    from ingress import _wake_timeout

    section("cold-turn timeout classification")
    check("Envoy's upstream timeout is retryable",
          _wake_timeout(504, b"upstream request timeout"))
    # Seen live while the actor was STATUS_RESUMING. A different hop, the same
    # condition, a different body - matching only Envoy's wording turned the
    # retry off exactly when it was needed.
    check("atenet's resume timeout is retryable too",
          _wake_timeout(504, b"actor hermes/agent-ce9cbcdb4edb request timed out"))
    check("a 504 from the agent itself is not swallowed",
          not _wake_timeout(504, b'{"error":"model provider timed out"}'))
    check("a normal 500 is untouched", not _wake_timeout(500, b"boom"))

    from ingress import (RESUME_WAIT, STALE_UPSTREAM_RETRIES,
                         _actor_missing, _stale_upstream)

    # Envoy answers 503 identically for a dead pooled socket and for an actor
    # that is still being restored. The budget must cover the slower one: a
    # 3-try/3-second budget failed an ordinary wake from suspend and told the
    # user the connection was lost, while the actor came up fine seconds
    # later. Restore measures ~2s, plus scheduling.
    check("a 503 with no upstream is retryable",
          _stale_upstream(503, b"upstream connect error or disconnect/reset before headers"))
    # The fix is NOT a bigger retry budget. Every request to a suspended actor
    # makes atenet issue another ResumeActor, so retrying restarts the restore
    # being waited on - six ResumeActor calls in 26s, and a failed turn against a
    # healthy actor. So: few retries, and a long wait on the control plane.
    check("retries stay few, so a restore is not restarted",
          STALE_UPSTREAM_RETRIES <= 3, f"{STALE_UPSTREAM_RETRIES} retries")
    check("the wait for RUNNING is generous", RESUME_WAIT >= 60, f"{RESUME_WAIT:.0f}s")

    check("a deleted actor is recognised",
          _actor_missing(404, b'actor hermes/agent-ce9cbcdb4edb not found'))
    check("and is not confused with a missing message",
          not _actor_missing(404, b'{"error":"message not found"}'))
    check("a successful turn is untouched", not _wake_timeout(200, b"{}"))


def test_runtime_signals() -> None:
    """The token has to actually reach the agent, or every call is refused."""
    from agents import AgentRegistry
    import ingress

    # ---- turn-signal plugin -------------------------------------------
    # It ships in the actor image, not the gateway, but its correctness decides
    # whether an actor is suspended on top of running work - so it is tested
    # here, where the suite already runs, rather than nowhere.
    # The ActorTemplate must stay loadable by a Substrate that no longer accepts
    # env.valueFrom.secretKeyRef, and an env value must be a string: the field is
    # required with MinLength=0, so empty is fine but absent is not, and bare
    # "value:" in YAML is null rather than "".
    section("actor template survives the CRD without secretKeyRef")

    import re as _re
    _tpl_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                             "k8s", "hermes-actortemplate.yaml")
    _tpl = open(_tpl_path, encoding="utf-8").read()
    _body = "\n".join(l for l in _tpl.splitlines() if not l.strip().startswith("#"))
    check("no secretKeyRef outside comments", "secretKeyRef" not in _body)
    check("no valueFrom outside comments", "valueFrom" not in _body)

    # Render the way hermesctl does, with the empty default for the model key.
    _rendered = _tpl.replace("REPLACE_WITH_ACTOR_MODEL_KEY", "")
    _rendered = _rendered.replace("REPLACE_WITH_API_SERVER_KEY", "abc123")
    _rendered = _re.sub(r"REPLACE_WITH_[A-Z_]+", "x", _rendered)

    def _find_env(o):
        if isinstance(o, dict):
            if isinstance(o.get("env"), list):
                return o["env"]
            for v in o.values():
                r = _find_env(v)
                if r:
                    return r
        if isinstance(o, list):
            for v in o:
                r = _find_env(v)
                if r:
                    return r
        return None

    try:
        import yaml as _yaml
        _env = _find_env(_yaml.safe_load(_rendered)) or []
        check("the rendered template parses", bool(_env))
        check("every env value is a string, never null",
              all(isinstance(e.get("value"), str) for e in _env))
        _byname = {e["name"]: e.get("value") for e in _env}
        check("an unset model key renders as empty, not missing",
              _byname.get("MODEL_API_KEY") == "")
        check("the api server key still renders", _byname.get("API_SERVER_KEY") == "abc123")
    except ImportError:
        check("yaml unavailable, template parse skipped", True)

    section("turn-signal: what counts as finished")

    import importlib.util as _ilu
    _spec = _ilu.spec_from_file_location(
        "turn_signal",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                     "actor", "plugins", "turn-signal", "__init__.py"))
    _ts = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_ts)

    # A process name can contain spaces and parentheses - `sh (build step)` is
    # legal - and splitting the stat line from the left mis-parses exactly the
    # commands an agent is most likely to background.
    check("ppid parses from an ordinary stat line",
          _ts._ppid_from_stat(b"42 (bash) S 7 42 42 0 -1 4194304") == 7)
    # This case is chosen because it DISCRIMINATES: parsing from the first ')'
    # instead of the last returns None here, whereas `sh (build step)` returns 7
    # either way and so cannot catch the bug. A test that passes under the broken
    # implementation is worse than no test, because it certifies it.
    check("ppid parses when comm contains an embedded close-paren",
          _ts._ppid_from_stat(b"42 (sh (a) b) S 7 42 42 0 -1 4194304") == 7)
    check("ppid parses when comm ends in a stray close-paren",
          _ts._ppid_from_stat(b"42 (my) proc) S 7 42 42 0 -1 4194304") == 7)
    check("a malformed stat line yields None, not a wrong parent",
          _ts._ppid_from_stat(b"garbage") is None)

    tree = {20: 10, 30: 20, 99: 1, 10: 1}
    check("descendants follow the whole chain",
          _ts._descendants_of(tree, 10) == {20, 30})
    check("unrelated processes are not claimed",
          99 not in _ts._descendants_of(tree, 10))
    check("a cyclic parent chain terminates instead of hanging",
          _ts._descendants_of({1: 2, 2: 3, 3: 2}, 10) == set())

    _sent, _working = [], []
    _ts._notify = lambda: _sent.append(1)
    _ts._notify_working = lambda: _working.append(1) or True

    _ts._descendants = lambda _root: {100, 101}
    _ts._on_turn_start(turn_id="t1")
    _ts._on_turn_end(turn_id="t1")
    check("a turn that spawned nothing reports done at once", len(_sent) == 1)
    check("a turn that spawned nothing does not claim to be working",
          len(_working) == 0)

    _ts._descendants = lambda _root: {100, 101}
    _ts._on_turn_start(turn_id="t2")
    _ts._descendants = lambda _root: {100, 101, 202}
    _ts._on_turn_end(turn_id="t2")
    check("a turn that left work running does NOT report done", len(_sent) == 1)
    # Withholding "done" is useless unless the gateway is told to hold the actor,
    # because nothing else tells it.
    check("a turn that left work running DOES assert still-working",
          len(_working) == 1)

    _ts._descendants = lambda _root: None
    _ts._on_turn_start(turn_id="t3")
    _ts._on_turn_end(turn_id="t3")
    check("an unreadable /proc reports nothing rather than guessing done",
          len(_sent) == 1)

    # Concurrency: with two turns in flight, the first to finish must NOT clear
    # the latch - the actor is only done when the last turn is.
    _sent.clear(); _working.clear()
    _ts._active.clear(); _ts._watching.clear()
    _ts._descendants = lambda _root: {100}
    _ts._on_turn_start(turn_id="a")
    _ts._on_turn_start(turn_id="b")
    _ts._on_turn_end(turn_id="a")
    check("one turn finishing does not report done while another is in flight",
          len(_sent) == 0)
    _ts._on_turn_end(turn_id="b")
    check("the last turn to finish reports done", len(_sent) == 1)

    # A turn that spawned work must not be released by an unrelated turn
    # finishing cleanly.
    _sent.clear(); _working.clear()
    _ts._active.clear(); _ts._watching.clear()
    _ts._descendants = lambda _root: {100}
    _ts._on_turn_start(turn_id="slow")
    _ts._descendants = lambda _root: {100, 555}
    _ts._on_turn_end(turn_id="slow")          # leaves 555 running -> working
    check("spawned work asserts working", len(_working) == 1)
    _ts._descendants = lambda _root: {100, 555}
    _ts._on_turn_start(turn_id="quick")
    _ts._on_turn_end(turn_id="quick")         # spawned nothing of its own
    check("an unrelated quick turn does not release work it did not start",
          len(_sent) == 0)

    # Retries, and their asymmetry. Losing "working" means the actor is never
    # latched and gets suspended with work running; losing "done" only holds a
    # worker until the ceiling. So one retries and the other need not.
    _attempts = []

    class _Boom(Exception):
        pass

    def _always_fail(_req, timeout=None):
        _attempts.append(1)
        raise _Boom("gateway said no")

    _real_urlopen = _ts.urllib.request.urlopen
    _ts.urllib.request.urlopen = _always_fail
    _ts.RETRY_DELAY_SECONDS = 0.001
    try:
        _attempts.clear()
        _ts._post(_ts.WORKING_ENDPOINT, _ts.WORKING_ATTEMPTS)
        check("the working signal retries rather than giving up at once",
              len(_attempts) > 1, f"{len(_attempts)} attempts")
        check("retries are bounded", len(_attempts) == _ts.WORKING_ATTEMPTS)

        _attempts.clear()
        _ts._post(_ts.ENDPOINT, _ts.DONE_ATTEMPTS)
        check("the done signal does not retry, so a turn is not delayed",
              len(_attempts) == 1, f"{len(_attempts)} attempts")

        _attempts.clear()
        check("a failed post reports failure rather than raising",
              _ts._post(_ts.WORKING_ENDPOINT, 1) is False)
    finally:
        _ts.urllib.request.urlopen = _real_urlopen

    # Lifecycle routes must be installed unconditionally.
    import turn_signals as _wb
    check("turn signalling has its own entry point",
          hasattr(_wb, "register_turn_signals"))
    _sig = _wb.register_turn_signals.__code__.co_varnames[
        :_wb.register_turn_signals.__code__.co_argcount
        + _wb.register_turn_signals.__code__.co_kwonlyargcount]
    check("it needs no credential", "token_for" not in _sig)
    check("it needs no policy or minter",
          "policy" not in _sig and "minter" not in _sig)
    # And it must actually mount BOTH routes: the earlier version of this test
    # only proved the function existed, so deleting a route slipped through.
    import inspect as _inspect
    _body = _inspect.getsource(_wb.register_turn_signals)
    check("it mounts turn-ended", "add_post(TURN_ENDED" in _body)
    check("it mounts turn-working", "add_post(TURN_WORKING" in _body)

    _ing = open(ingress.__file__, encoding="utf-8").read()
    # Assert that route registration is not inside any conditional.
    import ast as _ast
    _tree = _ast.parse(_ing)
    _guarded = set()
    for _node in _ast.walk(_tree):
        if isinstance(_node, _ast.If):
            for _child in _ast.walk(_node):
                if isinstance(_child, _ast.Call):
                    _f = _child.func
                    if getattr(_f, "attr", None) == "register_turn_signals":
                        _guarded.add(_child.lineno)
    check("turn signalling is not nested inside ANY conditional",
          not _guarded, f"guarded at lines {sorted(_guarded)}")
    check("turn signalling is mounted during startup",
          "register_turn_signals(" in _ing)

    from turn_signals import TURN_WORKING, TURN_ENDED
    check("turn-working is a distinct endpoint from turn-ended",
          TURN_WORKING != TURN_ENDED)
    check("neither endpoint takes an actor name in the path",
          "{" not in TURN_WORKING and "{" not in TURN_ENDED)

    section("turn identity: a retry is not a second turn")

    # The failure this guards is not lost messages. Under an ungraceful restart
    # every message was delivered and processed and every response was lost, so
    # the channel retries and the work happens twice. What is missing is an
    # identity the gateway recognises on the second delivery.
    import tempfile as _tf
    from turn_dedup import TurnDedup, DONE, INFLIGHT, ORPHANED

    _dir = _tf.mkdtemp()
    _store = os.path.join(_dir, "turns.json")

    d1 = TurnDedup(_store)
    check("a new turn is claimable", d1.begin("telegram:1") is None)
    d1.finish("telegram:1", 200, b'{"choices":[]}', "application/json")
    again = d1.begin("telegram:1")
    check("a repeat of a finished turn is recognised", again is not None)
    check("and replays the answer instead of re-running it",
          TurnDedup.replay(again) == (200, b'{"choices":[]}', "application/json"),
          "the retry is what delivers the answer lost with the connection")

    # The claim must be on disk before the actor is asked to do anything, or a
    # crash in between leaves the hole it exists to close.
    d2 = TurnDedup(_store)
    d2.begin("telegram:2")
    del d2
    d3 = TurnDedup(_store)                      # the gateway restarts
    orphan = d3.begin("telegram:2")
    check("a turn in flight at a crash survives the restart", orphan is not None)
    check("and is never re-dispatched",
          orphan["state"] == ORPHANED,
          "whether the actor applied it is unknowable; the evidence says it did")
    check("an orphan has no answer to replay", TurnDedup.replay(orphan) is None)
    check("a finished turn still replays after a restart",
          TurnDedup.replay(d3.begin("telegram:1")) is not None)

    check("a concurrent duplicate in one process is refused",
          d3.begin("telegram:3") is None and d3.begin("telegram:3")["state"] == INFLIGHT)

    d3.begin("telegram:4"); d3.abandon("telegram:4")
    check("a turn that never reached the actor can be retried",
          d3.begin("telegram:4") is None,
          "dispatch failing before anything applied is not a turn that happened")

    d4 = TurnDedup(os.path.join(_dir, "cap.json"), capacity=3)
    for _i in range(6):
        d4.begin(f"k{_i}"); d4.finish(f"k{_i}", 200, b"x", "text/plain")
    check("the store stays bounded", len(d4._records) == 3,
          "user content does not accumulate on the volume without limit")

    _bad = os.path.join(_dir, "bad.json")
    with open(_bad, "w") as _fh:
        _fh.write("{ not json")
    check("a corrupt store does not stop the gateway serving",
          TurnDedup(_bad).begin("x") is None,
          "losing records costs duplicate work; refusing to start costs every turn")

    check("turn keys are per-channel and stable across that channel's retries",
          all(k in ingress_src for k in ('turn_key=f"whatsapp:{msg.wamid}"',
                                         'turn_key=f"telegram:{msg.update_id}"')),
          "wamid, update_id and message_id are what each channel repeats")



async def test_caller_resolution() -> None:
    """Verify callers behind an authenticated egress proxy."""
    section("authenticating a caller behind the egress proxy")

    from caller_auth import ActorResolver as CallerResolver, actor_from_spiffe

    ATESPACE = "hermes-accept"
    WORKER_IP, PROXY_IP, STRANGER_IP = "10.80.5.7", "10.80.0.20", "10.80.1.99"
    ACTOR = "agent-9538abe0ccee"
    OTHER = "agent-d748b2c05d4e"          # running, but not the caller
    SPIFFE = f"spiffe://substrate-actor.local/atespace/{ATESPACE}/actor/{ACTOR}"

    # The shape the control plane actually returns today, copied from
    # `kubectl-ate get actor -o json` against the acceptance cluster rather than
    # written from memory. The previous fake here used the older flat shape, so
    # it passed while the real lookup matched nothing at all.
    def _entry(name, uid, state="ACTOR_STATE_RUNNING"):
        return {"actorTemplateName": "hermes",
                "metadata": {"name": name, "atespace": ATESPACE, "version": "3"},
                "status": {"state": state,
                           "workerAssignment": {"workerPodUid": uid,
                                                "workerPod": "hermes-x",
                                                "workerPodIp": "10.80.3.14"}}}

    class _Ate:
        async def _run(self, args):
            return 0, json.dumps({"actors": [
                _entry(ACTOR, "uid-1"),
                _entry(OTHER, "uid-2"),
                _entry("agent-suspended", "uid-3", "ACTOR_STATE_SUSPENDED"),
            ]})

    def resolver(proxy=(PROXY_IP,)):
        async def _pods():
            return {WORKER_IP: "uid-1"}

        async def _proxies():
            return set(proxy)

        return CallerResolver(atespace=ATESPACE, namespace="hermes",
                              ate_client=_Ate(),
                              k8s_pods=_pods,
                               proxy_pods=_proxies)

    # Read the state out of both document shapes. Getting this wrong does not
    # raise: `a.get("status")` is truthy either way, so every "is it running?"
    # test simply answers no, everywhere at once.
    from ateapi_client import ACTOR_RUNNING, actor_state, actor_worker_pod_uid
    check("the current document shape reads as RUNNING",
          actor_state(_entry(ACTOR, "uid-1")) == ACTOR_RUNNING)
    check("and its worker is found where it now lives",
          actor_worker_pod_uid(_entry(ACTOR, "uid-1")) == "uid-1")
    check("the older flat shape still reads as RUNNING",
          actor_state({"status": "STATUS_RUNNING"}) == ACTOR_RUNNING)
    check("and its worker too",
          actor_worker_pod_uid({"ateomPodUid": "uid-9"}) == "uid-9")
    check("an unscheduled actor has no worker",
          actor_worker_pod_uid({"status": {"state": ACTOR_RUNNING}}) == "")
    check("a suspended actor is not RUNNING",
          actor_state(_entry(ACTOR, "u", "ACTOR_STATE_SUSPENDED")) != ACTOR_RUNNING)

    check("a spiffe id parses to its actor",
          actor_from_spiffe(SPIFFE, ATESPACE) == ACTOR)
    check("another install's atespace does not name our actors",
          actor_from_spiffe(
              f"spiffe://substrate-actor.local/atespace/other/actor/{ACTOR}",
              ATESPACE) is None)
    for junk in ("", "not a spiffe id", "spiffe://x/actor/a",
                 f"spiffe://x/atespace/{ATESPACE}/actor/", "../../etc/passwd",
                 f"spiffe://x/atespace/{ATESPACE}/actor/{ACTOR}/extra"):
        check(f"junk identity {junk!r} is refused",
              actor_from_spiffe(junk, ATESPACE) is None)

    # The direct case, unchanged: address wins and nothing is asserted.
    check("a worker's own address still identifies it",
          await resolver().actor_for(WORKER_IP) == ACTOR)

    # An actor that reaches us directly is identified by WHERE it is. It must not
    # be able to talk its way into being someone else by adding the header.
    other_spiffe = f"spiffe://substrate-actor.local/atespace/{ATESPACE}/actor/{OTHER}"
    check("an actor calling directly cannot assert a different identity",
          await resolver().actor_for(WORKER_IP, other_spiffe) == ACTOR)

    # The bug this fixes.
    check("the proxy's stamp identifies the actor behind it",
          await resolver().actor_for(PROXY_IP, SPIFFE) == ACTOR)

    # The exploit it must not open: any pod in the cluster naming any actor.
    check("a stamp from anywhere else is refused",
          await resolver().actor_for(STRANGER_IP, SPIFFE) is None)
    check("with no proxy configured, no stamp is believed",
          await CallerResolver(
              atespace=ATESPACE, namespace="hermes",
              ate_client=_Ate(),
              k8s_pods=lambda: _noop_pods(),
               ).actor_for(PROXY_IP, SPIFFE) is None)
    check("the proxy alone, with no stamp, is nobody",
          await resolver().actor_for(PROXY_IP) is None)
    check("a stamp naming an actor that is not RUNNING is refused",
          await resolver().actor_for(
              PROXY_IP,
              f"spiffe://substrate-actor.local/atespace/{ATESPACE}"
              "/actor/agent-suspended") is None)
    check("a stamp naming an actor that does not exist is refused",
          await resolver().actor_for(
              PROXY_IP,
              f"spiffe://substrate-actor.local/atespace/{ATESPACE}"
              "/actor/agent-invented") is None)

    check("an ad-hoc agent can still be identified",
          await resolver().actor_for(WORKER_IP) == ACTOR)

    # The cache is keyed by peer AND stamp, or the proxy's first caller answers
    # for every actor behind it for the next five seconds.
    r = resolver()
    check("two actors behind one proxy do not share a cached answer",
          await r.actor_for(PROXY_IP, SPIFFE) == ACTOR
          and await r.actor_for(PROXY_IP, other_spiffe) == OTHER)


async def _noop_pods():
    return {}


ingress_src = open(ingress.__file__).read()


async def test_vertex_proxy() -> None:
    section("model proxy: provider credentials stay at the gateway")

    from vertex_proxy import VertexProxy, VertexProxyConfig

    cfg = VertexProxyConfig(
        enabled=True,
        backend="vertex",
        project="example-project",
        location="global",
        model="google/gemini-example",
        openai_model="gpt-example",
    )
    check("the Vertex endpoint is project-scoped",
          cfg.endpoint == ("https://aiplatform.googleapis.com/v1/projects/"
                           "example-project/locations/global/endpoints/openapi/"
                           "chat/completions"))
    check("the configured backend chooses the model",
          cfg.active_model == "google/gemini-example")

    async def trusted() -> set[str]:
        return {"10.0.0.2"}

    proxy = VertexProxy(cfg, trusted)

    class _Req:
        def __init__(self, headers, remote):
            self.headers = headers
            self.remote = remote

    old_key = os.environ.get("API_SERVER_KEY")
    os.environ["API_SERVER_KEY"] = "internal-only"
    try:
        check("the actor's existing internal key authorizes the bridge",
              await proxy._authorized(_Req(
                  {"Authorization": "Bearer internal-only"}, "10.0.0.9")))
        check("the verified egress identity is accepted only from the proxy",
              await proxy._authorized(_Req(
                  {"x-hermes-actor": "spiffe://actor"}, "10.0.0.2")))
        check("an actor header from any other address is refused",
              not await proxy._authorized(_Req(
                  {"x-hermes-actor": "spiffe://actor"}, "10.0.0.9")))
        check("an unauthenticated direct request is refused",
              not await proxy._authorized(_Req({}, "10.0.0.9")))
    finally:
        if old_key is None:
            os.environ.pop("API_SERVER_KEY", None)
        else:
            os.environ["API_SERVER_KEY"] = old_key

    try:
        VertexProxy(VertexProxyConfig(enabled=True, backend="other",
                                      project="example-project"))
        check("an unknown backend is refused at startup", False)
    except ValueError:
        check("an unknown backend is refused at startup", True)


async def main() -> int:
    test_cron()
    test_registry()
    await test_idle()
    await test_wake_timeout_never_duplicates()
    await test_working_signal()
    await test_presence()
    await test_scheduler()
    test_failure_feedback()
    test_systemic_failure_message()
    test_wake_timeout()
    await test_caller_resolution()
    test_runtime_signals()
    await test_vertex_proxy()
    await test_delivery()
    print(f"\n{PASSED} passed, {FAILED} failed")
    return 1 if FAILED else 0


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=os.environ.get("SELFTEST_LOG", "ERROR"))
    sys.exit(asyncio.run(main()))
