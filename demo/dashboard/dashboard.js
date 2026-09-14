// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { exec } from "node:child_process";

const app = new Hono();

const NS = process.env.NAMESPACE || "openclaw";
const ATE_NS = "ate-system";
const ATE_ENDPOINT = process.env.ATE_ENDPOINT || "api.ate-system.svc.cluster.local:443";
const GATEWAY_URL = process.env.GATEWAY_URL || `http://openclaw-gateway.${NS}.svc.cluster.local:18789`;
const KUBECTL_ATE = process.env.KUBECTL_ATE || "kubectl-ate";
// Atespace(s) the demo actors live in (current OSS actor model). Comma-separated.
const ATESPACES = (process.env.ATESPACES || "openclaw-demo").split(",").map(s => s.trim()).filter(Boolean);

// Deliberately no channel state here, and no channel panel below.
//
// The dashboard used to report WhatsApp link state, list the thread and drive
// pairing. All of it duplicated WhatsApp Web, which is on screen next to this
// during the recording, and all of it was wrong at some point: a green light on
// an account that was never linked, a pairing button shelling out to a script
// that is not in the image, and a message list nothing ever wrote to. It was
// also the only reason this needed `pods/exec`, which is the one privilege a
// read-only panel should not hold.
//
// What is left is what only this can show: which worker pod holds which actor,
// and what the fleet did over time. One source, read-only, nothing to drift.

// Deliberately no suspend/resume latency on this panel. The figure that would
// go there is control-plane handler time from a 1,000-actor benchmark, and it
// is nothing like the end-to-end wake a viewer is watching, which carries the
// sandbox restore on top and lands in seconds. A tile showing one while the
// screen shows the other invites the number to be quoted in the wrong bracket,
// and a demo dashboard is the worst possible first place for that to happen.
// Latency belongs in the results doc, next to the profile it came from.

const state = {
  pods: [],
  actors: [],
  gatewayHealth: { ok: false, ready: false },
  events: [],
  timeline: [],
  stats: {
    totalResumes: 0,
    totalSuspends: 0,
    totalLogicalActiveSec: 0,
    totalPhysicalActiveSec: 0,
    // End-to-end wake: the request that causes the resume through to the
    // response that proves the actor is serving. Not the same quantity as the
    // control-plane handler time a benchmark reports, and a demo cluster of five
    // workers is not a sample worth showing, so this stays off camera and is
    // kept for the event stream only. See fireBurst.
    lastSwapLatencyMs: 0,
    avgSwapLatencyMs: 0,
    swapSamples: 0,
    lastSync: Date.now(),
  },
};

const MAX_EVENTS = 200;

// Rolling window for the achieved-density figure. Long enough that a single
// conversation turn does not swing it, short enough that it still reflects what
// the fleet is doing now rather than everything since the pod started. Fifteen
// minutes also means a warm-up run before a demo is still inside the window
// when the demo starts, so the card is showing a measurement at the cold open
// instead of a dash.
const DENSITY_WINDOW_MS = 15 * 60 * 1000;

// Least busy worker-time in the window before each figure is worth printing.
// The peak ratio needs far less because it is not a division by a small
// measured number; see the notes on /api/state.
const MIN_BUSY_WORKER_SEC_AVG = 60;
const MIN_BUSY_WORKER_SEC_PEAK = 10;

// Occupancy samples, one per sync, trimmed to the window above.
//
// Deriving a number from a polling loop is how the old "Worker Swap Latency"
// tile got it wrong, so it is worth being precise about why this one is sound.
// A latency is the gap between two instants, and sampling it every 2s quantises
// it to 2s and puts a floor under it. Occupancy is a state held over time, and
// integrating it is a Riemann sum whose error averages out the longer you
// watch. The one thing that would break it is a busy interval shorter than the
// poll; a turn holds a worker for ~10s, so it lands in about five samples.
//
// Deliberately not inside `state`: /api/state spreads that object wholesale and
// this would ship 300 entries of noise to the browser every two seconds.
let occupancySamples = [];

function runCmd(cmd, timeoutMs = 10000) {
  return new Promise((resolve) => {
    exec(cmd, { timeout: timeoutMs }, (error, stdout, stderr) => {
      if (error && !stdout) {
        console.error(`runCmd error: ${cmd.slice(0, 60)}... => ${error.message}`);
      }
      resolve(stdout?.trim() || "");
    });
  });
}

// Epoch milliseconds, not a formatted string. The server is a pod and its
// clock is UTC, so anything formatted here reads an hour or eight away from the
// clock in the dashboard header and the one in a terminal beside it. Three
// disagreeing clocks on one screen is a distraction in a recording whose whole
// point is that two independent views agree. The browser formats it.
function addEvent(module, message) {
  state.events.push({ at: Date.now(), module, message });
  if (state.events.length > MAX_EVENTS) state.events.shift();
}

// Agent-task lifecycle timeline (per-actor resume→active→suspend transitions).
function addTimeline(actor, event, detail) {
  state.timeline.unshift({ at: Date.now(), actor, event, detail });
  if (state.timeline.length > 60) state.timeline.pop();
}

async function syncState() {
  const now = Date.now();
  try {
    // Live actors (current OSS actor model: not k8s objects, listed from ateapi
    // per atespace via kubectl-ate).
    //
    // There is no golden-template panel because there is nothing to read it
    // from. ActorTemplate is an ateapi resource in this release, not a k8s CRD,
    // and kubectl-ate has no `get actortemplates`. The old code queried
    // actortemplates.ate.dev with `|| echo '{}'`, so the CRD's absence was
    // swallowed and the panel just stayed empty forever.
    const actorJsons = await Promise.all(
      ATESPACES.map((as) =>
        runCmd(`${KUBECTL_ATE} get actors -a ${as} -o json 2>/dev/null || echo '{}'`)
      )
    );
    const podsOut = await runCmd(
      `kubectl get pods -n ${NS} -l ate.dev/worker-pool --no-headers -o wide 2>&1`,
      15000
    );

    // Worker state straight from the control plane, which is the same command
    // the terminal pane in the corner runs. Occupancy used to come only from
    // the actor-to-pod join below, and that join is empty for an actor that has
    // been booked onto a worker but has not landed on it yet. Under churn that
    // put 2/5 on the panel while the pane said five BUSY, which is the one
    // disagreement beat 6 cannot survive. If the call fails the map is empty
    // and the join alone decides, which is the old behaviour.
    const workersOut = await runCmd(
      `${KUBECTL_ATE} get workers 2>/dev/null || echo ''`,
      15000
    );
    const workerState = {};
    for (const line of (workersOut || "").split("\n")) {
      // NAMESPACE POOL CLASS POD STATUS, counted from the right the way
      // watch-fleet.py does it, so a column added on the left does not
      // silently start reading the wrong two fields.
      const cols = line.trim().split(/\s+/);
      if (cols.length < 5 || cols[0] === "NAMESPACE") continue;
      workerState[cols[cols.length - 2]] = cols[cols.length - 1].toUpperCase();
    }

    const liveActors = [];
    for (const raw of actorJsons) {
      if (!raw || !raw.trim().startsWith("{")) continue;
      try {
        for (const a of (JSON.parse(raw).actors || [])) {
          // status is an object, and the state enum is ACTOR_STATE_*. Where the
          // actor is running lives under status.workerAssignment, and that key
          // is absent entirely while the actor is suspended, which is the
          // normal resting state rather than an error.
          const st = a.status || {};
          const wa = st.workerAssignment || {};
          liveActors.push({
            name: `${a.metadata?.name} @${a.metadata?.atespace}`,
            status: String(st.state || "").replace(/^ACTOR_STATE_/, "") || "UNKNOWN",
            ip: wa.workerPodIp || "n/a",
            pod: wa.workerPod || "-",
            worker: wa.workerPool || "n/a",
          });
        }
      } catch {}
    }

    state.actors = liveActors;

    // Track status transitions → resume/suspend counts, and nothing else. This
    // loop deliberately does not time anything. It runs on a 2s poll, so the
    // only thing it can measure is the gap between the poll that first saw
    // RESUMING and the poll that first saw RUNNING. That is quantised to the
    // poll interval, can never report less than one interval no matter how fast
    // the restore is, and wanders by seconds on sampling luck alone. It was
    // being displayed as "Worker Swap Latency", which made a sampling artifact
    // look like a measurement. Real timings come from fireBurst, which knows t0.
    state._prev = state._prev || {};
    for (const a of liveActors) {
      const prev = state._prev[a.name];
      if (prev !== a.status) {
        if (a.status === "RESUMING") {
          if (prev) addTimeline(a.name, "resume", "restore-on-demand from GCS snapshot");
        } else if (a.status === "RUNNING" && (prev === "RESUMING" || prev === "SUSPENDED")) {
          state.stats.totalResumes++;
          addTimeline(a.name, "active", "restored & serving");
        } else if (a.status === "SUSPENDED" && (prev === "RUNNING" || prev === "SUSPENDING")) {
          state.stats.totalSuspends++;
          addTimeline(a.name, "suspend", "checkpointed to GCS · worker freed");
        }
        state._prev[a.name] = a.status;
      }
    }

    if (podsOut) {
      try {
        const lines = podsOut.split("\n").filter(l => l.trim() && !l.startsWith("Error"));
        state.pods = lines.map((line) => {
          const cols = line.trim().split(/\s+/);
          const podName = cols[0] || "unknown";
          // Which live actor (if any) is currently restored onto this worker.
          const active = liveActors.find(
            (a) => a.pod === podName && (a.status === "RUNNING" || a.status === "RESUMING")
          );
          // Busy is the control plane's word ORed with the join, which is the
          // rule the terminal pane already uses. A worker holds exactly one
          // actor, so a booked worker is occupied whether or not its occupant
          // has finished restoring onto it.
          const booked = workerState[podName] && workerState[podName] !== "FREE";
          return {
            name: podName,
            phase: cols[2] || "Unknown",
            ip: cols[5] || "n/a",
            activeActor: active ? active.name : "idle",
            busy: Boolean(active) || Boolean(booked),
          };
        });
      } catch {}
    }

    try {
      const healthRes = await fetch(`${GATEWAY_URL}/healthz`, { signal: AbortSignal.timeout(2000) });
      const readyRes = await fetch(`${GATEWAY_URL}/readyz`, { signal: AbortSignal.timeout(2000) });
      state.gatewayHealth.ok = healthRes.ok;
      try {
        const readyData = await readyRes.json();
        state.gatewayHealth.ready = readyData.ready === true;
      } catch {
        state.gatewayHealth.ready = readyRes.ok;
      }
    } catch {
      state.gatewayHealth.ok = false;
      state.gatewayHealth.ready = false;
    }

    const elapsed = (now - state.stats.lastSync) / 1000;
    state.stats.lastSync = now;
    const runningActors = state.actors.filter(
      (a) => a.status === "RUNNING" || a.status === "RESUMING"
    ).length;
    const activePods = state.pods.filter((p) => p.busy).length;
    state.stats.totalLogicalActiveSec += runningActors * elapsed;
    state.stats.totalPhysicalActiveSec += activePods * elapsed;

    // Sample for the rolling density window. `elapsed` is carried on the sample
    // rather than assumed, because a slow kubectl-ate call stretches the gap and
    // that interval genuinely was longer.
    occupancySamples.push({ t: now, dt: elapsed, busyWorkers: activePods, runningActors });
    const cutoff = now - DENSITY_WINDOW_MS;
    while (occupancySamples.length && occupancySamples[0].t < cutoff) occupancySamples.shift();
  } catch (e) {
    addEvent("sys", `Sync error: ${e.message}`);
  }
  setTimeout(syncState, 2000);
}

app.get("/api/state", (c) => {
  // Total managed logical actors (any state, excluding the golden template) vs the
  // physical worker pool: the multiplexing/oversubscription story. Most actors sit
  // suspended in GCS; the running ones share the workers on demand.
  const managedActors = state.actors.filter((a) => !a.name.includes("(golden)")).length;
  const runningActors = state.actors.filter(
    (a) => a.status === "RUNNING" || a.status === "RESUMING"
  ).length;
  const physicalWorkers = state.pods.length;
  const occupiedWorkers = state.pods.filter((p) => p.busy).length;
  // Cost vs always-on: each managed actor would otherwise be a full always-on pod.
  // With Substrate you pay only for the currently-occupied worker footprint.
  const footprint = Math.max(1, occupiedWorkers);
  const costReductionX = Math.max(1, managedActors) / footprint;

  // Achieved density: the measured version of the ratio on the headline card.
  //
  // What was here before was totalLogicalActiveSec / totalPhysicalActiveSec,
  // running actors over busy workers. A worker holds exactly one actor at a
  // time, so that quotient is pinned at 1.00 by construction and it duly read
  // 1.00 forever. It was never measuring oversubscription, it was measuring an
  // invariant of the scheduler.
  //
  // The quantity that answers the question is how much worker the fleet
  // actually draws, so integrate busy workers over the window. That gives two
  // divisors and they are worth keeping apart. Against mean demand the ratio is
  // exactly 1/duty-cycle, so it grows without bound as the workload gets
  // sparser and a big number says how idle the agents are rather than how well
  // they pack. Against peak it says how many workers the fleet has ever needed
  // at once, which is what sizes a pool. Peak is the headline for that reason.
  let windowSec = 0;
  let busyWorkerSec = 0;
  let runningActorSec = 0;
  let peakBusyWorkers = 0;
  for (const s of occupancySamples) {
    windowSec += s.dt;
    busyWorkerSec += s.busyWorkers * s.dt;
    runningActorSec += s.runningActors * s.dt;
    if (s.busyWorkers > peakBusyWorkers) peakBusyWorkers = s.busyWorkers;
  }
  const avgBusyWorkers = windowSec > 0 ? busyWorkerSec / windowSec : 0;

  // The headline is peak-based, and the average has been demoted to the
  // sub-line. Both are true, but they behave very differently on a screen.
  //
  // The average divides by a small measured number, so it is unstable at the
  // bottom and it drifts upward on its own: leave the fleet alone and the
  // window empties, mean demand falls, and the card climbs while nothing is
  // happening. One short wake was enough to make it read 1337:1. Peak divides
  // by an integer that only ever moves when real work arrives, so it holds
  // still, and it is the number that actually sizes a pool, because peak
  // concurrency is what you have to buy.
  //
  // It also says the thing the demo is claiming. Twenty actors that never
  // needed more than five workers at once is 4:1, which is checkable against
  // the fleet grid on the same screen.
  //
  // Two busy workers is the floor, not one. A fleet of twenty that has served a
  // single turn has genuinely run at 20:1, but printing it is worse than
  // printing nothing: the card then falls to 4:1 once the fleet is actually
  // loaded, so the number a viewer sees moves the wrong way as the demo gets
  // more impressive. Below two workers there is no packing to report on yet.
  const peakRatio =
    peakBusyWorkers >= 2 && busyWorkerSec >= MIN_BUSY_WORKER_SEC_PEAK
      ? `${(managedActors / peakBusyWorkers).toFixed(1)}:1`
      : null;
  const avgRatio =
    busyWorkerSec >= MIN_BUSY_WORKER_SEC_AVG && avgBusyWorkers > 0
      ? `${(managedActors / avgBusyWorkers).toFixed(1)}:1`
      : null;
  const dutyRaw =
    managedActors > 0 && windowSec > 0
      ? (100 * runningActorSec) / (managedActors * windowSec)
      : 0;
  const dutyCyclePct = dutyRaw.toFixed(2);

  // The headline, and the same measurement as the average ratio stated so it
  // cannot be quoted as one. 98.9% idle, 1.09% duty and 91.8:1 are one fact in
  // three forms, but only the ratio form reads as a platform capability, and
  // only the ratio form swings by more than 2x across a two-minute take while
  // the underlying behaviour barely moves. As a percentage it sits still.
  const idlePct = (100 - dutyRaw).toFixed(1);

  return c.json({
    ...state,
    stats: {
      ...state.stats,
      churnRunning: churn.running,
      churnCycles: churn.cycles,
      churnSecsLeft: churn.running ? Math.max(0, Math.round((churn.until - Date.now()) / 1000)) : 0,
      peakRatio,
      avgRatio,
      avgBusyWorkers: avgBusyWorkers.toFixed(2),
      peakBusyWorkers,
      dutyCyclePct,
      idlePct,
      densityWindowMin: Math.round(DENSITY_WINDOW_MS / 60000),
      observedSec: Math.round(windowSec),
      savings: (100 - 100 / costReductionX).toFixed(1),
      managedActors,
      runningActors,
      physicalWorkers,
      occupiedWorkers,
      oversubscription: `${managedActors}:${physicalWorkers}`,
      costReductionX: costReductionX.toFixed(1),
      swapLatencySec: (state.stats.lastSwapLatencyMs / 1000).toFixed(1),
      avgSwapLatencySec: (state.stats.avgSwapLatencyMs / 1000).toFixed(1),
    },
  });
});

// Empty the event stream and the timeline, for a clean cold open.
//
// Both live on the server, so reloading the browser does nothing to them: the
// page comes back showing last night's burst. That matters more than it sounds
// for a recording, because the warm-up run that puts a real number on the
// density card also puts twenty lines in the stream, and the first shot is
// supposed to be a fleet that has done nothing.
//
// Occupancy samples deliberately survive. They are the warm-up, and throwing
// them away here would undo the thing this exists to make possible.
app.post("/api/reset-view", (c) => {
  const cleared = state.events.length + state.timeline.length;
  state.events.length = 0;
  state.timeline.length = 0;
  // The cycle counter too, or the last run's total is still sitting under the
  // burst buttons at the cold open of the next take.
  churn.cycles = 0;
  churn.refused = 0;
  // And the headline counter, which is a different one and was missed. Warming
  // the density card costs a 45-second churn run and about forty-five
  // transitions, so without this the next cold open reads "45 cycles" before
  // anything has happened, and the beat where it climbs starts from a number
  // the viewer cannot account for.
  //
  // Deliberately not touching occupancySamples: the warm-up run is the whole
  // reason the density card has anything to say, and clearing the panels must
  // not throw it away. The response reports what survived.
  state.stats.totalResumes = 0;
  state.stats.totalSuspends = 0;
  addEvent("sys", "Ready");
  return c.json({ ok: true, cleared, occupancySamplesKept: occupancySamples.length });
});

// Churn: the same multiplexing as a burst, but kept moving.
//
// A burst wakes N actors and stops. Nothing ever puts them back, because the
// idle timeout only follows conversations the gateway drove, so the fleet grid
// lights up once and then sits there until somebody suspends it by hand. The
// static picture is also the least interesting half of the claim: five lit
// chips out of twenty says a pool can be shared, and says nothing about how
// fast, or about the sharing continuing to work once the pool is full.
//
// So each actor runs its own loop -- wake, serve, park, pause, again -- and the
// loops are jittered against each other rather than stepped in lock. Freeing a
// worker is what lets one of the refused actors land, so the pool stays at five
// while the occupants keep changing, which is the actual claim and is also the
// thing worth looking at.
//
// The fleet is oc-agent-1..18 plus the unnumbered oc-agent and one conversation
// actor per chat, which is twenty: a clean 4:1 against five workers and five
// clean rows of four in the grid. Churning past 18 creates a twenty-first actor
// and costs both of those, so the cap is the fleet rather than a round number.
const FLEET_SIZE = 18;

const churn = { running: false, stop: false, until: 0, cycles: 0, refused: 0, reportedCycles: 0 };

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// A refusal every couple of seconds per waiting actor would be most of the
// event stream. They are counted and reported in a rolling line instead, which
// is more legible and is also the more useful fact: not that one actor was
// refused, but that the pool is saturated and staying that way.
//
// "and retried" is load-bearing, not softening. Refusal is the designed
// response to a full pool and every refused actor comes back a moment later;
// without that clause a reader watching fifteen of these scroll past has no way
// to tell backpressure from dropped work, and assumes the worse one.
//
// Each line also carries the actors that got through since the last one. Only
// the refusals used to be reported, so a churn run left the stream showing four
// near-identical lines about a pool saying no and nothing at all about it saying
// yes, which reads as a system failing rather than one under load. The landings
// are the half that makes the refusals mean backpressure, and they are counted
// already.
function reportRefusals() {
  const landed = churn.cycles - churn.reportedCycles;
  churn.reportedCycles = churn.cycles;
  if (churn.refused > 0) {
    addEvent(
      "substrate",
      `Pool saturated: ${churn.refused} resume${churn.refused === 1 ? "" : "s"} refused with HTTP 503 and retried, ` +
        `${landed} actor${landed === 1 ? "" : "s"} served and parked, all ${state.pods.length} ateoms stayed busy`
    );
    churn.refused = 0;
  }
}

async function churnActor(name, atespace, hold) {
  const url = `http://${name}.${atespace}.actors.resources.substrate.ate.dev/healthz`;
  // Start somewhere random inside the cycle so twenty loops do not fire on the
  // same tick. Without this they synchronise into a slow pulse, which looks
  // staged and hides the refusals.
  await sleep(Math.random() * hold * 2);
  while (!churn.stop && Date.now() < churn.until) {
    let served = false;
    try {
      const r = await fetch(url, { signal: AbortSignal.timeout(30000) });
      if (r.ok) served = true;
      else if (r.status === 503) churn.refused++;
    } catch {}

    if (!served) {
      // Refused or timed out. Back off, with jitter so the waiting actors do
      // not retry into the same instant, but not for long: the gap between a
      // worker coming free and somebody knocking on it is dead air on the
      // panel, and with fourteen actors waiting there is no reason for it.
      await sleep(150 + Math.random() * 350);
      continue;
    }

    // Hold the worker briefly, then give it up. This is the part a plain burst
    // never does, and it is what lets somebody else land.
    await sleep(hold);
    await runCmd(`${KUBECTL_ATE} suspend actor ${name} -a ${atespace} 2>/dev/null || true`, 30000);
    churn.cycles++;
    await sleep(100 + Math.random() * 300);
  }
}

app.post("/api/churn", async (c) => {
  if (churn.running) return c.json({ ok: false, error: "already running" }, 409);
  let count = 10;
  let seconds = 45;
  let hold = 1500;
  try {
    const b = await c.req.json();
    count = Math.min(FLEET_SIZE, Math.max(2, parseInt(b.count, 10) || count));
    seconds = Math.min(300, Math.max(5, parseInt(b.seconds, 10) || seconds));
    hold = Math.min(10000, Math.max(200, parseInt(b.hold, 10) || hold));
  } catch {}
  const atespace = ATESPACES[0] || "openclaw-demo";
  const names = [];
  for (let i = 1; i <= count; i++) {
    const name = `oc-agent-${i}`;
    await runCmd(
      `${KUBECTL_ATE} create actor ${name} -a ${atespace} --template-ref openclaw-agent 2>/dev/null || true`,
      15000
    );
    names.push(name);
  }

  Object.assign(churn, {
    running: true,
    stop: false,
    until: Date.now() + seconds * 1000,
    cycles: 0,
    refused: 0,
    reportedCycles: 0,
  });
  addEvent(
    "substrate",
    `Churn: ${count} actors cycling through ${state.pods.length} ateoms for ${seconds}s`
  );

  // Ten seconds, not three. At three a 45s run emits fifteen near-identical
  // saturation lines and they are the only thing left in the panel, which on a
  // screen reads as a system failing rather than as one pushing back. Four
  // lines make the same point, and each one now carries the landings alongside
  // the refusals so the stream shows the pool working rather than only saying no.
  const ticker = setInterval(reportRefusals, 10000);
  // Not awaited: the loops outlive the request, and the button wants an answer
  // now rather than in forty-five seconds.
  Promise.all(names.map((n) => churnActor(n, atespace, hold)))
    .then(async () => {
      clearInterval(ticker);
      reportRefusals();
      // Leave the fleet parked. Anything still holding a worker when the clock
      // ran out would otherwise sit there awake for the rest of the session,
      // and the next thing anyone does is look at a cold open.
      //
      // Twice, with a pause. The sweep takes a few seconds to walk the fleet,
      // and an actor that was mid-resume when the sweep went past it lands
      // behind the sweep and stays awake. One straggler out of eighteen is
      // exactly the sort of thing nobody notices until it is on camera.
      for (let pass = 0; pass < 2; pass++) {
        for (const n of names) {
          await runCmd(`${KUBECTL_ATE} suspend actor ${n} -a ${atespace} 2>/dev/null || true`, 30000);
        }
        if (pass === 0) await sleep(3000);
      }
      addEvent("substrate", `Churn: done, ${churn.cycles} wake-serve-park cycles, fleet parked`);
      churn.running = false;
    })
    .catch(() => {
      clearInterval(ticker);
      churn.running = false;
    });

  return c.json({ ok: true, count, seconds, hold, actors: names });
});

app.post("/api/churn/stop", (c) => {
  churn.stop = true;
  return c.json({ ok: true, wasRunning: churn.running, cycles: churn.cycles });
});

// Burst: create N logical actors and fire an agent task at each, to demonstrate
// many suspendable actors multiplexing onto a small worker pool.
app.post("/api/burst", async (c) => {
  let count = 5;
  try {
    const b = await c.req.json();
    count = Math.min(10, Math.max(1, parseInt(b.count, 10) || 5));
  } catch {}
  const atespace = ATESPACES[0] || "openclaw-demo";
  addEvent("substrate", `Burst: launching ${count} agent tasks across ${count} actors…`);
  const names = [];
  for (let i = 1; i <= count; i++) {
    // Named as part of the fleet rather than oc-burst-N, so a pre-created fleet
    // is reused instead of grown: the create below is idempotent, so bursting
    // wakes actors that were already sitting there suspended. It also keeps the
    // fleet panel readable on camera, where "oc-burst-3" looks like scaffolding.
    const name = `oc-agent-${i}`;
    // Idempotent: create the actor from the golden template if it doesn't exist.
    //
    // The flag is --template-ref, and it resolves the name inside --atespace, so
    // it takes a bare name. This used to pass `--template openclaw/openclaw-agent`
    // -- a flag the CLI doesn't have, and a namespace-qualified reference it would
    // reject anyway -- with the error swallowed by `|| true`. Burst then fired
    // HTTP requests at actors that had never been created, and the pod map stayed
    // empty while the button reported success.
    await runCmd(
      `${KUBECTL_ATE} create actor ${name} -a ${atespace} --template-ref openclaw-agent 2>/dev/null || true`,
      15000
    );
    names.push(name);
  }
  // Fire resume-on-demand at each actor (async, so it doesn't block the HTTP response).
  // atenet routes <actor>.<atespace>.actors.resources.substrate.ate.dev to a worker.
  for (const name of names) {
    const url = `http://${name}.${atespace}.actors.resources.substrate.ate.dev/healthz`;
    // This request is what causes the wake, and the response is the actor
    // serving, so the round trip is the resume-on-demand latency with nothing
    // inferred. It is the only place in the dashboard that can honestly time a
    // resume: everywhere else is reading a 2s poll.
    const t0 = Date.now();
    fetch(url, { signal: AbortSignal.timeout(120000) })
      .then((r) => {
        // Only a served response is a sample. A 503 measures how fast the pool
        // said no, and a 504 is the timeout, not the restore.
        if (r.ok) {
          const ms = Date.now() - t0;
          state.stats.lastSwapLatencyMs = ms;
          state.stats.swapSamples++;
          state.stats.avgSwapLatencyMs =
            (state.stats.avgSwapLatencyMs * (state.stats.swapSamples - 1) + ms) /
            state.stats.swapSamples;
        }
        // An ateom hosts one actor at a time, so a burst wider than the worker
        // pool gets the excess refused with a 503 rather than queued. A resolved
        // response isn't a thrown error, so this used to vanish into the .catch()
        // and the actor just sat SUSPENDED while the timeline claimed a task had
        // been fired at it.
        if (r.status === 503) {
          addEvent(
            "substrate",
            `${name}: no worker free (HTTP 503); pool is ${state.pods.length} ateoms, one actor each`
          );
        } else if (r.status === 504) {
          // Not the same fact as a 503, and it used to be reported as one. The
          // pool had room, atenet accepted the actor and the restore outran the
          // gateway's timeout: the actor usually comes up a second or two later
          // and the fleet panel shows it RESUMING while this line is still on
          // screen. Calling that "no worker free" contradicts the panel next to
          // it.
          addEvent("substrate", `${name}: restore outran the request timeout (HTTP 504); actor is still coming up`);
        } else if (!r.ok) {
          addEvent("substrate", `${name}: resume request failed HTTP ${r.status}`);
        }
      })
      .catch(() => {});
  }
  addEvent("substrate", `Burst: fired ${count} tasks, actors now multiplexing onto the worker pool`);
  return c.json({ ok: true, count, actors: names });
});

app.get("/", (c) =>
  c.html(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>OpenClaw on Substrate</title>
<style>
:root{--bg:#0d1117;--panel:#161b22;--panel-2:#010409;--line:#30363d;--text:#e6edf3;--muted:#8b949e;--accent:#58a6ff;--green:#3fb950;--green-bg:rgba(63,185,80,0.1);--red:#f85149;--cyan:#79c0ff;--yellow:#e3b341;--orange:#d29922;--pink:#f778ba}
*{box-sizing:border-box}
body{font-family:'SF Mono',ui-monospace,'Cascadia Code',monospace;margin:0;padding:20px;background:var(--bg);color:var(--text);line-height:1.5;font-size:13px;max-width:100vw;overflow-x:hidden}
header{border-bottom:2px solid var(--green);padding-bottom:12px;margin-bottom:20px;display:flex;justify-content:space-between;align-items:center}
h1{font-size:16px;margin:0;color:var(--green);font-weight:800;letter-spacing:0.5px}
h1 span{font-size:11px;color:var(--muted);font-weight:400;vertical-align:middle;margin-left:8px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:6px;padding:16px;min-width:0}
.card h2{font-size:10px;margin:0 0 4px;color:var(--muted);text-transform:uppercase;letter-spacing:1px;border-left:3px solid var(--green);padding-left:8px}
.card .desc{font-size:11px;color:var(--muted);margin-bottom:10px;font-style:italic}
.row{display:grid;gap:16px;margin-bottom:16px}
.row-4{grid-template-columns:repeat(4,1fr)}
/* Efficiency stats: three cards for the operator, two once the demo layout
   hides Economic Savings. Its own class so the count follows what is visible. */
.row-eff{grid-template-columns:repeat(3,1fr)}
.row-2{grid-template-columns:1fr 1fr}
.row-1{grid-template-columns:1fr}
.stat-card{text-align:center;padding:16px}
.stat-val{font-size:28px;font-weight:800;margin:6px 0 2px}
.stat-label{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:700;text-transform:uppercase;border:1px solid var(--line)}
.badge.RUNNING{background:var(--green-bg);color:var(--green);border-color:var(--green);animation:pulse 2s infinite}
.badge.SUSPENDED{background:rgba(139,148,158,0.1);color:var(--muted);border-color:var(--muted)}
.badge.RESUMING{background:rgba(121,192,255,0.1);color:var(--cyan);border-color:var(--cyan);animation:pulse 1s infinite}
.badge.SUSPENDING{background:rgba(227,179,65,0.1);color:var(--yellow);border-color:var(--yellow)}
.box{background:var(--panel-2);border:1px solid var(--line);padding:12px;margin-bottom:8px;border-radius:4px;transition:all 0.3s}
.box-hd{display:flex;justify-content:space-between;align-items:center;gap:8px}
.box .sub{font-size:11px;color:var(--muted);margin-top:4px}
/* The occupying actor on the pod's header line. Only the recording layout
   shows it; in the operator view the same name is in the detail line under
   the IP. */
.box .occ{display:none;font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
#timeline{max-height:260px;overflow-y:auto}
.box.active{border-color:var(--green);box-shadow:0 0 12px rgba(63,185,80,0.15)}
.shell{background:var(--panel-2);border:1px solid #000;padding:12px;height:320px;overflow-y:auto;font-size:12px}
.shell-line{margin-bottom:4px;white-space:pre-wrap;padding-left:8px;border-left:2px solid transparent}
.shell-line.substrate{color:var(--cyan);border-left-color:var(--cyan)}
.shell-line.sys{color:var(--muted)}
.flow{display:flex;align-items:center;gap:16px;justify-content:center;padding:12px 0;flex-wrap:wrap}
.flow-node{background:var(--panel-2);border:1px solid var(--line);border-radius:6px;padding:10px 20px;text-align:center;font-size:11px;min-width:130px}
.flow-node.gw{border-color:var(--green)}
.flow-node.ate{border-color:var(--cyan)}
.flow-node.actor{border-color:var(--pink)}
.flow-arrow{color:var(--muted);font-size:20px}
/* A hop that isn't carrying anything right now goes grey. The gateway node
   never dims, which is the whole point of the picture: the right-hand half of
   the path disappears on suspend and the left-hand half doesn't. */
.flow-node,.flow-arrow{transition:opacity 0.4s,filter 0.4s}
.flow-node.dim{opacity:0.3;filter:grayscale(1)}
.flow-arrow.dim{opacity:0.2}
/* The hop doing the work right now, as opposed to the hops that are merely up.
   Only ever set from a state the control plane reported: RESUMING is atenet
   pulling the actor back off a snapshot, RUNNING is the actor holding a turn.
   Nothing here is on a timer. */
.flow-node.active{animation:pulse 1.2s infinite}
.tl-row{display:flex;align-items:center;gap:8px;font-size:12px;padding:7px 2px;border-bottom:1px dashed var(--line)}
.tl-time{color:var(--muted);font-size:10px;font-variant-numeric:tabular-nums;white-space:nowrap;flex-shrink:0}
.tl-badge{display:inline-block;padding:1px 6px;border-radius:4px;font-size:9px;font-weight:800;text-transform:uppercase;border:1px solid;flex-shrink:0}
.tl-detail{color:var(--muted);font-size:11px;margin-left:auto;text-align:right}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.5}}
.burst-btn{background:var(--yellow);color:#0d1117;border:none;border-radius:5px;padding:7px 14px;font-size:12px;font-weight:800;cursor:pointer;font-family:inherit;transition:opacity 0.2s}
.burst-btn:hover{opacity:0.85}
.burst-btn:disabled{opacity:0.4;cursor:not-allowed}
@media(max-width:900px){.row-4{grid-template-columns:repeat(2,1fr)}.row-2,.row-eff{grid-template-columns:1fr}}

/* Recording layout: ?layout=demo.
   The operator view has to be scrolled, which is fine at a desk and useless on
   camera, where the dashboard shares the screen with WhatsApp Web and a
   terminal. Nothing is hidden here except the Economic Savings card. The rest
   is the same panels, tightened and paired up two to a row, so the whole thing
   lands inside 1080p with nothing below the fold. If a panel needs scrolling to
   see the current state, it is the wrong height, not the wrong panel. */
body.demo{padding:14px}
body.demo header{margin-bottom:12px}
body.demo header h1{font-size:18px}
body.demo .demo-hide{display:none}
body.demo .row{gap:12px;margin-bottom:12px}
/* Economic Savings is hidden here, so two cards are left, not three. */
body.demo .row-eff{grid-template-columns:repeat(2,1fr)}
body.demo .card{padding:12px}
body.demo .card .desc{display:none}
body.demo .stat-card{padding:8px}
body.demo .stat-val{font-size:24px}
body.demo .flow{gap:10px;padding:6px 0}
body.demo .flow-node{min-width:106px;padding:8px 12px}
/* Nothing here scrolls if the cluster is the size the demo README says it is:
   five workers and sixteen actors. A panel that has to be scrolled to see the
   current state is the same as a panel that is wrong. */
body.demo .ats{display:none}
/* No max-height on the fleet panel. It used to be capped at 228px, which fits
   the four rows sixteen actors make at four across. There are seventeen: the
   sixteen burst actors plus one conversation actor per chat, and that fifth row
   sat under the fold behind a scrollbar. Browser zoom does not help, because the
   cap is in CSS pixels and shrinks with the content. Burst is the one beat where
   the whole grid lighting up is the payoff, so the panel sizes to its content and
   the row it shares stretches to match. */
body.demo #pods{max-height:228px;overflow-y:auto}
body.demo #actors{max-height:none;overflow:visible}
body.demo #pods .box{padding:7px 10px;margin-bottom:6px}
body.demo #pods .box .occ{display:block;flex:1;text-align:right}
body.demo #pods .box .sub{display:none}
/* Sixteen chips, four across. The status badge sits under the name rather than
   beside it: at this width they would collide, and the badge is the thing the
   eye is tracking as the fleet wakes up. */
body.demo #actors{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;align-content:start}
body.demo #actors .box{padding:5px 8px;margin-bottom:0;line-height:1.15}
body.demo #actors .box-hd{display:block}
/* Forced onto its own line even when the name is short enough to sit beside the
   badge, so every chip is the same height. Sixteen chips of two heights reads
   as a rendering bug. */
body.demo #actors .box-hd b{display:block;font-size:12px}
body.demo #actors .badge{font-size:8px;padding:0 5px;margin-top:2px}
body.demo #actors .box .sub{display:none}
/* The timeline and the event stream share a row, so they get the same height:
   two panels of different heights side by side reads as one of them being
   broken. */
/* An exact multiple of the row height, so the panel does not end on a row
   sliced in half. */
body.demo #timeline{max-height:200px}
body.demo .shell{height:200px}
body.demo .tl-row{padding:5px 2px;font-size:11px}
body.demo .tl-detail{padding-right:6px}
</style>
</head>
<body>
<script>
// Set before first paint so the recording layout doesn't flash the full one.
if(new URLSearchParams(location.search).get("layout")==="demo")document.body.className="demo";
</script>
<header>
  <h1>OpenClaw on Substrate<span>Split Architecture Demo</span></h1>
  <div id="sync" style="font-size:11px;color:var(--muted)">Connecting...</div>
</header>

<div class="row row-4">
  <div class="card stat-card">
    <div class="stat-label">Actor Status</div>
    <div class="stat-val" id="s-actor" style="color:var(--muted)">--</div>
    <div class="stat-label" id="s-actor-label">Loading</div>
  </div>
  <div class="card stat-card">
    <div class="stat-label">Gateway</div>
    <div class="stat-val" id="s-gw" style="color:var(--muted)">--</div>
    <div class="stat-label" id="s-gw-label">Loading</div>
  </div>
  <div class="card stat-card">
    <div class="stat-label">Suspend/Resume Cycles</div>
    <div class="stat-val" id="s-cycles" style="color:var(--cyan)">0</div>
    <div class="stat-label" id="s-cycles-label">Total transitions</div>
  </div>
  <!-- Occupancy rather than a message count: it is read straight off the pod
       list, it goes 0/5 → 5/5 → 0/5 across a burst, and it is the number the
       oversubscription argument actually rests on. -->
  <div class="card stat-card" style="border-color:var(--pink)">
    <div class="stat-label">Workers Occupied</div>
    <div class="stat-val" id="s-occ" style="color:var(--pink)">--</div>
    <div class="stat-label" id="s-occ-label">one actor per ateom</div>
  </div>
</div>

<div class="row row-1">
  <div class="card">
    <h2 style="border-left-color:var(--yellow)">Operational Efficiency</h2>
    <div class="desc">Multiplexing many suspendable actors onto a small worker pool, vs an always-on pod per instance</div>
    <div class="row row-eff" style="margin-bottom:0">
      <div class="stat-card" style="padding:10px">
        <div class="stat-label">Oversubscription Ratio</div>
        <div class="stat-val" id="eff-ratio" style="color:var(--cyan);font-size:24px">--</div>
        <!-- "workers", not "busy workers": this card divides by the size of the
             pool, not by what is occupied. Dividing by busy workers is the card
             to the right, and it is a different claim. Only on screen until the
             first poll two seconds later, but two seconds is enough to be in a
             cold open, and it is wrong in the repo either way. -->
        <div class="stat-label" id="eff-ratio-sub">logical actors : workers</div>
      </div>
      <!-- The measured counterpart of the card on its left: that one is what the
           deployment was configured to attempt, this one is what was actually
           observed. They coincide only while churn is saturating the pool; any
           window where peak busy < pool size pulls them apart, which is most
           windows that are not a demo.

           Idle time sits in the sub-line rather than the headline on purpose. It
           is the reason the packing works, but read cold, off a screen, by
           someone who has not heard the setup, a big "90% idle" reads as
           underutilisation. It explains the headline; it is not the claim.

           Ceiling worth knowing: 20 actors over 5 workers cannot measure above
           4.0:1, so this card can catch the one on its left but never beat it.
           Anything larger comes from demo/measure/density.py model. -->
      <div class="stat-card" style="padding:10px">
        <div class="stat-label">Achieved Density</div>
        <div class="stat-val" id="eff-density" style="color:var(--green);font-size:24px">--</div>
        <div class="stat-label" id="eff-density-sub">measured, rolling window</div>
      </div>
      <!-- A derived number presented as a measurement, and the same fact as the
           oversubscription ratio next to it in a form that is harder to defend.
           Kept for the operator view, out of the recording. -->
      <div class="stat-card demo-hide" style="padding:10px">
        <div class="stat-label">Economic Savings</div>
        <div class="stat-val" id="eff-savings" style="color:var(--yellow);font-size:24px">--</div>
        <div class="stat-label" id="eff-savings-sub">vs always-on pods</div>
      </div>
    </div>
    <div style="display:flex;align-items:center;gap:10px;margin-top:14px;flex-wrap:wrap">
      <span style="font-size:11px;color:var(--muted)">Demo multiplexing:</span>
      <button class="burst-btn" onclick="churn(10,45)">⚡ Churn 10 agents</button>
      <!-- No number on this one. It churns oc-agent-1..18, which is accurate, but
           on screen it sits a few centimetres from "20 managed" and a viewer
           reads the two as disagreeing rather than as the fleet minus the
           unnumbered actor and the conversation. The count is not the point of
           the button anyway. -->
      <button class="burst-btn" onclick="churn(18,60)">⚡ Churn the fleet</button>
      <button class="burst-btn" id="churn-stop" onclick="churnStop()" style="background:var(--muted)">■ Stop</button>
      <button class="burst-btn" onclick="burst(10)" style="background:#30363d;color:#8b949e">Burst 10 (hold)</button>
      <span id="burst-status" style="font-size:11px;color:var(--cyan)"></span>
    </div>
  </div>
</div>

<div class="row row-1">
  <div class="card">
    <h2 style="border-left-color:var(--cyan)">Architecture Flow</h2>
    <div class="desc">Live request path: messages flow left-to-right through the split architecture</div>
    <div class="flow">
      <div class="flow-node"><b>WhatsApp</b><br><span style="color:var(--muted)">User message</span></div>
      <div class="flow-arrow">→</div>
      <div class="flow-node gw" id="flow-gw"><b>Gateway</b><br><span style="color:var(--green)">Always-on</span></div>
      <div class="flow-arrow" id="flow-a2">→</div>
      <div class="flow-node ate" id="flow-ate"><b>atenet</b><br><span id="flow-ate-status" style="color:var(--cyan)">Resume-on-demand</span></div>
      <div class="flow-arrow" id="flow-a3">→</div>
      <div class="flow-node actor" id="flow-actor"><b>Agent Actor</b><br><span id="flow-actor-status" style="color:var(--muted)">--</span></div>
      <div class="flow-arrow" id="flow-a4">→</div>
      <div class="flow-node" id="flow-llm"><b>Gemini API</b><br><span style="color:var(--muted)">LLM response</span></div>
    </div>
  </div>
</div>

<div class="row row-2">
  <div class="card">
    <h2>Worker Pod Map</h2>
    <div class="desc">Physical Kubernetes pods: shows which actor is landed on each</div>
    <div id="pods"></div>
  </div>
  <div class="card">
    <h2 style="border-left-color:var(--pink)">Logical Actor Fleet</h2>
    <div class="desc">Actors managed by Substrate, suspended in GCS snapshots until needed</div>
    <div id="actors"></div>
  </div>
</div>

<!-- Side by side, and both on camera. They look like the same panel twice and
     are not: the timeline is per-actor and says what the control plane did,
     the stream is per-operation and says what was asked of it. The one that
     earns the stream its place is the 503 on a burst wider than the pool,
     which is the only visible sign that a worker holds one actor at a time. -->
<div class="row row-2">
  <div class="card">
    <h2 style="border-left-color:var(--cyan)">Agent Task Timeline</h2>
    <div class="desc">Per-actor lifecycle: resume-on-demand → serving → suspend, newest first</div>
    <div id="timeline"></div>
  </div>
  <div class="card">
    <h2>Event Stream</h2>
    <div class="desc">Per-operation log: what was asked of the control plane, and what it refused</div>
    <div id="shell" class="shell"></div>
  </div>
</div>

<script>
// Stable per-actor color so an actor and the worker it occupies visually match.
const ACTOR_PALETTE=["#5ac8fa","#ff6b9d","#ffd60a","#30d158","#bf5af2","#ff9f0a","#64d2ff","#ff375f"];
// Local wall-clock, to match the header and whatever terminal is on screen.
function clock(ms){
  return new Date(ms).toLocaleTimeString([],{hour12:false,hour:"2-digit",minute:"2-digit",second:"2-digit"});
}

function colorFor(name){
  if(!name||name==="idle")return null;
  let h=0;for(let i=0;i<name.length;i++){h=(h*31+name.charCodeAt(i))>>>0;}
  return ACTOR_PALETTE[h%ACTOR_PALETTE.length];
}
// Actor names carry their atespace ("oc-agent-3 @openclaw-demo"). In the demo
// there is exactly one atespace, so that tail is the same fourteen characters
// on every row of three panels. It stays in the operator view, where more than
// one atespace is possible, and the recording layout hides it: that width is
// what the fleet grid and the timeline's detail column are short of.
function actorHtml(name,color){
  const at=name.indexOf(" @");
  const st=color?' style="color:'+color+'"':'';
  if(at<0)return '<b'+st+'>'+escHtml(name)+'</b>';
  return '<b'+st+'>'+escHtml(name.slice(0,at))+'<span class="ats">'+escHtml(name.slice(at))+'</span></b>';
}
async function refresh(){
  try{
    const res=await fetch("/api/state?t="+Date.now());
    const d=await res.json();
    const el=id=>document.getElementById(id);

    el("sync").innerHTML="● "+new Date().toLocaleTimeString();

    // Actor status card, and the flow panel under it.
    //
    // Both read the whole fleet, not one actor. They used to read d.actors[0],
    // which is whichever actor the API happens to list first: during a burst of
    // oc-agent-1 through 5 that one is still suspended, so this card said
    // SUSPENDED and the flow panel greyed out its right-hand half while the pod
    // map two panels down showed five workers occupied. Two panels contradicting
    // each other on camera costs more than either of them is worth.
    if(d.actors.length){
      const colors={RUNNING:"var(--green)",SUSPENDED:"var(--muted)",RESUMING:"var(--cyan)",SUSPENDING:"var(--yellow)"};
      const resumingN=d.actors.filter(a=>a.status==="RESUMING").length;
      const runningN=d.actors.filter(a=>a.status==="RUNNING").length;
      // Resuming wins the headline: it is the transient one, it is the thing
      // the demo is claiming is fast, and it is on screen for about two polls.
      const headline=resumingN?"RESUMING":runningN?"RUNNING":"SUSPENDED";
      const parts=[];
      if(resumingN)parts.push(resumingN+" resuming");
      if(runningN)parts.push(runningN+" serving");
      el("s-actor").textContent=headline;
      el("s-actor").style.color=colors[headline];
      el("s-actor-label").textContent=parts.length
        ? parts.join(" · ")+" of "+d.actors.length
        : "all "+d.actors.length+" suspended";
      el("flow-actor-status").textContent=headline;
      el("flow-actor-status").style.color=colors[headline];
      el("flow-actor").style.borderColor=colors[headline];

      // Light the hops that are actually carrying the request. On suspend the
      // right-hand half of the path greys out and the gateway stays lit, which
      // is the design the video is trying to teach.
      //
      // The lit set advances one box at a time because the actor's own state
      // says which hop is doing the work: RESUMING is atenet restoring from a
      // snapshot, and nothing downstream of it exists yet. There is no timer
      // and no scripted sweep here. A travelling pulse would have to be
      // invented, since a turn's hops take milliseconds and this polls every
      // two seconds.
      const resuming=resumingN>0;
      const running=runningN>0;
      const reached={
        "flow-a2":resuming||running, "flow-ate":resuming||running,
        "flow-a3":running, "flow-actor":running,
        "flow-a4":running, "flow-llm":running,
      };
      for(const id in reached) el(id).classList.toggle("dim",!reached[id]);
      el("flow-ate").classList.toggle("active",resuming);
      el("flow-actor").classList.toggle("active",running);
      el("flow-ate-status").textContent=resuming?"Restoring snapshot":"Resume-on-demand";
    }

    // Gateway status. Reports the gateway process, not any channel it carries:
    // /readyz says the gateway is up and serving, and says nothing about
    // whether WhatsApp is linked. This used to claim "WhatsApp Connected" off
    // exactly that signal.
    el("s-gw").textContent=d.gatewayHealth.ok?"LIVE":"DOWN";
    el("s-gw").style.color=d.gatewayHealth.ok?"var(--green)":"var(--red)";
    el("s-gw-label").textContent=d.gatewayHealth.ready?"Ready · always-on":"Starting…";
    el("flow-gw").style.borderColor=d.gatewayHealth.ok?"var(--green)":"var(--red)";

    // Cycles + occupancy
    el("s-cycles").textContent=d.stats.totalResumes+d.stats.totalSuspends;
    el("s-cycles-label").textContent=d.stats.totalResumes+" resumes / "+d.stats.totalSuspends+" suspends";
    el("s-occ").textContent=d.stats.occupiedWorkers+"/"+d.stats.physicalWorkers;
    el("s-occ-label").textContent=(d.stats.physicalWorkers-d.stats.occupiedWorkers)+" ateoms free";

    // Operational efficiency
    el("eff-ratio").textContent=d.stats.oversubscription||"--";
    el("eff-ratio-sub").textContent=d.stats.managedActors+" managed · "+d.stats.runningActors+" running on "+d.stats.occupiedWorkers+"/"+d.stats.physicalWorkers+" workers";
    // Achieved ratio headlines, idle time explains it. Gated on peak >= 2:
    // with nothing running the idle figure is a true and useless 100%, and
    // there is no packing to report until at least two workers have been busy
    // at the same moment.
    if(d.stats.peakRatio){
      el("eff-density").textContent=d.stats.peakRatio;
      // Idle first in the sub-line, peak second. Idle is the one a viewer can
      // act on ("that is why five is enough"); peak is the arithmetic behind
      // the headline and only matters to someone checking the working.
      el("eff-density-sub").textContent="last "+d.stats.densityWindowMin+"m · agents idle "+d.stats.idlePct+"% · peak "+d.stats.peakBusyWorkers+" of "+d.stats.physicalWorkers+" workers busy";
    }else{
      el("eff-density").textContent="--";
      el("eff-density-sub").textContent="last "+d.stats.densityWindowMin+"m · nothing has run yet";
    }
    // Live cycle counter while churn runs. A grid that keeps blinking is the
    // point, but blinking alone does not say how much has happened, and the
    // count is the thing somebody will want after the fact.
    const bs=el("burst-status");
    if(bs&&d.stats.churnRunning){
      bs.textContent=d.stats.churnCycles+" wake-serve-park cycles · "+d.stats.churnSecsLeft+"s left";
    }else if(bs&&d.stats.churnCycles===0){
      bs.textContent="";
    }
    el("eff-savings").textContent=d.stats.savings+"%";
    el("eff-savings-sub").textContent="~"+d.stats.costReductionX+"× fewer pods vs always-on";

    // Event stream
    el("shell").innerHTML=d.events.map(e=>{
      const cls=e.module||"sys";
      return '<div class="shell-line '+cls+'">['+clock(e.at)+'] ['+cls.toUpperCase()+'] '+e.message+'</div>';
    }).join("");
    el("shell").scrollTop=el("shell").scrollHeight;

    // Pods. The occupying actor is rendered twice, once on the header line and
    // once in the detail line, and CSS shows exactly one of them: the operator
    // view wants the pod IP, the recording wants all five pods visible at once
    // and an IP nobody will read is what costs it the second row.
    el("pods").innerHTML=d.pods.length?d.pods.map(p=>{
      // Two different questions, and they answer differently for a few seconds
      // during a resume. Occupied is the control plane's word and drives the
      // badge, so the panel says what the terminal pane beside it says. Landed
      // is the actor-to-pod join and is the only thing that can name or colour
      // an occupant, because a booked worker has no occupant to name yet.
      const busy=p.busy;
      const landed=p.activeActor!=="idle";
      const c=landed?colorFor(p.activeActor):null;
      const bstyle=c?' style="border-left:4px solid '+c+'"':'';
      const occ=landed?actorHtml(p.activeActor,c)
        :busy?'<span style="color:var(--muted)">actor resuming</span>'
        :'<span style="color:var(--muted)">no actor landed</span>';
      return '<div class="box'+(busy?" active":"")+'"'+bstyle+'>'
        +'<div class="box-hd"><b>'+p.name.split("-").slice(-2).join("-")+'</b>'
        +'<span class="occ">'+occ+'</span>'
        +'<span class="badge '+(busy?"RUNNING":"SUSPENDED")+'">'+(busy?"OCCUPIED":"FREE")+'</span></div>'
        +'<div class="sub">IP: '+p.ip+(landed?' · '+actorHtml(p.activeActor,c):'')+'</div></div>';
    }).join(""):'<div style="color:var(--muted);padding:20px;text-align:center">No worker pods found</div>';

    // Actors. Same markup either way; the recording layout turns this into a
    // grid of chips so the whole fleet is on screen, because the argument is
    // sixteen actors against five workers and a list showing three of sixteen
    // makes it instead.
    el("actors").innerHTML=d.actors.length?d.actors.map(a=>{
      const active=a.status==="RUNNING"||a.status==="RESUMING";
      // Same color as the worker this actor occupies (colorFor keys on the actor name).
      const c=active?colorFor(a.name):null;
      const bstyle=c?' style="border-left:4px solid '+c+'"':'';
      return '<div class="box'+(active?" active":"")+'"'+bstyle+'>'
        +'<div class="box-hd">'+actorHtml(a.name,c)+'<span class="badge '+a.status+'">'+a.status+'</span></div>'
        +'<div class="sub">'+(active?"Pod: "+a.pod+" · IP: "+a.ip:"Snapshot stored in GCS")+'</div></div>';
    }).join(""):'<div style="color:var(--muted);padding:20px;text-align:center">No actors created yet</div>';

    // Agent Task Timeline
    const tlColors={resume:"var(--cyan)",active:"var(--green)",suspend:"var(--yellow)"};
    const tlLabels={resume:"RESUME",active:"ACTIVE",suspend:"SUSPEND"};
    el("timeline").innerHTML=(d.timeline&&d.timeline.length)?d.timeline.map(t=>{
      const bc=tlColors[t.event]||"var(--muted)";
      const nc=colorFor(t.actor)||"var(--text)";
      const label=tlLabels[t.event]||t.event.toUpperCase();
      return '<div class="tl-row">'
        +'<span class="tl-time">'+clock(t.at)+'</span>'
        +'<span class="tl-badge" style="color:'+bc+';border-color:'+bc+'">'+label+'</span>'
        +actorHtml(t.actor,nc)
        +'<span class="tl-detail">'+escHtml(t.detail||"")+'</span>'
        +'</div>';
    }).join(""):'<div style="color:var(--muted);padding:20px;text-align:center">No agent tasks yet. Hit Burst, or send the agent a message</div>';

  }catch(e){}
}
function escHtml(s){return s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")}
// Churn returns as soon as the loops are started, so the buttons come straight
// back and the countdown comes from the server rather than a local timer that
// would drift away from what the grid is doing.
async function churn(n,secs){
  const s=document.getElementById("burst-status");
  document.querySelectorAll(".burst-btn").forEach(b=>b.disabled=true);
  if(s)s.textContent="starting "+n+" agents…";
  try{
    const r=await fetch("/api/churn",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({count:n,seconds:secs})});
    const d=await r.json();
    if(s)s.textContent=d.ok?(d.count+" agents cycling for "+d.seconds+"s"):("error: "+(d.error||"failed"));
  }catch(e){ if(s)s.textContent="error: "+e.message; }
  finally{ setTimeout(()=>document.querySelectorAll(".burst-btn").forEach(b=>b.disabled=false),1500); refresh(); }
}
async function churnStop(){
  const s=document.getElementById("burst-status");
  try{ await fetch("/api/churn/stop",{method:"POST"}); if(s)s.textContent="stopping, parking the fleet…"; }catch(e){}
}
async function burst(n){
  const s=document.getElementById("burst-status");
  document.querySelectorAll(".burst-btn").forEach(b=>b.disabled=true);
  if(s)s.textContent="firing "+n+" tasks…";
  try{
    const r=await fetch("/api/burst",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({count:n})});
    const d=await r.json();
    if(s)s.textContent=d.ok?("launched "+d.count+" actors, watch the ratio climb"):("error: "+(d.error||"failed"));
  }catch(e){ if(s)s.textContent="error: "+e.message; }
  finally{ setTimeout(()=>{document.querySelectorAll(".burst-btn").forEach(b=>b.disabled=false);if(s)setTimeout(()=>s.textContent="",6000);},1500); refresh(); }
}
setInterval(refresh,2000);refresh();
</script>
</body></html>`)
);

const port = parseInt(process.env.PORT || "8090", 10);
serve({ fetch: app.fetch, port, hostname: "0.0.0.0" }, () => {
  console.log(`Dashboard running on http://0.0.0.0:${port}`);
  addEvent("sys", "Dashboard started");
  syncState();
});