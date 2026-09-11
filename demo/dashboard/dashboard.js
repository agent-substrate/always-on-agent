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
import { execSync, exec } from "node:child_process";

const app = new Hono();

const NS = process.env.NAMESPACE || "openclaw";
const ATE_NS = "ate-system";
const ATE_ENDPOINT = process.env.ATE_ENDPOINT || "api.ate-system.svc.cluster.local:443";
const GATEWAY_URL = process.env.GATEWAY_URL || `http://openclaw-gateway.${NS}.svc.cluster.local:18789`;
const KUBECTL_ATE = process.env.KUBECTL_ATE || "kubectl-ate";
// Atespace(s) the demo actors live in (current OSS actor model). Comma-separated.
const ATESPACES = (process.env.ATESPACES || "openclaw-demo").split(",").map(s => s.trim()).filter(Boolean);

const GATEWAY_TOKEN = process.env.GATEWAY_TOKEN || "";
// Off by default. The WhatsApp panel is visible for the whole recording, so the
// linked number shows as "linked" unless someone deliberately opts in.
const SHOW_NUMBER = process.env.SHOW_WHATSAPP_NUMBER === "true";

const state = {
  pods: [],
  actors: [],
  gatewayHealth: { ok: false, ready: false, channels: {} },
  whatsapp: { status: "unknown", connected: false, number: null, lastInbound: null, qrDataUrl: null },
  messages: [],
  events: [],
  timeline: [],
  stats: {
    totalResumes: 0,
    totalSuspends: 0,
    totalMessages: 0,
    totalLogicalActiveSec: 0,
    totalPhysicalActiveSec: 0,
    lastSwapLatencyMs: 0,
    avgSwapLatencyMs: 0,
    swapSamples: 0,
    lastSync: Date.now(),
  },
};

const MAX_EVENTS = 200;
const MAX_MESSAGES = 100;

// `openclaw channels status` boots the gateway CLI to answer, so it costs tens
// of seconds, not milliseconds. It gets its own slow cadence and its own
// timeout, well clear of the 2s sync loop.
const CHANNEL_POLL_MS = 60000;
const CHANNEL_POLL_TIMEOUT_MS = 45000;
let lastChannelPoll = 0;
let channelPollInFlight = false;
let gatewayPodName = "";

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

function addEvent(module, message) {
  const ts = new Date().toISOString().slice(11, 19);
  state.events.push({ timestamp: ts, module, message });
  if (state.events.length > MAX_EVENTS) state.events.shift();
}

// Agent-task lifecycle timeline (per-actor resume→active→suspend transitions).
function addTimeline(actor, event, detail) {
  const ts = new Date().toISOString().slice(11, 19);
  state.timeline.unshift({ timestamp: ts, actor, event, detail });
  if (state.timeline.length > 60) state.timeline.pop();
}

// WhatsApp link state, asked of the gateway rather than inferred from it.
//
// /readyz answers a question about the gateway process, not about the channel,
// and it returns ready on a gateway with no channels configured at all.
// Deriving "WhatsApp connected" from it put a green light on screen for an
// account that was never linked, which is the one lie this panel must not tell
// during a recording. `channels status` is the gateway's own answer and carries
// per-account linked/connected flags.
//
// Deliberately not awaited by the sync loop. The CLI boots the whole gateway
// app to answer, which takes tens of seconds, and link state doesn't change on
// the 2s timescale the rest of the dashboard runs at. So it refreshes slowly in
// the background and the panel shows the last known answer in between.
async function refreshChannelStatus(now) {
  if (channelPollInFlight || now - lastChannelPoll < CHANNEL_POLL_MS) return;
  channelPollInFlight = true;
  lastChannelPoll = now;
  try {
    if (!gatewayPodName) {
      gatewayPodName = await runCmd(
        `kubectl -n ${NS} get pods -l app=openclaw-gateway -o jsonpath='{.items[0].metadata.name}'`
      );
    }
    if (!gatewayPodName) return;

    const raw = await runCmd(
      `kubectl -n ${NS} exec ${gatewayPodName} -c gateway -- openclaw channels status --json 2>/dev/null`,
      CHANNEL_POLL_TIMEOUT_MS
    );
    const j = JSON.parse(raw);
    // channelAccounts maps a channel id to an array of account snapshots. An
    // absent whatsapp key means the channel isn't set up at all, which is a
    // different problem from set up and offline, so the panel says which.
    const accounts = (j.channelAccounts || {}).whatsapp || [];
    const acct = accounts.find((a) => a.connected) || accounts[0];
    const wasStatus = state.whatsapp.status;

    state.whatsapp.connected = acct ? acct.connected === true : false;
    state.whatsapp.status = !acct
      ? "not configured"
      : acct.connected === true
        ? "connected"
        : acct.linked === true
          ? "linked, offline"
          : "not linked";
    // The number itself is withheld unless SHOW_WHATSAPP_NUMBER is set: this
    // panel is on screen for the whole recording, and a real number in one
    // frame outlives the video.
    // An unlinked account still has an accountId, so gating only on that put the
    // word "linked" in the number field of a panel whose status line said "not
    // linked" directly above it.
    if (acct && acct.accountId && acct.linked === true) {
      const number = "+" + String(acct.accountId).split(":")[0].split("@")[0];
      state.whatsapp.number = SHOW_NUMBER ? number : "linked";
    } else {
      state.whatsapp.number = null;
    }
    if (acct && acct.lastInboundAt) {
      state.whatsapp.lastInbound = new Date(acct.lastInboundAt).toISOString().slice(11, 19);
    }
    if (state.whatsapp.status !== wasStatus) {
      addEvent("whatsapp", `WhatsApp ${state.whatsapp.status}`);
    }
  } catch {
    // Leave the last known state rather than flapping the panel on one slow or
    // failed exec.
  } finally {
    channelPollInFlight = false;
  }
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

    // Track status transitions → resume/suspend counts + worker-swap latency
    // (time from restore-on-demand start to the actor serving = RESUMING→RUNNING).
    state._prev = state._prev || {};
    state._resumeStart = state._resumeStart || {};
    for (const a of liveActors) {
      const prev = state._prev[a.name];
      if (prev !== a.status) {
        if (a.status === "RESUMING") {
          state._resumeStart[a.name] = Date.now();
          if (prev) addTimeline(a.name, "resume", "restore-on-demand from GCS snapshot");
        } else if (a.status === "RUNNING" && (prev === "RESUMING" || prev === "SUSPENDED")) {
          state.stats.totalResumes++;
          const t0 = state._resumeStart[a.name];
          if (t0) {
            const ms = Date.now() - t0;
            state.stats.lastSwapLatencyMs = ms;
            state.stats.swapSamples++;
            state.stats.avgSwapLatencyMs =
              (state.stats.avgSwapLatencyMs * (state.stats.swapSamples - 1) + ms) /
              state.stats.swapSamples;
            delete state._resumeStart[a.name];
            addTimeline(a.name, "active", `restored & serving · ${(ms / 1000).toFixed(1)}s swap`);
          } else {
            addTimeline(a.name, "active", "serving");
          }
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
          return {
            name: podName,
            phase: cols[2] || "Unknown",
            ip: cols[5] || "n/a",
            activeActor: active ? active.name : "idle",
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
        state.gatewayHealth.channels = readyData;
      } catch {
        state.gatewayHealth.ready = readyRes.ok;
      }
    } catch {
      state.gatewayHealth.ok = false;
      state.gatewayHealth.ready = false;
    }

    refreshChannelStatus(now);

    // Fetch messages from gateway session history API
    try {
      const histRes = await fetch(
        `${GATEWAY_URL}/sessions/agent:main:main/history`,
        { headers: { Authorization: `Bearer ${GATEWAY_TOKEN}` }, signal: AbortSignal.timeout(5000) }
      );
      if (histRes.ok) {
        const hist = await histRes.json();
        const items = hist.items || [];
        const newMessages = [];
        for (const item of items) {
          const ts = item.timestamp ? new Date(item.timestamp).toISOString().slice(11, 19) : "";
          if (item.role === "user" && typeof item.content === "string") {
            newMessages.push({ timestamp: ts, from: "user", text: item.content, direction: "inbound" });
          } else if (item.role === "assistant") {
            const parts = Array.isArray(item.content) ? item.content : [item.content];
            const textParts = parts.filter(p => p?.type === "text" && p.text).map(p => p.text);
            if (textParts.length) {
              newMessages.push({ timestamp: ts, from: "assistant", text: textParts.join("\n"), direction: "outbound" });
            }
          }
        }
        if (newMessages.length !== state.messages.length) {
          state.stats.totalMessages = newMessages.filter(m => m.direction === "inbound").length;
        }
        state.messages = newMessages.slice(-MAX_MESSAGES);
        const lastIn = [...newMessages].reverse().find(m => m.direction === "inbound");
        state.whatsapp.lastInbound = lastIn ? lastIn.timestamp : (state.whatsapp.lastInbound || null);
      }
    } catch {}

    const elapsed = (now - state.stats.lastSync) / 1000;
    state.stats.lastSync = now;
    const runningActors = state.actors.filter(
      (a) => a.status === "RUNNING" || a.status === "RESUMING"
    ).length;
    const activePods = state.pods.filter((p) => p.activeActor !== "idle").length;
    state.stats.totalLogicalActiveSec += runningActors * elapsed;
    state.stats.totalPhysicalActiveSec += activePods * elapsed;
  } catch (e) {
    addEvent("sys", `Sync error: ${e.message}`);
  }
  setTimeout(syncState, 2000);
}

app.post("/api/whatsapp/connect", async (c) => {
  addEvent("whatsapp", "Starting WhatsApp pairing process...");
  try {
    const gatewayPod = await runCmd(
      `kubectl -n ${NS} get pods -l app=openclaw-gateway -o jsonpath='{.items[0].metadata.name}'`
    );
    if (!gatewayPod) {
      return c.json({ ok: false, error: "Gateway pod not found" });
    }

    // Safety guard: never wipe a live session (a stray click was what unpaired
    // it before). This used to guard on /readyz, which reports gateway process
    // health and says ready even with no channel configured, so it refused
    // every click including the legitimate ones. Guard on the link state the
    // gateway actually reports instead.
    if (state.whatsapp.connected === true) {
      return c.json({ ok: false, error: "WhatsApp is already connected, refusing to reset. Only use this when disconnected." });
    }

    // Hand pairing to the supported CLI path. The old code shelled out to
    // /app/pair-qr.cjs, a script that does not exist in the gateway image, and
    // to a Baileys creds directory the current OpenClaw doesn't use, so the
    // button reported success and nothing happened.
    await runCmd(`kubectl -n ${NS} exec ${gatewayPod} -c gateway -- rm -f /tmp/whatsapp-qr.txt /tmp/pair-log.txt`);
    await runCmd(
      `kubectl -n ${NS} exec ${gatewayPod} -c gateway -- sh -c ` +
      `'nohup timeout 180 openclaw channels login --channel whatsapp > /tmp/pair-log.txt 2>&1 &'`
    );

    addEvent("whatsapp", "Pairing process started (3 min window)");
    state.whatsapp.status = "pairing";
    return c.json({ ok: true, message: "Pairing started. QR will appear shortly." });
  } catch (e) {
    addEvent("whatsapp", `Start error: ${e.message}`);
    return c.json({ ok: false, error: e.message });
  }
});

app.get("/api/whatsapp/qr", async (c) => {
  try {
    const gatewayPod = await runCmd(
      `kubectl -n ${NS} get pods -l app=openclaw-gateway -o jsonpath='{.items[0].metadata.name}'`
    );
    if (!gatewayPod) return c.json({ status: "no_gateway" });

    // Read the login CLI's own output. There is no whatsapp-qr.txt: that file
    // was written by a pairing script that isn't in the image, so this endpoint
    // used to answer "waiting" forever.
    //
    // The pairing payload is the ref string WhatsApp encodes in the QR, which
    // Baileys emits with a `2@` prefix. If it hasn't appeared yet, hand back the
    // log tail rather than a bare "waiting", so an operator staring at a stuck
    // pairing can see what the gateway is actually saying.
    const log = await runCmd(`kubectl -n ${NS} exec ${gatewayPod} -c gateway -- tail -c 4000 /tmp/pair-log.txt 2>/dev/null`);
    if (!log) return c.json({ status: "waiting" });

    if (state.whatsapp.connected === true) {
      return c.json({ status: "connected" });
    }

    const ref = log.match(/2@[A-Za-z0-9+/=_-]{20,}/g);
    if (ref) return c.json({ status: "qr_ready", qrData: ref[ref.length - 1] });

    return c.json({ status: "pairing", log: log.slice(-1500) });
  } catch {
    return c.json({ status: "error" });
  }
});

app.get("/api/state", (c) => {
  const density =
    state.stats.totalPhysicalActiveSec > 0
      ? state.stats.totalLogicalActiveSec / state.stats.totalPhysicalActiveSec
      : 1.0;
  // Total managed logical actors (any state, excluding the golden template) vs the
  // physical worker pool: the multiplexing/oversubscription story. Most actors sit
  // suspended in GCS; the running ones share the workers on demand.
  const managedActors = state.actors.filter((a) => !a.name.includes("(golden)")).length;
  const runningActors = state.actors.filter(
    (a) => a.status === "RUNNING" || a.status === "RESUMING"
  ).length;
  const physicalWorkers = state.pods.length;
  const occupiedWorkers = state.pods.filter((p) => p.activeActor !== "idle").length;
  // Cost vs always-on: each managed actor would otherwise be a full always-on pod.
  // With Substrate you pay only for the currently-occupied worker footprint.
  const footprint = Math.max(1, occupiedWorkers);
  const costReductionX = Math.max(1, managedActors) / footprint;
  return c.json({
    ...state,
    stats: {
      ...state.stats,
      density: Math.max(1.0, density).toFixed(2),
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
    fetch(url, { signal: AbortSignal.timeout(120000) })
      .then((r) => {
        // An ateom hosts one actor at a time, so a burst wider than the worker
        // pool gets the excess refused with a 503 rather than queued. A resolved
        // response isn't a thrown error, so this used to vanish into the .catch()
        // and the actor just sat SUSPENDED while the timeline claimed a task had
        // been fired at it.
        if (!r.ok) {
          addEvent(
            "substrate",
            `${name}: no worker free (HTTP ${r.status}); pool is ${state.pods.length} ateoms, one actor each`
          );
        }
      })
      .catch(() => {});
  }
  state.stats.totalMessages += count;
  addEvent("substrate", `Burst: fired ${count} tasks, actors now multiplexing onto the worker pool`);
  return c.json({ ok: true, count, actors: names });
});

app.post("/api/message", async (c) => {
  const body = await c.req.json();
  state.messages.push({
    timestamp: new Date().toISOString().slice(11, 19),
    from: body.from || "user",
    text: body.text || "",
    direction: body.direction || "inbound",
  });
  state.stats.totalMessages++;
  if (state.messages.length > MAX_MESSAGES) state.messages.shift();
  addEvent("whatsapp", `${body.direction === "outbound" ? "→" : "←"} ${body.text?.slice(0, 80)}`);
  return c.json({ ok: true });
});

app.get("/", (c) =>
  c.html(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>OpenClaw on Substrate</title>
<script src="https://cdn.jsdelivr.net/npm/qrcode-generator@1.4.4/qrcode.min.js"></script>
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
.row-3{grid-template-columns:repeat(3,1fr)}
.row-2{grid-template-columns:1fr 1fr}
.row-2-wide{grid-template-columns:2fr 1fr}
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
.box.active{border-color:var(--green);box-shadow:0 0 12px rgba(63,185,80,0.15)}
.shell{background:var(--panel-2);border:1px solid #000;padding:12px;height:320px;overflow-y:auto;font-size:12px}
.shell-line{margin-bottom:4px;white-space:pre-wrap;padding-left:8px;border-left:2px solid transparent}
.shell-line.whatsapp{color:var(--green);border-left-color:var(--green)}
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
.msg{padding:8px 12px;margin-bottom:6px;border-radius:4px;font-size:12px}
.msg.inbound{background:var(--panel-2);border:1px solid var(--line);margin-right:20%}
.msg.outbound{background:rgba(63,185,80,0.1);border:1px solid rgba(63,185,80,0.2);margin-left:20%;text-align:right}
.msg-meta{font-size:10px;color:var(--muted);margin-top:2px}
.wa-row{display:flex;justify-content:space-between;align-items:center;font-size:13px;padding:9px 2px;border-bottom:1px dashed var(--line);color:var(--muted)}
.wa-row b{color:var(--text);font-weight:700}
.tl-row{display:flex;align-items:center;gap:8px;font-size:12px;padding:7px 2px;border-bottom:1px dashed var(--line)}
.tl-time{color:var(--muted);font-size:10px;font-variant-numeric:tabular-nums;white-space:nowrap;flex-shrink:0}
.tl-badge{display:inline-block;padding:1px 6px;border-radius:4px;font-size:9px;font-weight:800;text-transform:uppercase;border:1px solid;flex-shrink:0}
.tl-detail{color:var(--muted);font-size:11px;margin-left:auto;text-align:right}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.5}}
.burst-btn{background:var(--yellow);color:#0d1117;border:none;border-radius:5px;padding:7px 14px;font-size:12px;font-weight:800;cursor:pointer;font-family:inherit;transition:opacity 0.2s}
.burst-btn:hover{opacity:0.85}
.burst-btn:disabled{opacity:0.4;cursor:not-allowed}
@media(max-width:900px){.row-4{grid-template-columns:repeat(2,1fr)}.row-2,.row-2-wide,.row-3{grid-template-columns:1fr}}

/* Recording layout: ?layout=demo.
   The operator view is nine panels tall and has to be scrolled, which is fine
   at a desk and useless on camera, where the dashboard shares the screen with
   WhatsApp Web and a terminal. This drops it to what the video argues with and
   fits the rest in one column with no scrolling. */
body.demo{padding:14px}
body.demo header{margin-bottom:12px}
body.demo header h1{font-size:18px}
body.demo .demo-hide{display:none}
body.demo .row{gap:12px;margin-bottom:12px}
body.demo .row-3{grid-template-columns:repeat(2,1fr)}
body.demo .card{padding:12px}
body.demo .card .desc{display:none}
body.demo .stat-card{padding:10px}
body.demo .stat-val{font-size:24px}
body.demo .flow{gap:10px;padding:6px 0}
body.demo .flow-node{min-width:106px;padding:8px 12px}
body.demo #pods,body.demo #actors{max-height:230px;overflow-y:auto}
body.demo #timeline{max-height:210px}
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
  <div class="card stat-card" style="border-color:var(--pink)">
    <div class="stat-label">Messages Processed</div>
    <div class="stat-val" id="s-msgs" style="color:var(--pink)">0</div>
    <div class="stat-label">Via WhatsApp</div>
  </div>
</div>

<div class="row row-1">
  <div class="card">
    <h2 style="border-left-color:var(--yellow)">Operational Efficiency</h2>
    <div class="desc">Multiplexing many suspendable actors onto a small worker pool, vs an always-on pod per instance</div>
    <div class="row row-3" style="margin-bottom:0">
      <div class="stat-card" style="padding:10px">
        <div class="stat-label">Oversubscription Ratio</div>
        <div class="stat-val" id="eff-ratio" style="color:var(--cyan);font-size:24px">--</div>
        <div class="stat-label" id="eff-ratio-sub">logical actors : busy workers</div>
      </div>
      <div class="stat-card" style="padding:10px">
        <div class="stat-label">Worker Swap Latency</div>
        <div class="stat-val" id="eff-latency" style="color:var(--green);font-size:24px">--</div>
        <div class="stat-label" id="eff-latency-sub">snapshot → serving</div>
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
      <button class="burst-btn" onclick="burst(5)">⚡ Burst 5 tasks</button>
      <button class="burst-btn" onclick="burst(10)">⚡ Burst 10 tasks</button>
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
      <div class="flow-node ate" id="flow-ate"><b>atenet</b><br><span style="color:var(--cyan)">Resume-on-demand</span></div>
      <div class="flow-arrow" id="flow-a3">→</div>
      <div class="flow-node actor" id="flow-actor"><b>Agent Actor</b><br><span id="flow-actor-status" style="color:var(--muted)">--</span></div>
      <div class="flow-arrow" id="flow-a4">→</div>
      <div class="flow-node" id="flow-llm"><b>Gemini API</b><br><span style="color:var(--muted)">LLM response</span></div>
    </div>
  </div>
</div>

<!-- Both of these are replaced on camera by the real thing: WhatsApp Web is on
     screen next to the dashboard, and the terminal carries the event log. -->
<div class="row row-2-wide demo-hide">
  <div class="card">
    <h2>Event Stream</h2>
    <div class="desc">Real-time orchestration events: actor lifecycle, WhatsApp messages, and system operations</div>
    <div id="shell" class="shell"></div>
  </div>
  <div class="card" id="wa-card">
    <h2 style="border-left-color:var(--green)">WhatsApp Gateway</h2>
    <div class="desc">Presence layer: always-on, never suspended</div>
    <div style="display:flex;align-items:center;gap:10px;margin:10px 0 16px">
      <span id="wa-dot" style="width:15px;height:15px;border-radius:50%;background:var(--muted);display:inline-block"></span>
      <span id="wa-state" style="font-size:24px;font-weight:800;color:var(--muted)">Checking…</span>
    </div>
    <div class="wa-row"><span>Channel</span><b>WhatsApp · persistent WebSocket</b></div>
    <div class="wa-row"><span>Linked number</span><b id="wa-number">-</b></div>
    <div class="wa-row"><span>Gateway process</span><b id="wa-gw">-</b></div>
    <div class="wa-row"><span>Last inbound</span><b id="wa-last">-</b></div>
    <div class="wa-row" style="border:none"><span>Messages processed</span><b id="wa-count">0</b></div>
  </div>
</div>

<div class="row row-3">
  <div class="card demo-hide">
    <h2 style="border-left-color:var(--pink)">WhatsApp Messages</h2>
    <div class="desc">Conversation flow between user and the Substrate-backed agent</div>
    <div id="messages" style="height:240px;overflow-y:auto;padding:8px"></div>
  </div>
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

<div class="row row-1">
  <div class="card">
    <h2 style="border-left-color:var(--cyan)">Agent Task Timeline</h2>
    <div class="desc">Per-actor lifecycle: resume-on-demand → serving → suspend, newest first</div>
    <div id="timeline" style="max-height:260px;overflow-y:auto"></div>
  </div>
</div>

<script>
// Stable per-actor color so an actor and the worker it occupies visually match.
const ACTOR_PALETTE=["#5ac8fa","#ff6b9d","#ffd60a","#30d158","#bf5af2","#ff9f0a","#64d2ff","#ff375f"];
function colorFor(name){
  if(!name||name==="idle")return null;
  let h=0;for(let i=0;i<name.length;i++){h=(h*31+name.charCodeAt(i))>>>0;}
  return ACTOR_PALETTE[h%ACTOR_PALETTE.length];
}
async function refresh(){
  try{
    const res=await fetch("/api/state?t="+Date.now());
    const d=await res.json();
    const el=id=>document.getElementById(id);

    el("sync").innerHTML="● "+new Date().toLocaleTimeString();

    // Actor status card
    const actor=d.actors[0];
    if(actor){
      const colors={RUNNING:"var(--green)",SUSPENDED:"var(--muted)",RESUMING:"var(--cyan)",SUSPENDING:"var(--yellow)"};
      el("s-actor").textContent=actor.status;
      el("s-actor").style.color=colors[actor.status]||"var(--muted)";
      el("s-actor-label").textContent=actor.name;
      el("flow-actor-status").textContent=actor.status;
      el("flow-actor-status").style.color=colors[actor.status]||"var(--muted)";
      el("flow-actor").style.borderColor=colors[actor.status]||"var(--line)";

      // Light the hops that are actually carrying the request. On suspend the
      // right-hand half of the path greys out and the gateway stays lit, which
      // is the design the video is trying to teach.
      const live=actor.status==="RUNNING"||actor.status==="RESUMING";
      for(const id of ["flow-a2","flow-ate","flow-a3","flow-actor","flow-a4","flow-llm"]){
        el(id).classList.toggle("dim",!live);
      }
    }

    // Gateway status
    const waConnected=d.gatewayHealth.ready&&d.gatewayHealth.ok;
    el("s-gw").textContent=d.gatewayHealth.ok?"LIVE":"DOWN";
    el("s-gw").style.color=d.gatewayHealth.ok?"var(--green)":"var(--red)";
    el("s-gw-label").textContent=waConnected?"WhatsApp Connected":"Channels Connecting...";
    el("flow-gw").style.borderColor=d.gatewayHealth.ok?"var(--green)":"var(--red)";

    // WhatsApp Gateway status panel (live, read-only)
    const wc=d.whatsapp.connected;
    el("wa-dot").style.background=wc?"var(--green)":"var(--red)";
    el("wa-dot").style.animation=wc?"pulse 2s infinite":"none";
    el("wa-state").textContent=wc?"Connected":"Disconnected";
    el("wa-state").style.color=wc?"var(--green)":"var(--red)";
    el("wa-number").textContent=d.whatsapp.number||"not linked";
    el("wa-gw").textContent=d.gatewayHealth.ok?"LIVE (always-on)":"down";
    el("wa-gw").style.color=d.gatewayHealth.ok?"var(--green)":"var(--red)";
    el("wa-last").textContent=d.whatsapp.lastInbound||"-";
    el("wa-count").textContent=d.stats.totalMessages;

    // Cycles + Messages
    el("s-cycles").textContent=d.stats.totalResumes+d.stats.totalSuspends;
    el("s-cycles-label").textContent=d.stats.totalResumes+" resumes / "+d.stats.totalSuspends+" suspends";
    el("s-msgs").textContent=d.stats.totalMessages;

    // Operational efficiency
    el("eff-ratio").textContent=d.stats.oversubscription||"--";
    el("eff-ratio-sub").textContent=d.stats.managedActors+" managed · "+d.stats.runningActors+" running on "+d.stats.occupiedWorkers+"/"+d.stats.physicalWorkers+" workers · avg "+d.stats.density+"× over time";
    if(d.stats.swapSamples>0){
      el("eff-latency").textContent=d.stats.swapLatencySec+"s";
      el("eff-latency-sub").textContent="last · avg "+d.stats.avgSwapLatencySec+"s over "+d.stats.swapSamples;
    }else{
      el("eff-latency").textContent="-";
      el("eff-latency-sub").textContent="awaiting a resume";
    }
    el("eff-savings").textContent=d.stats.savings+"%";
    el("eff-savings-sub").textContent="~"+d.stats.costReductionX+"× fewer pods vs always-on";

    // Event stream
    el("shell").innerHTML=d.events.map(e=>{
      const cls=e.module||"sys";
      return '<div class="shell-line '+cls+'">['+e.timestamp+'] ['+cls.toUpperCase()+'] '+e.message+'</div>';
    }).join("");
    el("shell").scrollTop=el("shell").scrollHeight;

    // Messages
    if(d.messages.length){
      el("messages").innerHTML=d.messages.map((m,i)=>{
        const full=escHtml(m.text);
        const maxLen=120;
        const truncated=full.length>maxLen;
        const preview=truncated?full.slice(0,maxLen)+'...':full;
        const id='msg-'+i;
        return '<div class="msg '+m.direction+'">'
          +'<div id="'+id+'-short" data-toggle="'+id+'"'+(truncated?' style="cursor:pointer"':'')+'>'+preview+(truncated?' <span style="color:var(--cyan);font-size:10px">[show more]</span>':'')+'</div>'
          +(truncated?'<div id="'+id+'-full" data-toggle="'+id+'" style="display:none;cursor:pointer;white-space:pre-wrap">'+full+' <span style="color:var(--cyan);font-size:10px">[show less]</span></div>':'')
          +'<div class="msg-meta">'+m.timestamp+' · '+m.from+'</div></div>';
      }).join("");
      el("messages").onclick=function(ev){var t=ev.target.closest("[data-toggle]");if(t)toggleMsg(t.getAttribute("data-toggle"));};
      el("messages").scrollTop=el("messages").scrollHeight;
    }else{
      el("messages").innerHTML='<div style="text-align:center;color:var(--muted);padding:40px">Waiting for WhatsApp messages...</div>';
    }

    // Pods
    el("pods").innerHTML=d.pods.length?d.pods.map(p=>{
      const active=p.activeActor!=="idle";
      const c=active?colorFor(p.activeActor):null;
      const bstyle=c?' style="border-left:4px solid '+c+'"':'';
      return '<div class="box'+(active?" active":"")+'"'+bstyle+'><div style="display:flex;justify-content:space-between"><b>'+p.name.split("-").slice(-2).join("-")+'</b><span class="badge '+(active?"RUNNING":"SUSPENDED")+'">'+(active?"OCCUPIED":"FREE")+'</span></div><div style="font-size:11px;color:var(--muted);margin-top:4px">IP: '+p.ip+(active?' · <b style="color:'+c+'">'+p.activeActor+'</b>':'')+'</div></div>';
    }).join(""):'<div style="color:var(--muted);padding:20px;text-align:center">No worker pods found</div>';

    // Actors
    el("actors").innerHTML=d.actors.length?d.actors.map(a=>{
      const active=a.status==="RUNNING"||a.status==="RESUMING";
      // Same color as the worker this actor occupies (colorFor keys on the actor name).
      const c=active?colorFor(a.name):null;
      const bstyle=c?' style="border-left:4px solid '+c+'"':'';
      const nameHtml=c?'<b style="color:'+c+'">'+a.name+'</b>':'<b>'+a.name+'</b>';
      return '<div class="box'+(active?" active":"")+'"'+bstyle+'><div style="display:flex;justify-content:space-between">'+nameHtml+'<span class="badge '+a.status+'">'+a.status+'</span></div><div style="font-size:11px;color:var(--muted);margin-top:4px">'+(active?"Pod: "+a.pod+" · IP: "+a.ip:"Snapshot stored in GCS")+'</div></div>';
    }).join(""):'<div style="color:var(--muted);padding:20px;text-align:center">No actors created yet</div>';

    // Agent Task Timeline
    const tlColors={resume:"var(--cyan)",active:"var(--green)",suspend:"var(--yellow)"};
    const tlLabels={resume:"RESUME",active:"ACTIVE",suspend:"SUSPEND"};
    el("timeline").innerHTML=(d.timeline&&d.timeline.length)?d.timeline.map(t=>{
      const bc=tlColors[t.event]||"var(--muted)";
      const nc=colorFor(t.actor)||"var(--text)";
      const label=tlLabels[t.event]||t.event.toUpperCase();
      return '<div class="tl-row">'
        +'<span class="tl-time">'+t.timestamp+'</span>'
        +'<span class="tl-badge" style="color:'+bc+';border-color:'+bc+'">'+label+'</span>'
        +'<b style="color:'+nc+'">'+escHtml(t.actor)+'</b>'
        +'<span class="tl-detail">'+escHtml(t.detail||"")+'</span>'
        +'</div>';
    }).join(""):'<div style="color:var(--muted);padding:20px;text-align:center">No agent tasks yet. Click Burst or send a WhatsApp message</div>';

  }catch(e){}
}
function escHtml(s){return s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")}
function toggleMsg(id){
  const s=document.getElementById(id+"-short");
  const f=document.getElementById(id+"-full");
  if(!s||!f)return;
  if(f.style.display==="none"){f.style.display="block";s.style.display="none";}
  else{f.style.display="none";s.style.display="block";}
}
let qrPollInterval=null;
async function connectWhatsApp(){
  const btn=document.getElementById("wa-connect-btn");
  const st=document.getElementById("wa-status-text");
  btn.disabled=true;btn.textContent="Starting...";
  st.textContent="Connecting to WhatsApp servers...";
  try{
    await fetch("/api/whatsapp/connect",{method:"POST"});
    st.textContent="Waiting for QR code...";
    if(qrPollInterval)clearInterval(qrPollInterval);
    qrPollInterval=setInterval(pollQR,2000);
    setTimeout(()=>pollQR(),1000);
  }catch(e){st.textContent="Error: "+e.message;btn.textContent="Retry";btn.disabled=false;}
}
async function pollQR(){
  const st=document.getElementById("wa-status-text");
  const img=document.getElementById("wa-qr-img");
  const btn=document.getElementById("wa-connect-btn");
  try{
    const res=await fetch("/api/whatsapp/qr");
    const data=await res.json();
    if(data.status==="connected"){
      clearInterval(qrPollInterval);
      img.innerHTML='<div style="font-size:48px;color:var(--green)">&#10003;</div>';
      st.textContent="WhatsApp linked successfully!";
      btn.textContent="Connected";btn.disabled=true;btn.style.background="var(--green)";
      return;
    }
    if(data.status==="qr_ready"&&data.qrData){
      const canvas=document.createElement("canvas");
      canvas.width=280;canvas.height=280;
      renderQR(canvas,data.qrData);
      img.innerHTML="";img.appendChild(canvas);
      st.innerHTML="Scan with WhatsApp<br><b>Settings → Linked Devices → Link a Device</b><br><small style='color:var(--muted)'>QR refreshes automatically every ~20s</small>";
      btn.textContent="Restart";btn.disabled=false;
    }else if(data.status==="pairing"){
      // No QR payload yet. Show what the gateway is saying instead of a spinner
      // that hides an error.
      const tail=(data.log||"").trim().split("\n").slice(-3).join("\n");
      st.innerHTML="Pairing...<pre style='text-align:left;font-size:10px;color:var(--muted);white-space:pre-wrap;margin-top:6px'>"+escHtml(tail)+"</pre>";
    }else if(data.status==="waiting"){
      st.textContent="Waiting for QR code...";
    }
  }catch(e){}
}
function renderQR(canvas,text){
  const modules=generateQRMatrix(text);
  if(!modules){canvas.getContext("2d").fillStyle="#333";canvas.getContext("2d").fillRect(0,0,280,280);return;}
  const ctx=canvas.getContext("2d");
  const size=modules.length;
  const px=Math.floor(280/size);
  const off=Math.floor((280-px*size)/2);
  ctx.fillStyle="#ffffff";ctx.fillRect(0,0,280,280);
  ctx.fillStyle="#000000";
  for(let r=0;r<size;r++)for(let c=0;c<size;c++){
    if(modules[r][c])ctx.fillRect(off+c*px,off+r*px,px,px);
  }
}
function generateQRMatrix(data){
  try{
    if(typeof qrcode!=="undefined"){
      const qr=qrcode(0,"L");qr.addData(data);qr.make();
      const cnt=qr.getModuleCount();
      const m=[];for(let r=0;r<cnt;r++){m[r]=[];for(let c=0;c<cnt;c++)m[r][c]=qr.isDark(r,c);}
      return m;
    }
  }catch(e){}
  return null;
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