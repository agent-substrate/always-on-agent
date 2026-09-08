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
/**
 * Substrate ACP runtime backend (gateway side).
 * Implements the AcpRuntime contract by forwarding each turn over HTTP to the
 * actor's OpenAI-compatible /v1/chat/completions endpoint (routed through
 * atenet, which resumes the actor on demand) and translating the SSE stream
 * back into AcpRuntimeEvents.
 */
import type {
  AcpRuntime,
  AcpRuntimeEnsureInput,
  AcpRuntimeEvent,
  AcpRuntimeHandle,
  AcpRuntimeTurn,
  AcpRuntimeTurnInput,
  AcpRuntimeTurnResult,
} from "openclaw/plugin-sdk/acp-runtime-backend";
import { actorNameForConversation, actorUrlFor, DEFAULT_ACTOR_DOMAIN } from "./actor-router.js";
import type { Provisioner } from "./actor-provisioner.js";

export type SubstrateAcpRuntimeConfig = {
  /** Atespace the conversation actors live in. */
  atespace: string;
  /** Default golden ActorTemplate ("<namespace>/<name>") to derive actors from. */
  template: string;
  /** Optional per-persona template override, keyed by resolved agentId. */
  templateForAgent?: Record<string, string>;
  /** DNS domain actors are addressed under (default actors.resources.substrate.ate.dev). */
  actorDomain?: string;
  /** Bearer token for the actor's /v1/chat/completions API. */
  actorToken?: string;
  /** Creates the conversation actor from its golden template if absent. */
  provisioner: Provisioner;
  /** Idle-suspend hooks (the sandboxed actor can't suspend itself, so the
   *  gateway drives it). onActivity marks liveness; onTurnStart/onTurnEnd bracket
   *  an in-flight turn so the actor is never suspended mid-turn. */
  onActivity?: (actorName: string) => void;
  onTurnStart?: (actorName: string) => void;
  onTurnEnd?: (actorName: string) => void;
  logger?: { info: (m: string) => void; warn?: (m: string) => void; error?: (m: string) => void };
};

export function createSubstrateAcpRuntime(config: SubstrateAcpRuntimeConfig): AcpRuntime {
  const domain = config.actorDomain ?? DEFAULT_ACTOR_DOMAIN;
  const authHeaders = (): Record<string, string> =>
    config.actorToken ? { Authorization: `Bearer ${config.actorToken}` } : {};
  const templateFor = (agent?: string): string =>
    (agent && config.templateForAgent?.[agent]) || config.template;
  // Placement is a pure function of the conversation key, so both ensureSession
  // and startTurn derive the same actor URL with no side map.
  const urlForSession = (sessionKey: string): string =>
    actorUrlFor(actorNameForConversation(sessionKey), config.atespace, domain);
  // The gateway's ACP session key uses reserved internal namespaces
  // (e.g. "agent:main:acp:binding:..."), which the actor's
  // /v1/chat/completions rejects via X-OpenClaw-Session-Key ("reserved
  // internal session namespaces"). Each conversation has its own actor, so we
  // key the in-actor session by the (stable, non-reserved) actor name instead.
  const actorSessionKey = (sessionKey: string): string =>
    actorNameForConversation(sessionKey);

  const runtime: AcpRuntime = {
    async ensureSession(input: AcpRuntimeEnsureInput): Promise<AcpRuntimeHandle> {
      // One actor per conversation: create-if-absent from the golden template.
      // Session state lives in that actor's DurableDir; no remote call to warm.
      const name = actorNameForConversation(input.sessionKey);
      const agent = (input as { agent?: string }).agent;
      await config.provisioner.ensure(name, templateFor(agent));
      config.onActivity?.(name);
      return {
        sessionKey: input.sessionKey,
        backend: "substrate",
        runtimeSessionName: input.sessionKey,
        cwd: input.cwd,
        // Stash the resolved URL on the handle; startTurn also recomputes it.
        ...({ actorUrl: urlForSession(input.sessionKey) } as object),
      };
    },

    startTurn(input: AcpRuntimeTurnInput): AcpRuntimeTurn {
      // Deterministic from the conversation key; handle carries it as a fast path.
      const baseUrl =
        (input.handle as { actorUrl?: string }).actorUrl ?? urlForSession(input.handle.sessionKey);
      const turnActor = actorNameForConversation(input.handle.sessionKey);
      config.onTurnStart?.(turnActor);
      const abort = new AbortController();
      input.signal?.addEventListener("abort", () => abort.abort(input.signal?.reason));
      let resolveResult!: (v: AcpRuntimeTurnResult) => void;
      const result = new Promise<AcpRuntimeTurnResult>((r) => (resolveResult = r));
      result.finally(() => config.onTurnEnd?.(turnActor)).catch(() => {});
      const events = streamTurn(
        baseUrl,
        input,
        actorSessionKey(input.handle.sessionKey),
        authHeaders(),
        abort.signal,
        resolveResult,
      );
      return {
        requestId: input.requestId,
        events,
        result,
        async cancel() {
          abort.abort("cancelled");
          await fetch(
            `${baseUrl}/sessions/${encodeURIComponent(actorSessionKey(input.handle.sessionKey))}/kill`,
            { method: "POST", headers: authHeaders() },
          ).catch(() => {});
        },
        async closeStream() {
          abort.abort("stream closed");
        },
      };
    },

    async *runTurn(input: AcpRuntimeTurnInput): AsyncIterable<AcpRuntimeEvent> {
      yield* runtime.startTurn!(input).events;
    },

    getCapabilities() {
      return { controls: [] };
    },

    async cancel(input) {
      const baseUrl =
        (input.handle as { actorUrl?: string }).actorUrl ?? urlForSession(input.handle.sessionKey);
      await fetch(
        `${baseUrl}/sessions/${encodeURIComponent(actorSessionKey(input.handle.sessionKey))}/kill`,
        { method: "POST", headers: authHeaders() },
      ).catch(() => {});
    },

    async close() {
      /* no-op: the gateway's idle suspender suspends the actor (idle-suspender.ts) */
    },
  };
  return runtime;
}

async function* streamTurn(
  baseUrl: string,
  input: AcpRuntimeTurnInput,
  sessionKey: string,
  headers: Record<string, string>,
  signal: AbortSignal,
  resolveResult: (v: AcpRuntimeTurnResult) => void,
): AsyncIterable<AcpRuntimeEvent> {
  let res: Response;
  try {
    res = await fetch(`${baseUrl}/v1/chat/completions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-OpenClaw-Session-Key": sessionKey,
        ...headers,
      },
      body: JSON.stringify({
        messages: [{ role: "user", content: input.text }],
        stream: true,
      }),
      signal,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    yield { type: "error", message, retryable: err instanceof TypeError };
    resolveResult({ status: "failed", error: { message, retryable: err instanceof TypeError } });
    return;
  }

  if (!res.ok) {
    const message = `Actor HTTP ${res.status} ${res.statusText}`;
    yield { type: "error", message, retryable: res.status >= 500 };
    resolveResult({ status: "failed", error: { message, retryable: res.status >= 500 } });
    return;
  }
  if (!res.body) {
    resolveResult({ status: "completed" });
    return;
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const line of lines) {
        const t = line.trim();
        if (!t.startsWith("data: ")) continue;
        const data = t.slice(6);
        if (data === "[DONE]") {
          yield { type: "done", status: "completed" };
          resolveResult({ status: "completed" });
          return;
        }
        const ev = parseChunk(data);
        if (ev) yield ev;
      }
    }
    resolveResult({ status: "completed" });
  } catch (err) {
    if (signal.aborted) {
      resolveResult({ status: "cancelled", stopReason: "aborted" });
    } else {
      const message = err instanceof Error ? err.message : String(err);
      yield { type: "error", message };
      resolveResult({ status: "failed", error: { message } });
    }
  }
}

function parseChunk(data: string): AcpRuntimeEvent | null {
  try {
    const choice = JSON.parse(data)?.choices?.[0];
    const delta = choice?.delta;
    if (delta?.content) return { type: "text_delta", text: delta.content, tag: "agent_message_chunk" };
    if (delta?.tool_calls?.length) {
      const tc = delta.tool_calls[0];
      return { type: "tool_call", text: tc.function?.name ?? "", toolCallId: tc.id, tag: "tool_call" };
    }
    if (choice?.finish_reason) return { type: "done", status: "completed", stopReason: choice.finish_reason };
    return null;
  } catch {
    return null;
  }
}