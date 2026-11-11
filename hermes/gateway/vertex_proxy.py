# Copyright 2026 Nous Research
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
"""Internal OpenAI-compatible bridge from Hermes actors to Vertex AI.

Actors authenticate to this gateway with the API-server key they already hold.
The gateway obtains a short-lived Google token from GKE Workload Identity and
replaces that internal credential before forwarding. No Google credential enters
an actor or an actor snapshot.
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import os
import time
from collections.abc import Awaitable, Callable

from aiohttp import ClientSession, ClientTimeout, web


METADATA_TOKEN = (
    "http://metadata.google.internal/computeMetadata/v1/instance/"
    "service-accounts/default/token"
)


def _enabled(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclasses.dataclass(frozen=True)
class VertexProxyConfig:
    enabled: bool = False
    backend: str = "vertex"
    project: str = ""
    location: str = "global"
    model: str = "google/gemini-3.1-pro-preview"
    openai_model: str = "gpt-5.6-luna"
    openai_endpoint: str = "https://api.openai.com/v1/chat/completions"
    timeout: float = 300.0

    @classmethod
    def from_env(cls) -> "VertexProxyConfig":
        return cls(
            enabled=_enabled(os.environ.get("VERTEX_PROXY_ENABLED", "0")),
            backend=os.environ.get("MODEL_PROXY_BACKEND", "vertex").strip().lower(),
            project=os.environ.get("VERTEX_PROJECT", "").strip(),
            location=os.environ.get("VERTEX_LOCATION", "global").strip() or "global",
            model=os.environ.get(
                "VERTEX_MODEL", "google/gemini-3.1-pro-preview"
            ).strip(),
            openai_model=os.environ.get("OPENAI_MODEL", "gpt-5.6-luna").strip(),
            openai_endpoint=os.environ.get(
                "OPENAI_CHAT_ENDPOINT", "https://api.openai.com/v1/chat/completions"
            ).strip(),
            timeout=float(os.environ.get("VERTEX_TIMEOUT_SECONDS", "300")),
        )

    @property
    def endpoint(self) -> str:
        return (
            "https://aiplatform.googleapis.com/v1/projects/"
            f"{self.project}/locations/{self.location}/endpoints/openapi/"
            "chat/completions"
        )

    @property
    def active_model(self) -> str:
        return self.model if self.backend == "vertex" else self.openai_model


class MetadataToken:
    """Small async token cache backed by the GKE metadata server."""

    def __init__(self) -> None:
        self._token = ""
        self._expires_at = 0.0
        self._lock = asyncio.Lock()

    def invalidate(self) -> None:
        self._expires_at = 0.0

    async def get(self, http: ClientSession) -> str:
        if self._token and time.time() < self._expires_at - 60:
            return self._token
        async with self._lock:
            if self._token and time.time() < self._expires_at - 60:
                return self._token
            async with http.get(
                METADATA_TOKEN,
                headers={"Metadata-Flavor": "Google"},
                timeout=ClientTimeout(total=10),
            ) as response:
                if response.status != 200:
                    detail = (await response.text())[:300]
                    raise RuntimeError(
                        f"Workload Identity token failed ({response.status}): {detail}"
                    )
                body = await response.json()
            self._token = str(body.get("access_token") or "")
            if not self._token:
                raise RuntimeError("Workload Identity returned no access token")
            self._expires_at = time.time() + float(body.get("expires_in") or 0)
            return self._token


class VertexProxy:
    def __init__(
        self,
        cfg: VertexProxyConfig,
        trusted_proxy_ips: Callable[[], Awaitable[set[str]]] | None = None,
    ) -> None:
        if cfg.backend not in {"vertex", "openai"}:
            raise ValueError("MODEL_PROXY_BACKEND must be vertex or openai")
        if cfg.backend == "vertex" and not cfg.project:
            raise ValueError("VERTEX_PROXY_ENABLED=1 needs VERTEX_PROJECT")
        self.cfg = cfg
        self.tokens = MetadataToken()
        self.trusted_proxy_ips = trusted_proxy_ips

    async def _authorized(self, req: web.Request) -> bool:
        """Accept the shared key or Substrate's verified actor identity.

        Requests from an actor reach this service through atenet-egress.  Our
        ext-proc plugin removes any actor-supplied identity header, verifies the
        caller certificate, and writes ``x-hermes-actor`` itself for an
        in-cluster destination.  Checking both that header and the egress pod's
        source address is stronger than depending on how a particular OpenAI
        client serializes its API key.
        """
        expected = os.environ.get("API_SERVER_KEY", "")
        supplied = req.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        if expected and supplied == expected:
            return True

        actor = req.headers.get("x-hermes-actor", "").strip()
        if not actor or self.trusted_proxy_ips is None:
            return False
        return (req.remote or "") in await self.trusted_proxy_ips()

    async def _send(self, http: ClientSession, body: dict) -> web.Response:
        if self.cfg.backend == "vertex":
            token = await self.tokens.get(http)
            endpoint = self.cfg.endpoint
        else:
            token = os.environ.get("OPENAI_API_KEY", "").strip()
            if not token:
                raise RuntimeError("MODEL_PROXY_BACKEND=openai needs OPENAI_API_KEY")
            endpoint = self.cfg.openai_endpoint
            # Newer OpenAI reasoning models reject the legacy field that the
            # pinned Hermes OpenAI client emits. Preserve its value while using
            # the current wire name expected by those models.
            if "max_tokens" in body and "max_completion_tokens" not in body:
                body["max_completion_tokens"] = body.pop("max_tokens")
            # Chat Completions rejects reasoning plus function tools for this
            # model. Hermes always exposes tools, so use ordinary tool-capable
            # generation on the rollback path.
            if body.get("tools"):
                body["reasoning_effort"] = "none"
        async with http.post(
            endpoint,
            json=body,
            headers={"Authorization": f"Bearer {token}"},
            timeout=ClientTimeout(total=self.cfg.timeout),
        ) as upstream:
            raw = await upstream.read()
            content_type = upstream.headers.get("Content-Type", "application/json")
            if self.cfg.backend == "vertex" and upstream.status == 401:
                self.tokens.invalidate()
            return web.Response(
                status=upstream.status,
                body=raw,
                headers={"Content-Type": content_type},
            )

    async def chat_completions(self, req: web.Request) -> web.Response:
        if not await self._authorized(req):
            return web.json_response({"error": "unauthorized"}, status=401)
        try:
            body = await req.json()
        except (json.JSONDecodeError, ValueError):
            return web.json_response({"error": "invalid JSON"}, status=400)
        if not isinstance(body, dict):
            return web.json_response({"error": "request must be an object"}, status=400)

        # The template chooses the provider; an actor cannot turn this fixed
        # internal route into arbitrary billable access to other Vertex models.
        body["model"] = self.cfg.active_model
        try:
            return await self._send(req.app["http"], body)
        except Exception as exc:
            return web.json_response(
                {"error": {"message": f"Vertex proxy failed: {exc}"}}, status=502
            )
