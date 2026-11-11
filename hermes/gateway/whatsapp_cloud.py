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

"""WhatsApp Business Cloud API protocol layer for the Substrate gateway.

This is the *channel glue* that lets Meta's official WhatsApp Cloud API drive our
per-conversation suspend/resume actors. It deliberately lives in the always-on
gateway, NOT inside an actor: a suspended actor has no worker to receive Meta's
webhooks, and Meta needs a single stable public URL with a fast verify handshake.
So the gateway owns the channel (exactly as it owns the cron clock) and each
WhatsApp sender is mapped to its own actor, which resumes on demand, answers, and
suspends again.

This module is the transport/protocol half only - webhook verification, HMAC
signature checking, inbound parsing, wamid de-duplication, and the Graph API text
sender. Orchestration (map sender -> actor, run the turn, send the reply) lives in
ingress.py, which calls these functions. Hermes itself stays unmodified; we do not
import or run Hermes' in-process platform loop.

Contract references (verified against NousResearch/hermes-agent
gateway/platforms/whatsapp_cloud.py):
  - inbound webhook:  GET  verify handshake (hub.mode/hub.verify_token/hub.challenge)
                      POST payload signed with X-Hub-Signature-256 = HMAC-SHA256(app_secret, raw_body)
  - outbound:         POST https://graph.facebook.com/<ver>/<phone_number_id>/messages
                      Authorization: Bearer <access_token>

Env (all optional; adapter is 'enabled' only when phone_number_id + access_token are set):
  WHATSAPP_CLOUD_PHONE_NUMBER_ID   Graph URL path component (the "From" id, not the number)
  WHATSAPP_CLOUD_ACCESS_TOKEN      System User / temporary token (Bearer)
  WHATSAPP_CLOUD_APP_SECRET        HMAC key for X-Hub-Signature-256 (verification skipped if unset)
  WHATSAPP_CLOUD_VERIFY_TOKEN      shared secret for the GET verify handshake
  WHATSAPP_CLOUD_API_VERSION       default v20.0
  WHATSAPP_CLOUD_WEBHOOK_PATH      default /whatsapp/webhook
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:  # annotations only - keeps the protocol layer importable without aiohttp
    from aiohttp import ClientSession

log = logging.getLogger("gateway.whatsapp")

GRAPH_API_BASE = "https://graph.facebook.com"
DEFAULT_API_VERSION = "v20.0"
DEFAULT_WEBHOOK_PATH = "/whatsapp/webhook"
# WhatsApp renders up to ~65K but long messages are unreadable on mobile; Meta's
# text body hard cap is 4096. Truncate to stay well-formed.
MAX_TEXT_LEN = 4096
# Meta retries a failed webhook for up to 7 days; the real risk is duplicate
# delivery within minutes. A bounded FIFO of recent wamids is plenty.
WAMID_CACHE_SIZE = 5000


@dataclass(frozen=True)
class WhatsAppConfig:
    phone_number_id: str
    access_token: str
    app_secret: str
    verify_token: str
    api_version: str
    webhook_path: str

    @property
    def enabled(self) -> bool:
        """Adapter is live only when we can both receive and send."""
        return bool(self.phone_number_id and self.access_token)

    @classmethod
    def from_env(cls) -> "WhatsAppConfig":
        return cls(
            phone_number_id=os.environ.get("WHATSAPP_CLOUD_PHONE_NUMBER_ID", "").strip(),
            access_token=os.environ.get("WHATSAPP_CLOUD_ACCESS_TOKEN", "").strip(),
            app_secret=os.environ.get("WHATSAPP_CLOUD_APP_SECRET", "").strip(),
            verify_token=os.environ.get("WHATSAPP_CLOUD_VERIFY_TOKEN", "").strip(),
            api_version=os.environ.get("WHATSAPP_CLOUD_API_VERSION", DEFAULT_API_VERSION).strip(),
            webhook_path=os.environ.get("WHATSAPP_CLOUD_WEBHOOK_PATH", DEFAULT_WEBHOOK_PATH).strip(),
        )


@dataclass(frozen=True)
class InboundMessage:
    """One inbound WhatsApp text message, flattened from Meta's nested payload."""
    from_wa_id: str          # sender's WhatsApp id (their phone number, digits only)
    phone_number_id: str     # the business number that received it (tenant key)
    text: str
    wamid: str               # Meta's message id, used for de-duplication
    name: str = ""           # sender profile name, if present


class WamidDedup:
    """Bounded FIFO set of seen message ids - guards against Meta's webhook retries.

    Meta re-delivers if we don't 200 fast enough; without this, a slow cold-restore
    turn could be triggered two or three times for one user message.
    """

    def __init__(self, capacity: int = WAMID_CACHE_SIZE) -> None:
        self._cap = capacity
        self._seen: "OrderedDict[str, None]" = OrderedDict()

    def seen_before(self, wamid: str) -> bool:
        """Record wamid; return True if it was already present (i.e. a duplicate)."""
        if not wamid:
            return False
        if wamid in self._seen:
            self._seen.move_to_end(wamid)
            return True
        self._seen[wamid] = None
        if len(self._seen) > self._cap:
            self._seen.popitem(last=False)
        return False


def verify_challenge(cfg: WhatsAppConfig, query: "dict[str, str]") -> Optional[str]:
    """Handle Meta's GET verify handshake.

    Returns the hub.challenge string to echo back (HTTP 200) when the mode is
    'subscribe' and the token matches; None otherwise (caller returns 403).
    """
    mode = query.get("hub.mode")
    token = query.get("hub.verify_token")
    challenge = query.get("hub.challenge")
    if mode == "subscribe" and token and hmac.compare_digest(token, cfg.verify_token):
        return challenge or ""
    return None


def verify_signature(cfg: WhatsAppConfig, raw_body: bytes, signature_header: Optional[str]) -> bool:
    """Constant-time X-Hub-Signature-256 check over the *raw* request body.

    Header form: 'sha256=<hex>'. If no app secret is configured we cannot verify,
    so we accept and log - configure WHATSAPP_CLOUD_APP_SECRET to enforce.
    """
    if not cfg.app_secret:
        log.warning("WHATSAPP_CLOUD_APP_SECRET unset - skipping webhook signature verification")
        return True
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected = hmac.new(cfg.app_secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    provided = signature_header.split("=", 1)[1].strip()
    return hmac.compare_digest(expected, provided)


def parse_messages(payload: dict) -> "list[InboundMessage]":
    """Flatten Meta's webhook payload into inbound *text* messages.

    Non-text messages (media, reactions, button replies) and status callbacks
    (delivered/read receipts) are ignored - we only drive a chat turn on real text.
    """
    out: "list[InboundMessage]" = []
    for entry in payload.get("entry", []) or []:
        for change in entry.get("changes", []) or []:
            value = change.get("value", {}) or {}
            phone_number_id = str(
                value.get("metadata", {}).get("phone_number_id", "")
            ).strip()
            # Map wa_id -> profile name from the contacts array, when present.
            names = {
                str(c.get("wa_id", "")): (c.get("profile", {}) or {}).get("name", "")
                for c in (value.get("contacts", []) or [])
            }
            for msg in value.get("messages", []) or []:
                if msg.get("type") != "text":
                    continue
                body = (msg.get("text", {}) or {}).get("body", "")
                if not body:
                    continue
                sender = str(msg.get("from", "")).strip()
                out.append(
                    InboundMessage(
                        from_wa_id=sender,
                        phone_number_id=phone_number_id,
                        text=body,
                        wamid=str(msg.get("id", "")),
                        name=names.get(sender, ""),
                    )
                )
    return out


async def send_text(http: "ClientSession", cfg: WhatsAppConfig, to_wa_id: str, text: str) -> bool:
    """Send a text reply via the Graph API. Returns True on HTTP 200."""
    from aiohttp import ClientTimeout

    if not text:
        text = "(no response)"
    if len(text) > MAX_TEXT_LEN:
        text = text[: MAX_TEXT_LEN - 1] + "…"  # ellipsis
    url = f"{GRAPH_API_BASE}/{cfg.api_version}/{cfg.phone_number_id}/messages"
    body = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to_wa_id,
        "type": "text",
        "text": {"preview_url": False, "body": text},
    }
    headers = {
        "Authorization": f"Bearer {cfg.access_token}",
        "Content-Type": "application/json",
    }
    try:
        async with http.post(
            url, json=body, headers=headers, timeout=ClientTimeout(total=30)
        ) as resp:
            raw = await resp.text()
            if resp.status == 200:
                log.info("whatsapp send -> %s ok", to_wa_id)
                return True
            log.error("whatsapp send -> %s failed %d: %s", to_wa_id, resp.status, raw[:500])
            return False
    except Exception as exc:  # network / timeout - log and drop (Meta user just sees no reply)
        log.error("whatsapp send -> %s error: %s", to_wa_id, exc)
        return False
