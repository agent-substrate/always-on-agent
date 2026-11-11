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

"""Telegram Bot API (long-poll) channel for the Substrate gateway.

Why this exists: it is the fastest honest way to show the suspend/resume story to a
room. WhatsApp Cloud needs a public HTTPS callback, a Meta app, a verified business
number and a token dance; Telegram needs a bot token from @BotFather and nothing
else - **no public endpoint at all**, because the gateway calls *out* to
`getUpdates` and holds the connection open. That matters here: a demo runs from a
laptop behind NAT with no tunnel, so we never stand up the unauthenticated public
ingress that would otherwise expose the LLM.

Long-polling does NOT make the gateway stateful in any way that blocks suspension.
The poll loop belongs to the always-on gateway, exactly like the WhatsApp webhook
and the cron clock; the *actors* still hold no connection and suspend freely. One
gateway process multiplexes every chat, so the per-user always-on holder process
that shaped the OpenClaw architecture is not reintroduced.

This module is the transport/protocol half only - config, the getUpdates cursor,
inbound parsing, and the sendMessage/sendChatAction senders. Orchestration (map
chat -> actor, run the turn, send the reply) lives in ingress.py. Hermes itself
stays unmodified.

Contract (Telegram Bot API):
  - inbound:   GET https://api.telegram.org/bot<token>/getUpdates
               ?offset=<last_update_id+1>&timeout=<seconds>
               Long-poll: the server holds the request open until a message
               arrives or `timeout` elapses. `offset` doubles as the ack - asking
               for N+1 permanently drops everything <= N, so it is the de-dup
               mechanism and there is no wamid-style cache to keep.
  - outbound:  POST https://api.telegram.org/bot<token>/sendMessage
               {chat_id, text}

Env (adapter is 'enabled' only when the bot token is set):
  TELEGRAM_BOT_TOKEN         from @BotFather
  TELEGRAM_ALLOWED_CHAT_IDS  comma-separated allowlist; UNSET MEANS ANYONE WHO
                             FINDS THE BOT CAN SPEND YOUR LLM BUDGET
  TELEGRAM_POLL_TIMEOUT      long-poll seconds, default 25
  TELEGRAM_API_BASE          default https://api.telegram.org
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:  # annotations only - keeps the protocol layer importable without aiohttp
    from aiohttp import ClientSession

log = logging.getLogger("gateway.telegram")

DEFAULT_API_BASE = "https://api.telegram.org"
DEFAULT_POLL_TIMEOUT = 25
# Telegram's hard cap on a text message body.
MAX_TEXT_LEN = 4096


@dataclass(frozen=True)
class TelegramConfig:
    bot_token: str
    allowed_chat_ids: frozenset
    poll_timeout: int
    api_base: str

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token)

    @property
    def api_url(self) -> str:
        return f"{self.api_base}/bot{self.bot_token}"

    def allows(self, chat_id: str) -> bool:
        """Empty allowlist = open to all (logged loudly at startup)."""
        return not self.allowed_chat_ids or chat_id in self.allowed_chat_ids

    @classmethod
    def from_env(cls) -> "TelegramConfig":
        raw_ids = os.environ.get("TELEGRAM_ALLOWED_CHAT_IDS", "")
        try:
            timeout = int(os.environ.get("TELEGRAM_POLL_TIMEOUT", DEFAULT_POLL_TIMEOUT))
        except ValueError:
            log.warning("bad TELEGRAM_POLL_TIMEOUT - falling back to %ds", DEFAULT_POLL_TIMEOUT)
            timeout = DEFAULT_POLL_TIMEOUT
        return cls(
            bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", "").strip(),
            allowed_chat_ids=frozenset(p.strip() for p in raw_ids.split(",") if p.strip()),
            poll_timeout=max(1, timeout),
            api_base=os.environ.get("TELEGRAM_API_BASE", DEFAULT_API_BASE).strip(),
        )


@dataclass(frozen=True)
class InboundMessage:
    """One inbound Telegram text message, flattened from the update envelope."""
    chat_id: str          # conversation key (a DM's chat id is the user's id)
    text: str
    update_id: int        # cursor position, acked via the next offset
    from_id: str = ""     # sender user id (differs from chat_id in groups)
    name: str = ""        # sender display name, if present


def parse_updates(payload: dict) -> "tuple[list[InboundMessage], Optional[int]]":
    """Flatten a getUpdates response into text messages + the new cursor.

    Returns (messages, next_offset). next_offset is highest update_id + 1, and is
    returned even when every update was filtered out as uninteresting - otherwise a
    single unsupported update (a sticker, an edit, a join event) would be re-fetched
    forever and wedge the poll loop.
    """
    out: "list[InboundMessage]" = []
    highest: Optional[int] = None
    for upd in payload.get("result", []) or []:
        uid = upd.get("update_id")
        if isinstance(uid, int):
            highest = uid if highest is None else max(highest, uid)
        # Ignore edited_message/channel_post/callbacks - only fresh text drives a turn.
        msg = upd.get("message") or {}
        text = (msg.get("text") or "").strip()
        chat_id = str((msg.get("chat") or {}).get("id", "")).strip()
        if not text or not chat_id:
            continue
        frm = msg.get("from") or {}
        name = " ".join(
            p for p in [frm.get("first_name", ""), frm.get("last_name", "")] if p
        ).strip() or frm.get("username", "")
        out.append(
            InboundMessage(
                chat_id=chat_id,
                text=text,
                update_id=uid if isinstance(uid, int) else -1,
                from_id=str(frm.get("id", "")).strip(),
                name=name,
            )
        )
    return out, (highest + 1 if highest is not None else None)


async def get_updates(
    http: "ClientSession", cfg: TelegramConfig, offset: Optional[int]
) -> "tuple[list[InboundMessage], Optional[int]]":
    """One long-poll round. Raises on transport failure; caller backs off."""
    from aiohttp import ClientTimeout

    params: "dict[str, str]" = {
        "timeout": str(cfg.poll_timeout),
        # We only care about plain messages; narrowing this keeps the cursor moving
        # past update types we would otherwise fetch and discard every round.
        "allowed_updates": '["message"]',
    }
    if offset is not None:
        params["offset"] = str(offset)
    # Client timeout must exceed the server-side long-poll window, or we would
    # cancel every idle round and hammer the API.
    async with http.get(
        f"{cfg.api_url}/getUpdates",
        params=params,
        timeout=ClientTimeout(total=cfg.poll_timeout + 15),
    ) as resp:
        body = await resp.json(content_type=None)
        if resp.status != 200 or not body.get("ok"):
            desc = (body or {}).get("description", "")
            if resp.status == 409:
                # Another poller (or a registered webhook) owns this bot token.
                raise RuntimeError(f"telegram getUpdates conflict - {desc}")
            raise RuntimeError(f"telegram getUpdates {resp.status}: {desc}")
        return parse_updates(body)


async def send_text(http: "ClientSession", cfg: TelegramConfig, chat_id: str, text: str) -> bool:
    """Send a reply. Returns True on ok."""
    from aiohttp import ClientTimeout

    if not text:
        text = "(no response)"
    if len(text) > MAX_TEXT_LEN:
        text = text[: MAX_TEXT_LEN - 1] + "…"
    try:
        async with http.post(
            f"{cfg.api_url}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=ClientTimeout(total=30),
        ) as resp:
            if resp.status == 200:
                log.info("telegram send -> %s ok", chat_id)
                return True
            log.error(
                "telegram send -> %s failed %d: %s",
                chat_id, resp.status, (await resp.text())[:500],
            )
            return False
    except Exception as exc:  # network / timeout - user just sees no reply
        log.error("telegram send -> %s error: %s", chat_id, exc)
        return False


async def send_typing(http: "ClientSession", cfg: TelegramConfig, chat_id: str) -> None:
    """Best-effort 'typing…' indicator.

    A cold restore plus an LLM turn is ~10s. Telegram only shows this for ~5s, which
    is not the whole wait, but silence for ten seconds reads as a broken bot and this
    does not. Failures are ignored on purpose - never let chrome break a turn.
    """
    from aiohttp import ClientTimeout

    try:
        await http.post(
            f"{cfg.api_url}/sendChatAction",
            json={"chat_id": chat_id, "action": "typing"},
            timeout=ClientTimeout(total=10),
        )
    except Exception:
        pass
