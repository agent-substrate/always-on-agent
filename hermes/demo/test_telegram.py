#!/usr/bin/env python3
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

"""Prove the Telegram long-poll plumbing - with or without a real bot token.

OFFLINE (default): unit-tests the protocol layer (gateway/telegram.py) - update
parsing, the cursor/ack arithmetic that stops the poll loop wedging, the chat
allowlist, and text truncation. No network, no credentials, runs anywhere.

LIVE (optional): if TELEGRAM_BOT_TOKEN is set, it calls getMe to prove the token
works, then does one real getUpdates round against Telegram's servers. It sends
nothing, so it is safe to run against a production bot.

Note there is no signature/verify test as there is for WhatsApp: Telegram
long-poll has no inbound authentication problem to solve, because nothing inbound
is accepted at all - the gateway dials out. That is the whole reason this channel
needs no public endpoint. The security control is the chat allowlist instead.

Usage:
  python3 demo/test_telegram.py
  TELEGRAM_BOT_TOKEN=<token> python3 demo/test_telegram.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "gateway"))
import telegram as tg  # noqa: E402

CFG = tg.TelegramConfig(
    bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", "test-token"),
    allowed_chat_ids=frozenset(),
    poll_timeout=25,
    api_base=tg.DEFAULT_API_BASE,
)


def update(update_id, text="hello hermes", chat_id=4242, first="Maya", last="Wang"):
    return {
        "update_id": update_id,
        "message": {
            "message_id": update_id * 10,
            "from": {"id": 777, "first_name": first, "last_name": last,
                     "username": "demo-user"},
            "chat": {"id": chat_id, "type": "private"},
            "date": 1785263293,
            "text": text,
        },
    }


def check(name, cond):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
    if not cond:
        check.failed += 1
check.failed = 0


def offline_tests():
    print("OFFLINE protocol tests (gateway/telegram.py):")

    # 1. Parse
    msgs, offset = tg.parse_updates({"result": [update(101, "what is my name?")]})
    check("parses one inbound text message", len(msgs) == 1)
    m = msgs[0]
    check("extracts chat id", m.chat_id == "4242")
    check("extracts text", m.text == "what is my name?")
    check("extracts sender id", m.from_id == "777")
    check("extracts display name", m.name == "Maya Wang")
    check("cursor is highest update_id + 1", offset == 102)

    # 2. Cursor still advances past updates we ignore - otherwise one sticker
    #    would be refetched forever and the poll loop would never progress.
    sticker = {"update_id": 200, "message": {"chat": {"id": 1}, "sticker": {}}}
    edited = {"update_id": 201, "edited_message": {"chat": {"id": 1}, "text": "x"}}
    msgs, offset = tg.parse_updates({"result": [sticker, edited]})
    check("ignores non-text and edited updates", msgs == [])
    check("cursor advances past ignored updates", offset == 202)

    # 3. Empty poll (the common case) must not move or reset the cursor.
    msgs, offset = tg.parse_updates({"result": []})
    check("empty poll yields no messages", msgs == [])
    check("empty poll yields no cursor change", offset is None)

    # 4. Mixed batch keeps the max, not the last.
    msgs, offset = tg.parse_updates({"result": [update(310), update(309)]})
    check("batch parses every text message", len(msgs) == 2)
    check("cursor takes the highest id in the batch", offset == 311)

    # 5. Allowlist
    open_cfg = CFG
    check("empty allowlist admits any chat", open_cfg.allows("999"))
    locked = tg.TelegramConfig(bot_token="t", allowed_chat_ids=frozenset({"4242"}),
                               poll_timeout=25, api_base=tg.DEFAULT_API_BASE)
    check("allowlist admits a listed chat", locked.allows("4242"))
    check("allowlist rejects an unlisted chat", not locked.allows("9999"))

    # 6. Config
    check("enabled requires a bot token", locked.enabled)
    check("no token means disabled",
          not tg.TelegramConfig(bot_token="", allowed_chat_ids=frozenset(),
                                poll_timeout=25, api_base="x").enabled)
    check("api url embeds the token", locked.api_url.endswith("/bott"))

    # 7. Env parsing tolerates spacing and junk timeouts
    os.environ["TELEGRAM_ALLOWED_CHAT_IDS"] = " 1, 2 ,, 3 "
    os.environ["TELEGRAM_POLL_TIMEOUT"] = "not-a-number"
    os.environ["TELEGRAM_BOT_TOKEN"] = "envtoken"
    env_cfg = tg.TelegramConfig.from_env()
    check("allowlist splits and trims", env_cfg.allowed_chat_ids == frozenset({"1", "2", "3"}))
    check("bad poll timeout falls back to default",
          env_cfg.poll_timeout == tg.DEFAULT_POLL_TIMEOUT)
    for k in ("TELEGRAM_ALLOWED_CHAT_IDS", "TELEGRAM_POLL_TIMEOUT", "TELEGRAM_BOT_TOKEN"):
        del os.environ[k]


async def live_test(token):
    from aiohttp import ClientSession, ClientTimeout

    cfg = tg.TelegramConfig(bot_token=token, allowed_chat_ids=frozenset(),
                            poll_timeout=1, api_base=tg.DEFAULT_API_BASE)
    print("\nLIVE check against api.telegram.org:")
    async with ClientSession() as http:
        async with http.get(f"{cfg.api_url}/getMe",
                            timeout=ClientTimeout(total=20)) as r:
            body = await r.json(content_type=None)
        ok = r.status == 200 and body.get("ok")
        check("getMe accepts the bot token", bool(ok))
        if ok:
            me = body["result"]
            print(f"     -> bot @{me.get('username')} ({me.get('first_name')})")
        try:
            msgs, offset = await tg.get_updates(http, cfg, None)
            check("getUpdates long-poll round succeeds", True)
            print(f"     -> {len(msgs)} pending message(s), cursor {offset}")
        except Exception as exc:
            check(f"getUpdates long-poll round succeeds ({exc})", False)


if __name__ == "__main__":
    offline_tests()
    tok = os.environ.get("TELEGRAM_BOT_TOKEN")
    if tok:
        asyncio.run(live_test(tok))
    else:
        print("\n(set TELEGRAM_BOT_TOKEN to also run the live getMe/getUpdates check)")
    print(f"\n{'FAILED ' + str(check.failed) if check.failed else 'ALL PASS'}")
    sys.exit(1 if check.failed else 0)
