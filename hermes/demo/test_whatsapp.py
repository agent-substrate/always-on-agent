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

"""Prove the WhatsApp Cloud webhook plumbing - with or without real Meta creds.

OFFLINE (default): unit-tests the protocol layer (gateway/whatsapp_cloud.py) -
the GET verify handshake, X-Hub-Signature-256 HMAC check (accept + reject), and
inbound payload parsing. No network, no credentials, runs anywhere.

LIVE (optional): if GATEWAY_URL is set, it also fires a *correctly signed*
Meta-shaped webhook at the running gateway and asserts a fast 200 EVENT_RECEIVED
- exercising the real dispatch path (which then resumes the actor and, with real
Graph credentials, sends a WhatsApp reply out-of-band).

Usage:
  python3 demo/test_whatsapp.py
  # live smoke against a deployed/tunnelled gateway:
  GATEWAY_URL=https://<public-url> \
  WHATSAPP_CLOUD_APP_SECRET=<secret> WHATSAPP_CLOUD_VERIFY_TOKEN=<token> \
  WHATSAPP_CLOUD_PHONE_NUMBER_ID=<id> WHATSAPP_CLOUD_ACCESS_TOKEN=x \
    python3 demo/test_whatsapp.py
"""
import hashlib
import hmac
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "gateway"))
import whatsapp_cloud as wa  # noqa: E402

APP_SECRET = os.environ.get("WHATSAPP_CLOUD_APP_SECRET", "test-app-secret")
VERIFY_TOKEN = os.environ.get("WHATSAPP_CLOUD_VERIFY_TOKEN", "test-verify-token")
PHONE_ID = os.environ.get("WHATSAPP_CLOUD_PHONE_NUMBER_ID", "1234567890")

CFG = wa.WhatsAppConfig(
    phone_number_id=PHONE_ID,
    access_token=os.environ.get("WHATSAPP_CLOUD_ACCESS_TOKEN", "x"),
    app_secret=APP_SECRET,
    verify_token=VERIFY_TOKEN,
    api_version="v20.0",
    webhook_path=os.environ.get("WHATSAPP_CLOUD_WEBHOOK_PATH", "/whatsapp/webhook"),
)


def sample_payload(text="hello hermes", wamid="wamid.TESTID001", sender="15551234567"):
    return {
        "object": "whatsapp_business_account",
        "entry": [{
            "id": "WABA_ID",
            "changes": [{
                "field": "messages",
                "value": {
                    "messaging_product": "whatsapp",
                    "metadata": {"display_phone_number": "15550001111",
                                 "phone_number_id": PHONE_ID},
                    "contacts": [{"profile": {"name": "Maya"}, "wa_id": sender}],
                    "messages": [{
                        "from": sender, "id": wamid, "timestamp": "1710000000",
                        "type": "text", "text": {"body": text},
                    }],
                },
            }],
        }],
    }


def sign(raw: bytes) -> str:
    return "sha256=" + hmac.new(APP_SECRET.encode(), raw, hashlib.sha256).hexdigest()


def check(name, cond):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
    if not cond:
        check.failed += 1
check.failed = 0


def offline_tests():
    print("OFFLINE protocol tests (gateway/whatsapp_cloud.py):")

    # 1. GET verify handshake
    ok = wa.verify_challenge(CFG, {"hub.mode": "subscribe",
                                   "hub.verify_token": VERIFY_TOKEN,
                                   "hub.challenge": "42"})
    check("verify handshake echoes challenge on correct token", ok == "42")
    bad = wa.verify_challenge(CFG, {"hub.mode": "subscribe",
                                    "hub.verify_token": "wrong",
                                    "hub.challenge": "42"})
    check("verify handshake rejects wrong token", bad is None)

    # 2. HMAC signature
    raw = json.dumps(sample_payload()).encode()
    check("valid X-Hub-Signature-256 accepted", wa.verify_signature(CFG, raw, sign(raw)))
    check("tampered body rejected", not wa.verify_signature(CFG, raw + b"x", sign(raw)))
    check("missing signature rejected", not wa.verify_signature(CFG, raw, None))

    # 3. Parse
    msgs = wa.parse_messages(sample_payload(text="what is my name?"))
    check("parses one inbound text message", len(msgs) == 1)
    m = msgs[0]
    check("extracts sender wa_id", m.from_wa_id == "15551234567")
    check("extracts text body", m.text == "what is my name?")
    check("extracts tenant phone_number_id", m.phone_number_id == PHONE_ID)
    check("extracts profile name", m.name == "Maya")
    # status callbacks / non-text are ignored
    status_only = {"entry": [{"changes": [{"value": {"statuses": [{"status": "read"}]}}]}]}
    check("ignores status callbacks", wa.parse_messages(status_only) == [])

    # 4. Dedup
    d = wa.WamidDedup()
    check("first wamid not seen", not d.seen_before("wamid.A"))
    check("repeat wamid detected", d.seen_before("wamid.A"))


def live_test(url):
    print(f"\nLIVE smoke against {url}:")
    path = CFG.webhook_path
    # GET verify
    q = f"?hub.mode=subscribe&hub.verify_token={VERIFY_TOKEN}&hub.challenge=live-check"
    with urllib.request.urlopen(url + path + q, timeout=15) as r:
        body = r.read().decode()
    check("GET verify returns challenge", r.status == 200 and body == "live-check")
    # POST signed event
    raw = json.dumps(sample_payload(text="e2e via whatsapp webhook")).encode()
    req = urllib.request.Request(url + path, data=raw, method="POST",
                                 headers={"Content-Type": "application/json",
                                          "X-Hub-Signature-256": sign(raw)})
    with urllib.request.urlopen(req, timeout=30) as r:
        body = r.read().decode()
    check("POST signed event returns 200 fast", r.status == 200)
    print("     -> gateway accepted the event and is processing the turn in the")
    print("        background; watch the actor wake via 'kubectl-ate get actors'.")


if __name__ == "__main__":
    offline_tests()
    gw = os.environ.get("GATEWAY_URL")
    if gw:
        live_test(gw.rstrip("/"))
    else:
        print("\n(set GATEWAY_URL=https://<public-url> to also run the live smoke test)")
    print(f"\n{'FAILED' if check.failed else 'ALL PASSED'} "
          f"({check.failed} failure(s))")
    sys.exit(1 if check.failed else 0)
