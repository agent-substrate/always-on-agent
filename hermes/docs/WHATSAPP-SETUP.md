# WhatsApp integration for Hermes-on-Substrate

Enables end users to chat with a Hermes agent **over WhatsApp**, where each sender
routes to a suspend/resume actor whose memory persists across suspends.

## How it works (and why it lives in the gateway)

WhatsApp is an **always-on channel**, and a Substrate actor is **suspended most of
the time** - it has no worker to receive Meta's webhooks, isn't publicly
addressable, and takes seconds to cold-restore. So the channel can't live inside
the actor. Instead the **always-on gateway owns the channel** (the same principle
as the cron clock). By default each channel conversation gets an actor; registry
bindings can instead route several conversations to the same actor:

```
 End user's WhatsApp app
        │  (normal free WhatsApp - no setup on the user side)
        ▼
 Meta WhatsApp Cloud API  ──webhook POST──►  hermes-gateway  /whatsapp/webhook
        ▲                                         │  verify HMAC, de-dup, map sender→actor
        │  Graph API POST (reply)                 ▼
        └────────────────────────────  atenet ──► agent-<hash>  (resume→answer→suspend)
```

- **Inbound:** Meta POSTs to the gateway; we verify `X-Hub-Signature-256`, de-dup by
  `wamid`, and return `200` immediately (a cold-restore turn is far slower than
  Meta's webhook timeout, so the turn runs in the background).
- **Routing:** session key `whatsapp:<phone_number_id>:<wa_id>` resolves through
  the optional registry. The resolved routing ID is hashed into `agent-<sha256[:12]>`.
  Without a binding, that ID is the full session key. Registry bindings can share
  an actor, so do not assume they enforce isolation between different senders.
- **Outbound:** the reply is sent back out-of-band via `graph.facebook.com/<ver>/<phone_number_id>/messages`.
- Hermes itself is **unmodified**; the actor stays a pure single-process
  `/v1/chat/completions` server. The Cloud API glue is `gateway/whatsapp_cloud.py`.

The unofficial **Baileys** bridge (`whatsapp.py` in Hermes) is intentionally *not*
used: it's a persistent WhatsApp Web websocket + Node subprocess with account-ban
risk - it can't survive suspend and would have to run always-on, defeating the model.

## 1. Create the Meta app (one-time, ~5 min, free)

At **developers.facebook.com** (log in with a personal Facebook account - no company required):

1. **My Apps → Create App → Other → Business** → name it → (create a Business
   Portfolio if prompted - it's free, your account owns it).
2. Add the **WhatsApp** product → **Set up**. Meta auto-creates a test WhatsApp
   Business Account **and a free test number**.
3. On **WhatsApp → API Setup**, copy: **Temporary access token** (24h),
   **Phone number ID** (the "From" id - not the number). Under **To**, add your
   personal WhatsApp number and confirm the code (test numbers can only message
   verified recipients).
4. **App settings → Basic**: copy **App Secret** and **App ID**.
5. **Verify token**: pick any string (or reuse the one already generated for this PoC).

> The temp token expires in 24h. For a stable deployment, mint a permanent
> **System User** token: Business Settings → Users → System Users → add a token
> with `whatsapp_business_messaging` + `whatsapp_business_management`.

## 2. Load credentials + deploy the gateway

Secrets go in `hermes-secrets` (never committed); non-secrets are Deployment env.

```bash
# From hermes/, with demo/env.sh configured and the gateway deployed.
# Load these environment variables through your secret-management workflow:
# WHATSAPP_CLOUD_ACCESS_TOKEN, WHATSAPP_CLOUD_APP_SECRET,
# WHATSAPP_CLOUD_VERIFY_TOKEN, WHATSAPP_CLOUD_PHONE_NUMBER_ID.
demo/whatsapp_go_live.sh
kubectl logs -n hermes deploy/hermes-gateway | grep -i whatsapp   # expect "channel enabled at /whatsapp/webhook"
```

The channel auto-enables only when `WHATSAPP_CLOUD_PHONE_NUMBER_ID` +
`WHATSAPP_CLOUD_ACCESS_TOKEN` are non-empty; otherwise the gateway runs normally
with the channel off.

The helper merges channel secrets into the existing Secret. Preserve both
`API_SERVER_KEY` and `GATEWAY_INGRESS_KEY`; do not replace the Secret with a
partial manifest. The helper also opens a temporary development tunnel. Use a
managed HTTPS ingress for a persistent deployment.

## 3. Expose the webhook publicly (Meta requires public HTTPS)

Pick one:

- **Dev tunnel (fastest for a demo):** `cloudflared tunnel --url http://localhost:8080`
  after `kubectl -n hermes port-forward deploy/hermes-gateway 8080:8080`. Gives an
  instant `https://<random>.trycloudflare.com` URL.
- **GKE LoadBalancer + managed cert (production-shaped):** add a public Service +
  Ingress with a `ManagedCertificate` on a domain you control, pointing at the
  `hermes-gateway` Service.

## 4. Point Meta at the gateway

In the Meta app: **WhatsApp → Configuration → Edit**:
- **Callback URL:** `https://<public-url>/whatsapp/webhook`
- **Verify token:** the `WHATSAPP_CLOUD_VERIFY_TOKEN` you set
- Save - Meta immediately GETs the URL; the gateway echoes the challenge and the
  webhook shows **Verified**.
- **Subscribe** the app to the **`messages`** field.

## 5. Go live

Message the test number from your personal WhatsApp. Watch the actor wake, answer,
and suspend:

```bash
kubectl-ate get actors -a <your-atespace>  # agent-<hash>: RESUMING→RUNNING→SUSPENDED
```

Send a name in one message and ask for it back in another - the reply proves
per-user state continuity across suspends, now over WhatsApp.

## Testing without going live

`demo/test_whatsapp.py` verifies the full webhook protocol offline (verify
handshake, HMAC accept/reject, parsing, dedup) - no credentials needed. Once the
gateway is reachable, run the same script with `GATEWAY_URL` set to fire a
correctly **signed** Meta-shaped event at it and confirm a fast `200`:

```bash
python3 demo/test_whatsapp.py                     # offline protocol tests
GATEWAY_URL=https://<public-url> \
WHATSAPP_CLOUD_APP_SECRET=<secret> WHATSAPP_CLOUD_VERIFY_TOKEN=<token> \
WHATSAPP_CLOUD_PHONE_NUMBER_ID=<id> WHATSAPP_CLOUD_ACCESS_TOKEN=x \
  python3 demo/test_whatsapp.py                    # + live signed-webhook smoke
```

## Notes / gaps

- **Text only** in this cut. Inbound media, voice notes (STT), and interactive
  buttons are parsed-and-ignored; adding them is a follow-up (Hermes' own adapter
  shows the media-download + ffmpeg opus path).
- **24-hour window:** outside Meta's 24h customer-service window you can only send
  pre-approved template messages, not free-form text. Fine for reactive Q&A; a
  proactive/cron push to a cold user needs a template.
- **Token/rotation:** a temp token stops all replies after 24h - use a System User
  token for anything lasting.
