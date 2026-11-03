#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Screenshot the dashboard at a given viewport and print what it measures.

The recording layout has one hard requirement: at 1280x1080, which is the left
two thirds of a 1080p capture, nothing scrolls and nothing sits below the fold.
That is a number, so check it rather than eyeballing a screenshot.

Prints the page's scroll height against the viewport, plus the height of every
visible row, which is what tells you which panel to cut when it doesn't fit.

Needs `pip install websockets` and a Chrome already listening for CDP:

    google-chrome --headless=new --disable-gpu --no-sandbox \\
      --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-preview &
    ./measure.py 'http://127.0.0.1:8099/?layout=demo' 1280 1080 /tmp/shot.png

`--screenshot` on its own is not enough: it fires before the first /api/state
lands, so the panels are still empty and every height comes out short.
"""
import asyncio
import base64
import json
import sys
import urllib.request

import websockets

CDP = "http://127.0.0.1:9222/json"
# Two poll cycles plus slack. The page refreshes every 2s and the first paint is
# always the empty state.
SETTLE_SEC = 6

PROBE = """
JSON.stringify({
  scrollH: document.body.scrollHeight,
  viewportH: window.innerHeight,
  rows: [...document.querySelectorAll('body > .row, body > header')]
          .filter(e => getComputedStyle(e).display !== 'none')
          .map(e => {
            const h = e.querySelector('h2');
            return [h ? h.textContent : e.tagName.toLowerCase(),
                    Math.round(e.getBoundingClientRect().height)];
          })
})
"""


async def main(url, width, height, out):
    tabs = json.load(urllib.request.urlopen(CDP))
    page = next(t for t in tabs if t["type"] == "page")
    async with websockets.connect(page["webSocketDebuggerUrl"], max_size=100_000_000) as ws:
        seq = [0]

        async def send(method, **params):
            seq[0] += 1
            await ws.send(json.dumps({"id": seq[0], "method": method, "params": params}))
            while True:
                msg = json.loads(await ws.recv())
                if msg.get("id") == seq[0]:
                    return msg.get("result", {})

        await send("Emulation.setDeviceMetricsOverride", width=width, height=height,
                   deviceScaleFactor=1, mobile=False)
        await send("Page.enable")
        await send("Page.navigate", url=url)
        await asyncio.sleep(SETTLE_SEC)

        result = await send("Runtime.evaluate", expression=PROBE, returnByValue=True)
        measured = json.loads(result["result"]["value"])
        print(json.dumps(measured))
        if measured["scrollH"] > measured["viewportH"]:
            print(f"OVERFLOW by {measured['scrollH'] - measured['viewportH']}px", file=sys.stderr)

        shot = await send("Page.captureScreenshot", format="png", captureBeyondViewport=False)
        with open(out, "wb") as f:
            f.write(base64.b64decode(shot["data"]))


if __name__ == "__main__":
    if len(sys.argv) != 5:
        raise SystemExit(f"usage: {sys.argv[0]} <url> <width> <height> <out.png>")
    asyncio.run(main(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]))
