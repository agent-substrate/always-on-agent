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
"""Local preview for the dashboard's layout, with no cluster and no build.

A layout change in dashboard.js otherwise costs a Cloud Build, a rollout and a
live cluster in the right state before you can see whether it fits. That is four
minutes an iteration, and it needs a burst in flight to see the busy case at all.

This serves the dashboard's static HTML straight out of dashboard.js next to a
canned /api/state, so the same page renders against whatever fleet you want in
about a second. It is a layout harness only: none of the server-side code in
dashboard.js runs, so it says nothing about whether the real thing works.

    # capture a state to replay, or hand-write one
    curl -s http://<dashboard-ip>:8090/api/state > /tmp/cold.json
    ./serve.py /tmp/cold.json &
    ./measure.py 'http://127.0.0.1:8099/?layout=demo' 1280 1080 /tmp/shot.png
"""
import http.server
import os
import socketserver
import sys

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dashboard.js")
PORT = int(os.environ.get("PORT", "8099"))


def html():
    """Pull the page out of the `c.html(`...`)` template literal in dashboard.js.

    Re-read per request so an edit shows up on refresh. The literal has no `${}`
    interpolation in it, which is what makes slicing it out sound rather than a
    trick: everything dynamic on the page arrives via /api/state.
    """
    s = open(SRC).read()
    i = s.index("c.html(`") + len("c.html(`")
    j = s.index("`)\n);", i)
    body = s[i:j]
    if "${" in body:
        raise SystemExit("dashboard.js now interpolates into its HTML; this harness can't serve it")
    return body


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        # The page cache-busts with /api/state?t=..., so match on the prefix.
        if self.path.startswith("/api/state"):
            body, ctype = open(STATE, "rb").read(), "application/json"
        else:
            body, ctype = html().encode(), "text/html; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} <state.json>")
    STATE = sys.argv[1]
    socketserver.TCPServer.allow_reuse_address = True
    print(f"preview on http://127.0.0.1:{PORT}/?layout=demo  (state: {STATE})")
    socketserver.TCPServer(("127.0.0.1", PORT), Handler).serve_forever()
