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

"""Client TLS for reaching actors through atenet-router on Substrate v1.

From v1 the router accepts ingress only over HTTPS, from a client presenting a
Substrate pod identity certificate whose SPIFFE ID the install lists in
--ingress-allowed-spiffe-ids. The gateway gets both halves from one projected
volume (see k8s/hermes-gateway.yaml):

  credential-bundle.pem      this pod's identity key and certificate chain,
                             from the podidentity signer
  router-trust-bundle.pem    the CA that signs the router's serving
                             certificate, from the servicedns signer

The kubelet rotates the certificate in place well before it expires, so the
context is rebuilt whenever either file changes rather than loaded once at
start. A gateway that has run for longer than one certificate lifetime would
otherwise start failing every turn with a handshake error.
"""
from __future__ import annotations

import os
import ssl

DEFAULT_BUNDLE = "/run/router-client/credential-bundle.pem"
DEFAULT_TRUST = "/run/router-client/router-trust-bundle.pem"


class RouterTLS:
    def __init__(self, bundle: str | None = None, trust: str | None = None):
        self.bundle = bundle or os.environ.get("ROUTER_CLIENT_BUNDLE", DEFAULT_BUNDLE)
        self.trust = trust or os.environ.get("ROUTER_TRUST_BUNDLE", DEFAULT_TRUST)
        self._key: tuple[float, float] | None = None
        self._ctx: ssl.SSLContext | None = None

    def context(self) -> ssl.SSLContext:
        key = (os.stat(self.bundle).st_mtime, os.stat(self.trust).st_mtime)
        if self._ctx is None or key != self._key:
            ctx = ssl.create_default_context(cafile=self.trust)
            # The bundle holds the private key and the chain in one PEM file,
            # which load_cert_chain reads when no separate key file is given.
            ctx.load_cert_chain(self.bundle)
            self._ctx, self._key = ctx, key
        return self._ctx
