#!/usr/bin/env python3
"""Add or remove our ext_proc filter in Substrate's egress Envoy config.

Reads the config on stdin, writes it on stdout. Text markers rather than a YAML
round trip, so Substrate's own comments and formatting survive untouched and a
removal puts the file back exactly as it was.

Our filter goes AFTER Substrate's own ext_proc, so it only ever sees requests
whose actor certificate their filter already validated.

    egress_filter.py add    [--service HOST:PORT]
    egress_filter.py remove
    egress_filter.py status          exit 0 if present, 1 if not
"""

import argparse
import sys

BEGIN = "# >>> hermes egress plugin (managed by hermesctl, do not edit by hand)"
END = "# <<< hermes egress plugin"

FILTER_ANCHOR = "          - name: envoy.filters.http.dynamic_forward_proxy"
CLUSTER_ANCHOR = "  - name: egress_forward_proxy"


def filter_block(service):
    host, _, port = service.partition(":")
    return f"""          {BEGIN}
          # Second ext_proc on the egress chain: per actor destination policy.
          # Placed after Substrate's own so it only sees authenticated actors.
          # Matches their timeout and fail closed posture.
          - name: envoy.filters.http.ext_proc
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
              grpc_service:
                envoy_grpc:
                  cluster_name: hermes_ext_proc
                timeout: 2s
              failure_mode_allow: false
              request_attributes:
              - xds.filter_chain_name
              processing_mode:
                request_header_mode: SEND
                response_header_mode: SKIP
                request_body_mode: NONE
                response_body_mode: NONE
                request_trailer_mode: SKIP
                response_trailer_mode: SKIP
          {END}
"""


def cluster_block(service):
    host, _, port = service.partition(":")
    port = port or "50051"
    return f"""  {BEGIN}
  - name: hermes_ext_proc
    type: STRICT_DNS
    lb_policy: ROUND_ROBIN
    connect_timeout: 1s
    typed_extension_protocol_options:
      envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
        "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
        explicit_http_config:
          http2_protocol_options: {{}}
    load_assignment:
      cluster_name: hermes_ext_proc
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address:
                address: {host}
                port_value: {port}
  {END}
"""


def strip(text):
    """Remove every marked block, leaving the rest byte for byte."""
    out, skipping = [], False
    for line in text.splitlines(keepends=True):
        if BEGIN in line:
            skipping = True
            continue
        if END in line:
            skipping = False
            continue
        if not skipping:
            out.append(line)
    return "".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["add", "remove", "status"])
    ap.add_argument("--service", default="hermes-egress-plugin.hermes-egress.svc.cluster.local:50051")
    args = ap.parse_args()

    text = sys.stdin.read()
    present = BEGIN in text

    if args.mode == "status":
        print("present" if present else "absent", file=sys.stderr)
        sys.exit(0 if present else 1)

    # Always strip first, so "add" is idempotent and never stacks duplicates.
    text = strip(text)

    if args.mode == "add":
        if FILTER_ANCHOR not in text:
            sys.exit(f"cannot find the filter anchor {FILTER_ANCHOR!r}: this Envoy "
                     "config is not shaped the way hermesctl expects, refusing to patch")
        if CLUSTER_ANCHOR not in text:
            sys.exit(f"cannot find the cluster anchor {CLUSTER_ANCHOR!r}: refusing to patch")
        text = text.replace(FILTER_ANCHOR, filter_block(args.service) + FILTER_ANCHOR, 1)
        text = text.replace(CLUSTER_ANCHOR, cluster_block(args.service) + CLUSTER_ANCHOR, 1)

    sys.stdout.write(text)


if __name__ == "__main__":
    main()
