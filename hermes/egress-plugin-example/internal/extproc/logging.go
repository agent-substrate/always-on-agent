// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is how a decision reads in the log. Values that could be secret
// are rendered by name only; see policy.RedactedHeaders for the list.

package extproc

import (
	"fmt"
	"log/slog"
	"slices"

	"github.com/agent-substrate/hermes/egress-plugin-example/policy"
)

// requestAttrs describes a request for a decision log line: who asked, for
// what, and with which headers.
func requestAttrs(req *policy.Request) []any {
	return []any{
		slog.String("authority", req.Authority),
		slog.String("method", req.Method),
		slog.String("path", req.Path),
		slog.String("scheme", req.Scheme),
		slog.String("actor", req.ActorSpiffe),
		slog.String("atespace", req.Atespace),
		slog.String("actor_name", req.ActorName),
		slog.Any("headers", loggedHeaders(req.Headers)),
	}
}

// loggedHeaders is a header set on its way into a log record.
// policy.Request.Headers is a plain map; this exists only at the log call
// sites, so that policy code handles headers as an ordinary map and only the
// rendering is special.
type loggedHeaders map[string]string

// LogValue renders the headers as a sorted group, one attribute per header.
func (h loggedHeaders) LogValue() slog.Value {
	keys := make([]string, 0, len(h))
	for key := range h {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	attrs := make([]slog.Attr, 0, len(keys))
	for _, key := range keys {
		if policy.RedactedHeaders[key] {
			attrs = append(attrs, slog.String(key, fmt.Sprintf("<redacted, %d bytes>", len(h[key]))))
			continue
		}
		attrs = append(attrs, slog.String(key, h[key]))
	}
	return slog.GroupValue(attrs...)
}

// loggedCredentials is a decision's credential set on its way into a log
// record. The header names reach the log; the values never do.
type loggedCredentials []policy.CredentialHeader

// LogValue renders the credential set as its header names.
func (c loggedCredentials) LogValue() slog.Value {
	names := make([]string, 0, len(c))
	for _, credential := range c {
		names = append(names, credential.Key)
	}
	return slog.AnyValue(names)
}
