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

package extproc

import (
	"strings"

	"google.golang.org/protobuf/types/known/structpb"
)

// actorFilterStateName is the filter-state object the gateway publishes the
// calling actor under.
const actorFilterStateName = "dev.ate.actor.identity"

// actorAttribute is the CEL expression the gateway's ext_proc filter has to
// list under request_attributes.
const actorAttribute = "filter_state['" + actorFilterStateName + "']"

// The gateway published these before
// https://github.com/agent-substrate/substrate/commit/e748574
// (merged 2026-09-01). Accepted so one plugin build spans the rename, which
// means the two can be upgraded in either order.
//
// Delete once no install this runs against predates that commit. The cluster
// that still does is reinstalled off the 2026-09-04 build for the second
// validation pass, so the window closes the week of 2026-09-15.
const legacyActorFilterStateName = "ate.actor.identity"
const legacyActorAttribute = "filter_state['" + legacyActorFilterStateName + "']"

// actorFromAttributes returns the actor the gateway asserted in filter state,
// or "" when it asserted none. The pre-#1198 spellings are checked last, so a
// gateway publishing both is read under the current name.
func actorFromAttributes(attr map[string]*structpb.Struct) string {
	for _, attrs := range attr {
		fields := attrs.GetFields()
		for _, key := range []string{
			actorAttribute, actorFilterStateName,
			legacyActorAttribute, legacyActorFilterStateName,
		} {
			if v, ok := fields[key]; ok {
				return v.GetStringValue()
			}
		}
	}
	return ""
}

// actorURIPrefix is everything before the atespace in an actor's SPIFFE ID.
// The trust domain is part of it on purpose: it is what separates an actor
// certificate from every other identity in the cluster, so matching without it
// would accept a name-shaped path under some other authority.
const actorURIPrefix = "spiffe://substrate-actor.local/atespace/"

// actorSeparator divides the atespace from the actor name.
const actorSeparator = "/actor/"

// parseActor splits an actor's SPIFFE ID into its atespace and name, returning
// "", "" for anything that is not exactly one.
func parseActor(uri string) (atespace, name string) {
	rest, ok := strings.CutPrefix(uri, actorURIPrefix)
	if !ok {
		return "", ""
	}
	atespace, name, ok = strings.Cut(rest, actorSeparator)
	if !ok || atespace == "" || name == "" {
		return "", ""
	}
	// Kubernetes object names cannot contain a slash, so anything further down
	// the path is a shape this does not know how to read.
	if strings.Contains(atespace, "/") || strings.Contains(name, "/") {
		return "", ""
	}
	return atespace, name
}
