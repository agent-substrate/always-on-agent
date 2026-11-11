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

package policy

import "testing"

// Deny has to set the action and not just the reason.
func TestDenySetsTheAction(t *testing.T) {
	if got := Deny("not permitted").Action; got != CalloutDeny {
		t.Errorf("Deny action = %q, want %q", got, CalloutDeny)
	}
}

// The zero CalloutResult must not be an allow. A policy that returns an unset
// result, or one built by a caller who forgot the field, has to fail closed --
// the alternative is a decision nobody made that passes traffic.
func TestZeroCalloutResultIsNotAnAllow(t *testing.T) {
	if (CalloutResult{}).Action == CalloutAllow {
		t.Error("the zero CalloutResult allows, want it to read as a denial")
	}
	if got := Allow().Action; got != CalloutAllow {
		t.Errorf("Allow action = %q, want %q", got, CalloutAllow)
	}
}
