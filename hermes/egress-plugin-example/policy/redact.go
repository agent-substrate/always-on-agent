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

// RedactedHeaders are logged by name with their value withheld. The gateway
// logs the full header set, and an actor's tunneled request can carry its own
// upstream credentials - so without this, turning on debug logging copies
// every actor's secrets into the cluster's log pipeline, where they outlive
// the request and are readable by anyone with log access.
//
// The list is an example and is meant to be customized. It covers the header
// names that carry credentials by convention; a deployment whose upstreams
// authenticate with their own - an x-<vendor>-token, a signed-request header
// - has to add them here, because nothing else knows they are secret.
// Redacting a header that turns out to be harmless costs one log field, while
// missing one costs a leaked credential, so err towards adding.
//
// It lives beside the policy rather than with the logging code because it is a
// deployment's own list, and this package is the one a deployment edits. Names
// must be lower-case: the glue folds header names before looking them up.
var RedactedHeaders = map[string]bool{
	"authorization":       true,
	"proxy-authorization": true,
	"cookie":              true,
	"set-cookie":          true,
	"x-api-key":           true,
}
