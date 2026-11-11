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

// CalloutAction is the allow or deny decision made by the extproc.
type CalloutAction string

const (
	CalloutAllow CalloutAction = "Allow"
	CalloutDeny  CalloutAction = "Deny"
)

// CalloutResult is a Policy's answer for one request.
type CalloutResult struct {
	// Action is what to do with the request.
	Action CalloutAction

	// Reason explains why a request is denied.
	Reason string

	// Credentials are headers to set on the request on its way upstream.
	//
	// A slice because one upstream can need more than one header to accept a
	// call: an API key beside a bearer token, or a tenant header the token is
	// only valid under.
	Credentials []CredentialHeader
}

// CredentialHeader is one header the gateway sets on an outbound request.
type CredentialHeader struct {
	// Key is the header name, e.g. "authorization". It must not be a
	// pseudo-header.
	Key string

	// Value is the secret. Never log it.
	Value string
}

// Allow permits a request, injecting nothing.
func Allow() CalloutResult { return CalloutResult{Action: CalloutAllow} }

// AllowWithCredential permits a request and sets one header on it upstream.
func AllowWithCredential(name, value string) CalloutResult {
	return AllowWithCredentials(CredentialHeader{Key: name, Value: value})
}

// AllowWithCredentials permits a request and sets each header on it upstream.
func AllowWithCredentials(credentials ...CredentialHeader) CalloutResult {
	return CalloutResult{Action: CalloutAllow, Credentials: credentials}
}

// Deny refuses a request.
func Deny(reason string) CalloutResult {
	return CalloutResult{Action: CalloutDeny, Reason: reason}
}
