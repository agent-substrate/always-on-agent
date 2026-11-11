package main

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

// panicSource fails the test if the request path ever tries to mint.
type panicSource struct{ t *testing.T }

func (p panicSource) Token() (*oauth2.Token, error) {
	p.t.Fatal("the request path fetched a token; it must only read the cache")
	return nil, nil
}

type fakeSource struct {
	token  string
	expiry time.Time
}

func (f *fakeSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: f.token, Expiry: f.expiry}, nil
}

type brokenCredential struct{}

func (brokenCredential) Name() string { return "broken" }
func (brokenCredential) Header() (Header, error) {
	return Header{}, fmt.Errorf("nothing usable")
}
