package drift

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// These exercise Deed.Link's Go bindings against a fake Deed listener
// (httptest.Server + DEED_URL) rather than a real Slice — callDeed has no
// local-dev fallback, so this is the only way to unit-test request/response
// marshaling without standing up a running slice.

func TestLinkBeginOmitsMetadataWhenNotGiven(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		json.NewEncoder(w).Encode(map[string]any{"session_id": "sess123"})
	}))
	defer srv.Close()
	t.Setenv("DEED_URL", srv.URL)

	sessionID, err := Deed.Link.Begin("pub")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if sessionID != "sess123" {
		t.Errorf("session_id = %q, want sess123", sessionID)
	}
	if _, ok := gotBody["metadata"]; ok {
		t.Errorf("metadata should be omitted from the request when not given, got %v", gotBody)
	}
}

func TestLinkBeginIncludesMetadataWhenGiven(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		json.NewEncoder(w).Encode(map[string]any{"session_id": "sess123"})
	}))
	defer srv.Close()
	t.Setenv("DEED_URL", srv.URL)

	if _, err := Deed.Link.Begin("pub", "box-pub-hex"); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if gotBody["metadata"] != "box-pub-hex" {
		t.Errorf("metadata = %v, want box-pub-hex", gotBody["metadata"])
	}
}

func TestLinkSessionInfo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["session_id"] != "sess123" {
			t.Errorf("session_id = %v, want sess123", body["session_id"])
		}
		json.NewEncoder(w).Encode(map[string]any{"new_pubkey": "abc", "metadata": "xyz"})
	}))
	defer srv.Close()
	t.Setenv("DEED_URL", srv.URL)

	info, err := Deed.Link.SessionInfo("sess123")
	if err != nil {
		t.Fatalf("SessionInfo: %v", err)
	}
	if info.NewPubkey != "abc" || info.Metadata != "xyz" {
		t.Errorf("SessionInfo = %+v, want {abc xyz}", info)
	}
}

func TestLinkQR(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["text"] != "sess123" {
			t.Errorf("text = %v, want sess123", body["text"])
		}
		json.NewEncoder(w).Encode(map[string]any{"svg": "<svg></svg>"})
	}))
	defer srv.Close()
	t.Setenv("DEED_URL", srv.URL)

	svg, err := Deed.Link.QR("sess123")
	if err != nil {
		t.Fatalf("QR: %v", err)
	}
	if svg != "<svg></svg>" {
		t.Errorf("svg = %q", svg)
	}
}

func TestLinkAttestOmitsSealedWhenNotGiven(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(204)
	}))
	defer srv.Close()
	t.Setenv("DEED_URL", srv.URL)

	if err := Deed.Link.Attest("id", "sess", "attester", "sig"); err != nil {
		t.Fatalf("Attest: %v", err)
	}
	if _, ok := gotBody["sealed"]; ok {
		t.Errorf("sealed should be omitted from the request when not given, got %v", gotBody)
	}
}

func TestLinkAttestIncludesSealedWhenGiven(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(204)
	}))
	defer srv.Close()
	t.Setenv("DEED_URL", srv.URL)

	if err := Deed.Link.Attest("id", "sess", "attester", "sig", "ciphertext-blob"); err != nil {
		t.Fatalf("Attest: %v", err)
	}
	if gotBody["sealed"] != "ciphertext-blob" {
		t.Errorf("sealed = %v, want ciphertext-blob", gotBody["sealed"])
	}
}

func TestLinkCompleteParsesSealed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"status": "attested", "identity": "id123", "sealed": "ciphertext-blob",
		})
	}))
	defer srv.Close()
	t.Setenv("DEED_URL", srv.URL)

	status, err := Deed.Link.Complete("sess")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if status.Status != "attested" || status.Identity != "id123" || status.Sealed != "ciphertext-blob" {
		t.Errorf("Complete = %+v", status)
	}
}

func TestLinkCompleteOmitsSealedWhenAbsent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"status": "pending"})
	}))
	defer srv.Close()
	t.Setenv("DEED_URL", srv.URL)

	status, err := Deed.Link.Complete("sess")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if status.Status != "pending" || status.Sealed != "" {
		t.Errorf("Complete = %+v, want pending/empty sealed", status)
	}
}
