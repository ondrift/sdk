package drift

import "testing"

func TestLinkEnvName(t *testing.T) {
	cases := map[string]string{
		"c12":           "C12",
		"myapp-staging": "MYAPP_STAGING",
		"a1b2":          "A1B2",
	}
	for in, want := range cases {
		if got := linkEnvName(in); got != want {
			t.Errorf("linkEnvName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCallerSlice(t *testing.T) {
	if got := CallerSlice(Request{Headers: map[string]string{"X-Drift-Slice": "app"}}); got != "app" {
		t.Errorf("canonical header: got %q", got)
	}
	// Case-insensitive (the runtime may lowercase header keys).
	if got := CallerSlice(Request{Headers: map[string]string{"x-drift-slice": "app"}}); got != "app" {
		t.Errorf("lowercase header: got %q", got)
	}
	if got := CallerSlice(Request{Headers: map[string]string{"other": "x"}}); got != "" {
		t.Errorf("absent header: want empty, got %q", got)
	}
}

func TestSliceResolveURL(t *testing.T) {
	// Not linked → error before any network I/O.
	if _, err := (SliceClient{name: "c12"}).resolveURL("/api/events"); err == nil {
		t.Error("resolveURL with no DRIFT_LINK_C12_URL: want error, got nil")
	}
	t.Setenv("DRIFT_LINK_C12_URL", "http://canvas.drift-slice-alice-c12.svc.cluster.local:8000")
	got, err := (SliceClient{name: "c12"}).resolveURL("/api/events")
	if err != nil {
		t.Fatalf("resolveURL: unexpected error %v", err)
	}
	if want := "http://canvas.drift-slice-alice-c12.svc.cluster.local:8000/api/events"; got != want {
		t.Errorf("resolveURL = %q, want %q", got, want)
	}
}
