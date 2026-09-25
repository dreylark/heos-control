package api

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBufferedPlaybackContract(t *testing.T) {
	s := testAPI(t)
	s.credentials[0].Scopes = []string{"operator"}
	submit := &captureSubmitter{}
	s.SetCoordinator(submit)
	refs := make([]string, 33)
	counts := make([]int, 33)
	for i := range refs {
		refs[i] = "part"
		counts[i] = 30
	}
	r, _ := json.Marshal(refs)
	n, _ := json.Marshal(counts)
	body := `{"item_refs":` + string(r) + `,"buffered":{"part_tracks":` + string(n) + `,"refill_threshold":10,"max_queue_tracks":100,"retain_previous":5,"max_session_seconds":86400},"queue_mode":"replace","shuffle":false,"repeat":"off","initial_volume":{"unit":"heos","level":0},"takeover":false}`
	req := httptest.NewRequest("POST", "/v1/players/room/playback", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "buffered")
	req.Header.Set("If-Match", `"revision"`)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body)
	}
	if submit.cmd.Buffered == nil || len(submit.cmd.Buffered.PartTracks) != 33 || submit.cmd.Buffered.MaxQueueTracks != 100 {
		t.Fatal(submit.cmd)
	}
	if !strings.Contains(string(submit.request.Body), `"buffered":`) {
		t.Fatal("idempotency body omitted buffer policy")
	}
}

func TestBufferedPlaybackContractRejectsInvalidModeCombinations(t *testing.T) {
	for _, scenario := range []string{"eager_33", "single", "shuffle", "repeat", "missing_policy_field", "over_256"} {
		t.Run(scenario, func(t *testing.T) {
			s := testAPI(t)
			s.credentials[0].Scopes = []string{"operator"}
			submit := &captureSubmitter{}
			s.SetCoordinator(submit)
			refs := make([]string, 33)
			for i := range refs {
				refs[i] = "part"
			}
			policy := map[string]any{"part_tracks": []int{30}, "refill_threshold": 10, "max_queue_tracks": 100, "retain_previous": 5, "max_session_seconds": 86400}
			body := map[string]any{"item_refs": refs, "buffered": policy, "queue_mode": "replace", "shuffle": false, "repeat": "off", "initial_volume": map[string]any{"unit": "heos", "level": 0}, "takeover": false}
			switch scenario {
			case "eager_33":
				delete(body, "buffered")
			case "single":
				delete(body, "item_refs")
				body["item_ref"] = "one"
			case "shuffle":
				body["shuffle"] = true
			case "repeat":
				body["repeat"] = "on_all"
			case "missing_policy_field":
				delete(policy, "max_queue_tracks")
			case "over_256":
				refs = make([]string, 257)
				for i := range refs {
					refs[i] = "part"
				}
				body["item_refs"] = refs
			}
			raw, _ := json.Marshal(body)
			req := httptest.NewRequest("POST", "/v1/players/room/playback", strings.NewReader(string(raw)))
			req.Header.Set("Authorization", "Bearer "+testToken)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", "invalid-buffered")
			req.Header.Set("If-Match", `"revision"`)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, req)
			if w.Code != 400 && w.Code != 422 {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
			if submit.cmd.Kind != "" {
				t.Fatal("invalid shape reached admission")
			}
		})
	}
}
