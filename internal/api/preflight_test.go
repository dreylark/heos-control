package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/control"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

type preflightDevice struct {
	calls int
	state string
}

func (d *preflightDevice) Snapshot() heos.Snapshot {
	v, m := 10, false
	state := d.state
	if state == "" {
		state = "stop"
	}
	return heos.Snapshot{Player: heos.Player{ID: "1", Serial: "serial"}, State: state, Volume: &v, Muted: &m, Connected: true, Verified: true, ObservedAt: time.Unix(100, 0)}
}
func (d *preflightDevice) Refresh(context.Context) error { d.calls++; return nil }
func (*preflightDevice) View() heos.View                 { return heos.View{Connected: true} }
func (d *preflightDevice) PlayerView(heos.ID) heos.View  { return d.View() }
func (*preflightDevice) Queue(context.Context, heos.ID, int, int) (heos.QueuePage, error) {
	panic("unexpected queue read")
}
func (d *preflightDevice) BrowseAll(context.Context, heos.ID, heos.ID) ([]heos.Item, error) {
	d.calls++
	return []heos.Item{{Source: "900", Name: "Gerbera", Type: "heos_server"}}, nil
}
func (*preflightDevice) Browse(context.Context, heos.ID, string, string, int) (heos.CatalogPage, error) {
	panic("unexpected catalog browse")
}
func (*preflightDevice) Resolve(_ heos.ID, ref string) (heos.Item, error) {
	if ref != "track-ref" {
		return heos.Item{}, heos.ErrStaleReference
	}
	return heos.Item{Source: "900", ContainerID: "album", MediaID: "track", Type: "song", Playable: "yes"}, nil
}

type preflightJournal struct {
	readJournal
	active bool
}

func (j preflightJournal) Active(context.Context, string) (journal.Operation, error) {
	if j.active {
		return journal.Operation{ID: "private-operation", Principal: "someone-else"}, nil
	}
	return journal.Operation{}, journal.ErrNotFound
}

const preflightBody = `{"item_ref":"track-ref","queue_mode":"replace","shuffle":true,"repeat":"off","initial_volume":{"unit":"heos","level":10},"takeover":false,"automation":{"target_volume":{"unit":"heos","level":40},"ramp_seconds":300,"duration_seconds":1200,"fade_seconds":30}}`

func TestConcretePlaybackPreflightContractAndAuthorization(t *testing.T) {
	for _, name := range []string{"ready", "paused", "reserved", "paused reserved", "takeover", "reader takeover", "foreign player", "bounds", "overlap", "stale ref", "duplicate", "missing field", "wrong unit", "unknown field"} {
		t.Run(name, func(t *testing.T) {
			s := testAPI(t)
			d := &preflightDevice{}
			if name == "paused" || name == "paused reserved" {
				d.state = "pause"
			}
			reserved := name == "reserved" || name == "paused reserved"
			ceiling := 40
			s.reads = control.NewReads("epoch", []control.Device{{Config: config.Player{Key: "room", Serial: "serial", WritesEnabled: true, VolumeCeiling: &ceiling}, Observer: d, Client: d, Catalogs: map[string]control.Browser{"music": d}}}, []config.Source{{Key: "music", Player: "room", Name: "Gerbera"}})
			s.journal = preflightJournal{active: reserved || name == "takeover"}
			body, status := preflightBody, 200
			switch name {
			case "takeover":
				s.credentials[0].Scopes = append(s.credentials[0].Scopes, "operator")
				body = strings.Replace(body, `"takeover":false`, `"takeover":true`, 1)
			case "reader takeover":
				body = strings.Replace(body, `"takeover":false`, `"takeover":true`, 1)
				status = 403
			case "foreign player":
				s.credentials[0].Players = []string{"other"}
				status = 403
			case "bounds":
				body = strings.Replace(body, `"level":40`, `"level":101`, 1)
				status = 422
			case "overlap":
				body = strings.Replace(body, `"ramp_seconds":300`, `"ramp_seconds":1200`, 1)
				status = 422
			case "stale ref":
				body = strings.Replace(body, "track-ref", "expired", 1)
				status = 409
			case "duplicate":
				body = strings.Replace(body, `"shuffle":true`, `"shuffle":true,"shuffle":false`, 1)
				status = 400
			case "missing field":
				body = strings.Replace(body, `"shuffle":true,`, "", 1)
				status = 400
			case "wrong unit":
				body = strings.ReplaceAll(body, `"heos"`, `"db"`)
				status = 400
			case "unknown field":
				body = strings.Replace(body, `"queue_mode":`, `"surprise":true,"queue_mode":`, 1)
				status = 400
			}
			r := httptest.NewRequest("POST", "/v1/players/room/preflight", strings.NewReader(body))
			r.Header.Set("Authorization", "Bearer "+testToken)
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != status {
				t.Fatalf("%d %s", w.Code, w.Body)
			}
			if status == 200 {
				var got control.Preflight
				if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
				if got.Ready == reserved || got.Player.Revision == "" {
					t.Fatal(got)
				}
				if reserved && (got.Player.ActiveOperation != nil || len(got.Warnings) != 1 || got.Warnings[0] != "device_reserved") {
					t.Fatal("reservation privacy", got)
				}
			}
			if (status == 400 || status == 403 || status == 422) && d.calls != 0 {
				t.Fatal("rejected input reached device")
			}
		})
	}
}
