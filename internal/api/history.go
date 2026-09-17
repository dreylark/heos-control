package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/control"
	"github.com/dreylark/heos-control/internal/journal"
)

const historyCursorMax = 2048

var historyTimestampPattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.[0-9]{1,9})?(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])$`)

// The cursor describes traversal only. Authorization is derived afresh from the
// credential and enforced by the journal before ordering and limiting its rows.
type historyCursor struct {
	Version  int             `json:"version"`
	Filters  historyFilters  `json:"filters"`
	Position historyPosition `json:"position"`
}

type historyFilters struct {
	Player        string `json:"player"`
	State         string `json:"state"`
	Kind          string `json:"kind"`
	Delivery      string `json:"delivery"`
	CreatedFrom   string `json:"created_from"`
	CreatedBefore string `json:"created_before"`
	Limit         int    `json:"limit"`
}

type historyPosition struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

func (s *Server) ListOperations(ctx context.Context, r ListOperationsRequestObject) (ListOperationsResponseObject, error) {
	p := identity(ctx)
	params := r.Params
	player := value(params.Player, "")
	if player != "" && !p.allows(player) {
		return nil, &apiFailure{403, "forbidden", false}
	}
	// Generated bindings may treat an empty optional value as absent. The public
	// contract distinguishes omission from an explicitly empty query parameter.
	rawQuery := ctx.Value(requestKey{}).(*http.Request).URL.Query()
	for _, values := range rawQuery {
		if values[0] == "" {
			return nil, &apiFailure{400, "invalid_request", false}
		}
	}
	// oapi-codegen/runtime also accepts date-only strings for time.Time. History
	// filters require an actual RFC 3339 instant, including its timezone.
	for _, name := range []string{"created_from", "created_before"} {
		if rawQuery.Has(name) && !historyTimestampPattern.MatchString(rawQuery.Get(name)) {
			return nil, &apiFailure{400, "invalid_request", false}
		}
	}
	q := journal.HistoryQuery{Principal: p.Principal, Operator: p.operator(), Players: p.Players,
		Player: player, State: journal.State(value(params.State, "")), Kind: string(value(params.Kind, "")),
		Delivery: string(value(params.Delivery, "")), CreatedFrom: utcTime(params.CreatedFrom), CreatedBefore: utcTime(params.CreatedBefore), Limit: value(params.Limit, 50)}
	if q.CreatedFrom != nil && q.CreatedBefore != nil && !q.CreatedFrom.Before(*q.CreatedBefore) {
		return nil, &apiFailure{400, "invalid_request", false}
	}
	filters := historyFilters{Player: q.Player, State: string(q.State), Kind: q.Kind, Delivery: q.Delivery,
		CreatedFrom: historyTime(q.CreatedFrom), CreatedBefore: historyTime(q.CreatedBefore), Limit: q.Limit}
	if params.Cursor != nil {
		position, err := decodeHistoryCursor(*params.Cursor, filters)
		if err != nil {
			return nil, &apiFailure{400, "invalid_request", false}
		}
		q.Before = &position
	}
	page, err := s.journal.List(ctx, q)
	if err != nil {
		return nil, err
	}
	result := OperationPage{Items: make([]Operation, 0, len(page.Items))}
	for _, op := range page.Items {
		result.Items = append(result.Items, control.ProjectOperation(op))
	}
	if page.Next != nil {
		cursor, err := encodeHistoryCursor(*page.Next, filters)
		if err != nil {
			return nil, err
		}
		result.NextCursor = &cursor
	}
	return ListOperations200JSONResponse{Body: result}, nil
}

func utcTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	utc := t.UTC()
	return &utc
}

func historyTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func encodeHistoryCursor(position journal.HistoryPosition, filters historyFilters) (string, error) {
	b, err := json.Marshal(historyCursor{Version: 1, Filters: filters, Position: historyPosition{CreatedAt: position.CreatedAt.UTC(), ID: position.ID}})
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(b)
	if len(encoded) > historyCursorMax {
		return "", errors.New("history cursor exceeds bound")
	}
	return encoded, nil
}

func decodeHistoryCursor(encoded string, filters historyFilters) (journal.HistoryPosition, error) {
	invalid := errors.New("invalid history cursor")
	if len(encoded) == 0 || len(encoded) > historyCursorMax {
		return journal.HistoryPosition{}, invalid
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil || config.UniqueJSON(b) != nil {
		return journal.HistoryPosition{}, invalid
	}
	var c historyCursor
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(&c) != nil || c.Version != 1 || c.Filters != filters || c.Position.CreatedAt.IsZero() ||
		c.Position.CreatedAt.Nanosecond()%1000 != 0 || !historyIDValid(c.Position.ID) {
		return journal.HistoryPosition{}, invalid
	}
	return journal.HistoryPosition{CreatedAt: c.Position.CreatedAt.UTC(), ID: c.Position.ID}, nil
}

func historyIDValid(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' {
			continue
		}
		return false
	}
	return true
}
