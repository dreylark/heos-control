// qualify runs only against the disposable checkout database and loopback HEOS.
// It measures a separate non-race service process, not the test runner.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/control"
)

const directory = ".local/qualification"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func save(name string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(directory, name), append(b, '\n'), 0600)
}

func docker(input io.Reader, output io.Writer, args ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "-f", "compose.yaml", "--project-name", "heos-control-dev", "exec", "-T", "postgres"}, args...)...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = input, output, os.Stderr
	return cmd.Run()
}

type harness struct {
	cfg        config.Config
	client     *http.Client
	url, token string
	cmd        *exec.Cmd
	done       chan error
	log        *os.File
}

func (h *harness) start(writes bool) error {
	h.cfg.Players[0].WritesEnabled = writes
	if err := h.cfg.Validate(); err != nil {
		return err
	}
	if _, err := config.LoadCredentials(h.cfg.CredentialsFile, h.cfg.Players); err != nil {
		return err
	}
	if err := save("runtime.json", h.cfg); err != nil {
		return err
	}
	log, err := os.OpenFile(filepath.Join(directory, "service.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	h.log = log
	h.cmd = exec.Command("./bin/heos-control", "serve", "-config", filepath.Join(directory, "runtime.json"))
	h.cmd.Stdout, h.cmd.Stderr = log, log
	if err := h.cmd.Start(); err != nil {
		return errors.Join(err, log.Close())
	}
	h.done = make(chan error, 1)
	go func() { h.done <- h.cmd.Wait() }()
	return h.await(func() bool { status, _, _, _ := h.request("GET", "/readyz", "", "", nil); return status == 200 })
}
func (h *harness) stop(crash bool) error {
	if h.cmd == nil {
		return nil
	}
	sig := syscall.SIGTERM
	if crash {
		sig = syscall.SIGKILL
	}
	_ = h.cmd.Process.Signal(sig)
	var err error
	select {
	case err = <-h.done:
	case <-time.After(28 * time.Second):
		_ = h.cmd.Process.Kill()
		<-h.done
		err = errors.New("shutdown exceeded 28 seconds")
	}
	closeErr := h.log.Close()
	h.cmd = nil
	if crash {
		return closeErr
	}
	return errors.Join(err, closeErr)
}
func (h *harness) request(method, path, key, match string, body []byte) (int, []byte, string, error) {
	r, err := http.NewRequest(method, h.url+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, "", err
	}
	r.Header.Set("Authorization", "Bearer "+h.token)
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	if match != "" {
		r.Header.Set("If-Match", match)
	}
	response, err := h.client.Do(r)
	if err != nil {
		return 0, nil, "", err
	}
	defer func() { _ = response.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	return response.StatusCode, b, response.Header.Get("ETag"), err
}
func (h *harness) await(f func() bool) error {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return errors.New("qualification condition timed out; inspect .local/qualification/service.log")
}
func (h *harness) fresh() (string, error) {
	var match string
	err := h.await(func() bool {
		status, b, etag, _ := h.request("GET", "/v1/players/fixture", "", "", nil)
		var p control.Player
		if status != 200 || json.Unmarshal(b, &p) != nil || p.Stale {
			return false
		}
		match = etag
		return true
	})
	return match, err
}

type admitted struct {
	ID, Match string
	Body      []byte
}

func (h *harness) play(key string, duration int) (admitted, error) {
	match, err := h.fresh()
	if err != nil {
		return admitted{}, err
	}
	status, b, _, err := h.request("GET", "/v1/sources/music/items", "", "", nil)
	var items control.Items
	if err != nil || status != 200 || json.Unmarshal(b, &items) != nil || len(items.Items) != 1 {
		return admitted{}, errors.New("fixture catalog unavailable")
	}
	body, _ := json.Marshal(map[string]any{"item_ref": items.Items[0].Ref, "queue_mode": "replace", "shuffle": true, "repeat": "off", "initial_volume": map[string]any{"unit": "heos", "level": 10}, "takeover": false, "automation": map[string]any{"target_volume": map[string]any{"unit": "heos", "level": 12}, "ramp_seconds": 5, "duration_seconds": duration, "fade_seconds": 3}})
	status, b, _, err = h.request("POST", "/v1/players/fixture/playback", key, match, body)
	var op control.Operation
	if err != nil || status != 202 || json.Unmarshal(b, &op) != nil {
		return admitted{}, fmt.Errorf("playback admission status %d", status)
	}
	return admitted{op.ID, match, body}, nil
}
func (h *harness) operation(id string) (control.Operation, error) {
	status, b, _, err := h.request("GET", "/v1/operations/"+id, "", "", nil)
	var op control.Operation
	if err != nil || status != 200 {
		return op, fmt.Errorf("operation read status %d", status)
	}
	err = json.Unmarshal(b, &op)
	return op, err
}

type measurement struct {
	Phase      string  `json:"phase"`
	Seconds    float64 `json:"seconds"`
	PeakRSSKiB uint64  `json:"peak_rss_kib"`
	CPUCores   float64 `json:"cpu_cores"`
}

func usage(pid int) (uint64, uint64, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, 0, err
	}
	fields := strings.Fields(string(b[strings.LastIndex(string(b), ")")+1:]))
	u, _ := strconv.ParseUint(fields[11], 10, 64)
	s, _ := strconv.ParseUint(fields[12], 10, 64)
	b, err = os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, 0, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			rss, err := strconv.ParseUint(strings.Fields(line)[1], 10, 64)
			return u + s, rss, err
		}
	}
	return 0, 0, errors.New("process RSS unavailable")
}
func (h *harness) measure(phase string, duration time.Duration) (measurement, error) {
	fmt.Printf("Qualification: measure %s for %s\n", phase, duration)
	freq, err := exec.Command("getconf", "CLK_TCK").Output()
	if err != nil {
		return measurement{}, err
	}
	hz, err := strconv.ParseFloat(strings.TrimSpace(string(freq)), 64)
	if err != nil {
		return measurement{}, err
	}
	start := time.Now()
	cpu, _, err := usage(h.cmd.Process.Pid)
	if err != nil {
		return measurement{}, err
	}
	m := measurement{Phase: phase}
	var current uint64
	for time.Since(start) < duration {
		var rss uint64
		current, rss, err = usage(h.cmd.Process.Pid)
		if err != nil {
			return m, err
		}
		m.PeakRSSKiB = max(m.PeakRSSKiB, rss)
		time.Sleep(250 * time.Millisecond)
	}
	m.Seconds = time.Since(start).Seconds()
	m.CPUCores = float64(current-cpu) / hz / m.Seconds
	fmt.Printf("%s: peak RSS %.1f MiB, mean CPU %.3f cores\n", phase, float64(m.PeakRSSKiB)/1024, m.CPUCores)
	return m, nil
}

func run() error {
	fmt.Println("Qualification: validate disposable PostgreSQL and prepare the synthetic TLS device")
	if runtime.GOOS != "linux" {
		return errors.New("qualification requires Linux /proc")
	}
	cfg, err := config.Load(".local/test-runtime.json")
	if err != nil {
		return err
	}
	if cfg.Database.Host != "localhost" || cfg.Database.Port != 15432 || cfg.Database.Name != "heos_test" || cfg.Database.User != "heos_runtime" {
		return errors.New("qualification accepts only disposable localhost:15432/heos_test runtime config")
	}
	owner, err := config.Load(".local/test-owner.json")
	if err != nil {
		return err
	}
	if owner.Database.Host != "localhost" || owner.Database.Port != 15432 || owner.Database.Name != "heos_test" || owner.Database.User != "heos_owner" {
		return errors.New("qualification accepts only local heos_test owner config")
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	var databaseVersion bytes.Buffer
	if err := docker(nil, &databaseVersion, "psql", "-U", "postgres", "-d", "heos_test", "-Atc", "SHOW server_version"); err != nil {
		return err
	}
	if !strings.HasPrefix(databaseVersion.String(), "18.") {
		return errors.New("qualification requires PostgreSQL 18")
	}
	fmt.Println("Qualification: reset the dedicated heos_test schema and run migrations")
	if err := docker(nil, io.Discard, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "heos_test", "-c", "DROP SCHEMA IF EXISTS heos CASCADE; CREATE SCHEMA heos AUTHORIZATION heos_owner; GRANT USAGE ON SCHEMA heos TO heos_runtime; ALTER DEFAULT PRIVILEGES FOR ROLE heos_owner IN SCHEMA heos GRANT SELECT ON TABLES TO heos_runtime;"); err != nil {
		return err
	}
	migrate := exec.Command("./bin/heos-control", "migrate", "-config", ".local/test-owner.json")
	if out, err := migrate.CombinedOutput(); err != nil {
		return fmt.Errorf("fixture migration: %s: %w", out, err)
	}
	d, pin, err := newDevice()
	if err != nil {
		return err
	}
	defer d.close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	address := listener.Addr().String()
	_ = listener.Close()
	cfg.Listen = address
	ceiling := 40
	cfg.Players = []config.Player{{Key: "fixture", Address: "127.0.0.1:1265", Serial: "qualification", Model: "synthetic", FingerprintSHA256: pin, VolumeCeiling: &ceiling, WritesEnabled: true}}
	cfg.Sources = []config.Source{{Key: "music", Player: "fixture", Name: "Gerbera"}}
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return err
	}
	token := hex.EncodeToString(random)
	hash := sha256.Sum256([]byte(token))
	if err := save("credentials.json", config.CredentialFile{Version: 1, Credentials: []config.Credential{{Principal: "qualification", TokenSHA256: fmt.Sprintf("%x", hash), Scopes: []string{"read", "control", "operator"}, Players: []string{"fixture"}}}}); err != nil {
		return err
	}
	cfg.CredentialsFile, err = filepath.Abs(filepath.Join(directory, "credentials.json"))
	if err != nil {
		return err
	}
	ca, err := os.ReadFile(".local/ca.crt")
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("invalid local CA")
	}
	h := &harness{cfg: cfg, token: token, url: "https://" + address, client: &http.Client{Timeout: 8 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}}}
	defer func() { _ = h.stop(false); h.client.CloseIdleConnections() }()
	if err := h.start(true); err != nil {
		return err
	}
	if _, err := h.fresh(); err != nil {
		return err
	}
	var measured []measurement
	for _, phase := range []string{"idle", "reconnect"} {
		before := d.accepted.Load()
		if phase == "reconnect" {
			d.offline.Store(true)
			d.disconnect()
		}
		m, err := h.measure(phase, 10*time.Second)
		if err != nil {
			return err
		}
		measured = append(measured, m)
		if phase == "reconnect" && d.accepted.Load() <= before {
			return errors.New("reconnect scenario did not reconnect")
		}
	}
	d.offline.Store(false)
	if _, err := h.fresh(); err != nil {
		return err
	}
	if d.writes.Load() != 0 {
		return errors.New("idle/reconnect sent device writes")
	}
	first, err := h.play("qualified", 15)
	if err != nil {
		return err
	}
	m, err := h.measure("bounded_playback", 17*time.Second)
	if err != nil {
		return err
	}
	measured = append(measured, m)
	if op, err := h.operation(first.ID); err != nil {
		return fmt.Errorf("read bounded run: %w", err)
	} else if op.State != "succeeded" {
		return fmt.Errorf("bounded run did not succeed: %s", op.State)
	}
	status, b, _, err := h.request("GET", "/metrics", "", "", nil)
	if err != nil || status != 200 || !bytes.Contains(b, []byte("heos_journal_ready 1")) {
		return errors.New("metrics unavailable")
	}
	dump, err := os.OpenFile(filepath.Join(directory, "baseline.dump"), os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer func() { _ = dump.Close() }()
	fmt.Println("Qualification: capture a PostgreSQL backup after successful bounded playback")
	if err := docker(nil, dump, "pg_dump", "-U", "postgres", "-d", "heos_test", "--schema=heos", "--format=custom"); err != nil {
		return err
	}
	lost, err := h.play("after-backup", 1200)
	if err != nil {
		return err
	}
	if err := h.await(func() bool { op, err := h.operation(lost.ID); return err == nil && op.Playback != nil }); err != nil {
		return err
	}
	fmt.Println("Qualification: crash an active run and check recovery without device writes")
	if err := h.stop(true); err != nil {
		return err
	}
	writes := d.writes.Load()
	if err := h.start(false); err != nil {
		return err
	}
	if op, err := h.operation(lost.ID); err != nil {
		return fmt.Errorf("read crash recovery state: %w", err)
	} else if op.State != "uncertain" || op.ErrorCode == nil || *op.ErrorCode != "process_interrupted" {
		return fmt.Errorf("crash recovery state: %s", op.State)
	}
	if d.writes.Load() != writes {
		return errors.New("crash recovery sent playback writes")
	}
	if err := h.stop(false); err != nil {
		return err
	}
	if _, err := dump.Seek(0, io.SeekStart); err != nil {
		return err
	}
	fmt.Println("Qualification: restore the older backup and check missing/retained idempotency keys")
	if err := docker(dump, io.Discard, "pg_restore", "-U", "postgres", "-d", "heos_test", "--clean", "--if-exists", "--exit-on-error"); err != nil {
		return err
	}
	if err := h.start(false); err != nil {
		return err
	}
	match, err := h.fresh()
	if err != nil {
		return err
	}
	status, _, _, err = h.request("GET", "/v1/operations/"+lost.ID, "", "", nil)
	if err != nil || status != 404 {
		return errors.New("old backup unexpectedly retained the later operation")
	}
	status, b, _, err = h.request("POST", "/v1/players/fixture/playback", "after-backup", match, lost.Body)
	var rejection struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err != nil || status != 422 || json.Unmarshal(b, &rejection) != nil || rejection.Error.Code != "writes_disabled" {
		return fmt.Errorf("writes-disabled restore admitted missing key: status %d", status)
	}
	status, b, _, err = h.request("POST", "/v1/players/fixture/playback", "qualified", first.Match, first.Body)
	var replay control.Operation
	if err != nil || status != 202 || json.Unmarshal(b, &replay) != nil || replay.ID != first.ID || replay.State != "succeeded" {
		return errors.New("restored retained key is not replayable")
	}
	if d.writes.Load() != writes {
		return errors.New("restored service replayed playback")
	}
	fmt.Println("Qualification: check graceful shutdown and write the measurement report")
	if err := h.stop(false); err != nil {
		return err
	}
	build, err := exec.Command("./bin/heos-control", "version").Output()
	if err != nil {
		return err
	}
	if err := save("report.json", map[string]any{"build": json.RawMessage(build), "platform": runtime.GOOS + "/" + runtime.GOARCH, "go": runtime.Version(), "postgres": strings.TrimSpace(databaseVersion.String()), "measurements": measured, "crash_recovery": "passed", "old_backup_missing_key": "rejected with writes disabled", "retained_key_replay": "passed", "recovery_device_writes": 0}); err != nil {
		return err
	}
	fmt.Println("Crash recovery, pg_dump/pg_restore, missing/retained idempotency keys, write opt-out and SIGTERM passed. Report: .local/qualification/report.json")
	return nil
}
