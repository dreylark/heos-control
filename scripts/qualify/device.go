package main

import (
	"bufio"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Synthetic Denon 4.2/4.4/5 fixture. The listener cannot bind a non-loopback
// address. This helper is never linked into the service binary.
type device struct {
	listener                     net.Listener
	mu                           sync.Mutex
	connections                  map[net.Conn]bool
	wg                           sync.WaitGroup
	offline                      atomic.Bool
	writes                       atomic.Int64
	accepted                     atomic.Int64
	level                        int
	state, mute, repeat, shuffle string
	queued                       bool
}

func newDevice() (*device, string, error) {
	cert, err := tls.LoadX509KeyPair(".local/server.crt", ".local/server.key")
	if err != nil {
		return nil, "", err
	}
	l, err := tls.Listen("tcp", "127.0.0.1:1265", &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}})
	if err != nil {
		return nil, "", err
	}
	d := &device{listener: l, connections: map[net.Conn]bool{}, level: 20, state: "stop", mute: "off", repeat: "off", shuffle: "off"}
	d.wg.Go(func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			d.accepted.Add(1)
			d.mu.Lock()
			d.connections[conn] = true
			d.mu.Unlock()
			d.wg.Go(func() {
				defer func() { _ = conn.Close(); d.mu.Lock(); delete(d.connections, conn); d.mu.Unlock() }()
				d.serve(conn)
			})
		}
	})
	return d, fmt.Sprintf("%x", sha256.Sum256(cert.Certificate[0])), nil
}

func (d *device) disconnect() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for conn := range d.connections {
		_ = conn.Close()
	}
}
func (d *device) close() { _ = d.listener.Close(); d.disconnect(); d.wg.Wait() }
func (d *device) serve(conn net.Conn) {
	if d.offline.Load() {
		return
	}
	reader := bufio.NewScanner(conn)
	reader.Buffer(make([]byte, 4096), 256<<10)
	for reader.Scan() {
		u, err := url.Parse(strings.TrimSpace(reader.Text()))
		if err != nil {
			return
		}
		command, params := u.Host+u.Path, u.Query()
		var payload any = []any{}
		var events []map[string]string
		event := func(name, message string) {
			events = append(events, map[string]string{"command": "event/" + name, "message": "pid=1" + message})
		}
		d.mu.Lock()
		switch command {
		case "system/register_for_change_events":
		case "player/get_players":
			payload = []map[string]any{{"pid": 1, "serial": "qualification", "model": "synthetic", "name": "Fixture"}}
		case "group/get_groups":
		case "player/get_play_state":
			params.Set("state", d.state)
		case "player/get_volume":
			params.Set("level", strconv.Itoa(d.level))
		case "player/get_mute":
			params.Set("state", d.mute)
		case "player/get_play_mode":
			params.Set("repeat", d.repeat)
			params.Set("shuffle", d.shuffle)
		case "player/get_now_playing_media":
			payload = map[string]any{}
			if d.queued {
				payload = map[string]any{"sid": 1024, "mid": "track", "qid": 1, "album": "Green"}
			}
		case "player/get_queue":
			params.Set("count", "0")
			params.Set("returned", "0")
			if d.queued {
				payload = []map[string]any{{"mid": "track", "qid": 1, "album": "Green"}}
				params.Set("count", "1")
				params.Set("returned", "1")
			}
		case "browse/browse":
			params.Set("count", "1")
			params.Set("returned", "1")
			if params.Get("sid") == "1024" {
				payload = []map[string]any{{"sid": 900, "name": "Gerbera", "type": "heos_server"}}
			} else if params.Get("cid") == "green" {
				payload = []map[string]any{{"mid": "track", "name": "Song", "type": "song", "container": "no", "playable": "yes"}}
			} else {
				payload = []map[string]any{{"cid": "green", "name": "Green", "container": "yes", "playable": "yes"}}
			}
		case "player/set_volume":
			d.writes.Add(1)
			d.level, _ = strconv.Atoi(params.Get("level"))
			event("player_volume_changed", fmt.Sprintf("&level=%d&mute=%s", d.level, d.mute))
		case "player/set_mute":
			d.writes.Add(1)
			d.mute = params.Get("state")
			event("player_volume_changed", fmt.Sprintf("&level=%d&mute=%s", d.level, d.mute))
		case "player/set_play_mode":
			d.writes.Add(1)
			d.repeat, d.shuffle = params.Get("repeat"), params.Get("shuffle")
			event("repeat_mode_changed", "&repeat="+d.repeat)
			event("shuffle_mode_changed", "&shuffle="+d.shuffle)
		case "player/set_play_state":
			d.writes.Add(1)
			d.state = params.Get("state")
			event("player_state_changed", "&state="+d.state)
		case "browse/add_to_queue":
			if params.Get("aid") != "4" || params.Get("sid") != "900" || params.Get("cid") != "green" {
				d.mu.Unlock()
				return
			}
			d.writes.Add(1)
			d.queued, d.state = true, "play"
			event("player_queue_changed", "")
			event("player_now_playing_changed", "")
			event("player_state_changed", "&state=play")
		default:
			d.mu.Unlock()
			return
		}
		d.mu.Unlock()
		for _, event := range events {
			b, _ := json.Marshal(map[string]any{"heos": event})
			if _, err := conn.Write(append(b, '\r', '\n')); err != nil {
				return
			}
		}
		b, _ := json.Marshal(map[string]any{"heos": map[string]string{"command": command, "result": "success", "message": params.Encode()}, "payload": payload})
		if _, err := conn.Write(append(b, '\r', '\n')); err != nil {
			return
		}
	}
}
