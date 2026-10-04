//go:build unix

package pluginhost

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"
)

// The observer forwards every worker stdout byte unchanged. It records bounded
// frame witnesses separately, never emits protocol messages or returns replies.
type interopWireWriter struct {
	output    io.Writer
	observer  *interopEventWriter
	pending   []byte
	direction string
}

func (w *interopWireWriter) Write(raw []byte) (int, error) {
	n, err := w.output.Write(raw)
	if err != nil {
		return n, err
	}
	for _, b := range raw {
		if len(w.pending) >= 8<<20 {
			return n, errors.New("harness stdout frame limit")
		}
		w.pending = append(w.pending, b)
		if b != '\n' {
			continue
		}
		hash := sha256.Sum256(w.pending)
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(w.pending, &envelope); err != nil {
			return n, err
		}
		direction := w.direction
		if direction == "" {
			direction = "worker-to-host"
		}
		event := map[string]any{"kind": "wire", "direction": direction, "bytes": len(w.pending), "sha256": hex.EncodeToString(hash[:]), "observed_at": time.Now().UTC().Format(time.RFC3339Nano), "id": envelope["id"], "method": envelope["method"]}
		if len(w.pending) <= 3000 {
			event["raw"] = string(w.pending)
		}
		line, err := json.Marshal(map[string]any{"fixture_event": event})
		if err != nil {
			return n, err
		}
		if _, err := w.observer.Write(append(line, '\n')); err != nil {
			return n, err
		}
		w.pending = w.pending[:0]
	}
	return n, nil
}

func (w *interopWireWriter) finish() error {
	if len(w.pending) != 0 {
		return errors.New("harness partial stdout frame at EOF")
	}
	return nil
}
