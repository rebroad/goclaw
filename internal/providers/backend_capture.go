package providers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	codexBackendCaptureEnvVar          = "CODEX_BACKEND_CAPTURE"
	codexBackendCaptureInputEnvVar     = "CODEX_BACKEND_CAPTURE_INPUT"
	codexBackendCaptureOutputEnvVar    = "CODEX_BACKEND_CAPTURE_OUTPUT"
	codexBackendCaptureReasoningEnvVar = "CODEX_BACKEND_CAPTURE_REASONING"
	codexBackendCaptureDirEnvVar       = "CODEX_BACKEND_CAPTURE_DIR"
	backendTrafficFilename             = "backend_traffic.ndjson"
)

var (
	backendCaptureWriteMu  sync.Mutex
	backendCaptureCounter  atomic.Uint64
	backendTrafficEventSeq atomic.Uint64
)

type backendCaptureConfig struct {
	enabled          bool
	captureInput     bool
	captureOutput    bool
	captureReasoning bool
	captureDir       string
}

type backendCaptureSession struct {
	id               string
	captureInput     bool
	captureOutput    bool
	captureReasoning bool
	inputPath        string
	outputPath       string
	reasoningPath    string
	trafficPath      string
	outputBuf        bytes.Buffer
	flushOnce        sync.Once
}

func envIsSet(name string) bool {
	value := os.Getenv(name)
	return value != ""
}

func activeBackendCaptureConfig() backendCaptureConfig {
	enabled := envIsSet(codexBackendCaptureEnvVar) ||
		envIsSet(codexBackendCaptureInputEnvVar) ||
		envIsSet(codexBackendCaptureOutputEnvVar) ||
		envIsSet(codexBackendCaptureReasoningEnvVar)
	captureDir := os.Getenv(codexBackendCaptureDirEnvVar)
	if captureDir == "" {
		captureDir = fmt.Sprintf("/var/tmp/codex-backend-capture.%d", os.Getpid())
	}
	return backendCaptureConfig{
		enabled:          enabled,
		captureInput:     enabled || envIsSet(codexBackendCaptureInputEnvVar),
		captureOutput:    enabled || envIsSet(codexBackendCaptureOutputEnvVar),
		captureReasoning: enabled || envIsSet(codexBackendCaptureReasoningEnvVar),
		captureDir:       captureDir,
	}
}

func nowUnixMs() int64 {
	return time.Now().UnixMilli()
}

func nextCaptureID() string {
	id := backendCaptureCounter.Add(1)
	return strconv.FormatUint(id, 10)
}

func nextTrafficEventID() uint64 {
	return backendTrafficEventSeq.Add(1)
}

func appendJSONLine(path string, payload any) {
	serialized, err := json.Marshal(payload)
	if err != nil {
		return
	}

	backendCaptureWriteMu.Lock()
	defer backendCaptureWriteMu.Unlock()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()

	_, _ = f.Write(serialized)
	_, _ = f.Write([]byte("\n"))
}

func startBackendCapture(kind string, requestPayload string, trafficRequest map[string]any) *backendCaptureSession {
	config := activeBackendCaptureConfig()
	if !config.enabled {
		return nil
	}

	id := nextCaptureID()
	inputPath := filepath.Join(config.captureDir, id+"_input.ndjson")
	outputPath := filepath.Join(config.captureDir, id+"_output.ndjson")
	reasoningPath := filepath.Join(config.captureDir, id+"_reasoning.ndjson")
	trafficPath := filepath.Join(config.captureDir, backendTrafficFilename)

	if config.captureInput {
		appendJSONLine(inputPath, map[string]any{
			"kind":      kind,
			"query_id":  id,
			"transport": kind,
			"payload":   requestPayload,
		})
	}

	if trafficRequest != nil {
		event := map[string]any{
			"event_seq":         nextTrafficEventID(),
			"timestamp_unix_ms": nowUnixMs(),
		}
		for key, value := range trafficRequest {
			event[key] = value
		}
		appendJSONLine(trafficPath, event)
	}

	return &backendCaptureSession{
		id:               id,
		captureInput:     config.captureInput,
		captureOutput:    config.captureOutput,
		captureReasoning: config.captureReasoning,
		inputPath:        inputPath,
		outputPath:       outputPath,
		reasoningPath:    reasoningPath,
		trafficPath:      trafficPath,
	}
}

func (s *backendCaptureSession) appendTrafficEvent(event map[string]any) {
	if s == nil || event == nil {
		return
	}
	withMeta := map[string]any{
		"event_seq":         nextTrafficEventID(),
		"timestamp_unix_ms": nowUnixMs(),
	}
	for key, value := range event {
		withMeta[key] = value
	}
	appendJSONLine(s.trafficPath, withMeta)
}

func (s *backendCaptureSession) appendOutputChunk(chunk []byte) {
	if s == nil || !s.captureOutput || len(chunk) == 0 {
		return
	}
	s.outputBuf.Write(chunk)
}

func (s *backendCaptureSession) appendReasoning(payload string) {
	if s == nil || !s.captureReasoning {
		return
	}
	appendJSONLine(s.reasoningPath, map[string]any{
		"query_id":  s.id,
		"transport": "provider_http_reasoning",
		"payload":   payload,
	})
}

func (s *backendCaptureSession) flushOutput(transport string) {
	if s == nil {
		return
	}
	s.flushOnce.Do(func() {
		if !s.captureOutput {
			return
		}
		payload := s.outputBuf.String()
		appendJSONLine(s.outputPath, map[string]any{
			"query_id":  s.id,
			"transport": transport,
			"payload":   payload,
		})
	})
}

type backendCaptureReadCloser struct {
	inner     io.ReadCloser
	session   *backendCaptureSession
	transport string
}

func (r *backendCaptureReadCloser) Read(p []byte) (int, error) {
	n, err := r.inner.Read(p)
	if n > 0 {
		r.session.appendOutputChunk(p[:n])
	}
	if err == io.EOF {
		r.session.flushOutput(r.transport)
	}
	return n, err
}

func (r *backendCaptureReadCloser) Close() error {
	err := r.inner.Close()
	r.session.flushOutput(r.transport)
	return err
}

func wrapBackendCaptureReadCloser(body io.ReadCloser, session *backendCaptureSession, transport string) io.ReadCloser {
	if session == nil {
		return body
	}
	return &backendCaptureReadCloser{
		inner:     body,
		session:   session,
		transport: transport,
	}
}

func headersToCaptureMap(headers http.Header) map[string][]string {
	result := make(map[string][]string, len(headers))
	for key, values := range headers {
		copied := append([]string(nil), values...)
		result[key] = copied
	}
	return result
}
