package goim

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/godeps/cc-connect/core"
)

// sendStubPlatform records everything the engine routes to it, so tests can
// assert which platform sender an attachment reached.
//
// It implements ReplyContextReconstructor so the engine resolves this platform
// from the session key alone: "stub:chat:user" names the platform in its
// prefix, which is the same proactive-send path used after a restart.
type sendStubPlatform struct {
	name string

	mu      sync.Mutex
	texts   []string
	images  []core.ImageAttachment
	files   []core.FileAttachment
	audios  [][]byte
	videos  [][]byte
	handler core.MessageHandler
}

func newSendStubPlatform(name string) *sendStubPlatform {
	return &sendStubPlatform{name: name}
}

func (p *sendStubPlatform) Name() string { return p.name }
func (p *sendStubPlatform) Start(h core.MessageHandler) error {
	p.handler = h
	return nil
}
func (p *sendStubPlatform) Reply(_ context.Context, _ any, content string) error {
	return p.Send(context.Background(), nil, content)
}

func (p *sendStubPlatform) Send(_ context.Context, _ any, content string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.texts = append(p.texts, content)
	return nil
}

func (p *sendStubPlatform) Stop() error { return nil }

func (p *sendStubPlatform) ReconstructReplyCtx(sessionKey string) (any, error) {
	return sessionKey, nil
}

func (p *sendStubPlatform) SendImage(_ context.Context, _ any, img core.ImageAttachment) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.images = append(p.images, img)
	return nil
}

func (p *sendStubPlatform) SendFile(_ context.Context, _ any, file core.FileAttachment) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.files = append(p.files, file)
	return nil
}

func (p *sendStubPlatform) SendAudio(_ context.Context, _ any, audio []byte, _ string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.audios = append(p.audios, audio)
	return nil
}

func (p *sendStubPlatform) SendVideo(_ context.Context, _ any, video []byte, _, _ string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.videos = append(p.videos, video)
	return nil
}

// snapshot copies the recorded payloads so assertions do not race with a send.
func (p *sendStubPlatform) snapshot() (texts []string, images []core.ImageAttachment, files []core.FileAttachment, audios, videos [][]byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.texts...),
		append([]core.ImageAttachment(nil), p.images...),
		append([]core.FileAttachment(nil), p.files...),
		append([][]byte(nil), p.audios...),
		append([][]byte(nil), p.videos...)
}

// sendStubRuntime satisfies Runtime without ever streaming; these tests only
// exercise the outbound path.
type sendStubRuntime struct{}

func (sendStubRuntime) RunStream(context.Context, Request) (<-chan StreamEvent, error) {
	ch := make(chan StreamEvent)
	close(ch)
	return ch, nil
}

// newSendTestEngine builds an engine bound to one stub platform and a
// temp-dir session store.
func newSendTestEngine(t *testing.T, platform *sendStubPlatform) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Project.Name = "test"
	return NewEngine(NewAgent(sendStubRuntime{}, "test"), []core.Platform{platform}, cfg)
}

const testSessionKey = "stub:chat:user"

func TestSendAttachmentDispatchesByKind(t *testing.T) {
	platform := newSendStubPlatform("stub")
	engine := newSendTestEngine(t, platform)

	err := engine.SendAttachment(testSessionKey, "here you go", []Attachment{
		{Kind: AttachmentKindImage, MimeType: "image/png", FileName: "a.png", Data: []byte("png-data")},
		{Kind: AttachmentKindFile, MimeType: "application/pdf", FileName: "b.pdf", Data: []byte("pdf-data")},
		{Kind: AttachmentKindAudio, MimeType: "audio/ogg", FileName: "c.ogg", Data: []byte("ogg-data")},
		{Kind: AttachmentKindVideo, MimeType: "video/mp4", FileName: "d.mp4", Data: []byte("mp4-data")},
	})
	if err != nil {
		t.Fatalf("SendAttachment: %v", err)
	}

	texts, images, files, audios, videos := platform.snapshot()
	// Text leads the audio/video messages rather than being stranded behind them.
	if len(texts) != 1 || texts[0] != "here you go" {
		t.Errorf("texts = %q, want [here you go]", texts)
	}
	if len(images) != 1 || images[0].FileName != "a.png" || string(images[0].Data) != "png-data" {
		t.Errorf("images = %+v, want a.png", images)
	}
	if len(files) != 1 || files[0].FileName != "b.pdf" || string(files[0].Data) != "pdf-data" {
		t.Errorf("files = %+v, want b.pdf", files)
	}
	if len(audios) != 1 || string(audios[0]) != "ogg-data" {
		t.Errorf("audios = %q, want [ogg-data]", audios)
	}
	if len(videos) != 1 || string(videos[0]) != "mp4-data" {
		t.Errorf("videos = %q, want [mp4-data]", videos)
	}
}

func TestSendAttachmentDocumentOnlyCarriesNoText(t *testing.T) {
	platform := newSendStubPlatform("stub")
	engine := newSendTestEngine(t, platform)

	err := engine.SendAttachment(testSessionKey, "", []Attachment{
		{Kind: AttachmentKindFile, MimeType: "text/plain", FileName: "notes.txt", Data: []byte("hi")},
	})
	if err != nil {
		t.Fatalf("SendAttachment: %v", err)
	}

	texts, _, files, _, _ := platform.snapshot()
	if len(texts) != 0 {
		t.Errorf("texts = %q, want none", texts)
	}
	if len(files) != 1 || files[0].FileName != "notes.txt" {
		t.Errorf("files = %+v, want notes.txt", files)
	}
}

// A caller that only knows the media type still reaches the image sender
// instead of shipping the picture as an opaque document.
func TestSendAttachmentInfersKindFromMimeType(t *testing.T) {
	platform := newSendStubPlatform("stub")
	engine := newSendTestEngine(t, platform)

	err := engine.SendAttachment(testSessionKey, "", []Attachment{
		{MimeType: "image/jpeg", FileName: "photo.jpg", Data: []byte("jpeg")},
	})
	if err != nil {
		t.Fatalf("SendAttachment: %v", err)
	}

	_, images, files, _, _ := platform.snapshot()
	if len(images) != 1 {
		t.Fatalf("images = %+v, want 1", images)
	}
	if len(files) != 0 {
		t.Errorf("files = %+v, want none", files)
	}
}

func TestSendAttachmentRequiresMessageOrAttachment(t *testing.T) {
	platform := newSendStubPlatform("stub")
	engine := newSendTestEngine(t, platform)

	if err := engine.SendAttachment(testSessionKey, "   ", nil); err == nil {
		t.Fatal("expected an error for an empty message and no attachments")
	}
	if err := engine.SendAttachment(testSessionKey, "", []Attachment{{Kind: AttachmentKindFile, Data: nil}}); err == nil {
		t.Fatal("expected an error when every attachment carries no data")
	}
}

func TestSendAttachmentUnknownSession(t *testing.T) {
	platform := newSendStubPlatform("stub")
	engine := newSendTestEngine(t, platform)

	err := engine.SendAttachment("nosuchplatform:chat:user", "hi", nil)
	if err == nil {
		t.Fatal("expected an error for a session key naming no known platform")
	}
}

func TestSendFileToSessionReadsAndDispatches(t *testing.T) {
	platform := newSendStubPlatform("stub")
	engine := newSendTestEngine(t, platform)

	path := filepath.Join(t.TempDir(), "chart.png")
	payload := []byte("\x89PNG\r\n\x1a\nfake-image-bytes")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	if err := engine.SendFileToSession(testSessionKey, "chart", path); err != nil {
		t.Fatalf("SendFileToSession: %v", err)
	}

	texts, images, _, _, _ := platform.snapshot()
	if len(texts) != 1 || texts[0] != "chart" {
		t.Errorf("texts = %q, want [chart]", texts)
	}
	if len(images) != 1 {
		t.Fatalf("images = %+v, want 1 (extension should pick the image path)", images)
	}
	if images[0].FileName != "chart.png" {
		t.Errorf("FileName = %q, want chart.png", images[0].FileName)
	}
	if string(images[0].Data) != string(payload) {
		t.Errorf("Data = %q, want the file contents", images[0].Data)
	}
	if images[0].MimeType != "image/png" {
		t.Errorf("MimeType = %q, want image/png", images[0].MimeType)
	}
}

func TestSendFileToSessionMissingFile(t *testing.T) {
	platform := newSendStubPlatform("stub")
	engine := newSendTestEngine(t, platform)

	err := engine.SendFileToSession(testSessionKey, "", filepath.Join(t.TempDir(), "absent.txt"))
	if err == nil {
		t.Fatal("expected an error for a missing file")
	}
	if _, _, files, _, _ := platform.snapshot(); len(files) != 0 {
		t.Errorf("files = %+v, want none", files)
	}
}

func TestActiveSessionKeysWithoutSessions(t *testing.T) {
	platform := newSendStubPlatform("stub")
	engine := newSendTestEngine(t, platform)

	if keys := engine.ActiveSessionKeys(); len(keys) != 0 {
		t.Errorf("ActiveSessionKeys() = %q, want none", keys)
	}
}

func TestNormalizeAttachmentKind(t *testing.T) {
	cases := []struct {
		kind, mediaType, want string
	}{
		{"image", "application/octet-stream", AttachmentKindImage},
		{"IMAGE", "", AttachmentKindImage},
		{" audio ", "", AttachmentKindAudio},
		{"file", "image/png", AttachmentKindFile},
		{"", "image/png", AttachmentKindImage},
		{"", "audio/mpeg", AttachmentKindAudio},
		{"", "video/mp4", AttachmentKindVideo},
		{"", "application/pdf", AttachmentKindFile},
		{"", "", AttachmentKindFile},
	}
	for _, tc := range cases {
		if got := normalizeAttachmentKind(tc.kind, tc.mediaType); got != tc.want {
			t.Errorf("normalizeAttachmentKind(%q, %q) = %q, want %q", tc.kind, tc.mediaType, got, tc.want)
		}
	}
}

func TestDetectMimeType(t *testing.T) {
	if got := detectMimeType("chart.png", nil); got != "image/png" {
		t.Errorf("detectMimeType(chart.png) = %q, want image/png", got)
	}
	// No extension: fall back to sniffing the content.
	if got := detectMimeType("payload", []byte("%PDF-1.7\n")); got != "application/pdf" {
		t.Errorf("detectMimeType(sniffed pdf) = %q, want application/pdf", got)
	}
	if got := detectMimeType("empty", nil); got != "application/octet-stream" {
		t.Errorf("detectMimeType(empty) = %q, want application/octet-stream", got)
	}
}

// sendNamingRuntime reports a session ID of its own on every stream event, the
// way saker does. cc-connect stores whatever the agent reports as that
// conversation's agent_session_id, so the ID the runtime knows its conversation
// by is a different string from the session_key cc-connect routes by. A stub
// that echoes the session key back hides that split; this one reproduces it.
type sendNamingRuntime struct{ agentSessionID string }

func (r sendNamingRuntime) RunStream(_ context.Context, _ Request) (<-chan StreamEvent, error) {
	ch := make(chan StreamEvent, 2)
	ch <- StreamEvent{Type: EventContentBlockDelta, Delta: &Delta{Text: "hello"}, SessionID: r.agentSessionID}
	ch <- StreamEvent{Type: EventMessageStop, SessionID: r.agentSessionID}
	close(ch)
	return ch, nil
}

// A tool sending a file into the chat knows its conversation only by the ID the
// runtime gave it, and in gateway mode that ID is saker's own agent session ID —
// not the cc-connect session key the engine routes by. Sending therefore has to
// resolve one to the other. Before it did, every im_send_file call from the chat
// failed with `no active session found`, because an agent session ID carries no
// platform prefix for the outbound path to reconstruct a target from.
func TestSendFileToSessionResolvesAgentSessionID(t *testing.T) {
	const agentSessionID = "cli-1789424487680080138"

	platform := newSendStubPlatform("stub")
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Project.Name = "test"
	engine := NewEngine(NewAgent(sendNamingRuntime{agentSessionID: agentSessionID}, "test"), []core.Platform{platform}, cfg)
	if err := engine.Start(); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	defer func() { _ = engine.Stop() }()

	if platform.handler == nil {
		t.Fatal("engine did not install a message handler")
	}
	platform.handler(platform, &core.Message{
		SessionKey: testSessionKey,
		Platform:   "stub",
		MessageID:  "m1",
		UserID:     "user",
		UserName:   "user",
		Content:    "hello",
		ReplyCtx:   testSessionKey,
	})

	// Wait for the turn to settle so cc-connect has recorded the agent ID the
	// runtime reported; that record is the only link between the two IDs.
	deadline := time.Now().Add(5 * time.Second)
	for {
		known := false
		for _, s := range engine.inner.GetSessions().AllSessions() {
			if s.GetAgentSessionID() == agentSessionID {
				known = true
				break
			}
		}
		if known {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("engine never recorded the agent session ID")
		}
		time.Sleep(20 * time.Millisecond)
	}

	path := filepath.Join(t.TempDir(), "notes.txt")
	if err := os.WriteFile(path, []byte("body"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := engine.SendFileToSession(agentSessionID, "here it is", path); err != nil {
		t.Fatalf("SendFileToSession(%q): %v", agentSessionID, err)
	}

	// The turn's own reply shares the recorded texts, so look for the caption
	// among them rather than expecting it alone.
	texts, _, files, _, _ := platform.snapshot()
	if len(files) != 1 || files[0].FileName != "notes.txt" || string(files[0].Data) != "body" {
		t.Errorf("files = %+v, want notes.txt carrying body", files)
	}
	caption := false
	for _, txt := range texts {
		if txt == "here it is" {
			caption = true
			break
		}
	}
	if !caption {
		t.Errorf("texts = %q, want them to include %q", texts, "here it is")
	}
}
