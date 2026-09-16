package goim

import (
	"fmt"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/godeps/cc-connect/core"
)

// Engine wraps cc-connect's core.Engine with goim-specific configuration.
type Engine struct {
	inner *core.Engine
}

// NewEngine creates a cc-connect Engine binding the given Agent adapter and Platforms.
func NewEngine(agent *Agent, platforms []core.Platform, cfg Config) *Engine {
	lang := ResolveLang(cfg.Language)
	dataDir := ResolveDataDir(cfg)
	sessionStorePath := dataDir + "/sessions"

	engine := core.NewEngine(cfg.Project.Name, agent, platforms, sessionStorePath, lang)

	// Apply stream preview config.
	engine.SetStreamPreviewCfg(cfg.StreamPreview.ToStreamPreviewCfg())

	return &Engine{inner: engine}
}

// Start starts all platforms and begins routing messages.
func (e *Engine) Start() error {
	return e.inner.Start()
}

// Stop gracefully shuts down all platforms and the agent.
func (e *Engine) Stop() error {
	return e.inner.Stop()
}

// SendAttachment delivers a message and its attachments to an IM session.
//
// sessionKey identifies the target conversation. It accepts either a cc-connect
// session key or the runtime's own session ID for that conversation — see
// resolveSendTarget. An empty key, or one that resolves to nothing, defers to
// cc-connect's resolution, which accepts exactly one active session when
// attachments are present and refuses an ambiguous send.
//
// Images and documents travel together in a single message. Audio and video
// each need their own message because they route to the platform's per-media
// sender; text combined with those is delivered first, so it reads as a
// caption.
func (e *Engine) SendAttachment(sessionKey, message string, atts []Attachment) error {
	sessionKey = e.resolveSendTarget(sessionKey)

	var images []core.ImageAttachment
	var files, audios, videos []core.FileAttachment
	for _, a := range atts {
		if len(a.Data) == 0 {
			continue
		}
		file := core.FileAttachment{MimeType: a.MimeType, Data: a.Data, FileName: a.FileName}
		switch normalizeAttachmentKind(a.Kind, a.MimeType) {
		case AttachmentKindImage:
			images = append(images, core.ImageAttachment{MimeType: a.MimeType, Data: a.Data, FileName: a.FileName})
		case AttachmentKindAudio:
			audios = append(audios, file)
		case AttachmentKindVideo:
			videos = append(videos, file)
		default:
			files = append(files, file)
		}
	}

	if strings.TrimSpace(message) == "" && len(images)+len(files)+len(audios)+len(videos) == 0 {
		return fmt.Errorf("message or attachment is required")
	}

	// Audio and video leave on their own, so hand the text over first rather
	// than leaving it stranded in the images/files call below.
	if (len(audios) > 0 || len(videos) > 0) && strings.TrimSpace(message) != "" {
		if err := e.inner.SendToSession(sessionKey, message); err != nil {
			return err
		}
		message = ""
	}
	if len(images) > 0 || len(files) > 0 || strings.TrimSpace(message) != "" {
		if err := e.inner.SendToSessionWithAttachments(sessionKey, message, images, files, nil, false); err != nil {
			return err
		}
	}
	if len(audios) > 0 {
		if err := e.inner.SendAudiosToSession(sessionKey, audios); err != nil {
			return err
		}
	}
	if len(videos) > 0 {
		if err := e.inner.SendVideosToSession(sessionKey, videos); err != nil {
			return err
		}
	}
	return nil
}

// resolveSendTarget maps whatever ID a caller holds to the session key
// cc-connect routes outbound messages by.
//
// A tool sending into the conversation only ever sees the runtime's own session
// ID — saker reports "cli-...", for instance — because that is the ID carried
// back on stream events, which cc-connect records as the conversation's agent
// session. cc-connect keys its routing by session key ("feishu:oc_...:ou_...")
// instead, and the session store is the only place the two are linked, so an
// agent session ID has to be resolved through it. Such an ID carries no platform
// prefix, so the outbound path cannot reconstruct a target from it either.
//
// Returning "" is not a failure. cc-connect then applies its own rules, which
// accept the only session when exactly one is active and refuse an attachment
// send when the choice would be ambiguous — so an unresolvable ID withholds the
// file rather than dropping it into the wrong conversation.
func (e *Engine) resolveSendTarget(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	// Already a session key: either the caller knows the conversation directly,
	// or it is replaying a key it read back from this engine.
	for _, key := range e.inner.ActiveSessionKeys() {
		if key == id {
			return id
		}
	}
	if key := e.sessionKeyForAgentID(id); key != "" {
		return key
	}
	// Neither form matched. A key carrying a platform prefix may still name a
	// conversation cc-connect can reconstruct on its own, so pass it through
	// untouched; anything else is an ID cc-connect has never seen, and the empty
	// key gets it the single-active-session rule instead of a bare rejection.
	if strings.Contains(id, ":") {
		return id
	}
	return ""
}

// sessionKeyForAgentID finds the session key of the active conversation whose
// agent session is the given ID. Only an active session qualifies: a stored one
// whose user has since switched away would deliver into a conversation they
// have left.
func (e *Engine) sessionKeyForAgentID(id string) string {
	sessions := e.inner.GetSessions()
	if sessions == nil {
		return ""
	}
	idToKey, activeIDs := sessions.SessionKeyMap()
	for _, s := range sessions.AllSessions() {
		if !activeIDs[s.ID] || s.GetAgentSessionID() != id {
			continue
		}
		if key, ok := idToKey[s.ID]; ok {
			return key
		}
	}
	return ""
}

// SendFileToSession reads a local file and sends it to an IM session. The
// media type is detected from the extension, falling back to content sniffing,
// and doubles as the kind, so images reach the platform's image path instead of
// arriving as opaque documents.
func (e *Engine) SendFileToSession(sessionKey, message, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read attachment: %w", err)
	}
	name := filepath.Base(path)
	mediaType := detectMimeType(name, data)
	return e.SendAttachment(sessionKey, message, []Attachment{{
		Kind:     normalizeAttachmentKind("", mediaType),
		MimeType: mediaType,
		FileName: name,
		Data:     data,
	}})
}

// ActiveSessionKeys returns the session keys of all active interactive
// sessions, which callers use to discover where an unsolicited message can go.
func (e *Engine) ActiveSessionKeys() []string {
	return e.inner.ActiveSessionKeys()
}

// normalizeAttachmentKind resolves an explicit kind, or infers one from the
// media type when the caller left it empty or named something unknown.
func normalizeAttachmentKind(kind, mediaType string) string {
	switch k := strings.ToLower(strings.TrimSpace(kind)); k {
	case AttachmentKindImage, AttachmentKindAudio, AttachmentKindVideo, AttachmentKindFile:
		return k
	}
	switch {
	case strings.HasPrefix(mediaType, "image/"):
		return AttachmentKindImage
	case strings.HasPrefix(mediaType, "audio/"):
		return AttachmentKindAudio
	case strings.HasPrefix(mediaType, "video/"):
		return AttachmentKindVideo
	default:
		return AttachmentKindFile
	}
}

// detectMimeType resolves a media type from the file name first and the content
// second: the extension survives formats that sniffing reports only as
// application/octet-stream.
func detectMimeType(name string, data []byte) string {
	if ext := strings.ToLower(filepath.Ext(name)); ext != "" {
		if t := mime.TypeByExtension(ext); t != "" {
			return t
		}
	}
	if len(data) > 0 {
		return http.DetectContentType(data)
	}
	return "application/octet-stream"
}
