package ccsock

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"
)

// maxRegistryFileBytes matches the cap Claude Code applies when reading a
// session registry file.
const maxRegistryFileBytes = 256 * 1024

var pidJSONRe = regexp.MustCompile(`^(\d+)\.json$`)

// ErrNotFound is returned when no session matches a lookup.
var ErrNotFound = errors.New("ccsock: no matching session")

// ErrAmbiguous is returned when a lookup matches more than one live session.
var ErrAmbiguous = errors.New("ccsock: lookup matched more than one session")

// Session is one Claude Code session as it registers itself on disk, in
// <config>/sessions/<pid>.json.
//
// The file is written by the session and refreshed as it runs, so a stale entry
// can outlive the process. Use Running to check the process and Reachable to
// check the inbox socket.
type Session struct {
	// PID of the Claude Code process, taken from the registry filename.
	PID int
	// SessionID is the session's UUID, the same value /status reports.
	SessionID string
	// SocketPath is the inbox socket the session bound. Empty when the session
	// has no inbox (bare mode, or messaging disabled).
	SocketPath string
	// Name is the name the session answers to, from --name, /rename, or
	// derived from the working directory.
	Name string
	// NameSource is "derived" or "collision" when Claude Code chose the name.
	NameSource string
	// CWD is the session's working directory.
	CWD string
	// Kind is "interactive", "bg", "daemon", or "daemon-worker".
	Kind string
	// Status is the session's coarse activity: "busy", "shell", "idle", or
	// "waiting". Empty when the session has not reported one.
	Status string
	// WaitingFor describes what a waiting session is blocked on.
	WaitingFor string
	// Entrypoint, Agent, JobID, LogPath and Tmux carry launch context.
	Entrypoint string
	Agent      string
	JobID      string
	LogPath    string
	Tmux       string
	// ProcStart is the kernel start token of the process, used to tell a live
	// session from a recycled PID.
	ProcStart string
	// PeerProtocol is the peer protocol version the session speaks.
	PeerProtocol int
	// StartedAt, UpdatedAt and StatusUpdatedAt are zero when unset.
	StartedAt       time.Time
	UpdatedAt       time.Time
	StatusUpdatedAt time.Time
	// File is the registry file this entry was read from.
	File string
	// Raw is the decoded registry file, for fields this struct does not model.
	Raw map[string]any
}

// Address returns the session's "uds:" reply address, or "" when it has no
// inbox socket.
func (s Session) Address() string {
	if s.SocketPath == "" {
		return ""
	}
	return Address(s.SocketPath)
}

// Verify reports whether this entry is really the session with the given
// ID. A registry entry is keyed by PID, and a PID can be recycled, so a
// caller holding a remembered (pid, session id) pair should confirm the
// entry still carries the session it expects before sending to it.
//
// Verify is a pure comparison against SessionID: it does not check
// liveness, which is what Running and Reachable are for.
func (s Session) Verify(sessionID string) bool {
	if sessionID == "" {
		return false
	}
	return s.SessionID == sessionID
}

// Running reports whether the registered PID still exists. It does not prove
// the process is still Claude Code: a PID can be recycled. Reachable is the
// stronger check.
func (s Session) Running() bool {
	if s.PID <= 1 {
		return false
	}
	return processAlive(s.PID)
}

// Reachable reports whether the session's inbox socket accepts a connection.
// This is the check Claude Code itself uses to decide a peer is live.
func (s Session) Reachable(timeout time.Duration) bool {
	if s.SocketPath == "" {
		return false
	}
	return Probe(s.SocketPath, timeout)
}

// ListSessions reads every session registry entry for the current user. A
// missing sessions directory yields no sessions and no error. Entries that
// cannot be parsed are skipped rather than failing the whole listing.
//
// Results are sorted by start time, oldest first.
func ListSessions() ([]Session, error) {
	dir, err := SessionsDir()
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading sessions directory %s: %w", dir, err)
	}

	var out []Session
	for _, e := range entries {
		m := pidJSONRe.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		pid, err := strconv.Atoi(m[1])
		if err != nil || strconv.Itoa(pid) != m[1] {
			continue
		}
		s, err := readSessionFile(filepath.Join(dir, e.Name()), pid)
		if err != nil {
			continue
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].StartedAt.Equal(out[j].StartedAt) {
			return out[i].PID < out[j].PID
		}
		return out[i].StartedAt.Before(out[j].StartedAt)
	})
	return out, nil
}

func readSessionFile(path string, pid int) (Session, error) {
	data, err := readSmallFile(path, maxRegistryFileBytes)
	if err != nil {
		if errors.Is(err, errNotSmallFile) {
			return Session{}, fmt.Errorf("%s is not a readable registry file", path)
		}
		return Session{}, err
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return Session{}, fmt.Errorf("parsing %s: %w", path, err)
	}
	return Session{
		PID:             pid,
		SessionID:       str(raw, "sessionId"),
		SocketPath:      str(raw, "messagingSocketPath"),
		Name:            str(raw, "name"),
		NameSource:      str(raw, "nameSource"),
		CWD:             str(raw, "cwd"),
		Kind:            str(raw, "kind"),
		Status:          str(raw, "status"),
		WaitingFor:      str(raw, "waitingFor"),
		Entrypoint:      str(raw, "entrypoint"),
		Agent:           str(raw, "agent"),
		JobID:           str(raw, "jobId"),
		LogPath:         str(raw, "logPath"),
		Tmux:            str(raw, "tmux"),
		ProcStart:       str(raw, "procStart"),
		PeerProtocol:    intOf(raw, "peerProtocol"),
		StartedAt:       epochMS(raw, "startedAt"),
		UpdatedAt:       epochMS(raw, "updatedAt"),
		StatusUpdatedAt: epochMS(raw, "statusUpdatedAt"),
		File:            path,
		Raw:             raw,
	}, nil
}

// errNotSmallFile marks a path that failed readSmallFile's regular-file-and-
// size check, so callers can attach their own wording to the failure.
var errNotSmallFile = errors.New("not a readable file")

// readSmallFile opens path without following a symlink, then stats the
// already-open file descriptor before reading it, so a path swapped for a
// symlink between check and read cannot be followed: Lstat-then-ReadFile is
// non-atomic and would still follow it.
func readSmallFile(path string, max int64) ([]byte, error) {
	f, err := openNoFollow(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() || fi.Size() > max {
		return nil, errNotSmallFile
	}
	return io.ReadAll(io.LimitReader(f, max))
}

func str(m map[string]any, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
}

func intOf(m map[string]any, k string) int {
	if v, ok := m[k].(float64); ok {
		return int(v)
	}
	return 0
}

// maxSaneEpochMS mirrors Claude Code's own guard against nonsense timestamps.
const maxSaneEpochMS = 4_000_000_000_000_000

func epochMS(m map[string]any, k string) time.Time {
	v, ok := m[k].(float64)
	if !ok || v < 0 || v > maxSaneEpochMS {
		return time.Time{}
	}
	return time.UnixMilli(int64(v))
}

// FindByPID returns the registered session with the given PID.
func FindByPID(pid int) (Session, error) {
	sessions, err := ListSessions()
	if err != nil {
		return Session{}, err
	}
	for _, s := range sessions {
		if s.PID == pid {
			return s, nil
		}
	}
	return Session{}, fmt.Errorf("%w with pid %d", ErrNotFound, pid)
}

// FindByPIDForSession is the safe form of FindByPID for a caller who
// remembered a (pid, session id) pair rather than just a pid: it returns the
// session registered under pid only when that entry's Verify(sessionID)
// confirms it still carries the expected session id, so a PID recycled onto
// an unrelated process cannot be mistaken for the session the caller meant.
func FindByPIDForSession(pid int, sessionID string) (Session, error) {
	s, err := FindByPID(pid)
	if err != nil {
		return Session{}, err
	}
	if !s.Verify(sessionID) {
		return Session{}, fmt.Errorf("%w with pid %d: entry carries a different session id", ErrNotFound, pid)
	}
	return s, nil
}

// MatchesProcess reports whether this entry belongs to the process that
// produced procStart, a token from ProcessStartToken. An empty argument never
// matches, and neither does an entry whose own ProcStart is empty.
//
// This is the join key to use when correlating against a registry of your own.
// A session's registry file carries its current session ID, which a /clear
// replaces while the process keeps running, so a caller that remembered a
// (pid, session id) pair can find them disagreeing for a session that is
// running and reachable. The start token is stable for the life of the
// process, so it does not drift that way.
func (s Session) MatchesProcess(procStart string) bool {
	return procStart != "" && s.ProcStart == procStart
}

// FindByPIDForProcess is FindByPID for a caller who remembered a
// (pid, process start token) pair: it returns the session registered under pid
// only when MatchesProcess confirms the PID has not been recycled onto an
// unrelated process since.
//
// Prefer this over FindByPIDForSession when the session ID you hold came from
// your own records rather than from the session itself, since a /clear changes
// the session's ID without changing its process. See MatchesProcess.
func FindByPIDForProcess(pid int, procStart string) (Session, error) {
	s, err := FindByPID(pid)
	if err != nil {
		return Session{}, err
	}
	if !s.MatchesProcess(procStart) {
		return Session{}, fmt.Errorf("%w with pid %d: entry belongs to a different process", ErrNotFound, pid)
	}
	return s, nil
}

// FindBySessionID returns the registered session with the given session UUID.
//
// A session ID survives a resume onto a new PID, so more than one registry
// entry can carry it. When several match, the reachable one wins; if several
// are reachable, FindBySessionID returns ErrAmbiguous.
func FindBySessionID(sessionID string) (Session, error) {
	return findBySessionID(sessionID, defaultProbeTimeout)
}

// findBySessionID is FindBySessionID with the reachability probe timeout as a
// parameter, so Client can apply its own.
func findBySessionID(sessionID string, probe time.Duration) (Session, error) {
	if sessionID == "" {
		return Session{}, fmt.Errorf("%w: empty session id", ErrNotFound)
	}
	sessions, err := ListSessions()
	if err != nil {
		return Session{}, err
	}
	var matches []Session
	for _, s := range sessions {
		if s.SessionID == sessionID {
			matches = append(matches, s)
		}
	}
	return pickOne(matches, fmt.Sprintf("session id %s", sessionID), probe)
}

// FindByName returns the session answering to a name. Names are not unique
// across sessions; when several live sessions share one, FindByName returns
// ErrAmbiguous and the caller should pick from ListSessions itself.
func FindByName(name string) (Session, error) {
	return findByName(name, defaultProbeTimeout)
}

// findByName is FindByName with the reachability probe timeout as a parameter,
// so Client can apply its own.
func findByName(name string, probe time.Duration) (Session, error) {
	if name == "" {
		return Session{}, fmt.Errorf("%w: empty name", ErrNotFound)
	}
	sessions, err := ListSessions()
	if err != nil {
		return Session{}, err
	}
	var matches []Session
	for _, s := range sessions {
		if s.Name == name {
			matches = append(matches, s)
		}
	}
	return pickOne(matches, fmt.Sprintf("name %q", name), probe)
}

// reachableFunc is the reachability check pickOne applies. Production always
// uses Session.Reachable; it is a variable so a test can observe the timeout
// that actually reaches the probe, which is the one thing a caller setting
// Client.ProbeTimeout is paying for.
var reachableFunc = Session.Reachable

func pickOne(matches []Session, what string, probe time.Duration) (Session, error) {
	switch len(matches) {
	case 0:
		return Session{}, fmt.Errorf("%w for %s", ErrNotFound, what)
	case 1:
		return matches[0], nil
	}
	var live []Session
	for _, s := range matches {
		if reachableFunc(s, probe) {
			live = append(live, s)
		}
	}
	switch len(live) {
	case 0:
		return Session{}, fmt.Errorf("%w: %d stale entries for %s, none reachable", ErrNotFound, len(matches), what)
	case 1:
		return live[0], nil
	default:
		return Session{}, fmt.Errorf("%w: %d live sessions share %s", ErrAmbiguous, len(live), what)
	}
}
