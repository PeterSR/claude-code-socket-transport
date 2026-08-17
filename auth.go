package ccsock

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// maxKeyFileBytes matches the cap Claude Code applies when reading an inbox
// auth key.
const maxKeyFileBytes = 4096

var (
	keyFileRe   = regexp.MustCompile(`^(\d+)\.[0-9a-f]{64}\.key$`)
	peerTokenRe = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// KeyFileName returns the auth-key filename a session with the given PID
// publishes for an inbox bound at socketPath: "<pid>.<sha256(path)>.key".
func KeyFileName(pid int, socketPath string) (string, error) {
	suffix, err := keyFileSuffix(socketPath)
	if err != nil {
		return "", err
	}
	return strconv.Itoa(pid) + suffix, nil
}

// keyFileSuffix returns ".<sha256hex(canonical path)>.key".
func keyFileSuffix(socketPath string) (string, error) {
	canonical, err := canonicalSocketPath(socketPath)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(canonical))
	return "." + hex.EncodeToString(sum[:]) + ".key", nil
}

type keyFile struct {
	PeerToken string `json:"peerToken"`
	ProcStart string `json:"procStart"`
}

// LookupToken finds the inbox auth token a session published for the inbox
// bound at socketPath.
//
// A session writes <config>/sessions/<listener pid>.<sha256(socket path)>.key
// when it binds its inbox, and removes it on shutdown. An empty token with a
// nil error means no key was published, which is not fatal: on macOS and Linux
// a session accepts unauthenticated frames, and the auth line only tells the
// receiver which permission class the sender belongs to.
func LookupToken(socketPath string) (string, error) {
	dir, err := SessionsDir()
	if err != nil {
		return "", err
	}
	suffix, err := keyFileSuffix(socketPath)
	if err != nil {
		return "", err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("reading sessions directory %s: %w", dir, err)
	}

	// Several key files can name the same socket when a PID was recycled or a
	// previous owner died without cleaning up. Rank them the way Claude Code
	// does and take the best: a live owner whose start token still matches
	// beats a live owner we cannot corroborate, which beats a dead owner.
	bestToken, bestRank := "", -1
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		m := keyFileRe.FindStringSubmatch(name)
		if m == nil {
			continue
		}
		ownerPID, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		kf, err := readKeyFile(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		if rank := rankKeyOwner(ownerPID, kf.ProcStart); rank > bestRank {
			bestToken, bestRank = kf.PeerToken, rank
		}
	}
	return bestToken, nil
}

func readKeyFile(path string) (keyFile, error) {
	data, err := readSmallFile(path, maxKeyFileBytes)
	if err != nil {
		if errors.Is(err, errNotSmallFile) {
			return keyFile{}, fmt.Errorf("%s is not a readable key file", path)
		}
		return keyFile{}, err
	}
	var kf keyFile
	if err := json.Unmarshal(data, &kf); err != nil {
		return keyFile{}, err
	}
	if !peerTokenRe.MatchString(kf.PeerToken) {
		return keyFile{}, fmt.Errorf("%s has no well-formed peerToken", path)
	}
	return kf, nil
}

// rankKeyOwner scores how much a key file's owning process corroborates it:
// 2 alive with a matching start token, 1 alive but unverifiable, 0 gone.
func rankKeyOwner(pid int, procStart string) int {
	if pid <= 1 || !processAlive(pid) {
		return 0
	}
	if procStart == "" {
		return 1
	}
	actual, err := processStartToken(pid)
	if err != nil {
		return 1
	}
	if actual == procStart {
		return 2
	}
	return 0
}
