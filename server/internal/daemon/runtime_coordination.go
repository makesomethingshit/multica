package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/multica-ai/multica/server/internal/cli"
)

// Rolling-upgrade safety for the runtime-owner claim (GH #8280).
//
// The owner claim is an OS lock, and an OS lock only excludes processes that know
// to take it. A daemon from a previous release registers, claims, heartbeats and
// recovers orphans without ever looking at it, so two daemons of different
// versions on one machine would both serve the same runtime - the exact failure
// this revision exists to prevent. Nothing inside the lock can detect that,
// because the legacy process is not misbehaving by its own rules.
//
// So a new daemon refuses to ACTIVATE a runtime while it can see a live
// same-machine, same-backend peer that does not advertise the coordination
// capability. The advertisement is additive on the health surface every daemon
// already exposes per profile, so no new protocol is needed.
//
// This is a fail-closed standby rather than a startup abort: the new daemon keeps
// probing and takes over once the legacy peer is gone, which is the same takeover
// path an owner crash uses.

// RuntimeCoordinationVersion is the capability a daemon advertises when it
// participates in the runtime-owner claim. Additive: a peer that does not
// advertise it is treated as a legacy daemon that cannot be coordinated with.
const RuntimeCoordinationVersion = 1

// runtimeCoordinationKey is the health-payload field carrying the capability.
const runtimeCoordinationKey = "runtime_coordination_version"

// peerProbeTimeout bounds one peer health probe. Short: a probe that hangs must
// not delay a daemon startup, and a peer that cannot answer within this budget is
// not evidence of anything, so the probe reports "not alive".
const peerProbeTimeout = 700 * time.Millisecond

// runtimeCoordinationPeer is one sibling daemon found on this machine.
type runtimeCoordinationPeer struct {
	Profile string
	URL     string
}

// runtimeCoordinationPeers lists the health endpoints of every OTHER Multica
// profile on this machine that points at the same normalized backend.
//
// The profile directories are the machine-local inventory of daemon processes:
// each profile owns one config (its backend) and one health port, both derived
// from names the daemon itself computed, so no service discovery or registry is
// needed. A profile whose config cannot be read is skipped rather than guessed
// at - an unreadable peer is indistinguishable from an absent one, and treating
// it as absent is the unsafe direction.
func runtimeCoordinationPeers(backend string, ownProfile string) []runtimeCoordinationPeer {
	root, err := cli.ProfileDir("")
	if err != nil {
		return nil
	}
	entries, err := os.ReadDir(filepath.Join(root, "profiles"))
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)

	peers := make([]runtimeCoordinationPeer, 0, len(names)+1)
	// The default profile is not a directory under profiles/, so it is named
	// explicitly - the same reason workStateOwners() names it explicitly.
	for _, name := range append([]string{""}, names...) {
		if name == ownProfile {
			continue
		}
		cfg, err := cli.LoadCLIConfigForProfile(name)
		if err != nil {
			continue
		}
		raw := strings.TrimSpace(cfg.ServerURL)
		if raw == "" {
			continue
		}
		peerBackend, err := NormalizeServerBaseURL(raw)
		if err != nil || peerBackend != backend {
			continue
		}
		peers = append(peers, runtimeCoordinationPeer{
			Profile: name,
			URL:     fmt.Sprintf("http://127.0.0.1:%d/health", healthPortForProfile(name)),
		})
	}
	return peers
}

// healthPortForProfile mirrors cmd/multica healthPortForProfile. The cmd/multica
// test TestWorkStateSharedWhileProfilesStayIsolated and this package's
// coordination test pin both derivations to the same values.
func healthPortForProfile(profile string) int {
	if profile == "" {
		return DefaultHealthPort
	}
	var h int
	for _, b := range []byte(profile) {
		h += int(b)
	}
	return DefaultHealthPort + 1 + (h % 1000)
}

// peerCoordinationStatus is what one probe concluded about a peer.
type peerCoordinationStatus struct {
	// Alive is true when the peer answered its health endpoint.
	Alive bool
	// Coordinates is true when the live peer advertises the capability, i.e. it
	// participates in the runtime-owner claim.
	Coordinates bool
}

// peerProbeFunc performs one health probe. Indirected so tests can model a legacy
// peer without standing up a second process.
var peerProbeFunc = probeRuntimeCoordinationPeer

// probeRuntimeCoordinationPeer asks one peer what it is.
//
// An unreachable or unparseable peer reports not-alive, which is the "no
// evidence" answer: an old daemon that does not serve health at all cannot be
// distinguished from a stopped one, and the caller decides what that means.
func probeRuntimeCoordinationPeer(ctx context.Context, url string) peerCoordinationStatus {
	probeCtx, cancel := context.WithTimeout(ctx, peerProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, url, nil)
	if err != nil {
		return peerCoordinationStatus{}
	}
	resp, err := (&http.Client{Timeout: peerProbeTimeout}).Do(req)
	if err != nil {
		return peerCoordinationStatus{}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return peerCoordinationStatus{}
	}
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		// A health endpoint that answers but is not JSON still proves the process
		// is alive; it just cannot prove coordination.
		return peerCoordinationStatus{Alive: true}
	}
	version, ok := payload[runtimeCoordinationKey].(float64)
	return peerCoordinationStatus{
		Alive:       true,
		Coordinates: ok && int(version) >= RuntimeCoordinationVersion,
	}
}

// legacyPeerDecision is what the caller should do about the peers it found.
type legacyPeerDecision struct {
	// Blocked is true when a live peer that cannot be coordinated with exists, so
	// this process must stay in standby rather than activate a runtime.
	Blocked bool
	// Peers lists the live non-coordinating peers, for the actionable log line.
	Peers []string
}

// checkRuntimeCoordinationPeers probes every same-backend sibling and reports
// whether any of them is a live daemon that cannot be coordinated with.
//
// A coordinating peer is not a blocker: it takes the same owner claim this
// process does, so the claim - not this probe - decides which of them serves a
// runtime, and the loser reports peer-owned candidates as standby.
func (d *Daemon) checkRuntimeCoordinationPeers(ctx context.Context) legacyPeerDecision {
	peers := runtimeCoordinationPeers(d.cfg.ServerBaseURL, d.cfg.Profile)
	if len(peers) == 0 {
		return legacyPeerDecision{}
	}
	var decision legacyPeerDecision
	for _, peer := range peers {
		status := peerProbeFunc(ctx, peer.URL)
		if !status.Alive || status.Coordinates {
			continue
		}
		decision.Blocked = true
		label := peer.Profile
		if label == "" {
			label = "default profile"
		}
		decision.Peers = append(decision.Peers, fmt.Sprintf("%s (%s)", label, peer.URL))
	}
	return decision
}

// logLegacyPeerStandby records the actionable reason a daemon is standing by.
func (d *Daemon) logLegacyPeerStandby(decision legacyPeerDecision) {
	d.logger.Warn("another Multica daemon on this machine serves the same backend but does not support runtime coordination; "+
		"standing by so this process cannot activate a runtime that peer is already serving. "+
		"Update that daemon (or stop it) to hand the runtime over.",
		"peers", decision.Peers)
}
