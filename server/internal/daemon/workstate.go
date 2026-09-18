package daemon

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/multica-ai/multica/server/internal/cli"
	"github.com/multica-ai/multica/server/internal/daemon/execenv"
)

// Work-state identity (GH #8280)
//
// Daemon identity is machine + backend on purpose: EnsureDaemonID reads one
// ~/.multica/daemon.id for every profile, and the server upserts a runtime row
// keyed on (workspace_id, daemon_id, provider). Two daemons on one host talking
// to one backend therefore ARE one runtime, no matter which profile launched
// them.
//
// Persistent work state used to disagree with that. The workspaces root, the
// Codex session namespace and the profile-relative Hermes/Reasonix/DSH stores
// were all keyed on the Multica profile *name*, so a CLI daemon (default
// profile) and a Desktop daemon (desktop-<host> profile) aimed at the same
// backend registered as one runtime while each served its tasks from a
// different tree. A task created by one of them could not be resumed by the
// other: the server handed the claiming daemon the prior work_dir and
// session_id (gated only on the runtime id, which matched), the daemon found
// that path outside its own root, prepared a fresh empty env, and the
// conversation restarted with no memory.
//
// WorkStateScope is the one daemon-level concept that replaces that. It is
// derived from the normalized backend - not from the profile name, not from the
// device name - and every persistent-state path the daemon owns hangs off it:
//
//	Multica profile   auth / config / log / pid / health-port boundary
//	work-state scope  physical machine + normalized backend boundary
//
// Two profiles on one machine pointing at one backend resolve one scope and
// therefore one tree. Different backends never share a tree.
//
// Scope is resolved by *adoption*, never by a silent cutover: an installation
// that already has state in a profile-scoped location keeps using that location,
// so an upgrade cannot turn a resumable conversation into a fresh one. Only a
// machine whose tree is empty (a new installation, or a backend never used here
// before) gets the backend-scoped default. When two non-empty legacy trees claim
// the same backend the daemon refuses to start rather than guessing - see
// WorkStateConflictError.

const (
	// workStateRootDirName is the machine-scoped directory under ~/.multica that
	// holds daemon-managed provider state for one backend. It is a sibling of
	// profiles/, so no profile can shadow or collide with it.
	workStateRootDirName = "work-state"

	// workspacesRootDirName is the base name of the daemon workspaces root:
	// ~/multica_workspaces for a legacy default-profile install (adopted as-is)
	// and ~/multica_workspaces_<workStateKey> for a backend-scoped default.
	workspacesRootDirName = "multica_workspaces"

	// workspacesRootEnv is the operator override for the workspaces root.
	workspacesRootEnv = "MULTICA_WORKSPACES_ROOT"
)

// workStateTreeNames are the profile-relative subtrees that hold persistent
// provider state: Hermes memory, Hermes session databases, Reasonix state homes
// and DSH session roots. The names are what the daemon store builders use
// (execenv.HermesMemoryStorePath, execenv.HermesSessionStorePath, and
// prepareReasonixTaskStateHome / prepareDshTaskSessionRoot below), so they are
// also what adoption looks for when deciding whether a legacy profile directory
// is worth claiming.
var workStateTreeNames = []string{
	"hermes-state",
	"hermes-sessions",
	reasonixStateDirName,
	dshSessionDirName,
}

// WorkStateScope is the resolved persistent work-state identity for the current
// machine + backend. It is stable across restarts, identical for every Multica
// profile pointing at the same backend, and different for different backends.
type WorkStateScope struct {
	// Key is the fixed-length, filesystem-safe digest of the normalized backend.
	Key string
	// WorkspacesRoot is the base directory for task execution environments
	// (workdirs, repo caches, GC ownership checks).
	WorkspacesRoot string
	// StateRoot is the directory holding daemon-managed provider state for this
	// backend, one fixed subtree per provider (workStateTreeNames).
	StateRoot string
	// CodexNamespace is the multica-sessions namespace segment holding this
	// backend per-conversation Codex stores. Those stores sit under the shared
	// Codex home, which is machine-wide, so they carry the same scope as the rest.
	CodexNamespace string
	// Profiles lists the Multica profiles on this machine that resolve to this
	// scope, sorted. Diagnostics only: it is what the conflict error names.
	Profiles []string
}

// WorkStateScopeParams are the inputs to ResolveWorkStateScope.
type WorkStateScopeParams struct {
	// ServerBaseURL is the normalized backend base URL of the daemon that is
	// starting (LoadConfig serverBaseURL).
	ServerBaseURL string
	// Profile is the Multica profile the daemon starts under ("" = default).
	Profile string
	// ExplicitWorkspacesRoot is the operator-selected workspaces root for
	// Profile - the CLI flag or the profile persisted config.json value - or
	// "" when none was given. MULTICA_WORKSPACES_ROOT is read here as well.
	ExplicitWorkspacesRoot string
}

// WorkStateKey returns the digest that namespaces this backend persistent work
// state. The input is the normalized base URL, so equivalent spellings resolve
// to one key: case is folded, and two profiles that reach the same backend write
// the same tree.
//
// Fixed length and hex-only: safe as a single path segment at any backend URL
// length, and collision-free without a lossy "strip unsafe characters" scheme.
func WorkStateKey(normalizedServerBaseURL string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(normalizedServerBaseURL))))
	return hex.EncodeToString(sum[:8])
}

// ResolveWorkStateScope resolves the persistent work-state scope for one
// machine + backend, adopting existing profile-scoped state where there is
// exactly one such tree and refusing to start where two compete.
func ResolveWorkStateScope(p WorkStateScopeParams) (WorkStateScope, error) {
	key := WorkStateKey(p.ServerBaseURL)
	multicaRoot, err := cli.ProfileDir("")
	if err != nil {
		return WorkStateScope{}, fmt.Errorf("resolve daemon work state: %w", err)
	}

	explicit := strings.TrimSpace(p.ExplicitWorkspacesRoot)
	if explicit == "" {
		explicit = strings.TrimSpace(os.Getenv(workspacesRootEnv))
	}
	if explicit == "" {
		// The starting profile's own persisted root, the same value the CLI folds
		// into the override. Reading it here too keeps every caller - the daemon and
		// a read-only disk-usage scan alike - on one answer, and keeps this profile's
		// explicit choice visible to the other profiles on the machine.
		if cfg, cfgErr := cli.LoadCLIConfigForProfile(p.Profile); cfgErr == nil {
			explicit = strings.TrimSpace(cfg.WorkspacesRoot)
		}
	}

	owners, err := workStateOwners(multicaRoot, key, p.Profile, explicit)
	if err != nil {
		return WorkStateScope{}, err
	}

	scope := WorkStateScope{Key: key, Profiles: make([]string, 0, len(owners))}
	for _, o := range owners {
		scope.Profiles = append(scope.Profiles, o.name)
	}

	// Workspaces root: legacy per-profile root or explicit override per owner,
	// plus the backend-scoped default for a machine that has no tree yet.
	canonicalRoot, err := canonicalWorkspacesRoot(key)
	if err != nil {
		return WorkStateScope{}, err
	}
	wsCandidates := make([]workStateCandidate, 0, 2*len(owners)+1)
	for _, o := range owners {
		if o.explicitRoot != "" {
			wsCandidates = append(wsCandidates, workStateCandidate{label: o.label(), path: absWorkStatePath(o.explicitRoot), explicit: true})
		}
		if legacy := o.legacyWorkspacesRoot(); legacy != "" {
			wsCandidates = append(wsCandidates, workStateCandidate{label: o.label(), path: legacy})
		}
	}
	wsCandidates = append(wsCandidates, workStateCandidate{label: "backend-scoped default", path: canonicalRoot})
	scope.WorkspacesRoot, err = resolveWorkStateTree("workspaces root", p.ServerBaseURL,
		wsCandidates, canonicalRoot, workStateRootHasState,
		"Set MULTICA_WORKSPACES_ROOT (or workspaces_root in the profile config.json) to the tree "+
			"this machine should keep - every profile for this backend then follows that choice - and "+
			"archive or remove the other once you have checked it.")
	if err != nil {
		return WorkStateScope{}, err
	}

	// Provider state root: the profile directory that owns hermes-state,
	// hermes-sessions, reasonix-state and dsh-sessions, or the backend-scoped
	// default. The Hermes *native* profile segment inside those stores is a
	// separate concept and stays exactly as it is.
	stateCandidates := make([]workStateCandidate, 0, len(owners))
	for _, o := range owners {
		stateCandidates = append(stateCandidates, workStateCandidate{label: o.label(), path: o.profileDir})
	}
	canonicalState := filepath.Join(multicaRoot, workStateRootDirName, key)
	scope.StateRoot, err = resolveWorkStateTree("provider state root", p.ServerBaseURL,
		stateCandidates, canonicalState, profileDirHasProviderState,
		"Archive or remove one of the trees once you have checked it. Multica will not merge or "+
			"silently choose between two non-empty provider state trees.")
	if err != nil {
		return WorkStateScope{}, err
	}

	// Codex session namespace: one namespace per profile before this change, one
	// per backend after it.
	codexCandidates := make([]workStateCandidate, 0, len(owners))
	for _, o := range owners {
		codexCandidates = append(codexCandidates, workStateCandidate{
			label: o.label(), path: execenv.CodexSessionNamespaceForProfile(o.name),
		})
	}
	canonicalNamespace := execenv.CodexSessionNamespaceForWorkState(key)
	scope.CodexNamespace, err = resolveWorkStateTree("Codex session namespace", p.ServerBaseURL,
		codexCandidates, canonicalNamespace, execenv.CodexSessionNamespaceHasState,
		"Archive or remove one of the namespaces under the shared Codex home "+
			"(<codex home>/multica-sessions) once you have checked it.")
	if err != nil {
		return WorkStateScope{}, err
	}

	return scope, nil
}

// WorkStateScopeForProfile resolves the scope for a profile from its recorded
// backend. Read-only callers (daemon disk-usage, aggregate scans) use this so
// they land on the same directory the running daemon uses. A profile with no
// recorded server_url resolves against the built-in default backend, the same
// fallback LoadConfig applies.
func WorkStateScopeForProfile(profile, explicitWorkspacesRoot string) (WorkStateScope, error) {
	raw := ""
	if cfg, err := cli.LoadCLIConfigForProfile(profile); err == nil {
		raw = strings.TrimSpace(cfg.ServerURL)
	}
	if raw == "" {
		raw = DefaultServerURL
	}
	baseURL, err := NormalizeServerBaseURL(raw)
	if err != nil {
		return WorkStateScope{}, fmt.Errorf("resolve work state for profile %q: %w", profile, err)
	}
	return ResolveWorkStateScope(WorkStateScopeParams{
		ServerBaseURL:          baseURL,
		Profile:                profile,
		ExplicitWorkspacesRoot: explicitWorkspacesRoot,
	})
}

// workStateOwner is one Multica profile on this machine that claims the current
// backend.
type workStateOwner struct {
	name         string // profile name, "" = default
	profileDir   string // ~/.multica or ~/.multica/profiles/<name>
	explicitRoot string // operator-selected workspaces root, or ""
}

func (o workStateOwner) label() string {
	if o.name == "" {
		return "default profile"
	}
	return "profile " + o.name
}

// legacyWorkspacesRoot is the pre-#8280, profile-derived workspaces root.
func (o workStateOwner) legacyWorkspacesRoot() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	if o.name == "" {
		return filepath.Join(home, workspacesRootDirName)
	}
	return filepath.Join(home, workspacesRootDirName+"_"+o.name)
}

// workStateOwners returns every profile on this machine that claims key,
// including the one that is starting. The starting profile always counts - it is
// talking to this backend right now, whatever its config.json says - while any
// other profile counts only when its recorded server_url normalizes to the same
// key. That asymmetry keeps another backend tree out of the candidate set while
// still adopting state from a profile that never recorded a URL.
func workStateOwners(multicaRoot, key, currentProfile, currentExplicitRoot string) ([]workStateOwner, error) {
	current, err := newWorkStateOwner(currentProfile, currentExplicitRoot)
	if err != nil {
		return nil, err
	}
	owners := []workStateOwner{current}

	namesOnDisk, err := profileNamesOnDisk(multicaRoot)
	if err != nil {
		return nil, err
	}
	// The default profile is not a directory under profiles/, so it is named
	// explicitly. Leaving it out would make its state invisible whenever a named
	// profile starts - which is precisely the case #8280 is about (the Desktop
	// daemon arriving to find the tree the CLI daemon wrote).
	names := append([]string{""}, namesOnDisk...)
	for _, name := range names {
		if name == currentProfile {
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
		baseURL, err := NormalizeServerBaseURL(raw)
		if err != nil || WorkStateKey(baseURL) != key {
			continue
		}
		owner, err := newWorkStateOwner(name, strings.TrimSpace(cfg.WorkspacesRoot))
		if err != nil {
			continue
		}
		owners = append(owners, owner)
	}

	sort.SliceStable(owners, func(i, j int) bool { return owners[i].name < owners[j].name })
	return owners, nil
}

func newWorkStateOwner(name, explicitRoot string) (workStateOwner, error) {
	dir, err := cli.ProfileDir(name)
	if err != nil {
		return workStateOwner{}, fmt.Errorf("resolve work state: profile %q: %w", name, err)
	}
	return workStateOwner{name: name, profileDir: dir, explicitRoot: explicitRoot}, nil
}

// profileNamesOnDisk lists the named profiles under <multica root>/profiles,
// sorted. A missing directory is not an error - a machine with only the default
// profile has none.
func profileNamesOnDisk(multicaRoot string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(multicaRoot, "profiles"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("resolve daemon work state: read profiles dir: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

// canonicalWorkspacesRoot is the backend-scoped default root a machine with no
// existing state uses: ~/multica_workspaces_<workStateKey>.
func canonicalWorkspacesRoot(key string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w (set MULTICA_WORKSPACES_ROOT to override)", err)
	}
	return filepath.Join(home, workspacesRootDirName+"_"+key), nil
}

// workStateCandidate is one possible location for a work-state tree.
type workStateCandidate struct {
	label    string
	path     string
	explicit bool
}

// resolveWorkStateTree picks the single location a work-state tree resolves to.
//
//   - An operator-selected location wins outright and becomes the canonical
//     mapping for this backend, so every profile on the machine follows it. Two
//     profiles that explicitly select different locations are a conflict.
//   - Otherwise exactly one adopted location may hold state. Adoption keeps the
//     path an existing installation already uses, which is what makes an upgrade
//     unable to orphan a resumable conversation.
//   - Two non-empty locations are a conflict. Choosing one would discard work the
//     user can still see on disk, and merging them is the recursive "hope"
//     behavior that lost state in the first place.
//   - With no state anywhere, the backend-scoped default applies.
func resolveWorkStateTree(tree, backend string, candidates []workStateCandidate, canonical string, hasState func(string) bool, hint string) (string, error) {
	candidates = dedupeWorkStateCandidates(candidates)

	var explicit []workStateCandidate
	for _, c := range candidates {
		if c.explicit && c.path != "" {
			explicit = append(explicit, c)
		}
	}
	if len(explicit) > 1 {
		return "", &WorkStateConflictError{
			Tree: tree, Backend: backend, Candidates: explicit,
			Choosing: "two profiles explicitly select different locations",
			Hint:     "Remove one of the explicit settings so every profile for this backend selects one tree.",
		}
	}
	if len(explicit) == 1 {
		return explicit[0].path, nil
	}

	var adopted []workStateCandidate
	for _, c := range candidates {
		if c.path != "" && hasState(c.path) {
			adopted = append(adopted, c)
		}
	}
	if len(adopted) > 1 {
		return "", &WorkStateConflictError{
			Tree: tree, Backend: backend, Candidates: adopted,
			Choosing: "more than one existing tree holds state for this backend",
			Hint:     hint,
		}
	}
	if len(adopted) == 1 {
		return adopted[0].path, nil
	}
	return canonical, nil
}

// dedupeWorkStateCandidates drops candidates that resolve to a path already in
// the list, keeping the first, so an explicit entry outlives a derived one that
// happens to agree with it. Comparison is case-insensitive on Windows, where two
// spellings of one directory are one directory.
func dedupeWorkStateCandidates(candidates []workStateCandidate) []workStateCandidate {
	out := make([]workStateCandidate, 0, len(candidates))
	for _, c := range candidates {
		if c.path == "" {
			continue
		}
		dup := false
		for _, kept := range out {
			if kept.explicit == c.explicit && sameWorkStatePath(kept.path, c.path) {
				dup = true
				break
			}
		}
		if !dup {
			out = append(out, c)
		}
	}
	return out
}

func sameWorkStatePath(a, b string) bool {
	a, b = filepath.Clean(a), filepath.Clean(b)
	if a == b {
		return true
	}
	return runtime.GOOS == "windows" && strings.EqualFold(a, b)
}

// absWorkStatePath makes a candidate absolute so two spellings of one root
// dedupe and a conflict error names a path the operator can act on. An
// unresolvable path is used as given.
func absWorkStatePath(path string) string {
	if abs, err := filepath.Abs(path); err == nil {
		return abs
	}
	return path
}

// workStateRootHasState reports whether a workspaces root holds authoritative
// state rather than the daemon own bookkeeping. Every daemon-owned entry at a
// workspaces root is a dot directory (.repos, .skill-cache, .task-roots,
// .multica/), while a workspace or task directory is a UUID.
func workStateRootHasState(path string) bool {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".") {
			return true
		}
	}
	return false
}

// profileDirHasProviderState reports whether a Multica profile directory owns
// persistent provider state. It looks only at the daemon-managed subtrees: the
// default profile directory always contains config.json, daemon.log and
// daemon.pid, and none of those mean "there is a task to continue".
func profileDirHasProviderState(profileDir string) bool {
	for _, tree := range workStateTreeNames {
		entries, err := os.ReadDir(filepath.Join(profileDir, tree))
		if err == nil && len(entries) > 0 {
			return true
		}
	}
	return false
}

// WorkStateConflictError reports that one persistent work-state tree resolved to
// more than one non-empty location for a single backend, so serving the runtime
// would silently pick a tree for tasks created under another. Both trees are left
// exactly as they are; the daemon refuses to start instead.
type WorkStateConflictError struct {
	Tree       string
	Backend    string
	Choosing   string
	Candidates []workStateCandidate
	Hint       string
}

func (e *WorkStateConflictError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "daemon work state conflict: the %s for backend %s is not unique on this machine: %s:", e.Tree, e.Backend, e.Choosing)
	for _, c := range e.Candidates {
		b.WriteString("\n  - " + c.path + " (" + c.label + ")")
	}
	b.WriteString("\nThis machine has one daemon id shared by every profile, so serving this backend from more than one of these trees loses the workdir and provider session of every task created under the other (GH #8280). Nothing was moved or deleted. " + e.Hint)
	return b.String()
}
