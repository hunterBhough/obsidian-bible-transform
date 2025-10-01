package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	InputParent  string
	OutputParent string
	DryRun       bool
	Workers      int
	LogFile      string
}

type JobType int

const (
	JobCopy JobType = iota
	JobLexicon
	JobChapterDir
)

type Job struct {
	Typ JobType
	// For JobCopy and JobLexicon: absolute input file path
	// For JobChapterDir: absolute chapter directory path
	Path string
}

type Stats struct {
	books        sync.Map // string -> struct{}
	chapters     atomic.Int64
	verses       atomic.Int64
	linksRewritt atomic.Int64
	filesCreate  atomic.Int64
	filesUpdate  atomic.Int64
	filesSkip    atomic.Int64
	start        time.Time
}

var (
	verseHeaderRe = regexp.MustCompile(`(?m)^###\s+(\d+)\b.*$`)
	h2AliasRe     = regexp.MustCompile(`(?m)^##\s+(.+)$`)
	// Matches [[H123]] / [[G123]] and with alias: [[H123|...]] / [[G123|...]]
	// Capture group 1 = full code (e.g., H7225 or G3056)
	linkRe = regexp.MustCompile(`\[\[((H|G)\d+)(?:\|[^]]+)?\]\]`)
	// block id at line end like ^something
	trailingBlockIDRe = regexp.MustCompile(`(?m)(?:^|\s)\^([A-Za-z0-9\-_]+)\s*$`)
	blockIDLineOnlyRe = regexp.MustCompile(`(?m)^\s*\^[A-Za-z0-9\-_]+\s*$`)
	blockIDAtEOLRe    = regexp.MustCompile(`\s*\^[A-Za-z0-9\-_]+$`)
)

type VerseBlock struct {
	Num     int
	Start   int // byte index of block start
	End     int // byte index of block end (exclusive)
	Content string
}

type Logger struct {
	mu  sync.Mutex
	log *log.Logger
}

func (l *Logger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log.Printf(format, args...)
}

func main() {
	cfg := parseFlags()
	logger := mustInitLogger(cfg)

	stats := &Stats{start: time.Now()}

	// 1) Collect chapter dirs, lexicon files, and "other" files for byte-for-byte copy
	chapterDirs, lexiconFiles, otherFiles, books, err := discover(cfg.InputParent)
	if err != nil {
		logger.Printf("[FATAL] discover: %v", err)
		os.Exit(1)
	}
	for b := range books {
		stats.books.Store(b, struct{}{})
	}

	// 2) Build alias map by scanning Lexicon/Hebrew and Lexicon/Greek
	aliasMap, err := buildAliasMap(lexiconFiles)
	if err != nil {
		logger.Printf("[FATAL] alias map: %v", err)
		os.Exit(1)
	}
	logger.Printf("[INFO] Alias entries loaded: %d", len(aliasMap))

	// 3) Prepare jobs
	jobs := make(chan Job, 1024)
	wg := &sync.WaitGroup{}

	// 4) Worker pool
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for job := range jobs {
				switch job.Typ {
				case JobCopy:
					if err := doCopyJob(cfg, job.Path, aliasMap, logger, stats); err != nil {
						logger.Printf("[ERROR] copy: %s: %v", job.Path, err)
					}
				case JobLexicon:
					if err := doLexiconJob(cfg, job.Path, aliasMap, logger, stats); err != nil {
						logger.Printf("[ERROR] lexicon: %s: %v", job.Path, err)
					}
				case JobChapterDir:
					if err := doChapterJob(cfg, job.Path, aliasMap, logger, stats); err != nil {
						logger.Printf("[ERROR] chapter: %s: %v", job.Path, err)
					}
				default:
					logger.Printf("[WARN] unknown job type for %s", job.Path)
				}
			}
		}(i + 1)
	}

	// 5) Enqueue jobs
	for _, p := range otherFiles {
		jobs <- Job{Typ: JobCopy, Path: p}
	}
	for _, p := range lexiconFiles {
		jobs <- Job{Typ: JobLexicon, Path: p}
	}
	for _, d := range chapterDirs {
		jobs <- Job{Typ: JobChapterDir, Path: d}
	}

	close(jobs)
	wg.Wait()

	// 6) Summary
	dur := time.Since(stats.start)
	booksCount := 0
	stats.books.Range(func(key, value any) bool { booksCount++; return true })
	logger.Printf("[SUMMARY] books=%d chapters=%d verses=%d links_rewritten=%d files: created=%d updated=%d skipped=%d duration=%s",
		booksCount, stats.chapters.Load(), stats.verses.Load(), stats.linksRewritt.Load(),
		stats.filesCreate.Load(), stats.filesUpdate.Load(), stats.filesSkip.Load(), dur)

	fmt.Printf("Done. See log: %s\n", cfg.LogFile)
}

func parseFlags() Config {
	var cfg Config
	flag.StringVar(&cfg.InputParent, "input-parent", "inputs/KJV_Tagged_Lexicon", "Input parent directory")
	flag.StringVar(&cfg.OutputParent, "output-parent", "outputs/vault_modified", "Output parent directory")
	flag.BoolVar(&cfg.DryRun, "dry-run", true, "If true, plan only (no writes except log directory)")
	flag.IntVar(&cfg.Workers, "workers", 8, "Number of concurrent workers")
	defLog := filepath.Join("outputs/vault_modified", "transform.log")
	flag.StringVar(&cfg.LogFile, "log-file", defLog, "Path to log file (default <output-parent>/transform.log)")
	flag.Parse()
	// if log-file still default but output-parent changed, align it
	if cfg.LogFile == defLog && cfg.OutputParent != "outputs/vault_modified" {
		cfg.LogFile = filepath.Join(cfg.OutputParent, "transform.log")
	}
	return cfg
}

func mustInitLogger(cfg Config) *Logger {
	if err := os.MkdirAll(filepath.Dir(cfg.LogFile), 0o755); err != nil {
		log.Fatalf("failed to create log dir: %v", err)
	}
	f, err := os.Create(cfg.LogFile) // truncate each run
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	mw := io.MultiWriter(f, os.Stdout)
	l := log.New(mw, "", log.LstdFlags)
	l.Printf("[INFO] obsidian-bible-transform (Go %s) dry-run=%v workers=%d", runtime.Version(), cfg.DryRun, cfg.Workers)
	return &Logger{log: l}
}

// discover scans input to identify Bible chapter directories, lexicon files, and other files to copy.
// It also returns a set of book names.
func discover(inputParent string) (chapterDirs []string, lexiconFiles []string, otherFiles []string, books map[string]struct{}, err error) {
	books = make(map[string]struct{})
	bibleRoot := filepath.Join(inputParent, "Bible")
	lexiconRoot := filepath.Join(inputParent, "Lexicon")

	err = filepath.WalkDir(inputParent, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, _ := filepath.Rel(inputParent, path)
		if rel == "." {
			return nil
		}
		if d.IsDir() {
			// A chapter directory looks like: Bible/<Book>/<Chapter>
			if isPotentialChapterDir(bibleRoot, path) {
				chapterDirs = append(chapterDirs, path)
				// record book name
				book := filepath.Base(filepath.Dir(path))
				books[book] = struct{}{}
				// Don't descend further (we'll scan inside during job)
				return fs.SkipDir
			}
			return nil
		}

		// Files:
		if strings.HasPrefix(path, lexiconRoot+string(filepath.Separator)) {
			// Lexicon files (Hebrew/Greek)—rewrite links later
			lexiconFiles = append(lexiconFiles, path)
			return nil
		}
		if strings.HasPrefix(path, bibleRoot+string(filepath.Separator)) {
			// Bible files will be handled by chapter jobs; skip copying here
			return nil
		}
		// Other files are copied byte-for-byte
		otherFiles = append(otherFiles, path)
		return nil
	})
	return
}

func isPotentialChapterDir(bibleRoot, path string) bool {
	// Must have exactly: .../Bible/<Book>/<ChapterDir>
	if !strings.HasPrefix(path, bibleRoot+string(filepath.Separator)) {
		return false
	}
	rel, _ := filepath.Rel(bibleRoot, path)
	parts := splitClean(rel)
	if len(parts) != 2 { // Book + Chapter folder
		return false
	}
	// The directory must contain at least one file; we'll let the job validate further
	return true
}

func splitClean(p string) []string {
	if p == "." {
		return []string{}
	}
	parts := strings.Split(p, string(filepath.Separator))
	out := make([]string, 0, len(parts))
	for _, s := range parts {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func buildAliasMap(files []string) (map[string]string, error) {
	aliases := make(map[string]string)
	for _, p := range files {
		code := strings.TrimSuffix(filepath.Base(p), filepath.Ext(p)) // H7225, G3056, etc.
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("read lexicon %s: %w", p, err)
		}
		alias := extractFirstH2Alias(string(b))
		if alias != "" {
			aliases[code] = alias
		}
	}
	return aliases, nil
}

func extractFirstH2Alias(s string) string {
	m := h2AliasRe.FindStringSubmatch(s)
	if len(m) == 2 {
		return strings.TrimSpace(m[1])
	}
	return ""
}

// ===== Workers =====

func doCopyJob(cfg Config, inputFile string, aliasMap map[string]string, logger *Logger, stats *Stats) error {
	rel, _ := filepath.Rel(cfg.InputParent, inputFile)
	outPath := filepath.Join(cfg.OutputParent, rel)

	planned, action, err := writeBytesIfChanged(cfg, outPath, func() ([]byte, error) {
		b, err := os.ReadFile(inputFile)
		if err != nil {
			return nil, err
		}
		ext := strings.ToLower(filepath.Ext(inputFile))
		if ext == ".md" {
			body := normalizeNewlines(string(b))
			body = ensureEndsWithNewline(body)
			newBody, rew := rewriteLinks(body, aliasMap)
			if rew > 0 {
				stats.linksRewritt.Add(int64(rew))
			}
			return []byte(newBody), nil
		}
		// Non-markdown: copy byte-for-byte
		return b, nil
	}, strings.ToLower(filepath.Ext(inputFile)) != ".md")
	if err != nil {
		return err
	}
	logWrite(logger, action, outPath, planned)
	incrementFileStat(stats, action)
	return nil
}

func doLexiconJob(cfg Config, inputFile string, aliasMap map[string]string, logger *Logger, stats *Stats) error {
	rel, _ := filepath.Rel(cfg.InputParent, inputFile)
	outPath := filepath.Join(cfg.OutputParent, rel)

	planned, action, err := writeBytesIfChanged(cfg, outPath, func() ([]byte, error) {
		b, err := os.ReadFile(inputFile)
		if err != nil {
			return nil, err
		}
		body := normalizeNewlines(string(b))
		body = ensureEndsWithNewline(body)
		newBody, rew := rewriteLinks(body, aliasMap)
		if rew > 0 {
			stats.linksRewritt.Add(int64(rew))
		}
		return []byte(newBody), nil
	}, false)
	if err != nil {
		return err
	}
	logWrite(logger, action, outPath, planned)
	incrementFileStat(stats, action)
	return nil
}

func doChapterJob(cfg Config, chapterDir string, aliasMap map[string]string, logger *Logger, stats *Stats) error {
	// 1) identify chapter file and verse files in the directory
	entries, err := os.ReadDir(chapterDir)
	if err != nil {
		return fmt.Errorf("readdir: %w", err)
	}
	var chapterFile string
	var chapterBase string
	var ext string

	// find chapter file candidates: basename (without extension) contains no dot
	var chapterCandidates []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		base := strings.TrimSuffix(e.Name(), filepath.Ext(e.Name()))
		if !strings.Contains(base, ".") {
			chapterCandidates = append(chapterCandidates, filepath.Join(chapterDir, e.Name()))
		}
	}
	if len(chapterCandidates) == 0 {
		logger.Printf("[WARN] no chapter file detected in %s — falling back to mirror copy", chapterDir)
		return mirrorDirWithMarkdownRewrite(cfg, chapterDir, aliasMap, logger, stats)
	}

	dirBase := filepath.Base(chapterDir) // e.g., "Psalm 1"
	type cand struct{ path, base, ext string }
	cands := make([]cand, 0, len(chapterCandidates))
	for _, p := range chapterCandidates {
		cands = append(cands, cand{
			path: p,
			base: strings.TrimSuffix(filepath.Base(p), filepath.Ext(p)),
			ext:  strings.ToLower(filepath.Ext(p)),
		})
	}

	// Prefer candidates whose basename matches the chapter directory
	var match []cand
	for _, c := range cands {
		if c.base == dirBase {
			match = append(match, c)
		}
	}

	pick := func(list []cand) cand {
		// Prefer .md, else the first in lexicographic order
		best := list[0]
		for _, c := range list[1:] {
			if c.ext == ".md" && best.ext != ".md" {
				best = c
				continue
			}
			if c.ext == best.ext && c.path < best.path {
				best = c
			}
		}
		return best
	}

	switch {
	case len(match) == 1:
		chosen := match[0]
		chapterFile = chosen.path
		chapterBase = chosen.base
		ext = chosen.ext
	case len(match) > 1:
		chosen := pick(match)
		logger.Printf("[WARN] multiple matching chapter files in %s (by dir name). Picking %s among %v", chapterDir, chosen.path, chapterCandidates)
		chapterFile = chosen.path
		chapterBase = chosen.base
		ext = chosen.ext
	case len(chapterCandidates) == 1:
		// Only one candidate overall; accept it
		only := cands[0]
		chapterFile = only.path
		chapterBase = only.base
		ext = only.ext
	default:
		// Multiple overall, none match dirBase -> ambiguous; fall back to mirror
		logger.Printf("[WARN] multiple candidate chapter files in %s — falling back to mirror copy: %v", chapterDir, chapterCandidates)
		return mirrorDirWithMarkdownRewrite(cfg, chapterDir, aliasMap, logger, stats)
	}

	stats.chapters.Add(1)
	// 2) read and parse chapter content
	chapterBytes, err := os.ReadFile(chapterFile)
	if err != nil {
		return fmt.Errorf("read chapter: %w", err)
	}
	chStr := normalizeNewlines(string(chapterBytes))
	blocks, replacedTranslit, err := transformChapterContent(chStr, chapterBase, "kjv-transliteration")
	if err != nil {
		return fmt.Errorf("parse chapter: %w", err)
	}
	// Also render a KJV-only chapter variant with ^kjv embeds
	_, replacedKJV, err := transformChapterContent(chStr, chapterBase, "kjv")
	if err != nil {
		return fmt.Errorf("parse chapter (kjv variant): %w", err)
	}

	// 3) For each verse, update or create the corresponding verse file
	for _, vb := range blocks {
		stats.verses.Add(1)

		verseStem := fmt.Sprintf("%s.%d", chapterBase, vb.Num)
		// locate verse file in input (any extension) preferring same ext as chapter
		inputVersePath, inferredExt := findVerseFileInDir(entries, chapterDir, verseStem, ext)
		if inferredExt == "" {
			inferredExt = ".md" // fallback
		}

		relIn, _ := filepath.Rel(cfg.InputParent, chapterFile) // use chapter file to compute base
		chapterRelDir := filepath.Dir(relIn)
		outVerse := filepath.Join(cfg.OutputParent, chapterRelDir, verseStem+inferredExt)

		planned, action, err := writeBytesIfChanged(cfg, outVerse, func() ([]byte, error) {
			var fm string
			if inputVersePath != "" {
				b, err := os.ReadFile(inputVersePath)
				if err != nil {
					return nil, err
				}
				fm, _ = splitYAMLFrontMatter(string(b))
			}
			original := strings.TrimSpace(vb.Content)
			// KJV: keep original (no alias rewrite), mark as ^kjv
			kjvBody := ensureBlockIDAtEOL(original, "kjv")
			// Transliteration: alias-rewritten, mark as ^kjv-transliteration
			translitBody, rew := rewriteLinks(original, aliasMap)
			if rew > 0 {
				stats.linksRewritt.Add(int64(rew))
			}
			translitBody = ensureBlockIDAtEOL(translitBody, "kjv-transliteration")
			// Compose: YAML front matter + KJV + Transliteration
			// Ensure exactly one blank line between ^kjv and ^kjv-transliteration blocks.
			full := fm + kjvBody + "\n" + translitBody
			full = ensureEndsWithNewline(full)
			return []byte(full), nil
		}, false)
		if err != nil {
			return err
		}
		logWrite(logger, action, outVerse, planned)
		incrementFileStat(stats, action)
	}

	// 4) Write transformed chapter files to output

	// Default (unsuffixed) chapter: write the KJV embeds
	relChapter, _ := filepath.Rel(cfg.InputParent, chapterFile)
	outChapterDefault := filepath.Join(cfg.OutputParent, relChapter)
	plannedDef, actionDef, err := writeBytesIfChanged(cfg, outChapterDefault, func() ([]byte, error) {
		out := ensureEndsWithNewline(replacedKJV)
		return []byte(out), nil
	}, false)
	if err != nil {
		return err
	}
	logWrite(logger, actionDef, outChapterDefault, plannedDef)
	incrementFileStat(stats, actionDef)

	// Transliteration chapter as "<ChapterBase>-kjv-transliteration<ext>"
	relIn, _ := filepath.Rel(cfg.InputParent, chapterFile)
	chapterRelDir := filepath.Dir(relIn)
	translitChapterName := chapterBase + "-kjv-transliteration" + ext
	outChapterTranslit := filepath.Join(cfg.OutputParent, chapterRelDir, translitChapterName)
	plannedT, actionT, err := writeBytesIfChanged(cfg, outChapterTranslit, func() ([]byte, error) {
		out := ensureEndsWithNewline(replacedTranslit)
		return []byte(out), nil
	}, false)
	if err != nil {
		return err
	}
	logWrite(logger, actionT, outChapterTranslit, plannedT)
	incrementFileStat(stats, actionT)

	return nil
}

func findVerseFileInDir(entries []fs.DirEntry, dir, stem, preferredExt string) (string, string) {
	var candidates []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		base := strings.TrimSuffix(name, filepath.Ext(name))
		if base == stem {
			candidates = append(candidates, name)
		}
	}
	if len(candidates) == 0 {
		return "", ""
	}
	// prefer same extension as chapter
	sort.SliceStable(candidates, func(i, j int) bool {
		if filepath.Ext(candidates[i]) == preferredExt {
			return true
		}
		return false
	})
	chosen := candidates[0]
	return filepath.Join(dir, chosen), filepath.Ext(chosen)
}

// ===== Content transforms =====

func transformChapterContent(chapter string, chapterBase string, embedID string) (blocks []VerseBlock, replaced string, err error) {
	idxs := verseHeaderRe.FindAllStringSubmatchIndex(chapter, -1)
	if len(idxs) == 0 {
		// no verse headers; return unchanged
		return nil, chapter, nil
	}

	var out bytes.Buffer
	prev := 0
	for i, m := range idxs {
		fullStart, fullEnd, capStart, capEnd := m[0], m[1], m[2], m[3]
		numStr := chapter[capStart:capEnd]
		num, convErr := atoi(numStr)
		if convErr != nil {
			return nil, "", fmt.Errorf("bad verse number %q", numStr)
		}

		// Write whatever comes before this verse block
		out.WriteString(chapter[prev:fullStart])

		// Compute block end (either next header or EOF)
		var blockEnd int
		if i+1 < len(idxs) {
			blockEnd = idxs[i+1][0]
		} else {
			blockEnd = len(chapter)
		}

		// Keep the verse header as "### <N>" (normalized), then the embed, then a blank line
		out.WriteString(fmt.Sprintf("### %d\n", num))
		out.WriteString(fmt.Sprintf("![[%s.%d#^%s]]\n\n", chapterBase, num, embedID))

		// Save the block for verse-file extraction (content after header line)
		// Header line ends at fullEnd; content starts at fullEnd (which is right before the newline),
		// but our prior parser treats content as chapter[fullEnd:blockEnd].
		content := chapter[fullEnd:blockEnd]
		blocks = append(blocks, VerseBlock{
			Num:     num,
			Start:   fullStart,
			End:     blockEnd,
			Content: content,
		})

		prev = blockEnd
	}
	// append tail
	out.WriteString(chapter[prev:])

	replaced = out.String()
	return blocks, replaced, nil
}

func atoi(s string) (int, error) {
	var n int
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, errors.New("not a number")
		}
		n = n*10 + int(r-'0')
	}
	return n, nil
}

func splitYAMLFrontMatter(s string) (frontMatter string, rest string) {
	rs := normalizeNewlines(s)
	if !strings.HasPrefix(rs, "---\n") {
		return "", rs
	}
	idx := strings.Index(rs[len("---\n"):], "\n---\n")
	if idx < 0 {
		// malformed; treat as no YAML to avoid data loss
		return "", rs
	}
	// front matter includes both fences
	fm := rs[:len("---\n")+idx+len("\n---\n")]
	body := rs[len(fm):]
	return fm, body
}

func ensureBlockIDAtEOL(body, id string) string {
	s := normalizeNewlines(body)
	s = strings.TrimRight(s, "\r\n")
	lines := strings.Split(s, "\n")

	// Drop trailing blank or block-ID-only lines
	for len(lines) > 0 && (strings.TrimSpace(lines[len(lines)-1]) == "" || blockIDLineOnlyRe.MatchString(lines[len(lines)-1])) {
		lines = lines[:len(lines)-1]
	}
	if len(lines) == 0 {
		return "^" + id + "\n"
	}

	// Remove any trailing block ID from the last content line, then append ^id
	last := strings.TrimRight(blockIDAtEOLRe.ReplaceAllString(lines[len(lines)-1], ""), " \t")
	if last == "" {
		// If the (now cleaned) last line is empty, attach to the previous content line if possible
		for i := len(lines) - 2; i >= 0; i-- {
			if strings.TrimSpace(lines[i]) != "" && !blockIDLineOnlyRe.MatchString(lines[i]) {
				cleaned := strings.TrimRight(blockIDAtEOLRe.ReplaceAllString(lines[i], ""), " \t")
				lines[i] = cleaned + " ^" + id
				lines = lines[:i+1]
				return strings.Join(lines, "\n") + "\n"
			}
		}
		return "^" + id + "\n"
	}
	lines[len(lines)-1] = last + " ^" + id
	return strings.Join(lines, "\n") + "\n"
}

func rewriteLinks(s string, alias map[string]string) (string, int) {
	count := 0
	out := linkRe.ReplaceAllStringFunc(s, func(m string) string {
		sub := linkRe.FindStringSubmatch(m)
		code := sub[1] // H7225
		a, ok := alias[code]
		if !ok || strings.TrimSpace(a) == "" {
			return m // leave as-is if alias missing
		}
		count++
		return "[[" + code + "|" + a + "]]"
	})
	return out, count
}

func normalizeNewlines(s string) string {
	// convert CRLF -> LF
	return strings.ReplaceAll(s, "\r\n", "\n")
}

func ensureEndsWithNewline(s string) string {
	if !strings.HasSuffix(s, "\n") {
		return s + "\n"
	}
	return s
}

func logWrite(logger *Logger, action, outPath string, plannedBytes int) {
	logger.Printf("[%s] %s (%d bytes)", action, outPath, plannedBytes)
}

func incrementFileStat(st *Stats, action string) {
	switch action {
	case "CREATE", "PLAN-CREATE":
		st.filesCreate.Add(1)
	case "UPDATE", "PLAN-UPDATE":
		st.filesUpdate.Add(1)
	case "SKIP", "PLAN-SKIP":
		st.filesSkip.Add(1)
	}
}

func writeBytesIfChanged(cfg Config, outPath string, producer func() ([]byte, error), byteForByte bool) (plannedBytes int, action string, err error) {
	// read current output (if exists)
	exists := false
	var cur []byte
	if b, rerr := os.ReadFile(outPath); rerr == nil {
		exists = true
		cur = b
	}

	// produce desired content
	want, err := producer()
	if err != nil {
		return 0, "", err
	}

	if !byteForByte {
		// enforce trailing newline and UTF-8 w/out BOM (Go writes no BOM by default)
		want = []byte(ensureEndsWithNewline(string(want)))
	}

	if exists && bytes.Equal(cur, want) {
		return len(want), ternary(cfg.DryRun, "PLAN-SKIP", "SKIP"), nil
	}

	if cfg.DryRun {
		if exists {
			return len(want), "PLAN-UPDATE", nil
		}
		return len(want), "PLAN-CREATE", nil
	}

	// ensure directory
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return 0, "", err
	}
	// write
	mode := fs.FileMode(0o644)
	if err := os.WriteFile(outPath, want, mode); err != nil {
		return 0, "", err
	}
	if exists {
		return len(want), "UPDATE", nil
	}
	return len(want), "CREATE", nil
}

func ternary[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

// ===== Utilities kept public(ish) for tests =====

// Parse all verse blocks and return replaced content (embed links)
// Exposed for tests.
func ParseAndReplaceChapter(chapter, chapterBase string) ([]VerseBlock, string, error) {
	return transformChapterContent(chapter, chapterBase, "kjv")
}

func SplitYAMLFrontMatter(s string) (string, string) { return splitYAMLFrontMatter(s) }

func EnsureSingleTrailingBlockID(body, id string) string {
	return ensureBlockIDAtEOL(body, id)
}

func RewriteLinks(s string, alias map[string]string) (string, int) { return rewriteLinks(s, alias) }

// ===== Optional: small helper to read first line by regex (not used in tests) =====
func firstLineMatch(r io.Reader, re *regexp.Regexp) string {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := sc.Text()
		if re.MatchString(line) {
			return re.FindStringSubmatch(line)[1]
		}
	}
	return ""
}

// mirrorDirWithMarkdownRewrite mirrors a directory subtree from input to output.
// Markdown files are rewritten with lexicon aliases; other files are copied byte-for-byte.
func mirrorDirWithMarkdownRewrite(cfg Config, srcDir string, aliasMap map[string]string, logger *Logger, stats *Stats) error {
	return filepath.WalkDir(srcDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, _ := filepath.Rel(cfg.InputParent, path)
		outPath := filepath.Join(cfg.OutputParent, rel)

		if d.IsDir() {
			if cfg.DryRun {
				logger.Printf("[PLAN-MKDIR] %s", outPath)
				return nil
			}
			if err := os.MkdirAll(outPath, 0o755); err != nil {
				return err
			}
			logger.Printf("[MKDIR] %s", outPath)
			return nil
		}

		planned, action, err := writeBytesIfChanged(cfg, outPath, func() ([]byte, error) {
			b, err := os.ReadFile(path)
			if err != nil {
				return nil, err
			}
			if strings.ToLower(filepath.Ext(path)) == ".md" {
				body := normalizeNewlines(string(b))
				body = ensureEndsWithNewline(body)
				newBody, rew := rewriteLinks(body, aliasMap)
				if rew > 0 {
					stats.linksRewritt.Add(int64(rew))
				}
				return []byte(newBody), nil
			}
			return b, nil
		}, strings.ToLower(filepath.Ext(path)) != ".md")
		if err != nil {
			return err
		}
		logWrite(logger, action, outPath, planned)
		incrementFileStat(stats, action)
		return nil
	})
}
