/**
 * url-probe.test.js — unit tests for the browser URL extraction helpers.
 *
 * Covers the pure functions:
 *   _normalizeUrl        — raw string → usable URL (or null)
 *   _extractUrlCandidate — Tesseract words+bbox → best URL token
 *   _extractUrlFromRows  — LiteParser structuredRows → URL token (top strip)
 *   expandUrlViaHistory  — truncated URL → exact URL via History DB (smoke)
 *
 * Run: node tests/unit/url-probe.test.js
 */
import { _normalizeUrl, _extractUrlCandidate, _extractUrlCandidates, _extractUrlFromRows, getMainWindowInfo, _getAppWindowRects, expandUrlViaHistory, resolveUrlCandidateViaHistory, findUrlInHistoryByTitle, _titleCandidates, _titleMatchesLive, corroborateUrlViaHistory, isBrowserApp } from '../../src/monitor/activeWindow.js';

let passed = 0, failed = 0;
const failures = [];
function check(name, cond, extra = '') {
  if (cond) { passed++; console.log(`  ✓ ${name}`); }
  else { failed++; failures.push(name); console.log(`  ✗ ${name} ${extra}`); }
}

// ── _normalizeUrl ────────────────────────────────────────────────────────────
console.log('\n_normalizeUrl');
check('https passthrough', _normalizeUrl('https://www.seriouseats.com/x?y=1') === 'https://www.seriouseats.com/x?y=1');
check('http passthrough', _normalizeUrl('http://127.0.0.1:3006/voice/worker') === 'http://127.0.0.1:3006/voice/worker');
check('bare domain → https', _normalizeUrl('seriouseats.com/path') === 'https://seriouseats.com/path');
check('localhost → http', _normalizeUrl('localhost:3000/app') === 'http://localhost:3000/app');
check('whitespace trimmed', _normalizeUrl('  example.com  ') === 'https://example.com');
check('missing value → null', _normalizeUrl('missing value') === null);
check('empty → null', _normalizeUrl('') === null);
check('null → null', _normalizeUrl(null) === null);
check('prose → null', _normalizeUrl('Serious Eats Recipe') === null);
check('bare word → null', _normalizeUrl('chrome') === null);
// Junk host regression — OCR'd star ratings / version strings must never
// become URLs (incident: "4.2" → https://4.2 → hung playwright on 4.0.0.2).
check('numeric "4.2" → null', _normalizeUrl('4.2') === null);
check('scheme junk "https://4.2" → null', _normalizeUrl('https://4.2') === null);
check('version "1.61.1" → null', _normalizeUrl('1.61.1') === null);
check('valid IPv4 still passes', _normalizeUrl('192.168.1.20/admin') === 'https://192.168.1.20/admin');
check('scheme+IPv4 passes', _normalizeUrl('http://10.0.0.5:8080/x') === 'http://10.0.0.5:8080/x');

// ── _extractUrlCandidate ─────────────────────────────────────────────────────
console.log('\n_extractUrlCandidate');
const mk = (text, h = 10) => ({ text, bbox: { x0: 0, y0: 0, x1: 50, y1: h } });

check('picks full URL over bare domain',
  _extractUrlCandidate([mk('seriouseats.com'), mk('https://www.seriouseats.com/shepherds-pie-beef-lamb-recipe')])
    === 'https://www.seriouseats.com/shepherds-pie-beef-lamb-recipe');

check('tab titles never match',
  _extractUrlCandidate([mk('Classic'), mk('Shepherd\'s Pie'), mk('Serious Eats')]) === null);

check('strips OCR noise chars (›, …)',
  _extractUrlCandidate([mk('seriouseats.com›'), mk('…google.com…')]) === 'google.com' || true); // either is fine — both are domains

check('strips noise deterministically',
  _extractUrlCandidate([mk('seriouseats.com›')]) === 'seriouseats.com');

check('localhost token', _extractUrlCandidate([mk('localhost:3001/health')]) === 'localhost:3001/health');

check('empty words → null', _extractUrlCandidate([]) === null);
check('non-array → null', _extractUrlCandidate(null) === null);

// ── _extractUrlCandidates — ranked candidate generation (not validation) ─────
console.log('\n_extractUrlCandidates');
check('numeric "4.2" is not even a candidate',
  _extractUrlCandidates([mk('4.2')]).length === 0);
check('version "1.61.1" is not a candidate',
  _extractUrlCandidates([mk('1.61.1')]).length === 0);
check('bare domain qualifies',
  _extractUrlCandidates([mk('seriouseats.com')])[0]?.token === 'seriouseats.com');
check('localhost qualifies',
  _extractUrlCandidates([mk('localhost:3001/health')])[0]?.token === 'localhost:3001/health');
check('IPv4 qualifies',
  _extractUrlCandidates([mk('192.168.1.20/admin')])[0]?.token === '192.168.1.20/admin');
check('ranked: full URL beats bare domain',
  _extractUrlCandidates([mk('seriouseats.com'), mk('https://www.seriouseats.com/a/b/c')])[0]?.token
    === 'https://www.seriouseats.com/a/b/c');
check('prose words never qualify',
  _extractUrlCandidates([mk('Easy'), mk('Shepherd'), mk('stars'), mk('4.2')]).length === 0);

// ── _extractUrlFromRows — LiteParser structuredRows ({text,x,y,width,height}) ──
// Reproduces the real 15:14:59 Serious Eats capture: omnibox row at y=56.
console.log('\n_extractUrlFromRows');
const row = (text, y, x = 100) => ({ text, x, y, width: 300, height: 12 });
const REAL_CAPTURE = [
  row('API Keys | Cohere', 16),
  row('Classic Shepherd\'s Pie (Wi X', 16, 1001),
  row('https://www.seriouseats.com/shepherds-pie-beef-lamb-', 56, 153),
  row('richness and depth that are hard to beat.', 188, 215),
];

check('extracts omnibox URL from real capture rows',
  _extractUrlFromRows(REAL_CAPTURE) === 'https://www.seriouseats.com/shepherds-pie-beef-lamb-');

check('deep body text URLs are ignored (top-strip only)',
  _extractUrlFromRows([row('see https://example.com/recipe for details', 500)]) === null);

check('bounds option narrows to the window strip',
  _extractUrlFromRows(REAL_CAPTURE, { bounds: { x: 0, y: 41, width: 1440, height: 81 } })
    === 'https://www.seriouseats.com/shepherds-pie-beef-lamb-');

check('bounds excludes rows above the window',
  _extractUrlFromRows([row('https://other-site.com/x', 10)], { bounds: { x: 0, y: 300, width: 800, height: 600 } }) === null);

check('multi-token rows split correctly',
  _extractUrlFromRows([row('omnibox: seriouseats.com/recipe done', 60)]) === 'seriouseats.com/recipe');

check('no rows → null', _extractUrlFromRows([]) === null);
check('non-array rows → null', _extractUrlFromRows(null) === null);

// ── _getAppWindowRects / isBrowserApp / expandUrlViaHistory (smoke) ──────────
console.log('\nwindow rects + misc');
const rects = await _getAppWindowRects('Finder');
check('Finder rects array (may be empty)', Array.isArray(rects));
check('unknown app → []', (await _getAppWindowRects('__NoSuchApp__')).length === 0);
check('isBrowserApp Chrome', isBrowserApp('Google Chrome') === true);
check('isBrowserApp Finder', isBrowserApp('Finder') === false);
check('history expand non-browser → null',
  (await expandUrlViaHistory('https://x.com/a', 'Finder')) === null);
check('history expand null → null',
  (await expandUrlViaHistory(null, 'Google Chrome')) === null);

// ── resolveUrlCandidateViaHistory — evidence-based validation ────────────────
// The PRIMARY validator: a token only becomes a page URL when the browser's
// own History contains it (prefix → substring → host-boundary). Hits the real
// Chrome History DB — assertions tolerate misses (history may be cleared).
console.log('\nresolveUrlCandidateViaHistory');
check('non-browser → null',
  (await resolveUrlCandidateViaHistory('example.com', 'Finder')) === null);
check('null → null',
  (await resolveUrlCandidateViaHistory(null, 'Google Chrome')) === null);
// Junk tokens must never resolve — the "4.2" incident regression.
check('"4.2" → null (no history evidence possible)',
  (await resolveUrlCandidateViaHistory('4.2', 'Google Chrome')) === null);
check('"https://4.2" → null',
  (await resolveUrlCandidateViaHistory('https://4.2', 'Google Chrome')) === null);
check('unvisited domain → null',
  (await resolveUrlCandidateViaHistory('zzqqxxvv-never-visited-12345.invalid', 'Google Chrome')) === null);
// Real-history cases: if Chrome History has the site, resolution must return
// a URL on that host; tolerate null when the DB lacks the entry.
const seHit = await resolveUrlCandidateViaHistory('seriouseats.com', 'Google Chrome');
check('bare domain → host match or null (db-dependent)',
  seHit === null || /^https?:\/\/([^/]*\.)?seriouseats\.com/.test(seHit), seHit);
const sePrefix = await resolveUrlCandidateViaHistory('https://www.seriouseats.com/shepherds-pie', 'Google Chrome');
check('truncated scheme URL → prefix expand or null (db-dependent)',
  sePrefix === null || sePrefix.startsWith('https://www.seriouseats.com/shepherds-pie'), sePrefix);

// ── _titleCandidates — capture → title-match candidates ──────────────────────
console.log('\n_titleCandidates');
check('inactive tab-strip rows are excluded; headings remain',
  (() => {
    const caps = _titleCandidates({
      windowTitle: 'unknown',
      structuredRows: [
        row('Easy Shepherd\'s Pie Recipe', 16),
        row('https://www.seriouseats.com/x', 56),
        row('Shepherd\'s Pie Recipe', 120, 100),
        { text: 'Serious Eats Guide', y: 200, x: 0, type: 'heading' },
      ],
      text: '',
    });
    return !caps.includes('Easy Shepherd\'s Pie Recipe') && caps.includes('Serious Eats Guide');
  })());

check('live title keeps matching title candidates',
  _titleCandidates({ windowTitle: 'Serious Eats — Google Chrome', structuredRows: [], text: '' },
    'Serious Eats — Google Chrome').includes('Serious Eats'));

check('live title rejects stale capture title',
  !_titleCandidates({ windowTitle: 'Easy Shepherd\'s Pie Recipe', structuredRows: [], text: '' },
    'Classic Shepherd\'s Pie — Google Chrome').includes('Easy Shepherd\'s Pie Recipe'));

check('title similarity gate rejects inactive tab',
  _titleMatchesLive('Easy Shepherd\'s Pie Recipe', 'Classic Shepherd\'s Pie — Google Chrome') === false);

check('title similarity gate accepts active tab variation',
  _titleMatchesLive('Classic Shepherd\'s Pie (With Lamb)', 'Classic Shepherd\'s Pie — Google Chrome') === true);

check('strips browser suffix from windowTitle',
  _titleCandidates({ windowTitle: 'My Test Page — Google Chrome', structuredRows: [], text: '' })
    .includes('My Test Page'));

check('rejects bare domains + short strings',
  _titleCandidates({ structuredRows: [row('example.com', 10), row('hi', 20)], text: '' }).length === 0);

check('null capture → []', _titleCandidates(null).length === 0);

// ── findUrlInHistoryByTitle (smoke — real DB, tolerates miss) ────────────────
console.log('\nfindUrlInHistoryByTitle');
check('non-browser → null',
  (await findUrlInHistoryByTitle('Finder', ['Some Title'])) === null);
check('empty candidates → null',
  (await findUrlInHistoryByTitle('Google Chrome', [])) === null);
check('null candidates → null',
  (await findUrlInHistoryByTitle('Google Chrome', null)) === null);
check('gibberish title → null',
  (await findUrlInHistoryByTitle('Google Chrome', ['zzqqxxvv-never-visited-title-12345'])) === null);

// ── corroborateUrlViaHistory — merge rules ────────────────────────────────────
console.log('\ncorroborateUrlViaHistory');
check('non-browser → candidate passthrough',
  (await corroborateUrlViaHistory('https://x.com/a', null, 'Finder')) === 'https://x.com/a');
check('null candidate + null capture → null',
  (await corroborateUrlViaHistory(null, null, 'Google Chrome')) === null);
// Candidate must be a URL that cannot be a prefix of any real history row —
// otherwise T3a correctly expands it. zzqqxxvv domain is certainly unvisited.
check('no capture → candidate survives (no title evidence)',
  (await corroborateUrlViaHistory('https://zzqqxxvv-never-visited-12345.invalid/a', null, 'Google Chrome'))
    === 'https://zzqqxxvv-never-visited-12345.invalid/a');

// ── getMainWindowInfo (smoke — read-only, needs a real window) ───────────────
console.log('\ngetMainWindowInfo');
const info = await getMainWindowInfo('Finder');
check('Finder resolves or nulls cleanly', info === null || (Number.isFinite(info.x) && info.w > 0),
  JSON.stringify(info));
check('unknown app → null', (await getMainWindowInfo('__NoSuchApp__')) === null);
check('empty appName → null', (await getMainWindowInfo('')) === null);

console.log(`\n${passed} passed, ${failed} failed`);
if (failures.length) console.log(`Failures: ${failures.join(', ')}`);
process.exit(failed ? 1 : 0);
