/**
 * semantic-judge.test.js — two-lane OR judging + vision throttle for
 * semanticMatch rules.
 *
 * The judge ORs two evidence lanes through phi4's /decision.answer:
 *   text lane   — OCR transcript, NO image attached (text decision model),
 *                 judged EVERY pass — it's the only lane that can read the
 *                 screen (regression: explicit lyrics judged NO by vision).
 *   vision lane — screenshot attached (vision model), THROTTLED globally to
 *                 SEMANTIC_VISION_MIN_MS because a ~3-6s moondream call every
 *                 pass saturated the box; except a never-judged image-only
 *                 capture (no usable text) still gets one immediate look.
 * Verdicts cache per lane — a gated vision lane can't poison a text verdict.
 *
 * A stub HTTP server plays phi4 and records which lane was asked
 * (payload.image present ⇒ vision). Verdicts are keyed by lane so each case
 * is deterministic.
 *
 * Run: node tests/unit/semantic-judge.test.js
 */

import http from 'http';

let passed = 0, failed = 0;
const failures = [];
function check(name, cond, extra = '') {
  if (cond) { passed++; console.log(`  ✓ ${name}`); }
  else { failed++; failures.push(name); console.log(`  ✗ ${name} ${extra}`); }
}

const LONG_TEXT = 'the quick brown fox jumps over the lazy dog and keeps running through the forest';
const CTX = (ocrText, image) => ({ appName: 'Google Chrome', url: 'https://x.test', windowTitle: 't', ocrText, image });
const laneCalls = (lane) => calls.filter(c => c.lane === lane).length;

// verdictFor(lane) → 'YES' | 'NO' | 'ERR' (http 500)
let verdictFor = () => 'NO';
let calls = []; // { lane }
const stub = http.createServer((req, res) => {
  if (req.url !== '/decision.answer') { res.writeHead(404); return res.end(); }
  let raw = '';
  req.on('data', c => { raw += c; });
  req.on('end', () => {
    let payload = {};
    try { payload = JSON.parse(raw).payload || {}; } catch (_) {}
    const lane = payload.image ? 'vision' : 'text';
    calls.push({ lane });
    const v = verdictFor(lane);
    if (v === 'ERR') { res.writeHead(500); return res.end('boom'); }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      version: 'mcp.v1', service: 'phi4', action: 'decision.answer',
      status: 'ok', data: { answers: { match: { bool: v === 'YES', probability: v === 'YES' ? 0.9 : 0.1 } } },
    }));
  });
});

await new Promise(r => stub.listen(0, '127.0.0.1', r));
const port = stub.address().port;
process.env.PHI4_URL = `http://127.0.0.1:${port}`;
process.env.PHI4_SERVICE_PORT = String(port);
process.env.SEMANTIC_VISION_MIN_MS = '30000';

const { judgeSemantic, _resetJudgeForTest } = await import('../../src/monitor/semanticJudge.js');

let frameSeq = 0;
const frame = () => `frame-${++frameSeq}`; // unique frame → fresh cache keys
const reset = () => { calls = []; _resetJudgeForTest(); };

console.log('\ntwo-lane judge + vision throttle');

// 1. Text lane YES short-circuits — vision must never be asked.
verdictFor = (lane) => (lane === 'text' ? 'YES' : 'NO');
reset();
check('text-YES + image → YES', await judgeSemantic(frame(), null, CTX(LONG_TEXT, 'aW1n')) === 'YES');
check('text-YES short-circuits vision', calls.length === 1 && calls[0].lane === 'text');

// 2. Text NO but vision YES → OR still fires (explicit image, benign text).
verdictFor = (lane) => (lane === 'vision' ? 'YES' : 'NO');
reset();
check('text-NO + vision-YES → YES', await judgeSemantic(frame(), null, CTX(LONG_TEXT, 'aW1n')) === 'YES');
check('both lanes asked', calls.length === 2 && calls[0].lane === 'text' && calls[1].lane === 'vision');

// 3. Both NO → NO; re-judging the same evidence costs zero calls — the
//    text verdict caches even though the vision throttle closed behind it.
verdictFor = () => 'NO';
reset();
const f3 = frame(), c3 = CTX(LONG_TEXT, 'aW1n');
check('both-NO → NO', await judgeSemantic(f3, null, c3) === 'NO' && calls.length === 2);
check('same evidence → all-cached NO (0 new calls)',
  await judgeSemantic(f3, null, c3) === 'NO' && calls.length === 2);

// 4. Vision throttle: a fresh image+text inside the closed window is NOT
//    vision-called — the text verdict alone decides this pass.
verdictFor = () => 'NO';
reset();
await judgeSemantic(frame(), null, CTX(LONG_TEXT, 'aW1n'));            // opens+consumes the window
calls = [];
check('new image inside closed window → vision gated',
  await judgeSemantic(frame(), null, CTX('a different benign page with plenty of on-screen text here', 'aW5n')) === 'NO'
  && laneCalls('vision') === 0 && laneCalls('text') === 1);

// 5. Image-only evidence bypasses the throttle once per image — the ONLY
//    lane available can't wait for a text window that will never open.
verdictFor = (lane) => (lane === 'vision' ? 'YES' : 'NO');
const f5 = frame();
check('no text + unseen image inside closed window → vision runs',
  await judgeSemantic(f5, null, CTX('short', 'bmV3LWltZw')) === 'YES' && laneCalls('vision') === 1);

// 6. Per-lane cache: a cached vision YES still fires later (the throttle
//    doesn't undo a known-positive image).
calls = [];
check('cached vision-YES image → YES with 0 calls',
  await judgeSemantic(f5, null, CTX('short', 'bmV3LWltZw')) === 'YES'
  && calls.length === 0);

// 7. Text lane down, vision window open answers NO → NO.
verdictFor = (lane) => (lane === 'text' ? 'ERR' : 'NO');
reset();
check('text-lane error + vision-NO → NO', await judgeSemantic(frame(), null, CTX(LONG_TEXT, 'aW1n')) === 'NO');

// 8. Every lane down → NO, uncached (second call retries the stub).
verdictFor = () => 'ERR';
reset();
const f8 = frame();
check('all lanes down → NO', await judgeSemantic(f8, null, CTX(LONG_TEXT, 'aW1n')) === 'NO');
await judgeSemantic(f8, null, CTX(LONG_TEXT, 'aW1n'));
check('all-down verdict uncached — lanes retried', calls.length >= 3 && laneCalls('vision') === 1);

// 9. Short text + no image → NO without any model call.
verdictFor = () => 'YES';
reset();
check('text<40 & no image → NO (no calls)', await judgeSemantic(frame(), null, CTX('too short', null)) === 'NO' && calls.length === 0);

// 10. Text-only ctx (no screenshot) → text lane alone can YES.
verdictFor = (lane) => (lane === 'text' ? 'YES' : 'NO');
check('no image → text lane alone can YES',
  await judgeSemantic(frame(), null, CTX(LONG_TEXT, null)) === 'YES' && calls.length === 1 && calls[0].lane === 'text');

console.log(`\n${passed} passed, ${failed} failed`);
stub.close();
process.exit(failed ? 1 : 0);
