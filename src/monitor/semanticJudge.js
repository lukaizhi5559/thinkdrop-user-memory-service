// ── Semantic rule judging (guardian semanticMatch/semanticExempt) ───────────
// Rules asserting when.semanticMatch name a content-scene ("frame") a model
// must confirm; when.semanticExempt names the anti-frame — same vocabulary,
// wrong scene (news/medical/scripture/fiction). Judged on deduped captures
// (monitorService coalesces/rate-caps the calls into this module).
//
// Two evidence lanes, OR'd — a frame need only present on one channel:
//   text   OCR transcript judged WITHOUT an image → phi4 uses the text
//          decision model (tev1) over the full 2500-char state. This is the
//          only lane that can read screen text — the vision model gets a
//          384px image + a 400-char context stub and cannot read lyrics,
//          articles, or chat text (observed: explicit song lyrics judged NO).
//   vision screenshot judged WITH an image → the vision model (moondream/
//          d3) catches imagery OCR can't read — nudity, gore — including on
//          pages whose visible text is benign.
//
// Lane economics: a text call is ~1-3s of tev1; a vision call is ~3-6s of
// moondream generation. Running both on every deduped capture saturated the
// judge (~6-9s/pass at an 8s floor → near-continuous inference + two resident
// models on an 8GB box → system-wide slowdown) AND pushed real fires ~30s
// behind queued passes. So: the text lane judges every pass; the vision lane
// is throttled globally to once per SEM_VISION_MIN_MS — imagery is sampled,
// not tracked frame-by-frame. ONE exception keeps safety coverage: an
// image-only capture (no usable text — the only evidence is the picture)
// gets a vision judgment the first time it's seen regardless of the window.
//
// Verdicts cache PER LANE by frame+exempt+evidence hash — a vision lane
// skipped by the throttle must never poison the text verdict (and the
// reverse). A lane that errors contributes nothing; when no lane produced a
// positive verdict the answer is 'NO' — uncached lanes simply re-run on the
// next pass. Text-only callers (no image) behave as one text pass.

import http from 'http';
import crypto from 'crypto';
import { PNG } from 'pngjs';
import logger from '../utils/logger.js';

// phi4 enforces API_KEY auth — comms-graph sends Bearer via decision.cjs;
// calls without it 401 silently (judge → 'NO', guards never fire). Read lazily:
// this module is imported before server.js's dotenv.config runs, so a top-level
// const would capture '' forever.
export const PHI4_URL = process.env.PHI4_URL || `http://127.0.0.1:${process.env.PHI4_SERVICE_PORT || '3009'}`;
export const phi4Key = () => process.env.MCP_PHI4_API_KEY || process.env.MCP_API_KEY || '';

const SEM_RULE_CACHE_MS = parseInt(process.env.SEMANTIC_RULE_CACHE_MS || '300000', 10);
const SEM_RULE_MIN_TEXT = 40;
// Global throttle for the vision lane — see header comment. A never-judged
// image-only capture bypasses this window (first look is free); text-bearing
// captures wait for it.
const SEM_VISION_MIN_MS = parseInt(process.env.SEMANTIC_VISION_MIN_MS || '30000', 10);
const _semVerdicts = new Map(); // sha(frame+exempt+lane+evidence) → {v, ts}
let _visionLastAt = 0;
let _semJudgeWarnedAt = 0;

export function postJsonLocal(port, urlPath, payload, timeoutMs = 8000, authToken = null) {
  return new Promise((resolve) => {
    try {
      const body = JSON.stringify(payload);
      const req = http.request({
        hostname: '127.0.0.1', port, path: urlPath, method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
          ...(authToken ? { Authorization: `Bearer ${authToken}` } : {}),
        },
        timeout: timeoutMs,
      }, (res) => {
        let raw = '';
        res.on('data', c => { raw += c; });
        res.on('end', () => { try { resolve(JSON.parse(raw)); } catch (_) { resolve(null); } });
      });
      req.on('error', () => resolve(null));
      req.on('timeout', () => { req.destroy(); resolve(null); });
      req.write(body);
      req.end();
    } catch (_) { resolve(null); }
  });
}

function _judgeUnavailable(res, lane) {
  // Judge unreachable/denied — contributes no verdict and is never cached:
  // a cached 'NO' would keep the rule dark for 5min after phi4 recovers.
  if (Date.now() - _semJudgeWarnedAt > 60000) {
    _semJudgeWarnedAt = Date.now();
    logger.warn('[monitorService] semantic judge unavailable — lane skipped (uncached)', { lane, status: res?.status || res?.error?.code || 'no-response' });
  }
  return null;
}

/**
 * judge(frame, exempt, ctx) → 'YES' | 'NO' — the evaluateAsync-injected
 * predicate. ctx: {appName, url, windowTitle, ocrText, image?}.
 */
export async function judgeSemantic(frame, exempt, ctx) {
  const text = String(ctx.ocrText || '').replace(/\s+/g, ' ').trim();
  const image = typeof ctx.image === 'string' && ctx.image ? ctx.image : null;
  const hasText = text.length >= SEM_RULE_MIN_TEXT;
  if (!hasText && !image) return 'NO';

  const laneKey = (lane, ev) =>
    crypto.createHash('sha256').update(`${frame}\n${exempt || ''}\n${lane}:${ev}`).digest('hex');
  const cached = (lane, ev) => {
    const h = _semVerdicts.get(laneKey(lane, ev));
    return h && Date.now() - h.ts < SEM_RULE_CACHE_MS ? h.v : undefined;
  };
  const remember = (lane, ev, v) => {
    if (_semVerdicts.size > 200) _semVerdicts.clear();
    _semVerdicts.set(laneKey(lane, ev), { v, ts: Date.now() });
  };

  const state = `App: ${ctx.appName}\nURL: ${ctx.url || ''}\nTitle: ${ctx.windowTitle || ''}\nScreen text:\n${text.slice(0, 2500)}`;
  // One fused yes/no question — frame + exemption qualifier in a single
  // model pass. Verified empirically on this machine's judge models:
  // lettered YES/NO choices produce false-YESes (first-option attractor)
  // and the enumerated exempt prompt returns empty from moondream; plain
  // "Answer yes or no" (noul) answers correctly on both tev1 and moondream
  // — and one inference instead of two halves judge cost.
  const question = exempt == null
    ? `Does this screen content present "${frame}" as the actual material itself — not a news article, medical page, or quote about it?`
    : `Does this screen content present "${frame}" as the actual material itself — not ${exempt}?`;

  const askLane = async (lane, ev, img) => {
    const known = cached(lane, ev);
    if (known !== undefined) return known;
    try {
      const res = await postJsonLocal(new URL(PHI4_URL).port || 3009, '/decision.answer', {
        version: 'mcp.v1',
        service: 'phi4',
        action: 'decision.answer',
        requestId: `sr_${lane === 'vision' ? 'v' : 't'}_${Date.now()}`,
        payload: {
          state,
          questions: [{ id: 'match', type: 'noul', question }],
          ...(img ? { image: img } : {}),
        },
      }, 15000, phi4Key());
      if (res?.status !== 'ok') return _judgeUnavailable(res, lane);
      const v = res?.data?.answers?.match?.bool === true ? 'YES' : 'NO';
      remember(lane, ev, v);
      return v;
    } catch (_) { return null; }
  };

  // Lane 1 — text: every pass, cheap, the only lane that can read the screen.
  if (hasText && (await askLane('text', `txt:${text}`, null)) === 'YES') return 'YES';

  // Lane 2 — vision: throttled globally; a never-judged image-only capture
  // (no text to judge — the image IS the evidence) still gets one look now.
  if (image) {
    const ev = 'img:' + crypto.createHash('sha256').update(image).digest('hex');
    const known = cached('vision', ev);
    if (known === 'YES') return 'YES';
    const windowOpen = Date.now() - _visionLastAt >= SEM_VISION_MIN_MS;
    if (known === undefined && (!hasText || windowOpen)) {
      _visionLastAt = Date.now(); // stamp the attempt too — error storms throttle, not loop
      if ((await askLane('vision', ev, image)) === 'YES') return 'YES';
    }
  }
  return 'NO';
}

/** Test hook — clears verdict caches and lane throttles. */
export function _resetJudgeForTest() {
  _semVerdicts.clear();
  _visionLastAt = 0;
  _semJudgeWarnedAt = 0;
}

// Downscale a PNG for the semantic judge — a full-retina shot is multi-MB
// base64 and stalls the vision POST + moondream inference (encode cost scales
// with pixels). moondream resizes input to ~378px internally, so ~384px loses
// nothing the model would see and encodes ~4x cheaper than 720px.
// Nearest-neighbor stride sampling, ~50ms.
export function downscalePng(buffer, targetW = parseInt(process.env.SEM_JUDGE_IMAGE_W || '384', 10)) {
  try {
    const png = PNG.sync.read(buffer);
    if (png.width <= targetW) return buffer;
    const scale = png.width / targetW;
    const W = targetW;
    const H = Math.max(1, Math.round(png.height / scale));
    const out = new PNG({ width: W, height: H });
    for (let y = 0; y < H; y++) {
      const sy = Math.min(png.height - 1, Math.floor(y * scale));
      for (let x = 0; x < W; x++) {
        const sx = Math.min(png.width - 1, Math.floor(x * scale));
        const s = (sy * png.width + sx) * 4;
        const d = (y * W + x) * 4;
        for (let k = 0; k < 4; k++) out.data[d + k] = png.data[s + k];
      }
    }
    return PNG.sync.write(out);
  } catch (_) {
    return buffer;
  }
}
