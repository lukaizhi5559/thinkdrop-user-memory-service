// audioJournal.js — ambient audio journaling.
//
// Consumes two event streams:
//   - /audio.context  — is-playing context (extension media probe, app switches)
//   - /audio.segment  — transcript lines from voice-service's ambient capture
//
// Verdict: a session is recordable when media is actually PLAYING (extension
// probe or audio I/O active on a media app) or a call context has live audio.
// Sessions split on context change and close on silence; each writes a
// Markdown journal to ~/.thinkdrop/audio/YYYY-MM-DD/ and indexes an
// 'audio_journal' memory. Raw audio is never retained here — the voice worker
// deletes chunks after transcription.
//
// Modes via AUDIO_JOURNAL_MODE:
//   auto   — record only when a media/call context is detected (default)
//   always — record whenever the service is up
//   off    — routes no-op
// Engine via AUDIO_JOURNAL_STT: auto (default), local, groq, none.
//
// ⚠ Silent recording journals whatever is heard in detected contexts — calls
// include other people's speech. Local rules apply to recording consent.

import fs from 'fs';
import os from 'os';
import path from 'path';
import http from 'http';
import crypto from 'crypto';
import { execFileSync } from 'child_process';
import { getMonitorService } from './monitorService.js';
import { getDatabaseService } from '../services/database.js';
import logger from '../utils/logger.js';

const MODE = (process.env.AUDIO_JOURNAL_MODE || 'auto').toLowerCase();
const ENGINE_OVERRIDE = (process.env.AUDIO_JOURNAL_STT || 'auto').toLowerCase();
const AUDIO_DIR = process.env.AUDIO_JOURNAL_DIR || path.join(os.homedir(), '.thinkdrop', 'audio');
const VOICE_PORT = parseInt(process.env.VOICE_SERVICE_PORT || '3006', 10);

const TICK_MS = 10000;            // verdict + silence re-evaluation cadence
const STALE_CTX_MS = 90000;       // probe heartbeats every 20s foregrounded, ~60s throttled in background tabs
const SILENCE_CLOSE_MS = 180000;  // 3min without segments closes the session
const MAX_SESSION_MS = 3600000;   // 1h cap — long sessions rotate into a new file
const ERR_RETRY_MS = 60000;       // a failed capture retries at most once a minute
const MAX_ATTEMPTS = 6;           // then capture flips to 'failed' (stops hammering)
const AUDIO_CHUNK_SEGS = 25;      // episodic rows: ~25 STT segments…
const AUDIO_CHUNK_BYTES = 5120;   // …or ~5KB of transcript, whichever first

const MEDIA_HOSTS = /(?:youtube\.com|youtu\.be|netflix\.com|twitch\.tv|vimeo\.com|spotify\.com|music\.apple\.com|hulu\.com|primevideo\.com|disneyplus\.com|tiktok\.com|soundcloud\.com|x\.com|twitter\.com|udemy\.com|coursera\.org|podcasts\.apple\.com|music\.youtube\.com)/i;
const MEDIA_APPS = /music|spotify|vlc|quicktime|iina|podcasts|tv\.app|photos/i;
const CALL_HOSTS = /zoom\.us|teams\.microsoft\.com|teams\.live\.com|meet\.google\.com|webex\.com|discord\.com|slack\.com|facetime/i;
const CALL_APPS = /zoom|microsoft teams|webex|facetime|discord|slack|skype/i;
const CALL_TITLE = /meeting|huddle|call|standup|sync|1:1|interview|weekly/i;

class AudioJournal {
  constructor() {
    this._webMedia = new Map();   // tabId → {url,title,mediaTitle,state,lastSeen}
    this._session = null;         // {app,title,url,engine,type,started,segs:[],lastActivity}
    this._tickTimer = null;
    this._audioActiveCache = { ts: 0, value: null };
    // Ambient capture confirmation — the old `_capturing` flag went true the
    // moment we POSTed a start, whether or not the voice worker's
    // getDisplayMedia ever came up. Replace it with a real state machine the
    // worker drives via /audio.capture:
    //   idle → requested (we POSTed start) → starting (worker acknowledged)
    //        → active (worker confirms capture, or a segment lands — the
    //          ground truth) → stopped. `error` on worker failure.
    this._capture = {
      state: 'idle',              // idle|requested|starting|active|stopped|error|failed
      requestedAt: 0,
      confirmedAt: 0,
      lastSegAt: 0,
      error: null,
      detail: null,               // worker diagnostics: {tier, visibility, errorName}
      errorAt: 0,                 // last error ts — error retries back off to ERR_RETRY_MS
      attempts: 0,
    };
    // Reset error/attempts when the recordable media context changes — a new
    // video playing is a fresh chance at capture even if the last one failed.
    this._ctxKey = null;
  }

  // Capture error → actionable classification for /audio.status consumers.
  // detail.source distinguishes the native helper (td-audio-helper — its own
  // TCC client) from the legacy Chrome worker path, so hints name the right
  // app to toggle in Screen Recording.
  static _classifyCaptureError(msg, detail) {
    const s = `${msg || ''} ${(detail && detail.errorName) || ''}`;
    if (detail && detail.denied) return 'screen-recording-permission';
    if (detail && detail.stalled) return 'capture-stalled';
    if (/denied|declin|no display|ScreenCapture/i.test(s) && detail && detail.source === 'td-audio-helper') return 'screen-recording-permission';
    if (/systemAudio|SystemAudioDenied|no system audio/i.test(s)) return 'system-audio';
    if (/video source|NotReadableError|NotAllowedError|Permission denied/i.test(s)) return 'screen-recording-permission';
    if (/OverconstrainedError|constraint/i.test(s)) return 'constraint';
    return 'unknown';
  }

  static _captureHint(kind, detail) {
    const client = detail && detail.source === 'td-audio-helper' ? 'td-audio-helper' : 'Google Chrome';
    if (kind === 'screen-recording-permission') {
      return `Screen Recording permission for ${client} looks stale or missing — toggle it in System Settings → Privacy & Security → Screen Recording, then fully quit and relaunch ${client === 'td-audio-helper' ? 'the voice service (it respawns the helper)' : 'Chrome'}.`;
    }
    if (kind === 'system-audio') {
      return `${client} captured the screen but denied system audio — macOS may block SCK audio; check Screen Recording for ${client}.`;
    }
    if (kind === 'capture-stalled') {
      return `${client} stalled before capture started — macOS ScreenCaptureKit (replayd) is unresponsive, often a TCC crash-loop after a client died mid-request. Restart the Mac (or log out/in), then re-check the ${client} grant in System Settings → Privacy & Security.`;
    }
    return null;
  }

  get _capturing() {
    return ['requested', 'starting', 'active'].includes(this._capture.state);
  }

  start() {
    if (MODE === 'off') { logger.info('[audioJournal] AUDIO_JOURNAL_MODE=off — ambient journaling disabled'); return; }
    if (this._tickTimer) return;
    fs.mkdirSync(AUDIO_DIR, { recursive: true });
    this._tickTimer = setInterval(() => this._tick().catch(() => {}), TICK_MS);
    this._tickTimer.unref?.();
    logger.info(`[audioJournal] started mode=${MODE} dir=${AUDIO_DIR}`);
  }

  async stop() {
    if (this._tickTimer) { clearInterval(this._tickTimer); this._tickTimer = null; }
    if (this._capturing) this._postAmbient({ action: 'stop' });
    this._capture.state = 'stopped';
    await this._closeSession('shutdown');
  }

  // GET /audio.status — live capture state for the journal.status planning
  // tool and manual bring-up checks.
  status() {
    const s = this._session;
    const c = this._capture;
    return {
      mode: MODE,
      capturing: this._capturing,
      capture: {
        state: c.state,
        requestedAt: c.requestedAt ? new Date(c.requestedAt).toISOString() : null,
        confirmedAt: c.confirmedAt ? new Date(c.confirmedAt).toISOString() : null,
        lastSegmentAt: c.lastSegAt ? new Date(c.lastSegAt).toISOString() : null,
        error: c.error,
        detail: c.detail,
        attempts: c.attempts,
        needsUserAction: c.state === 'error' || c.state === 'failed'
          ? AudioJournal._classifyCaptureError(c.error, c.detail)
          : null,
        hint: c.state === 'error' || c.state === 'failed'
          ? AudioJournal._captureHint(AudioJournal._classifyCaptureError(c.error, c.detail), c.detail)
          : null,
      },
      verdict: this._isRecordable(),
      session: s ? {
        app: s.app, title: s.title, url: s.url, type: s.type,
        started: new Date(s.started).toISOString(), segs: s.segs.length,
        segments: s.segs.length,
        lastActivity: new Date(s.lastActivity).toISOString(),
      } : null,
      lastSegment: s && s.segs.length ? s.segs[s.segs.length - 1].text : null,
      recentSegments: s ? s.segs.slice(-10).map(seg => ({ ts: seg.ts, text: seg.text })) : [],
      webMedia: [...this._webMedia.values()].map(m => ({ url: m.url, title: m.title, state: m.state })),
      dir: AUDIO_DIR,
    };
  }

  // POST /audio.context — {tabId?, url, title, mediaTitle?, state, app?}
  handleContext(payload = {}) {
    if (MODE === 'off') return;
    if (payload.tabId != null && payload.state) {
      if (payload.state === 'none') {
        this._webMedia.delete(payload.tabId);
      } else {
        this._webMedia.set(payload.tabId, {
          url: payload.url || null, title: payload.title || null,
          mediaTitle: payload.mediaTitle || null,
          state: payload.state, lastSeen: Date.now(),
        });
      }
    }
  }

  // POST /audio.segment — {text, ts?, energy?, context?}
  handleSegment(payload = {}) {
    if (MODE === 'off') return;
    const text = String(payload.text || '').trim();
    if (!text) return;
    const now = Date.now();
    // A landing segment is the ultimate capture confirmation — the worker is
    // up and STT is producing text regardless of what /audio.capture reported.
    this._capture.state = 'active';
    if (!this._capture.confirmedAt) this._capture.confirmedAt = now;
    this._capture.lastSegAt = now;
    this._capture.error = null;
    const verdict = this._isRecordable();
    if (!this._session) this._openSession(verdict);
    this._session.segs.push({ ts: payload.ts || now, text });
    this._session.lastActivity = now;
    if (this._session.segs.length === 1) {
      logger.info(`[audioJournal] session open: ${this._session.app || '?'} — "${(this._session.title || '').slice(0, 60)}"`);
    }
  }

  // POST /audio.capture — worker-side ambient capture lifecycle:
  //   {state:'requested'|'starting'|'active'|'stopped'|'error', error?}
  // Drive the confirmation machine; 'active' here is the worker asserting its
  // own getDisplayMedia started (segments still upgrade independently).
  handleCaptureState(payload = {}) {
    const st = String(payload.state || '').toLowerCase();
    const now = Date.now();
    if (st === 'starting') {
      this._capture.state = 'starting';
    } else if (st === 'active' || st === 'started') {
      this._capture.state = 'active';
      if (!this._capture.confirmedAt) this._capture.confirmedAt = now;
      this._capture.error = null;
      this._capture.detail = payload.detail || null;
    } else if (st === 'stopped') {
      this._capture.state = 'stopped';
      this._capture.errorAt = now; // worker-side stop (e.g. Chrome-UI revoke) backs off the same way
    } else if (st === 'error' || st === 'failed') {
      this._capture.state = 'error';
      this._capture.error = String(payload.error || 'unknown').slice(0, 300);
      this._capture.detail = payload.detail || null;
      this._capture.errorAt = now;
      const kind = AudioJournal._classifyCaptureError(this._capture.error, this._capture.detail);
      const vis = this._capture.detail && this._capture.detail.visibility;
      logger.warn(`[audioJournal] ambient capture error: ${this._capture.error}` +
        ` (class=${kind}${vis ? ` visibility=${vis}` : ''}${this._capture.detail && this._capture.detail.tier ? ` tier=${this._capture.detail.tier}` : ''})`);
    } else if (st === 'requested') {
      this._capture.state = 'requested';
      this._capture.requestedAt = now;
      this._capture.error = null;
    }
  }

  async _tick() {
    const verdict = this._isRecordable();
    const now = Date.now();

    // A NEW recordable context is a fresh chance at capture — clear the
    // backoff/failed state so e.g. a second video retries immediately.
    const ctxKey = verdict.recordable ? `${verdict.app}|${verdict.title}|${verdict.url}` : null;
    if (ctxKey !== this._ctxKey) {
      this._ctxKey = ctxKey;
      if (ctxKey && (this._capture.state === 'error' || this._capture.state === 'failed')) {
        this._capture.state = 'idle';
        this._capture.attempts = 0;
        this._capture.errorAt = 0;
      }
    }

    if (!verdict.recordable && this._capturing) {
      this._postAmbient({ action: 'stop' });
      this._capture.state = 'stopped';
    } else if (verdict.recordable && !this._capturing && this._capture.state !== 'failed') {
      const st = this._capture.state;
      // idle → immediate; error/stopped → once per ERR_RETRY_MS (the worker's
      // getDisplayMedia can't be raced — the old code hammered every tick).
      const due = st === 'idle' || now - this._capture.errorAt > ERR_RETRY_MS;
      if ((st === 'error' || st === 'stopped') && this._capture.attempts >= MAX_ATTEMPTS) {
        this._capture.state = 'failed';
        logger.warn(`[audioJournal] ambient capture failed after ${this._capture.attempts} attempts — giving up until media context changes`);
      } else if (due) {
        this._requestCapture(verdict);
      }
    } else if (verdict.recordable
        && this._capture.state !== 'active'
        && this._capture.state !== 'failed'
        && now - this._capture.requestedAt > 25000
        && this._capture.attempts < MAX_ATTEMPTS) {
      // Start was requested but never confirmed — the old code sat at
      // _capturing=true forever with a dead worker (observed: zero segments,
      // zero journal files, no error). Re-request on a bounded cadence.
      logger.info(`[audioJournal] ambient capture unconfirmed (state=${this._capture.state}) — retrying start`);
      this._requestCapture(verdict);
    }

    const s = this._session;
    if (!s) return;
    const drifted = ctxKey && `${s.app}|${s.title}|${s.url}` !== ctxKey;
    const silent = now - s.lastActivity > SILENCE_CLOSE_MS;
    const tooLong = now - s.started > MAX_SESSION_MS;
    if (drifted || silent || tooLong) {
      await this._closeSession(drifted ? 'context-change' : silent ? 'silence' : 'cap');
      if (verdict.recordable) this._openSession(verdict);
    }
  }

  _isRecordable() {
    if (MODE === 'always') {
      return { recordable: true, type: 'ambient', engine: this._engine('ambient'), app: 'ambient', title: 'ambient audio', url: null };
    }

    // Extension probe — freshest playing tab wins (web video, Meet/Zoom-in-browser).
    const now = Date.now();
    const playing = [...this._webMedia.values()]
      .filter(m => m.state === 'playing' && now - m.lastSeen < STALE_CTX_MS)
      .sort((a, b) => b.lastSeen - a.lastSeen)[0];
    if (playing) {
      const call = CALL_HOSTS.test(playing.url || '') || CALL_TITLE.test(playing.title || '');
      const type = call ? 'call' : 'media';
      return {
        recordable: true, type, engine: this._engine(type),
        app: 'web', title: playing.mediaTitle || playing.title || 'web media', url: playing.url,
      };
    }

    // Native-app fallback — frontmost context from the monitor + audio I/O.
    const monitor = getMonitorService();
    const ctx = monitor.currentContext ? monitor.currentContext() : {};
    const app = ctx.appName || '';
    const title = ctx.windowTitle || '';
    const url = ctx.url || '';
    const audioActive = this._audioDeviceActive();

    if ((CALL_APPS.test(app) || CALL_HOSTS.test(url)) && (audioActive === true || audioActive === null)) {
      // audioActive null = probe unavailable — trust the call context anyway.
      return { recordable: true, type: 'call', engine: this._engine('call'), app: app || 'web', title, url };
    }
    if ((MEDIA_APPS.test(app) || MEDIA_HOSTS.test(url)) && audioActive === true) {
      return { recordable: true, type: 'media', engine: this._engine('media'), app: app || 'web', title, url };
    }
    return { recordable: false };
  }

  _engine(type) {
    if (ENGINE_OVERRIDE !== 'auto') return ENGINE_OVERRIDE;
    return type === 'call' ? 'local' : 'groq';
  }

  // macOS audio I/O probe — IOAudioEngineState 1 means an engine is running
  // (input or output). Cached 10s; null when the probe fails (degrade, don't block).
  _audioDeviceActive() {
    const now = Date.now();
    if (now - this._audioActiveCache.ts < 10000) return this._audioActiveCache.value;
    let value = null;
    try {
      const out = execFileSync('ioreg', ['-rc', 'IOAudioEngine'], { timeout: 3000 }).toString();
      value = /"IOAudioEngineState"\s*=\s*1/.test(out);
    } catch (_) { /* probe unavailable — leave null */ }
    this._audioActiveCache = { ts: now, value };
    return value;
  }

  _openSession(verdict) {
    this._session = {
      app: verdict.app || 'unknown', title: verdict.title || '', url: verdict.url || null,
      type: verdict.type || 'ambient', engine: verdict.engine || 'groq',
      started: Date.now(), segs: [], lastActivity: Date.now(),
    };
  }

  _requestCapture(verdict) {
    this._postAmbient({ action: 'start', engine: verdict.engine });
    this._capture.state = 'requested';
    this._capture.requestedAt = Date.now();
    this._capture.attempts++;
  }

  _postAmbient(payload) {
    try {
      const body = JSON.stringify(payload);
      const req = http.request({
        hostname: '127.0.0.1', port: VOICE_PORT, path: '/voice.ambient',
        method: 'POST', headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
        timeout: 5000,
      }, (res) => res.resume());
      req.on('error', () => {});
      req.on('timeout', () => req.destroy());
      req.write(body);
      req.end();
    } catch (_) {}
  }

  async _closeSession(reason) {
    const s = this._session;
    this._session = null;
    if (!s || !s.segs.length) return;
    try {
      const filePath = this._writeJournal(s);
      await this._indexEpisodic(s, filePath);
      logger.info(`[audioJournal] session closed (${reason}): ${s.segs.length} segments → ${filePath}`);
    } catch (err) {
      logger.warn(`[audioJournal] close failed: ${err.message}`);
    }
  }

  _writeJournal(s) {
    const start = new Date(s.started);
    const dateDir = path.join(AUDIO_DIR, start.toISOString().slice(0, 10));
    fs.mkdirSync(dateDir, { recursive: true });
    const hhmm = `${String(start.getHours()).padStart(2, '0')}${String(start.getMinutes()).padStart(2, '0')}`;
    const slug = (s.title || s.app || 'session').toLowerCase()
      .replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 40) || 'session';
    const filePath = path.join(dateDir, `${hhmm}-${slug}.md`);
    const end = new Date(s.lastActivity);
    const fmt = (d) => d.toTimeString().slice(0, 8);
    const lines = [
      '---',
      `type: audio_journal`,
      `app: ${JSON.stringify(s.app)}`,
      `title: ${JSON.stringify(s.title)}`,
      `url: ${JSON.stringify(s.url)}`,
      `session_type: ${s.type}`,
      `engine: ${s.engine}`,
      `started: ${start.toISOString()}`,
      `ended: ${end.toISOString()}`,
      `segments: ${s.segs.length}`,
      '---',
      '',
      `# ${s.title || s.app || 'Audio session'}`,
      '',
      `_${s.app || 'unknown'} · ${s.type} · ${fmt(start)}–${fmt(end)}_`,
      '',
      ...s.segs.map(seg => `[${fmt(new Date(seg.ts))}] ${seg.text}`),
      '',
    ];
    fs.writeFileSync(filePath, lines.join('\n'));
    return filePath;
  }

  // Session transcript → chunked episodic_memory rows. Screen captures live
  // in the same table — audio and screen share the 365-day hot window, the
  // monthly parquet archive, and BM25 recall, so "what did that video say"
  // and "what was on my screen" answer through one store.
  //
  // Chunk policy (researched): ~25 STT segments OR ~5KB of text, whichever
  // first. Segments are already natural utterance boundaries; ~25 of them
  // lands in BM25's ~600–1000-token sweet spot while keeping a hit to a
  // tight few-minute window. No overlap — no measurable recall benefit.
  // The semantic `memory` table gets nothing — a 12-segment summary row was
  // write-only and orphaned the rest of the transcript in the .md file.
  async _indexEpisodic(s, filePath) {
    const db = getDatabaseService();
    // Same convention as monitorService screen rows — 'default'/'default_user'
    // here would write rows no search path can see (search filters user_id).
    const userId = process.env.MONITOR_USER_ID || 'local_user';
    const esc = (v) => String(v == null ? '' : v).replace(/'/g, "''");

    const chunks = [];
    let cur = [];
    let curBytes = 0;
    for (const seg of s.segs) {
      const line = `[${new Date(seg.ts).toISOString()}] ${seg.text}`;
      const b = Buffer.byteLength(line);
      if (cur.length && (cur.length >= AUDIO_CHUNK_SEGS || curBytes + b > AUDIO_CHUNK_BYTES)) {
        chunks.push(cur); cur = []; curBytes = 0;
      }
      cur.push({ seg, line });
      curBytes += b;
    }
    if (cur.length) chunks.push(cur);

    for (let i = 0; i < chunks.length; i++) {
      const c = chunks[i];
      const memoryId = `mem_${Date.now()}_${crypto.randomBytes(6).toString('hex')}`;
      const segFrom = s.segs.indexOf(c[0].seg);
      const segTo = s.segs.indexOf(c[c.length - 1].seg);
      const metadata = JSON.stringify({
        path: filePath,
        appName: s.app,
        windowTitle: s.title,
        title: s.title,
        url: s.url,
        sessionType: s.type,
        engine: s.engine,
        chunk: i + 1, chunks: chunks.length,
        segFrom, segTo, segments: s.segs.length,
        started: new Date(c[0].seg.ts).toISOString(),
        ended: new Date(c[c.length - 1].seg.ts).toISOString(),
      });
      const text = c.map(x => x.line).join('\n');
      await db.execute(`
        INSERT INTO episodic_memory (id, user_id, type, source_text, metadata, extracted_text, created_at, updated_at)
        VALUES ('${memoryId}', '${esc(userId)}', 'audio_journal', '${esc(text)}', '${esc(metadata)}', '${esc(text)}', now(), now())
      `);
      const entities = [
        { type: 'application', value: s.app || 'unknown', entity_type: 'APP' },
        { type: 'window', value: (s.title || '').substring(0, 255), entity_type: 'WINDOW_TITLE' },
        ...(s.title ? [{ type: 'media', value: s.title.substring(0, 255), entity_type: 'MEDIA_TITLE' }] : []),
      ];
      for (const e of entities) {
        if (!e.value) continue;
        const entityId = `ent_${Date.now()}_${crypto.randomBytes(6).toString('hex')}`;
        await db.execute(`
          INSERT INTO episodic_entities (id, memory_id, entity, type, entity_type, normalized_value)
          VALUES ('${entityId}', '${memoryId}', '${esc(e.value)}', '${e.type}', '${e.entity_type}', '${esc(e.value.toLowerCase())}')
        `);
      }
    }
    logger.info(`[audioJournal] indexed ${s.segs.length} segments as ${chunks.length} episodic chunks`);
  }
}

let _journal = null;
export function getAudioJournal() {
  if (!_journal) _journal = new AudioJournal();
  return _journal;
}
