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
import { execFileSync } from 'child_process';
import { getMonitorService } from './monitorService.js';
import { getMemoryService } from '../services/memory.js';
import logger from '../utils/logger.js';

const MODE = (process.env.AUDIO_JOURNAL_MODE || 'auto').toLowerCase();
const ENGINE_OVERRIDE = (process.env.AUDIO_JOURNAL_STT || 'auto').toLowerCase();
const AUDIO_DIR = process.env.AUDIO_JOURNAL_DIR || path.join(os.homedir(), '.thinkdrop', 'audio');
const VOICE_PORT = parseInt(process.env.VOICE_SERVICE_PORT || '3006', 10);

const TICK_MS = 10000;            // verdict + silence re-evaluation cadence
const STALE_CTX_MS = 45000;       // probe heartbeats every 20s — 2 misses = dead
const SILENCE_CLOSE_MS = 180000;  // 3min without segments closes the session
const MAX_SESSION_MS = 3600000;   // 1h cap — long sessions rotate into a new file

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
    this._capturing = false;
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
    await this._closeSession('shutdown');
  }

  // GET /audio.status — live capture state for the journal.status planning
  // tool and manual bring-up checks.
  status() {
    const s = this._session;
    return {
      mode: MODE,
      capturing: this._capturing,
      verdict: this._isRecordable(),
      session: s ? {
        app: s.app, title: s.title, url: s.url, type: s.type,
        started: new Date(s.started).toISOString(), segs: s.segs.length,
        lastActivity: new Date(s.lastActivity).toISOString(),
      } : null,
      lastSegment: s && s.segs.length ? s.segs[s.segs.length - 1].text : null,
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
    const verdict = this._isRecordable();
    if (!this._session) this._openSession(verdict);
    this._session.segs.push({ ts: payload.ts || now, text });
    this._session.lastActivity = now;
    if (this._session.segs.length === 1) {
      logger.info(`[audioJournal] session open: ${this._session.app || '?'} — "${(this._session.title || '').slice(0, 60)}"`);
    }
  }

  async _tick() {
    const verdict = this._isRecordable();
    const now = Date.now();

    if (!verdict.recordable && this._capturing) {
      this._postAmbient({ action: 'stop' });
      this._capturing = false;
    } else if (verdict.recordable && !this._capturing) {
      this._postAmbient({ action: 'start', engine: verdict.engine });
      this._capturing = true;
    }

    const s = this._session;
    if (!s) return;
    const ctxKey = verdict.recordable ? `${verdict.app}|${verdict.title}|${verdict.url}` : null;
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
      await this._indexMemory(s, filePath);
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

  async _indexMemory(s, filePath) {
    const memService = getMemoryService();
    const start = new Date(s.started);
    const summary = [
      `Audio journal: ${s.title || s.app || 'session'}`,
      `(${s.type}, ${s.segs.length} segments, ${start.toISOString()})`,
      '',
      ...s.segs.slice(0, 12).map(seg => seg.text),
    ].join('\n').slice(0, 4000);
    await memService.storeMemory({
      type: 'audio_journal',
      text: summary,
      metadata: {
        path: filePath, app: s.app, title: s.title, url: s.url,
        sessionType: s.type, engine: s.engine,
        started: start.toISOString(), ended: new Date(s.lastActivity).toISOString(),
        segments: s.segs.length,
      },
    }, { importance: 0.7, skipConsolidation: true });
  }
}

let _journal = null;
export function getAudioJournal() {
  if (!_journal) _journal = new AudioJournal();
  return _journal;
}
