import { getActiveWindow, isSystemIdle } from './activeWindow.js';
import { getScreenCaptureService } from './screenCapture.js';
import OCRService, { getOCRService } from './ocrService.js';
import { getDatabaseService } from '../services/database.js';
import { generateMemoryId } from '../utils/helpers.js';
import { structureOcrOverlayItems } from '../utils/ocrStructure.js';
import logger from '../utils/logger.js';
import { PNG } from 'pngjs';
import fs from 'fs';
import os from 'os';
import path from 'path';
import http from 'http';
import { execSync, spawn } from 'child_process';
import { EventEmitter } from 'events';

// ── Event-driven heartbeat notification ──────────────────────────────────────
// When monitor events fire, POST a notification to the personality-service
// so it can trigger an event-driven Tier 2 awareness check.
const PERSONALITY_SERVICE_PORT = parseInt(process.env.PERSONALITY_SERVICE_PORT || '3008', 10);
const HEARTBEAT_NOTIFY_ENABLED = process.env.HEARTBEAT_NOTIFY_ENABLED !== 'false';

function notifyPersonalityService(eventType, info) {
  if (!HEARTBEAT_NOTIFY_ENABLED) return;
  const body = JSON.stringify({
    version: 'mcp.v1',
    service: 'personality-service',
    action: 'monitor.event',
    payload: { eventType, info },
    requestId: 'mon_evt_' + Date.now(),
  });
  const req = http.request({
    hostname: '127.0.0.1',
    port: PERSONALITY_SERVICE_PORT,
    path: '/monitor.event',
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
    timeout: 3000,
  }, (res) => { res.resume(); });
  req.on('error', () => { /* personality-service may not be running — silent */ });
  req.on('timeout', () => { req.destroy(); });
  req.write(body);
  req.end();
}

const KNOWN_APPS = {
  'Google Chrome': 'browser', 'Safari': 'browser', 'Firefox': 'browser',
  'Microsoft Edge': 'browser', 'Brave Browser': 'browser',
  'Visual Studio Code': 'editor', 'Code': 'editor', 'Cursor': 'editor',
  'Windsurf': 'editor', 'Zed': 'editor', 'Sublime Text': 'editor',
  'TextEdit': 'editor', 'Devin': 'editor',
  'Slack': 'chat', 'Discord': 'chat', 'Microsoft Teams': 'chat',
  'Telegram': 'chat', 'WhatsApp': 'chat', 'Messages': 'chat',
  'Figma': 'design', 'Adobe Photoshop': 'design', 'Adobe Illustrator': 'design',
  'Sketch': 'design',
  'Terminal': 'terminal', 'iTerm': 'terminal', 'iTerm2': 'terminal',
  'Warp': 'terminal', 'Hyper': 'terminal',
  'Mail': 'email', 'Microsoft Outlook': 'email', 'Spark': 'email'
};

// ── Overlay detection ────────────────────────────────────────────────────────
// The ThinkDrop overlay is an always-on-top Electron window. When the monitor
// captures the whole screen, OCR reads the overlay text and misidentifies the
// active app. These phrases are unique to the overlay UI and are used to flag
// tainted captures so they can be excluded from context queries.

const OVERLAY_STRONG_PHRASES = [
  'Ask or Drag-Drop anything here',
  'Copilot Screen Assistant',
  'ThinkDrop',
  'You just switched to the',
];

function isOverlayTainted(text) {
  const lower = (text || '').toLowerCase();
  if (!lower) return false;

  // Strong, unambiguous overlay phrases
  for (const phrase of OVERLAY_STRONG_PHRASES) {
    if (lower.includes(phrase.toLowerCase())) return true;
  }

  // The two overlay tabs appear together
  const hasResults = lower.includes('results');
  const hasAgents = lower.includes('agents');
  if (hasResults && hasAgents) return true;

  // Electron identifier appears alongside one of the overlay tabs
  if (lower.includes('electron') && (hasResults || hasAgents)) return true;

  return false;
}

class MonitorService extends EventEmitter {
  constructor() {
    super();
    this.isRunning = false;
    this.intervalId = null;
    this.captureInterval = parseInt(process.env.SCREEN_CAPTURE_INTERVAL || '5000', 10);
    this.idleTimeout = parseInt(process.env.SCREEN_CAPTURE_IDLE_TIMEOUT || '300000', 10);
    this.lastAppName = null;
    this.lastWindowTitle = null;
    this.lastUrl = null;
    this.lastFilePath = null;
    this.captureCount = 0;
    this.skipCount = 0;
    this.errorCount = 0;
    this.screenCapture = getScreenCaptureService();
    this.ocr = getOCRService();
    this.db = getDatabaseService();
    this._watchModes = new Map();
    // The last non-overlay app the user was in before switching to ThinkDrop.
    // Used by getRecentOcr to prefer captures from the app the user was actually looking at.
    this.lastNonOverlayApp = null;
    // The open file path of the last non-overlay app (authoritative when available).
    this.lastNonOverlayFilePath = null;
    // Phase 2: Track screen dimensions for resize detection
    this.lastScreenWidth = 0;
    this.lastScreenHeight = 0;
    this.BOUNDARY_INVALIDATION_THRESHOLD = 0.30; // 30% pixel diff triggers cache invalidation
    // ── Active-app-history tracker (App-Flow) ────────────────────────────────
    // Sequential list of app switches for the current day, used by app.runner.cjs
    // to replace URL-first navigation in the browser flow.
    this.appHistory = [];
    this.appHistoryFile = path.join(os.homedir(), '.thinkdrop', 'app-history.json');
    this._appHistoryPersistTimer = null;
    // LiteParser CLI availability: null = unknown, true/false after first check.
    // When true, structured OCR rows are stored in memory metadata.
    // When false, falls back to Tesseract flat text only (existing behavior).
    this._litAvailable = null;
    this._loadAppHistory();
  }

  // ── Active-app-history tracker methods (App-Flow) ─────────────────────────
  // Maintains an in-memory + persisted list of app switches for the current day.
  // Used by app.runner.cjs to replace URL-first navigation in the browser flow.

  _loadAppHistory() {
    try {
      if (!fs.existsSync(this.appHistoryFile)) return;
      const raw = JSON.parse(fs.readFileSync(this.appHistoryFile, 'utf8'));
      // Filter to today's date
      const today = new Date().toISOString().slice(0, 10);
      this.appHistory = (Array.isArray(raw) ? raw : [])
        .filter(e => (e.timestamp || '').slice(0, 10) === today);
      logger.info(`[monitorService] Loaded ${this.appHistory.length} app-history entries for today`);
    } catch (_) { this.appHistory = []; }
  }

  _persistAppHistory() {
    // Debounced — coalesce rapid switches into a single write
    if (this._appHistoryPersistTimer) return;
    this._appHistoryPersistTimer = setTimeout(() => {
      this._appHistoryPersistTimer = null;
      try {
        const dir = path.dirname(this.appHistoryFile);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(this.appHistoryFile, JSON.stringify(this.appHistory.slice(-500)));
      } catch (e) {
        logger.debug(`[monitorService] app-history persist failed: ${e.message}`);
      }
    }, 2000);
  }

  _recordAppSwitch(appName, windowTitle, bounds, url, filePath) {
    if (!appName) return;
    const last = this.appHistory[this.appHistory.length - 1];
    // Dedupe consecutive same-app + same-file entries.
    // A new entry IS pushed when the same app opens a different file —
    // file-open events within the same app must be captured.
    if (last && last.appName === appName && (last.filePath || null) === (filePath || null)) return;
    const entry = {
      appName,
      category: KNOWN_APPS[appName] || 'other',
      windowTitle: windowTitle || '',
      bounds: bounds || null,
      timestamp: new Date().toISOString(),
      url: url || null,
      filePath: filePath || null,
    };
    this.appHistory.push(entry);
    // Cap at 500 entries per day
    if (this.appHistory.length > 500) this.appHistory.shift();
    this._persistAppHistory();
  }

  /**
   * Skip-list helper: overlay apps + the voice companion Chrome window.
   * The voice companion is a separate Chrome window (shell.openExternal to
   * localhost:5173?mode=voice-companion) that steals focus when opened.
   * Detected by appName === 'Google Chrome' && url?.includes('voice-companion').
   * @param {string} appName
   * @param {string|null} url
   * @returns {boolean}
   */
  _isSkipApp(appName, url) {
    if (!appName) return true;
    // ThinkDrop overlay + Electron
    if (['Electron', 'ThinkDrop'].some(s => appName.includes(s))) return true;
    // Voice companion Chrome window — detected by URL
    if (appName === 'Google Chrome' && url && /mode=voice-companion/.test(url)) return true;
    // Transient/system pseudo-apps — get-windows returns these during lock
    // screen, notification center, or transient failures. They pollute
    // appHistory and corrupt lastNonOverlayApp. isSystemIdle gates true idle;
    // this covers the first minutes post-lock and transient get-windows hiccups.
    if (appName === 'unknown') return true;
    const SYSTEM_PSEUDO_APPS = new Set([
      'loginwindow', 'UserNotificationCenter', 'WindowManager', 'Dock',
      'Control Center', 'Notification Center', 'Spotlight',
    ]);
    if (SYSTEM_PSEUDO_APPS.has(appName)) return true;
    return false;
  }

  /**
   * Resolve the category for an app name (browser/editor/chat/design/terminal/email/other).
   * @param {string} appName
   * @returns {string}
   */
  _getCategory(appName) {
    return KNOWN_APPS[appName] || 'other';
  }

  getAppHistory({ maxAgeHours = 24 } = {}) {
    const cutoff = Date.now() - maxAgeHours * 3600 * 1000;
    return this.appHistory.filter(e => new Date(e.timestamp).getTime() >= cutoff);
  }

  getPreviousActiveApp() {
    // Find the last non-overlay app entry from history (skips voice companion too)
    for (let i = this.appHistory.length - 1; i >= 0; i--) {
      const e = this.appHistory[i];
      if (!this._isSkipApp(e.appName, e.url)) return e;
    }
    // Fallback to lastNonOverlayApp
    if (this.lastNonOverlayApp) {
      return {
        appName: this.lastNonOverlayApp,
        category: KNOWN_APPS[this.lastNonOverlayApp] || 'other',
        windowTitle: '',
        bounds: null,
        timestamp: new Date().toISOString(),
        url: null,
        filePath: this.lastNonOverlayFilePath || null,
      };
    }
    return null;
  }

  // ── LiteParser structured OCR (progressive enhancement) ──────────────────
  // If the `lit` CLI is available, we run LiteParser on each screenshot to get
  // structured rows (with positions + types) and store them in memory metadata.
  // If LiteParser is not available, we fall back to Tesseract flat text only.

  _checkLiteParserAvailable() {
    if (this._litAvailable !== null) return this._litAvailable;
    try {
      execSync('which lit', { timeout: 2000, stdio: 'pipe' });
      this._litAvailable = true;
      logger.info('[monitorService] LiteParser CLI (lit) available — structured OCR enabled');
    } catch (_) {
      this._litAvailable = false;
      logger.info('[monitorService] LiteParser CLI (lit) not available — falling back to Tesseract only');
    }
    return this._litAvailable;
  }

  async _captureStructuredItems(screenshotBuffer) {
    if (!this._checkLiteParserAvailable()) return [];

    try {
      const tmpPath = path.join(os.tmpdir(), `monitor-${Date.now()}.png`);
      fs.writeFileSync(tmpPath, screenshotBuffer);

      const outputFile = path.join(os.tmpdir(), `monitor-liteparse-${Date.now()}.json`);
      return new Promise((resolve) => {
        const litProcess = spawn('lit', ['parse', tmpPath, '--format', 'json', '-o', outputFile], { timeout: 30000 });
        litProcess.on('error', () => {
          this._litAvailable = false;
          try { fs.unlinkSync(tmpPath); } catch (_) { /* empty */ }
          resolve([]);
        });
        litProcess.on('close', (code) => {
          try { fs.unlinkSync(tmpPath); } catch (_) { /* empty */ }
          if (code !== 0 || !fs.existsSync(outputFile)) { resolve([]); return; }
          try {
            const output = JSON.parse(fs.readFileSync(outputFile, 'utf8'));
            fs.unlinkSync(outputFile);
            const items = [];
            (output.pages || []).forEach(page => {
              (page.textItems || []).forEach(item => {
                items.push({
                  text: item.text || '',
                  x: item.x || 0,
                  y: item.y || 0,
                  width: item.width || 0,
                  height: item.height || 0,
                  confidence: item.confidence || 1,
                });
              });
            });
            resolve(items);
          } catch (_) { resolve([]); }
        });
      });
    } catch (e) {
      logger.debug(`[monitorService] _captureStructuredItems failed: ${e.message}`);
      return [];
    }
  }

  /**
   * Start the screen monitor loop.
   */
  async start() {
    if (this.isRunning) {
      logger.warn('Monitor is already running');
      return;
    }

    // Initialize OCR worker
    await this.ocr.initialize();

    // Cold-start: restore lastNonOverlayApp so getRecentOcr works correctly from the
    // very first request after a service restart.
    // Priority: 1) persisted state file (written when ThinkDrop overlay took focus),
    //           2) DB query for most recent non-overlay capture.
    const _stateFile = path.join(os.homedir(), '.thinkdrop', 'monitor-state.json');
    try {
      if (fs.existsSync(_stateFile)) {
        const _saved = JSON.parse(fs.readFileSync(_stateFile, 'utf8'));
        if (_saved?.lastNonOverlayApp) {
          this.lastNonOverlayApp = _saved.lastNonOverlayApp;
          this.lastNonOverlayFilePath = _saved.lastNonOverlayFilePath || null;
          logger.info(`[monitorService] Cold-start: restored lastNonOverlayApp = "${this.lastNonOverlayApp}" (from state file)${this.lastNonOverlayFilePath ? ` filePath="${this.lastNonOverlayFilePath}"` : ''}`);
        }
      }
    } catch (_) { /* state file unreadable — fall through to DB */ }
    // DB fallback if state file had nothing
    if (!this.lastNonOverlayApp) {
      try {
        const seedRows = await this.db.query(`
          SELECT json_extract_string(metadata, '$.appName') as appName,
                 json_extract_string(metadata, '$.filePath') as filePath
          FROM episodic_memory
          WHERE type = 'screen_capture'
            AND json_extract_string(metadata, '$.appName') NOT IN ('Electron', 'ThinkDrop', 'unknown')
            AND json_extract_string(metadata, '$.appName') IS NOT NULL
            AND json_extract_string(metadata, '$.overlayTainted') IS NULL
          ORDER BY created_at DESC
          LIMIT 1
        `);
        if (seedRows?.length > 0 && seedRows[0].appName) {
          this.lastNonOverlayApp = seedRows[0].appName;
          this.lastNonOverlayFilePath = seedRows[0].filePath || null;
          logger.info(`[monitorService] Cold-start: seeded lastNonOverlayApp = "${this.lastNonOverlayApp}" (from DB)${this.lastNonOverlayFilePath ? ` filePath="${this.lastNonOverlayFilePath}"` : ''}`);
        }
      } catch (seedErr) {
        logger.debug(`[monitorService] Cold-start DB seed skipped: ${seedErr.message}`);
      }
    }

    this.isRunning = true;
    logger.info('Screen monitor started', {
      captureInterval: this.captureInterval,
      idleTimeout: this.idleTimeout,
      diffThreshold: process.env.SCREEN_CAPTURE_DIFF_THRESHOLD || '0.15'
    });

    // Run first capture immediately
    await this.tick();

    // Then set up the interval
    this.intervalId = setInterval(() => {
      this.tick().catch(err => {
        logger.error('Monitor tick error', { error: err.message });
        this.errorCount++;
      });
    }, this.captureInterval);

    // Run one-time migration to flag any existing overlay-tainted captures.
    // Fire-and-forget — it should not block the monitor loop.
    this.runOverlayTaintMigration().catch(() => {});
  }

  /**
   * Stop the screen monitor.
   */
  async stop() {
    if (!this.isRunning) return;

    this.isRunning = false;
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }

    await this.ocr.terminate();

    logger.info('Screen monitor stopped', {
      totalCaptures: this.captureCount,
      totalSkips: this.skipCount,
      totalErrors: this.errorCount
    });
  }

  /**
   * Single tick of the monitor loop.
   * Implements the full dedup pipeline:
   *   1. Check idle
   *   2. Get active window
   *   3. Title changed? → capture
   *   4. Title same? → pixel diff → capture if >threshold
   *   5. OCR → text hash dedup
   *   6. Store episodic capture
   */
  async tick() {
    try {
      // Step 1: Cheap idle check (ioreg) — skip everything if user is idle
      if (await isSystemIdle(this.idleTimeout)) {
        this.skipCount++;
        return;
      }

      // Step 2: Get active window — cheap on same-app ticks via cached result
      const { appName, windowTitle, url, bounds, filePath } = await getActiveWindow();

      // Skip if the active app is the ThinkDrop overlay or the voice companion Chrome window
      if (this._isSkipApp(appName, url)) {
        // Record the last real (non-overlay) app so getRecentOcr can prefer it.
        // This captures the app the user was looking at before opening ThinkDrop.
        if (this.lastAppName && !this._isSkipApp(this.lastAppName, this.lastUrl)) {
          this.lastNonOverlayApp = this.lastAppName;
          this.lastNonOverlayFilePath = this.lastFilePath || null;
          // Persist to disk so it survives service restarts.
          try {
            const _stateDir = path.join(os.homedir(), '.thinkdrop');
            if (!fs.existsSync(_stateDir)) fs.mkdirSync(_stateDir, { recursive: true });
            fs.writeFileSync(path.join(_stateDir, 'monitor-state.json'),
              JSON.stringify({ lastNonOverlayApp: this.lastNonOverlayApp, lastNonOverlayFilePath: this.lastNonOverlayFilePath, ts: new Date().toISOString() }));
          } catch (_) { /* non-fatal — persist best-effort */ }
        }
        this.skipCount++;
        return;
      }

      // Step 3: Check if window context changed (app, title, or open file path)
      const titleChanged = appName !== this.lastAppName || windowTitle !== this.lastWindowTitle;
      const filePathChanged = (filePath || null) !== (this.lastFilePath || null);
      if ((titleChanged || filePathChanged) && appName) {
        // Phase 2: Invalidate previous app's boundary cache before switching
        if (this.lastAppName) {
          this._invalidateBoundaryCache(this.lastAppName, this.lastWindowTitle);
        }
        this._enqueueAppEnrichment(appName, windowTitle);
        // Event-driven heartbeat: emit app_change for subscribers (e.g. heartbeat.cjs)
        this.emit('app_change', { app: appName, title: windowTitle, previousApp: this.lastAppName, filePath });
        // Notify personality-service (cross-process) to trigger event-driven Tier 2
        notifyPersonalityService('app_change', { app: appName, title: windowTitle, previousApp: this.lastAppName });
        // ── App-Flow: record app switch (or file-open within same app) ──
        this._recordAppSwitch(appName, windowTitle, bounds, url, filePath);
      }
      this.lastAppName = appName;
      this.lastWindowTitle = windowTitle;
      this.lastUrl = url;
      this.lastFilePath = filePath || null;

      let screenshotBuffer;

      if (titleChanged) {
        // Window/app changed — take screenshot and update pixel diff baseline
        screenshotBuffer = await this.screenCapture.capture();
        if (!screenshotBuffer) {
          this.skipCount++;
          return;
        }
        // Phase 2: Check for screen resize
        this._checkScreenResize(screenshotBuffer, appName, windowTitle);
        // Update diff baseline for next same-window tick
        await this.screenCapture.computeDiff(screenshotBuffer);
      } else {
        // Step 4: Same window — take screenshot and run pixel diff
        // Only proceed to OCR+store if content visually changed
        const { changed, diffRatio, buffer } = await this.screenCapture.captureIfChanged();
        if (!changed) {
          // Screen unchanged — skip OCR and store entirely
          this.skipCount++;
          return;
        }
        screenshotBuffer = buffer;
        // Phase 2: Check for screen resize
        this._checkScreenResize(screenshotBuffer, appName, windowTitle);
        logger.debug('Pixel diff detected', { diffRatio: diffRatio.toFixed(3), appName, windowTitle });

        // Phase 2: Invalidate boundary cache if pixel diff > 30% (major visual change)
        if (diffRatio > this.BOUNDARY_INVALIDATION_THRESHOLD) {
          logger.info('[monitorService] Pixel diff >30%, invalidating boundary cache', { appName, windowTitle, diffRatio: diffRatio.toFixed(3) });
          this._invalidateBoundaryCache(appName, windowTitle);
          // Event-driven heartbeat: emit screen_change for major visual changes
          this.emit('screen_change', { app: appName, title: windowTitle, diffRatio });
          // Notify personality-service (cross-process) to trigger event-driven Tier 2
          notifyPersonalityService('screen_change', { app: appName, title: windowTitle, diffRatio: diffRatio.toFixed(3) });
        }
      }

      // Step 5: Run OCR
      const ocrResult = await this.ocr.extractText(screenshotBuffer);
      if (!ocrResult.text || ocrResult.text.length < 10) {
        // Too little text — likely a blank screen or image-heavy content
        this.skipCount++;
        return;
      }

      // Step 5b: Text hash dedup — bypassed on app switch so the new app is
      // immediately anchored in the DB even if screen content hasn't changed.
      const { isDifferent } = this.ocr.checkTextChanged(ocrResult.text);
      if (!isDifferent && !titleChanged) {
        this.skipCount++;
        return;
      }

      if (!isDifferent && titleChanged) {
        logger.info(`[monitorService] App switched to "${appName}" — bypassing text-hash dedup to anchor new app in DB`);
      }

      // Step 6: Store episodic capture
      await this.storeCapture(appName, windowTitle, ocrResult, url, screenshotBuffer, bounds, filePath);
      this.captureCount++;

      // Step 7: Check active watch modes — fire callbacks if new content detected
      if (this._watchModes.size > 0) {
        this._checkWatchModes(appName, ocrResult.text).catch(() => {});
      }

      logger.info('Screen capture stored', {
        appName,
        windowTitle: windowTitle.substring(0, 80),
        url: url || undefined,
        ocrLength: ocrResult.text.length,
        confidence: ocrResult.confidence,
        totalCaptures: this.captureCount
      });

    } catch (error) {
      this.errorCount++;
      logger.error('Monitor tick failed', { error: error.message });
    }
  }

  /**
   * Check all active watch modes for this appName.
   * Compares current OCR to each mode's baseline — fires onNewContent if new lines detected.
   * Runs LLM-free: just string comparison. LLM is in the onNewContent callback in app.agent.
   */
  _checkWatchModes(appName, currentText) {
    const promises = [];
    for (const [sessionId, watchSession] of this._watchModes.entries()) {
      if (watchSession.appName !== appName) continue;

      const baselineLines = new Set((watchSession.baselineOCR || '').split('\n').map(l => l.trim()).filter(Boolean));
      const currentLines = currentText.split('\n').map(l => l.trim()).filter(Boolean);
      const newLines = currentLines.filter(line => line.length > 5 && !baselineLines.has(line));

      if (newLines.length > 0) {
        logger.info(`[monitorService] watchMode ${sessionId}: ${newLines.length} new line(s) detected for ${appName}`);
        watchSession.baselineOCR = currentText;
        if (typeof watchSession.onNewContent === 'function') {
          promises.push(
            Promise.resolve(watchSession.onNewContent({ text: currentText, newLines })).catch(err => {
              logger.error(`[monitorService] watchMode onNewContent error: ${err.message}`);
            })
          );
        }
      }
    }
    return Promise.all(promises);
  }

  /**
   * Activate a watch mode session.
   * Hooks into the existing monitorService tick — no separate polling loop.
   * @param {Object} opts
   * @param {string} opts.sessionId       - Unique ID for this watch session
   * @param {string} opts.appName         - Only fire for this app
   * @param {string} opts.baselineOCR     - Snapshot of screen BEFORE we start watching
   * @param {string} [opts.stopKeyword]   - If found in new content, call onNewContent with DONE hint
   * @param {number} [opts.maxWaitMs]     - Timeout in ms (default 5 min)
   * @param {number} [opts.autoScrollMs]  - Interval for gentle scroll-down (default 15s). Pass 0 to disable.
   * @param {Object} [opts.mainRegion]    - { centerX, centerY } for auto-scroll mouse position
   * @param {Function} opts.onNewContent  - Callback: ({ text, newLines }) => void. LLM call happens here.
   * @param {Function} [opts.onTimeout]   - Callback fired when maxWaitMs elapses with no DONE
   */
  activateWatchMode({ sessionId, appName, baselineOCR, stopKeyword, maxWaitMs = 300000, autoScrollMs = 15000, mainRegion, onNewContent, onTimeout }) {
    if (this._watchModes.has(sessionId)) {
      logger.warn(`[monitorService] watchMode ${sessionId} already active — deactivating old one first`);
      this.deactivateWatchMode(sessionId);
    }

    const session = {
      sessionId,
      appName,
      baselineOCR: baselineOCR || '',
      stopKeyword: stopKeyword || null,
      onNewContent,
      onTimeout,
      startedAt: Date.now(),
      autoScrollInterval: null,
      timeoutHandle: null
    };

    if (maxWaitMs > 0) {
      session.timeoutHandle = setTimeout(() => {
        logger.info(`[monitorService] watchMode ${sessionId} timed out after ${maxWaitMs}ms`);
        this.deactivateWatchMode(sessionId);
        if (typeof onTimeout === 'function') onTimeout();
      }, maxWaitMs);
    }

    if (autoScrollMs > 0 && mainRegion) {
      session.autoScrollInterval = setInterval(async () => {
        try {
          const nut = require('@nut-tree-fork/nut-js');
          await nut.mouse.move([{ x: mainRegion.centerX, y: mainRegion.centerY }]);
          await new Promise(r => setTimeout(r, 200));
          await nut.mouse.scrollDown(2);
          logger.debug(`[monitorService] watchMode ${sessionId}: auto-scroll down 2 units`);
        } catch (error) {
          console.log('Error:', error);
        }
      }, autoScrollMs);
    }

    this._watchModes.set(sessionId, session);
    logger.info(`[monitorService] watchMode activated: ${sessionId} for ${appName} (maxWait: ${maxWaitMs}ms, autoScroll: ${autoScrollMs}ms)`);
  }

  /**
   * Deactivate a watch mode session and clean up timers.
   * @param {string} sessionId
   */
  deactivateWatchMode(sessionId) {
    const session = this._watchModes.get(sessionId);
    if (!session) return;

    if (session.timeoutHandle) clearTimeout(session.timeoutHandle);
    if (session.autoScrollInterval) clearInterval(session.autoScrollInterval);

    this._watchModes.delete(sessionId);
    logger.info(`[monitorService] watchMode deactivated: ${sessionId}`);
  }

  /**
   * Invalidate boundary cache for a specific app/window.
   * Called on app switch, resize, or major visual change (>30% pixel diff).
   * Phase 2: Cache invalidation triggers for boundary layout freshness.
   */
  _invalidateBoundaryCache(appName, windowTitle) {
    if (!appName) return;
    const cacheKey = `${appName.toLowerCase().replace(/\s+/g, '_')}_${(windowTitle || 'default').toLowerCase().replace(/[^a-z0-9]/g, '_')}`;
    // Fire-and-forget invalidation request to command-service
    const COMMAND_SERVICE_PORT = process.env.COMMAND_SERVICE_PORT || '3007';
    fetch(`http://127.0.0.1:${COMMAND_SERVICE_PORT}/skill/app.agent`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'clear_boundary_cache', appName, windowTitle })
    }).catch(() => {});
    logger.info(`[monitorService] Boundary cache invalidation triggered: ${cacheKey}`);
  }

  /**
   * Check for screen resize and invalidate boundary cache if dimensions changed.
   * Phase 2: Triggers when screen resize > 100px in either dimension.
   */
  _checkScreenResize(buffer, appName, windowTitle) {
    try {
      const png = PNG.sync.read(buffer);
      const { width, height } = png;
      if (this.lastScreenWidth === 0) {
        this.lastScreenWidth = width;
        this.lastScreenHeight = height;
        return;
      }
      const wDiff = Math.abs(width - this.lastScreenWidth);
      const hDiff = Math.abs(height - this.lastScreenHeight);
      if (wDiff > 100 || hDiff > 100) {
        logger.info('[monitorService] Screen resize detected', { appName, old: `${this.lastScreenWidth}x${this.lastScreenHeight}`, new: `${width}x${height}` });
        this._invalidateBoundaryCache(appName, windowTitle);
      }
      this.lastScreenWidth = width;
      this.lastScreenHeight = height;
    } catch (_) { /* non-critical — PNG parse errors don't affect monitor operation */ }
  }

  /**
   * Fire-and-forget background enrichment trigger.
   * Called on app switch to warm boundary + shortcut caches in app.agent.
   */
  _enqueueAppEnrichment(appName, windowTitle) {
    const COMMAND_SERVICE_PORT = process.env.COMMAND_SERVICE_PORT || '3007';
    fetch(`http://127.0.0.1:${COMMAND_SERVICE_PORT}/skill/app.agent`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'enrich_app_context', appName, windowTitle, background: true })
    }).catch(() => {});
  }

  async storeCapture(appName, windowTitle, ocrResult, url = null, screenshotBuffer = null, bounds = null, filePath = null) {
    const memoryId = generateMemoryId();
    const userId = process.env.MONITOR_USER_ID || 'local_user';

    // Process raw OCR text through cleanup pipeline
    const processed = OCRService.processOcrOutput(ocrResult.text);

    // Build the searchable text from cleaned output. Stored as source_text and
    // indexed by the FTS extension for BM25 search — no vector embedding is
    // generated for episodic rows (episodic search is BM25+filters only).
    const searchText = OCRService.summarizeForEmbedding(appName, windowTitle, processed);

    // Detect whether the captured image contains the ThinkDrop overlay
    const overlayTainted = isOverlayTainted(processed.filteredText) || isOverlayTainted(ocrResult.text);
    if (overlayTainted) {
      logger.info('[monitorService] Overlay detected in capture — flagging as tainted', { appName, windowTitle });
    }

    // Capture structured items via LiteParser (best-effort, falls back to empty)
    let structuredRows = [];
    if (screenshotBuffer) {
      const textItems = await this._captureStructuredItems(screenshotBuffer);
      if (textItems.length > 0) {
        structuredRows = structureOcrOverlayItems(textItems);
      }
    }

    // Build metadata with extracted files, code snippets, and OCR stats
    const category = KNOWN_APPS[appName] || 'other';
    const metadata = JSON.stringify({
      appName,
      windowTitle,
      category,
      ...(url ? { url } : {}),
      ...(bounds ? { bounds } : {}),
      ...(filePath ? { filePath } : {}),
      ocrConfidence: ocrResult.confidence,
      ocrRawLength: ocrResult.text.length,
      cleanedLength: processed.filteredText.length,
      files: processed.files,
      codeSnippets: processed.codeSnippets,
      capturedAt: new Date().toISOString(),
      ...(overlayTainted ? { overlayTainted: true } : {}),
      // Structured rows with positions + types (empty if LiteParser unavailable)
      ...(structuredRows.length > 0 ? { structuredRows: structuredRows.slice(0, 100) } : {}),
    });

    // source_text = cleaned text for embedding/search
    // extracted_text = filtered text (gibberish removed, redacted, no noise)
    const sql = `
      INSERT INTO episodic_memory (
        id, user_id, type, source_text, metadata,
        extracted_text, created_at, updated_at
      ) VALUES (
        '${memoryId}',
        '${userId}',
        'screen_capture',
        '${searchText.replace(/'/g, '\'\'')}',
        '${metadata.replace(/'/g, '\'\'')}',
        '${processed.filteredText.replace(/'/g, '\'\'')}',
        now(),
        now()
      )
    `;

    await this.db.execute(sql);

    // Store entities: app name and window title only. FILE entities were dropped
    // — they were ~57% of episodic_entities rows but only used for per-result
    // enrichment; file names remain in metadata.files for keyword search.
    const entities = [
      { type: 'application', value: appName, entity_type: 'APP' },
      { type: 'window', value: windowTitle.substring(0, 255), entity_type: 'WINDOW_TITLE' }
    ];

    for (const entity of entities) {
      const entityId = `ent_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      const entitySql = `
        INSERT INTO episodic_entities (id, memory_id, entity, type, entity_type, normalized_value)
        VALUES (
          '${entityId}',
          '${memoryId}',
          '${entity.value.replace(/'/g, '\'\'')}',
          '${entity.type}',
          '${entity.entity_type}',
          '${entity.value.toLowerCase().replace(/'/g, '\'\'')}'
        )
      `;
      await this.db.execute(entitySql);
    }
  }

  /**
   * One-time startup migration: flag existing screen captures that contain the
   * ThinkDrop overlay so they are excluded from getRecentOcr and context queries.
   * Idempotent — only updates rows where overlayTainted is not already set.
   */
  async runOverlayTaintMigration() {
    if (this._overlayTaintMigrationDone) return;
    this._overlayTaintMigrationDone = true;

    try {
      // Find candidate rows that are not already flagged
      const candidates = await this.db.query(`
        SELECT id, metadata, extracted_text, source_text
        FROM episodic_memory
        WHERE type = 'screen_capture'
          AND json_extract_string(metadata, '$.overlayTainted') IS NULL
      `);

      let updated = 0;
      for (const row of candidates || []) {
        const text = row.extracted_text || row.source_text || '';
        if (isOverlayTainted(text)) {
          const metadata = JSON.parse(row.metadata || '{}');
          metadata.overlayTainted = true;
          const safeMetadata = JSON.stringify(metadata).replace(/'/g, '\'\'');
          await this.db.execute(`
            UPDATE episodic_memory
            SET metadata = '${safeMetadata}'
            WHERE id = '${row.id.replace(/'/g, '\'\'')}'
          `);
          updated++;
        }
      }

      if (updated > 0) {
        logger.info('[monitorService] Overlay taint migration completed', { updatedRows: updated });
      } else {
        logger.info('[monitorService] Overlay taint migration: no rows to update');
      }
    } catch (err) {
      logger.warn(`[monitorService] Overlay taint migration failed: ${err.message}`);
    }
  }

  /**
   * Get monitor stats.
   */
  getStats() {
    return {
      isRunning: this.isRunning,
      captureInterval: this.captureInterval,
      totalCaptures: this.captureCount,
      totalSkips: this.skipCount,
      totalErrors: this.errorCount,
      lastApp: this.lastAppName,
      lastWindow: this.lastWindowTitle
    };
  }
}

let instance = null;

export function getMonitorService() {
  if (!instance) {
    instance = new MonitorService();
  }
  return instance;
}

export default MonitorService;
