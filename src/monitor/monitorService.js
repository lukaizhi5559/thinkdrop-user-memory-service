import { getActiveWindow, isSystemIdle } from './activeWindow.js';
import { getScreenCaptureService } from './screenCapture.js';
import OCRService, { getOCRService } from './ocrService.js';
import { getDatabaseService } from '../services/database.js';
import { getEmbeddingService } from '../services/embeddings.js';
import { generateMemoryId } from '../utils/helpers.js';
import logger from '../utils/logger.js';
import { PNG } from 'pngjs';
import fs from 'fs';
import os from 'os';
import path from 'path';

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

class MonitorService {
  constructor() {
    this.isRunning = false;
    this.intervalId = null;
    this.captureInterval = parseInt(process.env.SCREEN_CAPTURE_INTERVAL || '5000', 10);
    this.idleTimeout = parseInt(process.env.SCREEN_CAPTURE_IDLE_TIMEOUT || '300000', 10);
    this.lastAppName = null;
    this.lastWindowTitle = null;
    this.captureCount = 0;
    this.skipCount = 0;
    this.errorCount = 0;
    this.screenCapture = getScreenCaptureService();
    this.ocr = getOCRService();
    this.db = getDatabaseService();
    this.embeddings = getEmbeddingService();
    this._watchModes = new Map();
    // The last non-overlay app the user was in before switching to ThinkDrop.
    // Used by getRecentOcr to prefer captures from the app the user was actually looking at.
    this.lastNonOverlayApp = null;
    // Phase 2: Track screen dimensions for resize detection
    this.lastScreenWidth = 0;
    this.lastScreenHeight = 0;
    this.BOUNDARY_INVALIDATION_THRESHOLD = 0.30; // 30% pixel diff triggers cache invalidation
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
          logger.info(`[monitorService] Cold-start: restored lastNonOverlayApp = "${this.lastNonOverlayApp}" (from state file)`);
        }
      }
    } catch (_) { /* state file unreadable — fall through to DB */ }
    // DB fallback if state file had nothing
    if (!this.lastNonOverlayApp) {
      try {
        const seedRows = await this.db.query(`
          SELECT json_extract_string(metadata, '$.appName') as appName
          FROM memory
          WHERE type = 'screen_capture'
            AND json_extract_string(metadata, '$.appName') NOT IN ('Electron', 'ThinkDrop', 'unknown')
            AND json_extract_string(metadata, '$.appName') IS NOT NULL
            AND json_extract_string(metadata, '$.overlayTainted') IS NULL
          ORDER BY created_at DESC
          LIMIT 1
        `);
        if (seedRows?.length > 0 && seedRows[0].appName) {
          this.lastNonOverlayApp = seedRows[0].appName;
          logger.info(`[monitorService] Cold-start: seeded lastNonOverlayApp = "${this.lastNonOverlayApp}" (from DB)`);
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
   *   6. Generate embedding → store
   */
  async tick() {
    try {
      // Step 1: Cheap idle check (ioreg) — skip everything if user is idle
      if (await isSystemIdle(this.idleTimeout)) {
        this.skipCount++;
        return;
      }

      // Step 2: Get active window — cheap on same-app ticks via cached result
      const { appName, windowTitle, url } = await getActiveWindow();

      // Skip if the active app is the ThinkDrop overlay itself
      const SKIP_APPS = ['Electron', 'ThinkDrop'];
      if (SKIP_APPS.some(skip => appName?.includes(skip))) {
        // Record the last real (non-overlay) app so getRecentOcr can prefer it.
        // This captures the app the user was looking at before opening ThinkDrop.
        if (this.lastAppName && !SKIP_APPS.some(s => this.lastAppName?.includes(s))) {
          this.lastNonOverlayApp = this.lastAppName;
          // Persist to disk so it survives service restarts.
          try {
            const _stateDir = path.join(os.homedir(), '.thinkdrop');
            if (!fs.existsSync(_stateDir)) fs.mkdirSync(_stateDir, { recursive: true });
            fs.writeFileSync(path.join(_stateDir, 'monitor-state.json'),
              JSON.stringify({ lastNonOverlayApp: this.lastNonOverlayApp, ts: new Date().toISOString() }));
          } catch (_) { /* non-fatal — persist best-effort */ }
        }
        this.skipCount++;
        return;
      }

      // Step 3: Check if window context changed
      const titleChanged = appName !== this.lastAppName || windowTitle !== this.lastWindowTitle;
      if (titleChanged && appName) {
        // Phase 2: Invalidate previous app's boundary cache before switching
        if (this.lastAppName) {
          this._invalidateBoundaryCache(this.lastAppName, this.lastWindowTitle);
        }
        this._enqueueAppEnrichment(appName, windowTitle);
      }
      this.lastAppName = appName;
      this.lastWindowTitle = windowTitle;

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
        // Only proceed to OCR+embedding if content visually changed
        const { changed, diffRatio, buffer } = await this.screenCapture.captureIfChanged();
        if (!changed) {
          // Screen unchanged — skip OCR and embedding entirely
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

      // Step 6: Generate embedding and store
      await this.storeCapture(appName, windowTitle, ocrResult, url);
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

  async storeCapture(appName, windowTitle, ocrResult, url = null) {
    const memoryId = generateMemoryId();
    const userId = process.env.MONITOR_USER_ID || 'local_user';

    // Process raw OCR text through cleanup pipeline
    const processed = OCRService.processOcrOutput(ocrResult.text);

    // Build embedding text from cleaned output
    const embeddingText = OCRService.summarizeForEmbedding(appName, windowTitle, processed);

    // Generate embedding
    const embedding = await this.embeddings.generateEmbedding(embeddingText);
    const embeddingValues = embedding.map(v => v.toString()).join(',');

    // Detect whether the captured image contains the ThinkDrop overlay
    const overlayTainted = isOverlayTainted(processed.filteredText) || isOverlayTainted(ocrResult.text);
    if (overlayTainted) {
      logger.info('[monitorService] Overlay detected in capture — flagging as tainted', { appName, windowTitle });
    }

    // Build metadata with extracted files, code snippets, and OCR stats
    const category = KNOWN_APPS[appName] || 'other';
    const metadata = JSON.stringify({
      appName,
      windowTitle,
      category,
      ...(url ? { url } : {}),
      ocrConfidence: ocrResult.confidence,
      ocrRawLength: ocrResult.text.length,
      cleanedLength: processed.filteredText.length,
      files: processed.files,
      codeSnippets: processed.codeSnippets,
      capturedAt: new Date().toISOString(),
      ...(overlayTainted ? { overlayTainted: true } : {})
    });

    // source_text = cleaned text for embedding/search
    // extracted_text = filtered text (gibberish removed, redacted, no noise)
    const sql = `
      INSERT INTO memory (
        id, user_id, type, source_text, metadata,
        extracted_text, embedding, created_at, updated_at
      ) VALUES (
        '${memoryId}',
        '${userId}',
        'screen_capture',
        '${embeddingText.replace(/'/g, '\'\'')}',
        '${metadata.replace(/'/g, '\'\'')}',
        '${processed.filteredText.replace(/'/g, '\'\'')}',
        list_value(${embeddingValues}),
        now(),
        now()
      )
    `;

    await this.db.execute(sql);

    // Store entities: app name, window title, and extracted file names
    const entities = [
      { type: 'application', value: appName, entity_type: 'APP' },
      { type: 'window', value: windowTitle.substring(0, 255), entity_type: 'WINDOW_TITLE' }
    ];

    // Add extracted file names as entities for searchability
    for (const file of processed.files.slice(0, 10)) {
      entities.push({ type: 'file', value: file, entity_type: 'FILE' });
    }

    for (const entity of entities) {
      const entityId = `ent_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      const entitySql = `
        INSERT INTO memory_entities (id, memory_id, entity, type, entity_type, normalized_value)
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
        FROM memory
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
            UPDATE memory
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
