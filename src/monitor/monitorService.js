import { getActiveWindow, isSystemIdle } from './activeWindow.js';
import { getScreenCaptureService } from './screenCapture.js';
import { getOCRService } from './ocrService.js';
import { getDatabaseService } from '../services/database.js';
import { getEmbeddingService } from '../services/embeddings.js';
import { generateMemoryId } from '../utils/helpers.js';
import logger from '../utils/logger.js';

class MonitorService {
  constructor() {
    this.isRunning = false;
    this.intervalId = null;
    this.captureInterval = parseInt(process.env.SCREEN_CAPTURE_INTERVAL || '10000', 10);
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
      // Step 1: Check if system is idle
      if (isSystemIdle(this.idleTimeout)) {
        this.skipCount++;
        return;
      }

      // Step 2: Get active window info
      const { appName, windowTitle } = getActiveWindow();

      // Step 3: Check if window context changed
      const titleChanged = appName !== this.lastAppName || windowTitle !== this.lastWindowTitle;
      this.lastAppName = appName;
      this.lastWindowTitle = windowTitle;

      let screenshotBuffer;

      if (titleChanged) {
        // Window changed — always capture
        screenshotBuffer = await this.screenCapture.capture();
        if (!screenshotBuffer) {
          this.skipCount++;
          return;
        }
        // Update the diff baseline
        await this.screenCapture.computeDiff(screenshotBuffer);
      } else {
        // Step 4: Same window — check pixel diff
        const { changed, diffRatio, buffer } = await this.screenCapture.captureIfChanged();
        if (!changed) {
          this.skipCount++;
          return;
        }
        screenshotBuffer = buffer;
        logger.debug('Pixel diff detected', { diffRatio: diffRatio.toFixed(3), appName, windowTitle });
      }

      // Step 5: Run OCR
      const ocrResult = await this.ocr.extractText(screenshotBuffer);
      if (!ocrResult.text || ocrResult.text.length < 10) {
        // Too little text — likely a blank screen or image-heavy content
        this.skipCount++;
        return;
      }

      // Step 5b: Text hash dedup
      const { isDifferent } = this.ocr.checkTextChanged(ocrResult.text);
      if (!isDifferent) {
        this.skipCount++;
        return;
      }

      // Step 6: Generate embedding and store
      await this.storeCapture(appName, windowTitle, ocrResult);
      this.captureCount++;

      logger.info('Screen capture stored', {
        appName,
        windowTitle: windowTitle.substring(0, 80),
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
   * Store a screen capture as a memory record.
   */
  async storeCapture(appName, windowTitle, ocrResult) {
    const memoryId = generateMemoryId();
    const userId = process.env.MONITOR_USER_ID || 'local_user';

    // Build the text for embedding — structured for good semantic search
    const embeddingText = `${appName}: ${windowTitle}\n${ocrResult.text}`.substring(0, 2000);

    // Generate embedding
    const embedding = await this.embeddings.generateEmbedding(embeddingText);
    const embeddingValues = embedding.map(v => v.toString()).join(',');

    // Build metadata
    const metadata = JSON.stringify({
      appName,
      windowTitle,
      ocrConfidence: ocrResult.confidence,
      ocrLength: ocrResult.text.length,
      capturedAt: new Date().toISOString()
    });

    // Store the OCR text as source_text, embedding for search
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
        '${ocrResult.text.replace(/'/g, '\'\'')}',
        list_value(${embeddingValues}),
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
      )
    `;

    await this.db.execute(sql);

    // Store entities: app name and window title as searchable entities
    const entities = [
      { type: 'application', value: appName, entity_type: 'APP' },
      { type: 'window', value: windowTitle.substring(0, 255), entity_type: 'WINDOW_TITLE' }
    ];

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
