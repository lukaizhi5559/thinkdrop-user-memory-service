import fs from 'fs';
import path from 'path';
import os from 'os';
import logger from '../utils/logger.js';
import { getDatabaseService } from './database.js';

/**
 * Data Retention Service
 *
 * Per-table retention: each table is evaluated independently against its own
 * horizon and purged with a simple "delete rows older than X" rule.
 *
 *   - RETENTION_MAX_DAYS:            memory table horizon (default: 1825 = 5 years)
 *   - EPISODIC_HOT_DAYS:             episodic_memory horizon (default: 365 = 1 year)
 *   - RETENTION_CHECK_INTERVAL_HOURS: how often to check (default: 24 = daily)
 *   - RETENTION_ENABLED:             feature flag (default: true)
 *   - ARCHIVE_DIR:                   parquet export dir (default: ~/.thinkdrop/data/archive/episodic)
 *
 * Episodic rows are archived to monthly parquet files before deletion so
 * verbatim recall beyond the hot window is still possible by querying the
 * parquet files directly with DuckDB.
 */

let retentionInstance = null;

const ARCHIVE_FORMAT_VERSION = 1;

class RetentionService {
  constructor() {
    this.maxDays = parseInt(process.env.RETENTION_MAX_DAYS || '1825', 10);
    this.episodicHotDays = parseInt(process.env.EPISODIC_HOT_DAYS || '365', 10);
    this.checkIntervalHours = parseInt(process.env.RETENTION_CHECK_INTERVAL_HOURS || '24', 10);
    this.enabled = process.env.RETENTION_ENABLED !== 'false'; // enabled by default
    this.archiveDir = process.env.ARCHIVE_DIR ||
      path.join(os.homedir(), '.thinkdrop', 'data', 'archive', 'episodic');
    this.timer = null;
    this.db = null;
    this.lastPurge = null;
    this.totalPurged = 0;
    this.lastArchive = null;
    this.totalArchived = 0;
  }

  /**
   * Start the retention service.
   * Runs an initial check, then schedules periodic checks.
   */
  async start() {
    if (!this.enabled) {
      logger.info('Data retention service disabled');
      return;
    }

    this.db = getDatabaseService();

    logger.info('Data retention service starting', {
      maxDays: this.maxDays,
      episodicHotDays: this.episodicHotDays,
      archiveDir: this.archiveDir,
      checkIntervalHours: this.checkIntervalHours
    });

    // Run initial check on startup
    await this.check();

    // Schedule periodic checks
    const intervalMs = this.checkIntervalHours * 60 * 60 * 1000;
    this.timer = setInterval(() => this.check(), intervalMs);

    logger.info('Data retention service started', {
      nextCheckIn: `${this.checkIntervalHours}h`
    });
  }

  /**
   * Stop the retention service.
   * Runs a final retention check before stopping (handles short-lived app sessions).
   */
  async stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    // Run a final check on shutdown — critical for apps that don't run 24h
    if (this.enabled && this.db) {
      logger.info('Running retention check on shutdown...');
      await this.check();
    }

    logger.info('Data retention service stopped', {
      totalPurged: this.totalPurged,
      totalArchived: this.totalArchived,
      lastPurge: this.lastPurge,
      lastArchive: this.lastArchive
    });
  }

  /**
   * Purge rows older than their table's retention horizon.
   * Each table is evaluated independently — an empty memory table no longer
   * gates episodic cleanup.
   */
  async check() {
    try {
      // Episodic: archive then purge beyond the hot window
      await this._purgeTable({
        table: 'episodic_memory',
        entityTable: 'episodic_entities',
        retentionDays: this.episodicHotDays,
        archive: true
      });

      // Semantic memory: purge only (synthesized memories carry the long-term value)
      await this._purgeTable({
        table: 'memory',
        entityTable: 'memory_entities',
        retentionDays: this.maxDays,
        archive: false
      });
    } catch (error) {
      logger.error('Retention check failed', { error: error.message });
    }
  }

  /**
   * Purge rows from `table` older than `retentionDays`.
   * Entity rows are removed first for referential integrity.
   * When `archive` is true, expired rows are exported to monthly parquet files
   * before deletion.
   */
  async _purgeTable({ table, entityTable, retentionDays, archive }) {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - retentionDays);
    const cutoffIso = cutoff.toISOString();

    const countResult = await this.db.query(
      `SELECT COUNT(*) as count FROM ${table} WHERE created_at < '${cutoffIso}'` // eslint-disable-line quotes
    );
    const purgeCount = Number(countResult[0]?.count || 0);

    if (purgeCount === 0) {
      logger.debug(`Retention: ${table} within limits`, { retentionDays });
      return;
    }

    logger.info(`Retention purge starting for ${table}`, {
      purgeCount,
      cutoff: cutoffIso,
      retentionDays
    });

    if (archive) {
      await this._archiveEpisodic(cutoffIso);
    }

    if (entityTable) {
      await this.db.execute(`
        DELETE FROM ${entityTable}
        WHERE memory_id IN (
          SELECT id FROM ${table} WHERE created_at < '${cutoffIso}'
        )
      `);
    }

    await this.db.execute(
      `DELETE FROM ${table} WHERE created_at < '${cutoffIso}'`
    );

    if (typeof this.db.compactHnswIndex === 'function') {
      await this.db.compactHnswIndex().catch(e =>
        logger.warn('HNSW compact after purge failed', { error: e.message }));
    }
    await this.db.execute('CHECKPOINT').catch(e =>
      logger.warn('Checkpoint after purge failed', { error: e.message }));
    if (typeof this.db.rebuildHnswIndex === 'function') {
      await this.db.rebuildHnswIndex().catch(e =>
        logger.warn('HNSW rebuild after purge failed', { error: e.message }));
    }

    this.lastPurge = new Date().toISOString();
    this.totalPurged += purgeCount;

    logger.info(`Retention purge completed for ${table}`, {
      recordsPurged: purgeCount,
      totalPurgedLifetime: this.totalPurged,
      cutoff: cutoffIso
    });
  }

  /**
   * Export expired episodic rows to monthly parquet files under archiveDir.
   * Files are named episodic_YYYY-MM.parquet; if a month file already exists
   * (a month can expire gradually across runs) a numeric suffix is appended so
   * no archive is ever overwritten.
   */
  async _archiveEpisodic(cutoffIso) {
    fs.mkdirSync(this.archiveDir, { recursive: true });

    const months = await this.db.query(`
      SELECT DISTINCT strftime(created_at, '%Y-%m') AS month
      FROM episodic_memory
      WHERE created_at < '${cutoffIso}'
      ORDER BY month
    `);

    let archived = 0;
    for (const { month } of months) {
      if (!month) continue;

      let filePath = path.join(this.archiveDir, `episodic_${month}.parquet`);
      let suffix = 2;
      while (fs.existsSync(filePath)) {
        filePath = path.join(this.archiveDir, `episodic_${month}_p${suffix}.parquet`);
        suffix++;
      }

      const escaped = filePath.replace(/'/g, '\'\'');
      await this.db.execute(`
        COPY (
          SELECT * FROM episodic_memory
          WHERE created_at < '${cutoffIso}'
            AND strftime(created_at, '%Y-%m') = '${month}'
          ORDER BY created_at
        ) TO '${escaped}' (FORMAT PARQUET)
      `);

      const exported = await this.db.query(
        `SELECT COUNT(*) AS n FROM read_parquet('${escaped}')`
      );
      const expected = await this.db.query(
        `SELECT COUNT(*) AS n FROM episodic_memory
         WHERE created_at < '${cutoffIso}' AND strftime(created_at, '%Y-%m') = '${month}'`
      );

      const exportedN = Number(exported[0]?.n || 0);
      const expectedN = Number(expected[0]?.n || 0);
      if (exportedN !== expectedN) {
        throw new Error(
          `Archive verification failed for ${month}: exported ${exportedN}, expected ${expectedN}`
        );
      }

      // Entities ride along in a sidecar file — same month, same naming
      const entPath = filePath.replace('.parquet', '_entities.parquet');
      const entEscaped = entPath.replace(/'/g, '\'\'');
      await this.db.execute(`
        COPY (
          SELECT e.* FROM episodic_entities e
          JOIN episodic_memory m ON e.memory_id = m.id
          WHERE m.created_at < '${cutoffIso}'
            AND strftime(m.created_at, '%Y-%m') = '${month}'
        ) TO '${entEscaped}' (FORMAT PARQUET)
      `);

      archived += exportedN;
      logger.info('Archived episodic month', { month, rows: exportedN, file: filePath });
    }

    if (archived > 0) {
      this.lastArchive = new Date().toISOString();
      this.totalArchived += archived;
    }
  }

  /**
   * Get retention service status for health checks.
   */
  getStatus() {
    return {
      enabled: this.enabled,
      maxDays: this.maxDays,
      episodicHotDays: this.episodicHotDays,
      archiveDir: this.archiveDir,
      archiveFormatVersion: ARCHIVE_FORMAT_VERSION,
      checkIntervalHours: this.checkIntervalHours,
      lastPurge: this.lastPurge,
      lastArchive: this.lastArchive,
      totalPurged: this.totalPurged,
      totalArchived: this.totalArchived
    };
  }

  /**
   * Manually trigger a purge of records older than a specific number of days.
   * Useful for admin/API-triggered cleanup.
   */
  async manualPurge(olderThanDays) {
    const days = parseInt(olderThanDays, 10);
    if (!Number.isFinite(days) || days < 0) {
      logger.warn('Manual retention purge rejected: invalid days', { olderThanDays });
      return;
    }

    logger.info('Manual retention purge requested', { olderThanDays: days });

    await this._purgeTable({
      table: 'episodic_memory',
      entityTable: 'episodic_entities',
      retentionDays: days,
      archive: true
    });
    await this._purgeTable({
      table: 'memory',
      entityTable: 'memory_entities',
      retentionDays: days,
      archive: false
    });
  }
}

export function getRetentionService() {
  if (!retentionInstance) {
    retentionInstance = new RetentionService();
  }
  return retentionInstance;
}

export default RetentionService;
