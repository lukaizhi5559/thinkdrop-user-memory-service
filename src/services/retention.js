import logger from '../utils/logger.js';
import { getDatabaseService } from './database.js';

/**
 * Data Retention Service
 * 
 * Periodically checks if stored data exceeds the configured retention period
 * and purges the oldest records to keep the database within bounds.
 * 
 * Strategy:
 *   - RETENTION_MAX_DAYS: Maximum age of data to keep (default: 1825 = 5 years)
 *   - RETENTION_PURGE_DAYS: How many days of oldest data to purge when limit is hit (default: 365 = 1 year)
 *   - RETENTION_CHECK_INTERVAL_HOURS: How often to check (default: 24 = daily)
 *   - RETENTION_ENABLED: Feature flag (default: true)
 * 
 * Example with defaults:
 *   Data grows for 5 years → purge oldest 1 year → left with 4 years → grows again → repeat
 */

let retentionInstance = null;

class RetentionService {
  constructor() {
    this.maxDays = parseInt(process.env.RETENTION_MAX_DAYS || '1825', 10);       // 5 years
    this.purgeDays = parseInt(process.env.RETENTION_PURGE_DAYS || '365', 10);    // 1 year
    this.checkIntervalHours = parseInt(process.env.RETENTION_CHECK_INTERVAL_HOURS || '24', 10);
    this.enabled = process.env.RETENTION_ENABLED !== 'false'; // enabled by default
    this.timer = null;
    this.db = null;
    this.lastPurge = null;
    this.totalPurged = 0;
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
      purgeDays: this.purgeDays,
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
      lastPurge: this.lastPurge
    });
  }

  /**
   * Check if data exceeds retention period and purge if needed.
   */
  async check() {
    try {
      // Find the oldest record
      const oldestResult = await this.db.query(
        'SELECT MIN(created_at) as oldest, MAX(created_at) as newest, COUNT(*) as total FROM memory'
      );

      const row = oldestResult[0];
      if (!row || !row.oldest || row.total === 0) {
        logger.debug('Retention check: no data to evaluate');
        return;
      }

      const oldest = new Date(row.oldest);
      const newest = new Date(row.newest);
      const totalRecords = Number(row.total);
      const dataAgeDays = Math.floor((newest - oldest) / (1000 * 60 * 60 * 24));

      logger.info('Retention check', {
        totalRecords,
        dataAgeDays,
        maxDays: this.maxDays,
        oldestRecord: oldest.toISOString(),
        newestRecord: newest.toISOString()
      });

      if (dataAgeDays <= this.maxDays) {
        logger.info('Retention check passed — within limits', {
          daysRemaining: this.maxDays - dataAgeDays
        });
        return;
      }

      // Data exceeds retention period — purge oldest records
      await this.purge(oldest);

    } catch (error) {
      logger.error('Retention check failed', { error: error.message });
    }
  }

  /**
   * Purge records older than (oldest + purgeDays).
   * Also removes associated entities.
   */
  async purge(oldestDate) {
    const cutoffDate = new Date(oldestDate);
    cutoffDate.setDate(cutoffDate.getDate() + this.purgeDays);

    logger.info('Retention purge starting', {
      purgingBefore: cutoffDate.toISOString(),
      purgeDays: this.purgeDays
    });

    try {
      // Count records to be purged
      const countResult = await this.db.query(
        `SELECT COUNT(*) as count FROM memory WHERE created_at < '${cutoffDate.toISOString()}'` // eslint-disable-line quotes
      );
      const purgeCount = Number(countResult[0]?.count || 0);

      if (purgeCount === 0) {
        logger.info('Retention purge: no records to purge');
        return;
      }

      // Delete associated entities first (referential integrity)
      await this.db.execute(`
        DELETE FROM memory_entities 
        WHERE memory_id IN (
          SELECT id FROM memory WHERE created_at < '${cutoffDate.toISOString()}'
        )
      `);

      // Delete the memory records
      await this.db.execute(
        `DELETE FROM memory WHERE created_at < '${cutoffDate.toISOString()}'`
      );

      // Compact HNSW index to prune deleted entries
      await this.db.compactHnswIndex();

      // Checkpoint to flush WAL, then rebuild HNSW index for clean state
      await this.db.execute('CHECKPOINT');
      await this.db.rebuildHnswIndex();

      this.lastPurge = new Date().toISOString();
      this.totalPurged += purgeCount;

      logger.info('Retention purge completed', {
        recordsPurged: purgeCount,
        totalPurgedLifetime: this.totalPurged,
        cutoffDate: cutoffDate.toISOString()
      });

    } catch (error) {
      logger.error('Retention purge failed', { error: error.message });
    }
  }

  /**
   * Get retention service status for health checks.
   */
  getStatus() {
    return {
      enabled: this.enabled,
      maxDays: this.maxDays,
      purgeDays: this.purgeDays,
      checkIntervalHours: this.checkIntervalHours,
      lastPurge: this.lastPurge,
      totalPurged: this.totalPurged
    };
  }

  /**
   * Manually trigger a purge of records older than a specific number of days.
   * Useful for admin/API-triggered cleanup.
   */
  async manualPurge(olderThanDays) {
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - olderThanDays);

    logger.info('Manual retention purge requested', {
      olderThanDays,
      cutoffDate: cutoffDate.toISOString()
    });

    await this.purge(cutoffDate);
  }
}

export function getRetentionService() {
  if (!retentionInstance) {
    retentionInstance = new RetentionService();
  }
  return retentionInstance;
}

export default RetentionService;
