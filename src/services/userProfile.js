import { getDatabaseService } from './database.js';
import logger from '../utils/logger.js';

// ---------------------------------------------------------------------------
// UserProfileService — per-user identity facts and per-service credential
// pointers stored in DuckDB.
//
// Design: sensitive entries store a KEYTAR:<key> in value_ref; the actual
// secret stays in the OS keychain.  Callers receive the ref and resolve it
// themselves at execution time so raw secrets never travel over the MCP wire.
//
// Schema: user_profile(id, key, value_ref, sensitive, service, label, ...)
// ---------------------------------------------------------------------------

const SQ = "'";

function generateId() {
  return `up_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

class UserProfileService {
  constructor() {
    this.db = getDatabaseService();
  }

  /**
   * Insert or update a profile entry identified by key.
   * key is normalised to lowercase so reads are case-insensitive.
   */
  async set(key, valueRef, opts = {}) {
    const { sensitive = 0, service = null, label = null } = opts;
    if (!key || !valueRef) throw new Error('key and valueRef are required');

    const safeKey   = key.replace(/'/g, SQ + SQ).toLowerCase().trim();
    const safeValue = String(valueRef).replace(/'/g, SQ + SQ);
    const safeService = service
      ? `'${String(service).replace(/'/g, SQ + SQ).toLowerCase()}'`
      : 'NULL';
    const safeLabel = label
      ? `'${String(label).replace(/'/g, SQ + SQ)}'`
      : 'NULL';

    try {
      const existing = await this.db.query(
        `SELECT id FROM user_profile WHERE key = '${safeKey}'`
      );

      if (existing.length > 0) {
        await this.db.execute(`
          UPDATE user_profile
          SET value_ref = '${safeValue}',
              sensitive = ${sensitive ? 1 : 0},
              service   = ${safeService},
              label     = ${safeLabel},
              updated_at = now()
          WHERE key = '${safeKey}'
        `);
        logger.debug(`[UserProfileService] updated key="${safeKey}"`);
        return { id: existing[0].id, key: safeKey, created: false };
      }

      const id = generateId();
      await this.db.execute(`
        INSERT INTO user_profile (id, key, value_ref, sensitive, service, label, created_at, updated_at)
        VALUES (
          '${id}',
          '${safeKey}',
          '${safeValue}',
          ${sensitive ? 1 : 0},
          ${safeService},
          ${safeLabel},
          now(),
          now()
        )
      `);
      logger.debug(`[UserProfileService] inserted key="${safeKey}"`);
      return { id, key: safeKey, created: true };
    } catch (error) {
      logger.error('[UserProfileService] set failed:', error.message);
      throw error;
    }
  }

  /**
   * Retrieve one profile entry by key.
   * Returns null when not found.
   */
  async get(key) {
    if (!key) return null;
    const safeKey = key.replace(/'/g, SQ + SQ).toLowerCase().trim();
    try {
      const rows = await this.db.query(
        `SELECT * FROM user_profile WHERE key = '${safeKey}' LIMIT 1`
      );
      if (rows.length === 0) return null;
      return this._row(rows[0]);
    } catch (error) {
      logger.error('[UserProfileService] get failed:', error.message);
      return null;
    }
  }

  /**
   * List all entries, optionally filtered by service name.
   */
  async list(opts = {}) {
    const { service } = opts;
    const serviceFilter = service
      ? `WHERE service = '${String(service).replace(/'/g, SQ + SQ).toLowerCase()}'`
      : '';
    try {
      const rows = await this.db.query(
        `SELECT * FROM user_profile ${serviceFilter} ORDER BY service, key`
      );
      return rows.map(r => this._row(r));
    } catch (error) {
      logger.error('[UserProfileService] list failed:', error.message);
      return [];
    }
  }

  /**
   * Delete a profile entry by key.
   */
  async delete(key) {
    if (!key) throw new Error('key is required');
    const safeKey = key.replace(/'/g, SQ + SQ).toLowerCase().trim();
    try {
      await this.db.execute(
        `DELETE FROM user_profile WHERE key = '${safeKey}'`
      );
      logger.debug(`[UserProfileService] deleted key="${safeKey}"`);
      return { deleted: true, key: safeKey };
    } catch (error) {
      logger.error('[UserProfileService] delete failed:', error.message);
      throw error;
    }
  }

  /**
   * Store a raw credential value in the OS keychain and register its KEYTAR
   * ref pointer in the user_profile table.
   *
   * Security contract:
   *   - The raw `value` is passed to `security add-generic-password` via
   *     spawnSync with an explicit args array (NO shell interpolation — safe
   *     against injection regardless of what the value contains).
   *   - The raw value is NEVER stored in DuckDB, never returned in responses.
   *   - Only the `KEYTAR:<keytarKey>` pointer is persisted.
   */
  async storeSecret(keytarKey, value, service = null, label = null) {
    if (!keytarKey || value === undefined || value === null) {
      throw new Error('keytarKey and value are required');
    }
    const { spawnSync } = await import('child_process');

    // Write to macOS keychain (-U = update if already exists)
    const proc = spawnSync(
      'security',
      ['add-generic-password', '-s', 'thinkdrop', '-a', keytarKey, '-w', String(value), '-U'],
      { encoding: 'utf8' }
    );
    if (proc.status !== 0 && proc.status !== null) {
      // Log warning but don't throw — some machines may not have `security`
      logger.warn(`[UserProfileService] keychain write for "${keytarKey}" exited ${proc.status}: ${(proc.stderr || '').trim()}`);
    }

    // Register the KEYTAR ref in user_profile so credentialIntelligence can find it
    const keyRef  = `KEYTAR:${keytarKey}`;
    const refKey  = `${keytarKey.toLowerCase()}_ref`;
    const profile = await this.set(refKey, keyRef, { sensitive: 1, service, label });

    logger.info(`[UserProfileService] storeSecret: "${keytarKey}" → keychain + profile ref "${refKey}"`);
    return { stored: true, keytarKey, keyRef, profileId: profile.id };
  }

  _row(r) {
    return {
      id:        r.id,
      key:       r.key,
      valueRef:  r.value_ref,
      sensitive: r.sensitive === 1 || r.sensitive === true,
      service:   r.service   ?? null,
      label:     r.label     ?? null,
    };
  }
}

let _instance = null;

export function getUserProfileService() {
  if (!_instance) _instance = new UserProfileService();
  return _instance;
}
