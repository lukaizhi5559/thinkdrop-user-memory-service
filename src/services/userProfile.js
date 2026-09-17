import { getDatabaseService } from './database.js';
import logger from '../utils/logger.js';
import { encryptValue, isBridgeAvailable } from './cryptoBridgeClient.js';

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

const SQ = '\'';

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
   *
   * Sensitive-storage contract: when `sensitive` is set — or the key denotes a
   * credential (credential:*, *password*, *token*, *secret*, *api_key*) — the
   * value is encrypted via the crypto bridge (SAFE:) or macOS keychain
   * (KEYTAR:) before storage. Plaintext is never written for sensitive rows;
   * already-encrypted SAFE:/KEYTAR: refs pass through untouched.
   */
  async set(key, valueRef, opts = {}) {
    const { service = null, label = null } = opts;
    let { sensitive = 0 } = opts;
    if (!key || !valueRef) throw new Error('key and valueRef are required');

    const keyLower = String(key).toLowerCase();
    const isCredentialKey = /credential:|password|secret|token|api[_-]?key/.test(keyLower);
    const isRef = typeof valueRef === 'string' &&
      (valueRef.startsWith('SAFE:') || valueRef.startsWith('KEYTAR:'));

    if ((sensitive || isCredentialKey) && !isRef) {
      valueRef = await this._encryptForStorage(keyLower, valueRef);
      sensitive = 1;
    }

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
   * When the crypto bridge is available, SAFE: and KEYTAR: refs are transparently
   * decrypted so callers always receive the plaintext value in valueRef.
   */
  async get(key) {
    if (!key) return null;
    const safeKey = key.replace(/'/g, SQ + SQ).toLowerCase().trim();
    try {
      const rows = await this.db.query(
        `SELECT * FROM user_profile WHERE key = '${safeKey}' LIMIT 1`
      );
      if (rows.length === 0) return null;
      const row = this._row(rows[0]);
      // Transparent decryption — resolve SAFE: / KEYTAR: refs when possible
      if (row.valueRef && (row.valueRef.startsWith('SAFE:') || row.valueRef.startsWith('KEYTAR:'))) {
        try {
          const { resolveValueRef, isBridgeAvailable } = await import('./cryptoBridgeClient.js');
          if (isBridgeAvailable() || row.valueRef.startsWith('KEYTAR:')) {
            const plaintext = await resolveValueRef(row.valueRef);
            if (plaintext !== null && plaintext !== undefined) {
              row.valueRef = plaintext;
            }
          }
        } catch (_decryptErr) {
          // Non-fatal — return raw ref so caller can handle or ignore
          logger.debug(`[UserProfileService] get: decrypt skipped for key="${safeKey}": ${_decryptErr.message}`);
        }
      }
      return row;
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
   * Store a raw credential value using the crypto bridge (Electron safeStorage),
   * falling back to the macOS keychain when the bridge is not available.
   *
   * Security contract:
   *   - When the bridge is available: the raw `value` is encrypted via the
   *     main-process safeStorage bridge.  The ciphertext is stored as
   *     `SAFE:<base64>` in value_ref — never the raw secret.
   *   - When the bridge is unavailable (e.g. during tests or CLI): falls back to
   *     the macOS `security` command for backward compatibility.  The KEYTAR: prefix
   *     is preserved so existing reads continue to work.
   *   - The raw value is NEVER returned in responses or written to disk as plaintext.
   *
   * Existing `KEYTAR:` entries are read transparently by resolveValueRef()
   * in cryptoBridgeClient.js.
   */
  async storeSecret(keytarKey, value, service = null, label = null) {
    if (!keytarKey || value === undefined || value === null) {
      throw new Error('keytarKey and value are required');
    }

    let keyRef;

    if (isBridgeAvailable()) {
      // ── Crypto bridge path (preferred) ─────────────────────────────────────
      try {
        const ciphertext = await encryptValue(String(value));
        keyRef = `SAFE:${ciphertext}`;
        logger.info(`[UserProfileService] storeSecret: "${keytarKey}" → SAFE (crypto bridge)`);
      } catch (bridgeErr) {
        logger.warn(`[UserProfileService] Crypto bridge encrypt failed for "${keytarKey}", falling back to keychain: ${bridgeErr.message}`);
        keyRef = await this._storeInKeychain(keytarKey, value);
      }
    } else {
      // ── Legacy keychain fallback ─────────────────────────────────────────────
      logger.debug(`[UserProfileService] Crypto bridge unavailable — using keychain for "${keytarKey}"`);
      keyRef = await this._storeInKeychain(keytarKey, value);
    }

    // Register the ref in user_profile so credentialIntelligence can find it
    const refKey  = `${keytarKey.toLowerCase()}_ref`;
    const profile = await this.set(refKey, keyRef, { sensitive: 1, service, label });

    logger.info(`[UserProfileService] storeSecret: "${keytarKey}" → profile ref "${refKey}"`);
    return { stored: true, keytarKey, keyRef, profileId: profile.id };
  }

  /**
   * Encrypt a plaintext value for storage — crypto bridge (SAFE:) preferred,
   * macOS keychain (KEYTAR:) as fallback. Returns the ref string to store.
   * @private
   */
  async _encryptForStorage(key, plaintext) {
    if (isBridgeAvailable()) {
      try {
        const ciphertext = await encryptValue(String(plaintext));
        return `SAFE:${ciphertext}`;
      } catch (e) {
        logger.warn(`[UserProfileService] bridge encrypt failed for "${key}", falling back to keychain`, { error: e.message });
      }
    }
    return this._storeInKeychain(key, String(plaintext));
  }

  /**
   * One-time migration: re-encrypt sensitive rows that were written as
   * plaintext before the sensitive-storage contract was enforced.
   * Safe to run on every boot — skips rows already storing SAFE:/KEYTAR: refs.
   */
  async migratePlaintextSensitive() {
    try {
      const rows = await this.db.query(
        `SELECT id, key, value_ref FROM user_profile
         WHERE sensitive = 1
           AND value_ref NOT LIKE 'SAFE:%'
           AND value_ref NOT LIKE 'KEYTAR:%'`
      );
      if (rows.length === 0) return;

      logger.info(`[UserProfileService] Migrating ${rows.length} plaintext sensitive rows to encrypted refs`);
      for (const row of rows) {
        try {
          const ref = await this._encryptForStorage(row.key, row.value_ref);
          const safeRef = ref.replace(/'/g, SQ + SQ);
          await this.db.execute(
            `UPDATE user_profile SET value_ref = '${safeRef}', updated_at = now() WHERE id = '${row.id}'`
          );
        } catch (e) {
          logger.warn(`[UserProfileService] migration failed for key="${row.key}"`, { error: e.message });
        }
      }
    } catch (e) {
      logger.warn('[UserProfileService] plaintext sensitive migration failed', { error: e.message });
    }
  }

  /**
   * Store a value in macOS keychain and return the KEYTAR: ref string.
   * Internal helper — used as the legacy fallback inside storeSecret().
   * @private
   */
  async _storeInKeychain(keytarKey, value) {
    const { spawnSync } = await import('child_process');
    const proc = spawnSync(
      'security',
      ['add-generic-password', '-s', 'thinkdrop', '-a', keytarKey, '-w', String(value), '-U'],
      { encoding: 'utf8' }
    );
    if (proc.status !== 0) {
      // Throw rather than return a dangling ref — a KEYTAR: pointer to a
      // keychain entry that doesn't exist loses the value permanently.
      // Callers (migration/set) leave the plaintext row intact so it can
      // be retried on next boot instead of being corrupted.
      throw new Error(`keychain write for "${keytarKey}" exited ${proc.status}: ${(proc.stderr || proc.error?.message || '').trim()}`);
    }
    return `KEYTAR:${keytarKey}`;
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
