/**
 * cryptoBridgeClient.js  — User-Memory Service side
 *
 * Thin client that calls the main-process crypto bridge HTTP server.
 * The bridge config (URL + bearer token) is read from
 *   ~/.thinkdrop/.crypto-bridge.json
 * which is written by the main process on startup.
 *
 * Usage:
 *   import { encryptValue, decryptValue, isBridgeAvailable } from './cryptoBridgeClient.js';
 *
 *   const ciphertext = await encryptValue('my secret password');
 *   // → 'base64-encoded-encrypted-bytes'
 *
 *   const plaintext = await decryptValue(ciphertext);
 *   // → 'my secret password'
 *
 * Graceful degradation: if the bridge is not running (e.g. in tests or CLI),
 * isBridgeAvailable() returns false and encrypt/decrypt throw descriptive errors
 * rather than crashing silently.
 *
 * Value storage convention:
 *   Encrypted values are prefixed with 'SAFE:' in DuckDB, e.g.:
 *     SAFE:dGhpcyBpcyBlbmNyeXB0ZWQ=
 *   Legacy keychain references start with 'KEYTAR:' and are read via the
 *   macOS `security` command for backward compatibility.
 */

import fs from 'fs';
import path from 'path';
import os from 'os';
import https from 'https';
import http from 'http';
import logger from '../utils/logger.js';

const CONFIG_PATH = path.join(os.homedir(), '.thinkdrop', '.crypto-bridge.json');
const SAFE_PREFIX  = 'SAFE:';
const KEYTAR_PREFIX = 'KEYTAR:';

/**
 * Read the bridge config from disk.
 * @returns {{ url: string, token: string } | null}
 */
function _readConfig() {
  try {
    const raw = fs.readFileSync(CONFIG_PATH, 'utf8');
    return JSON.parse(raw);
  } catch (_) {
    return null;
  }
}

/**
 * Check whether the crypto bridge is available.
 * Returns true if the config file exists and is readable.
 */
export function isBridgeAvailable() {
  return _readConfig() !== null;
}

/**
 * Generic POST to the bridge.
 * @param {string} endpoint - '/encrypt' or '/decrypt'
 * @param {object} payload
 * @returns {Promise<object>}
 */
function _bridgePost(endpoint, payload) {
  return new Promise((resolve, reject) => {
    const cfg = _readConfig();
    if (!cfg) {
      return reject(new Error('[CryptoBridgeClient] Bridge config not found — is the main process running?'));
    }

    const bodyStr = JSON.stringify(payload);
    const url = new URL(cfg.url + endpoint);
    const options = {
      hostname: url.hostname,
      port:     Number(url.port),
      path:     endpoint,
      method:   'POST',
      headers:  {
        'Content-Type':   'application/json',
        'Content-Length': Buffer.byteLength(bodyStr),
        'Authorization':  `Bearer ${cfg.token}`,
      },
    };

    // Bridge always uses plain HTTP on loopback
    const transport = url.protocol === 'https:' ? https : http;
    const req = transport.request(options, (res) => {
      let body = '';
      res.on('data', (c) => { body += c; });
      res.on('end', () => {
        try {
          const json = JSON.parse(body);
          if (res.statusCode !== 200) {
            return reject(new Error(`[CryptoBridgeClient] Bridge returned ${res.statusCode}: ${json.error || body}`));
          }
          resolve(json);
        } catch (e) {
          reject(new Error(`[CryptoBridgeClient] Failed to parse bridge response: ${e.message}`));
        }
      });
    });

    req.on('error', (e) => reject(new Error(`[CryptoBridgeClient] Request failed: ${e.message}`)));
    req.setTimeout(5000, () => {
      req.destroy();
      reject(new Error('[CryptoBridgeClient] Request timed out'));
    });
    req.end(bodyStr);
  });
}

/**
 * Encrypt a plaintext string via the main-process bridge.
 * Returns a base64-encoded ciphertext (WITHOUT the SAFE: prefix).
 *
 * @param {string} plaintext
 * @returns {Promise<string>}  base64 ciphertext
 */
export async function encryptValue(plaintext) {
  const { ciphertext } = await _bridgePost('/encrypt', { plaintext });
  return ciphertext;
}

/**
 * Decrypt a base64 ciphertext string via the main-process bridge.
 *
 * @param {string} ciphertext - base64 ciphertext (with or without SAFE: prefix)
 * @returns {Promise<string>}  plaintext
 */
export async function decryptValue(ciphertext) {
  const ct = ciphertext.startsWith(SAFE_PREFIX)
    ? ciphertext.slice(SAFE_PREFIX.length)
    : ciphertext;
  const { plaintext } = await _bridgePost('/decrypt', { ciphertext: ct });
  return plaintext;
}

/**
 * Resolve a stored value_ref to its plaintext.
 * Handles three cases:
 *   1. 'SAFE:<base64>'  → decrypt via bridge
 *   2. 'KEYTAR:<key>'   → read from macOS keychain (backward compat)
 *   3. anything else    → return as-is (plain value, no encryption)
 *
 * @param {string} valueRef
 * @returns {Promise<string | null>}
 */
export async function resolveValueRef(valueRef) {
  if (!valueRef) return null;

  if (valueRef.startsWith(SAFE_PREFIX)) {
    try {
      return await decryptValue(valueRef);
    } catch (err) {
      logger.warn(`[CryptoBridgeClient] SAFE decrypt failed: ${err.message}`);
      return null;
    }
  }

  if (valueRef.startsWith(KEYTAR_PREFIX)) {
    // Legacy: read from macOS keychain via shell
    const keytarKey = valueRef.slice(KEYTAR_PREFIX.length);
    try {
      const { spawnSync } = await import('child_process');
      const proc = spawnSync(
        'security',
        ['find-generic-password', '-s', 'thinkdrop', '-a', keytarKey, '-w'],
        { encoding: 'utf8' }
      );
      if (proc.status === 0 && proc.stdout) {
        return proc.stdout.trim();
      }
    } catch (err) {
      logger.warn(`[CryptoBridgeClient] KEYTAR fallback read failed for "${keytarKey}": ${err.message}`);
    }
    return null;
  }

  // Plain value — no encryption
  return valueRef;
}
