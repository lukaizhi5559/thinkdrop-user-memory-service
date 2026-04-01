import express from 'express';
import { getDatabaseService } from '../services/database.js';
import { formatMCPResponse } from '../utils/helpers.js';
import logger from '../utils/logger.js';
import crypto from 'crypto';

const router = express.Router();

// ── SQL helper ─────────────────────────────────────────────────────────────────
function sq(val) {
  return '\'' + String(val).replace(/'/g, '\'\'') + '\'';
}

// ── EMA update ─────────────────────────────────────────────────────────────────
function emaUpdate(stored, incoming, alpha) {
  return stored.map((v, i) => v * (1 - alpha) + (incoming[i] || 0) * alpha);
}

// ── POST /fingerprint.enroll ───────────────────────────────────────────────────

/**
 * Enroll a new speaker or update an existing one.
 * If the speaker_id already exists, blends features via EMA (exponential moving average).
 * Also increments behavioral counters (angry_count, loud_count, whisper_count).
 */
router.post('/fingerprint.enroll', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const {
      speaker_id,
      speaker_name = 'Primary User',
      features,
      gender = 'unknown',
      age_group = 'adult',
      ema_alpha = 0.10,
      voice_pattern = {},
    } = payload || {};

    if (!speaker_id || !Array.isArray(features) || features.length < 3) {
      return res.status(400).json(formatMCPResponse('fingerprint.enroll', requestId, 'error', {
        error: 'speaker_id and features[] (min 3 dims) are required',
      }));
    }

    const db = getDatabaseService();
    const existing = await db.query(`SELECT * FROM voice_fingerprint WHERE speaker_id = ${sq(speaker_id)}`);

    if (existing && existing.length > 0) {
      // Update: blend features via EMA
      const row = existing[0];
      let storedFeatures;
      try { storedFeatures = JSON.parse(row.features_json); } catch (_) { storedFeatures = features; }

      const blended = ema_alpha < 1.0 ? emaUpdate(storedFeatures, features, ema_alpha) : features;

      const newAngryCount   = row.angry_count   + (voice_pattern.angry   ? 1 : 0);
      const newLoudCount    = row.loud_count    + (voice_pattern.loud    ? 1 : 0);
      const newWhisperCount = row.whisper_count + (voice_pattern.whisper ? 1 : 0);
      const newSampleCount  = (row.sample_count || 1) + 1;

      await db.execute(`
        UPDATE voice_fingerprint SET
          features_json = ${sq(JSON.stringify(blended))},
          sample_count  = ${newSampleCount},
          gender        = ${sq(gender !== 'unknown' ? gender : row.gender)},
          age_group     = ${sq(age_group)},
          angry_count   = ${newAngryCount},
          loud_count    = ${newLoudCount},
          whisper_count = ${newWhisperCount},
          updated_at    = now()
        WHERE speaker_id = ${sq(speaker_id)}
      `);

      logger.info('[Fingerprint] Updated speaker', {
        speaker_id,
        speaker_name: row.speaker_name,
        samples: newSampleCount,
        angry: newAngryCount,
        loud: newLoudCount,
        whisper: newWhisperCount,
      });

      return res.json(formatMCPResponse('fingerprint.enroll', requestId, 'ok', {
        updated: true,
        enrolled: false,
        speaker_id,
        speaker_name: row.speaker_name,
        sample_count: newSampleCount,
      }));
    }

    // New speaker — insert
    const id = 'fp_' + crypto.randomUUID().replace(/-/g, '').slice(0, 16);
    await db.execute(`
      INSERT INTO voice_fingerprint
        (id, speaker_id, speaker_name, features_json, sample_count, gender, age_group, angry_count, loud_count, whisper_count)
      VALUES
        (${sq(id)}, ${sq(speaker_id)}, ${sq(speaker_name)}, ${sq(JSON.stringify(features))}, 1, ${sq(gender)}, ${sq(age_group)}, 0, 0, 0)
    `);

    logger.info('[Fingerprint] Enrolled new speaker', { speaker_id, speaker_name, gender, age_group });

    return res.json(formatMCPResponse('fingerprint.enroll', requestId, 'ok', {
      enrolled: true,
      updated: false,
      speaker_id,
      speaker_name,
      sample_count: 1,
    }));
  } catch (error) {
    next(error);
  }
});

// ── POST /fingerprint.list ─────────────────────────────────────────────────────

/**
 * Return all stored fingerprints (used by voice-fingerprint.cjs for matching).
 */
router.post('/fingerprint.list', async (req, res, next) => {
  try {
    const { requestId } = req.body;
    const db = getDatabaseService();
    const rows = await db.query('SELECT * FROM voice_fingerprint ORDER BY sample_count DESC');

    return res.json(formatMCPResponse('fingerprint.list', requestId, 'ok', {
      fingerprints: rows || [],
      count: (rows || []).length,
    }));
  } catch (error) {
    next(error);
  }
});

// ── POST /fingerprint.get ──────────────────────────────────────────────────────

/**
 * Get a specific speaker's fingerprint by speaker_id.
 */
router.post('/fingerprint.get', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const { speaker_id } = payload || {};

    if (!speaker_id) {
      return res.status(400).json(formatMCPResponse('fingerprint.get', requestId, 'error', {
        error: 'speaker_id is required',
      }));
    }

    const db = getDatabaseService();
    const rows = await db.query(`SELECT * FROM voice_fingerprint WHERE speaker_id = ${sq(speaker_id)}`);
    const row = rows && rows[0];

    if (!row) {
      return res.json(formatMCPResponse('fingerprint.get', requestId, 'ok', { found: false }));
    }

    return res.json(formatMCPResponse('fingerprint.get', requestId, 'ok', {
      found: true,
      speaker_id: row.speaker_id,
      speaker_name: row.speaker_name,
      gender: row.gender,
      age_group: row.age_group,
      sample_count: row.sample_count,
      angry_count: row.angry_count,
      loud_count: row.loud_count,
      whisper_count: row.whisper_count,
      created_at: row.created_at,
      updated_at: row.updated_at,
    }));
  } catch (error) {
    next(error);
  }
});

// ── POST /fingerprint.rename ───────────────────────────────────────────────────

/**
 * Rename a speaker (e.g. "Primary User" → "Luka" or "Child — Emma").
 */
router.post('/fingerprint.rename', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const { speaker_id, speaker_name } = payload || {};

    if (!speaker_id || !speaker_name) {
      return res.status(400).json(formatMCPResponse('fingerprint.rename', requestId, 'error', {
        error: 'speaker_id and speaker_name are required',
      }));
    }

    const db = getDatabaseService();
    await db.execute(`
      UPDATE voice_fingerprint SET speaker_name = ${sq(speaker_name)}, updated_at = now()
      WHERE speaker_id = ${sq(speaker_id)}
    `);

    return res.json(formatMCPResponse('fingerprint.rename', requestId, 'ok', { speaker_id, speaker_name }));
  } catch (error) {
    next(error);
  }
});

// ── POST /fingerprint.delete ───────────────────────────────────────────────────

router.post('/fingerprint.delete', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const { speaker_id } = payload || {};

    if (!speaker_id) {
      return res.status(400).json(formatMCPResponse('fingerprint.delete', requestId, 'error', {
        error: 'speaker_id is required',
      }));
    }

    const db = getDatabaseService();
    await db.execute(`DELETE FROM voice_fingerprint WHERE speaker_id = ${sq(speaker_id)}`);

    return res.json(formatMCPResponse('fingerprint.delete', requestId, 'ok', { deleted: true, speaker_id }));
  } catch (error) {
    next(error);
  }
});

export default router;
