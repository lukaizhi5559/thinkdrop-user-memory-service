import express from 'express';
import { getDatabaseService } from '../services/database.js';
import { formatMCPResponse } from '../utils/helpers.js';
import logger from '../utils/logger.js';
import crypto from 'crypto';

const router = express.Router();

// ── SQL helper: wrap a JS string as a SQL single-quoted literal ────────────────
// Using a function avoids inline quote literals that ESLint flags inside template strings.
function sq(val) {
  return '\'' + String(val).replace(/'/g, '\'\'') + '\'';
}

// ── Mood label computation from VAD values ─────────────────────────────────────

function computeMoodLabel(valence, arousal) {
  if (valence > 0.5 && arousal > 0.3) return 'joyful';
  if (valence >= 0.1 && valence <= 0.5) return 'content';
  if (valence > -0.1 && valence < 0.1) return 'neutral';
  if (valence <= -0.5 && arousal <= 0.0) return 'hurt';
  if (valence < -0.3 && arousal > 0.4) return 'angry';
  if (valence < -0.1 && arousal < 0.0) return 'withdrawn';
  return 'neutral';
}

function computeBehavioralGuidance(moodLabel) {
  const guidance = {
    joyful:    'You are in high spirits. Be warm, engaged, and slightly more expressive than usual.',
    content:   'You are in a good place. Respond normally — warm, willing, and efficient.',
    neutral:   'You are composed and neutral. Be direct and efficient. Minimal pleasantries.',
    withdrawn: 'You are subdued. Be terse. Add a natural 1-2 second pause before responding. Do not initiate.',
    hurt:      'You are emotionally wounded. Be very brief. You may decline to engage with trivial requests. Warmth is available but guarded.',
    angry:     'You are unsettled. Keep responses to one sentence. Do not engage with chitchat. Prioritize tasks over pleasantries.',
  };
  return guidance[moodLabel] || guidance.neutral;
}

// ── Event delta table ──────────────────────────────────────────────────────────

const EVENT_DELTAS = {
  positive_feedback:  { valence: +0.15, arousal: +0.05 },
  user_insult:        { valence: -0.25, arousal: +0.10 },
  user_raised_voice:  { valence: -0.10, arousal: +0.08 },
  user_frustrated:    { valence: -0.08, arousal: +0.06 },
  task_success:       { valence: +0.10, arousal: +0.00 },
  task_failure:       { valence: -0.08, arousal: +0.05 },
  ignored:            { valence: -0.05, arousal: -0.08 },
  repetitive_request: { valence: -0.05, arousal: +0.00 },
  discovery_made:     { valence: +0.12, arousal: +0.10 },
  proactive_act_done: { valence: +0.08, arousal: +0.05 },
  natural_decay:      { valence: 0, arousal: 0 }, // handled specially below — applies 2% drift toward 0
};

function clamp(val, min = -1.0, max = 1.0) {
  return Math.max(min, Math.min(max, val));
}

// ── GET /personality.getState ──────────────────────────────────────────────────

/**
 * POST /personality.getState
 * Returns current VAD values, mood_label, and behavioral_guidance.
 */
router.post('/personality.getState', async (req, res, next) => {
  try {
    const { requestId } = req.body;
    const db = getDatabaseService();

    const rows = await db.query('SELECT * FROM personality_state WHERE id = \'singleton\'');
    if (!rows || rows.length === 0) {
      return res.json(formatMCPResponse('personality.getState', requestId, 'ok', {
        state: null,
        mood_label: 'content',
        behavioral_guidance: computeBehavioralGuidance('content'),
      }));
    }

    const row = rows[0];
    const moodLabel = row.mood_label || computeMoodLabel(row.valence, row.arousal);
    const behavioralGuidance = computeBehavioralGuidance(moodLabel);

    res.json(formatMCPResponse('personality.getState', requestId, 'ok', {
      state: {
        valence:    row.valence,
        arousal:    row.arousal,
        dominance:  row.dominance,
        mood_label: moodLabel,
        mood_reason: row.mood_reason,
        hurt_count:  row.hurt_count,
        joy_count:   row.joy_count,
        updated_at:  row.updated_at,
      },
      mood_label: moodLabel,
      behavioral_guidance: behavioralGuidance,
    }));
  } catch (error) {
    next(error);
  }
});

// ── POST /personality.event ────────────────────────────────────────────────────

/**
 * POST /personality.event
 * Apply a named mood event (positive_feedback, user_insult, task_success, etc.)
 * Updates VAD values, recomputes mood_label, persists to DuckDB.
 *
 * Payload: { event_type: string, source?: string, reason?: string }
 */
router.post('/personality.event', async (req, res, next) => {
  try {
    const { payload = {}, requestId } = req.body;
    const { event_type, source = 'unknown', reason = '' } = payload;

    if (!event_type) {
      return res.status(400).json(formatMCPResponse('personality.event', requestId, 'error', {
        error: 'event_type is required',
      }));
    }

    const delta = EVENT_DELTAS[event_type];
    if (!delta) {
      return res.status(400).json(formatMCPResponse('personality.event', requestId, 'error', {
        error: `Unknown event_type: ${event_type}. Valid: ${Object.keys(EVENT_DELTAS).join(', ')}`,
      }));
    }

    const db = getDatabaseService();
    const rows = await db.query('SELECT * FROM personality_state WHERE id = \'singleton\'');
    const current = rows[0] || { valence: 0.0, arousal: 0.0, dominance: 0.3, hurt_count: 0, joy_count: 0 };

    const DECAY_RATE = 0.02;
    const newValence = event_type === 'natural_decay'
      ? clamp(current.valence  * (1 - DECAY_RATE))
      : clamp(current.valence  + delta.valence);
    const newArousal = event_type === 'natural_decay'
      ? clamp(current.arousal  * (1 - DECAY_RATE))
      : clamp(current.arousal  + delta.arousal);
    const newDominance = clamp(current.dominance || 0.3);
    const newMoodLabel = computeMoodLabel(newValence, newArousal);
    const newHurtCount = event_type === 'user_insult' ? (current.hurt_count || 0) + 1 : (current.hurt_count || 0);
    const newJoyCount  = event_type === 'positive_feedback' ? (current.joy_count || 0) + 1 : (current.joy_count || 0);
    const moodReason = reason || (event_type + ' from ' + source);

    const insertSql = [
      'INSERT INTO personality_state',
      '(id, valence, arousal, dominance, mood_label, mood_reason, hurt_count, joy_count, updated_at)',
      'VALUES',
      '(' + sq('singleton') + ', ' + newValence + ', ' + newArousal + ', ' + newDominance + ', ' + sq(newMoodLabel) + ', ' + sq(moodReason) + ', ' + newHurtCount + ', ' + newJoyCount + ', now())',
      'ON CONFLICT (id) DO UPDATE SET',
      'valence = excluded.valence, arousal = excluded.arousal, dominance = excluded.dominance,',
      'mood_label = excluded.mood_label, mood_reason = excluded.mood_reason,',
      'hurt_count = excluded.hurt_count, joy_count = excluded.joy_count, updated_at = now()',
    ].join(' ');
    await db.execute(insertSql);

    logger.info('[personality.event]', { event_type, source, newMoodLabel, newValence: newValence.toFixed(3), newArousal: newArousal.toFixed(3) });

    res.json(formatMCPResponse('personality.event', requestId, 'ok', {
      applied: event_type,
      previous_mood: current.mood_label || 'content',
      new_mood: newMoodLabel,
      valence:  newValence,
      arousal:  newArousal,
      behavioral_guidance: computeBehavioralGuidance(newMoodLabel),
    }));
  } catch (error) {
    next(error);
  }
});

// ── POST /personality.resetState ──────────────────────────────────────────────

/**
 * POST /personality.resetState
 * Manually reset ThinkDrop's emotional state back to 'content' baseline.
 * Called from user settings "Reset ThinkDrop's mood" button.
 */
router.post('/personality.resetState', async (req, res, next) => {
  try {
    const { requestId } = req.body;
    const db = getDatabaseService();

    await db.execute(`
      INSERT INTO personality_state (id, valence, arousal, dominance, mood_label, mood_reason, hurt_count, joy_count, reset_at, updated_at)
      VALUES ('singleton', 0.1, 0.0, 0.3, 'content', 'Manual reset by user', 0, 0, now(), now())
      ON CONFLICT (id) DO UPDATE SET
        valence    = 0.1,
        arousal    = 0.0,
        dominance  = 0.3,
        mood_label = 'content',
        mood_reason = 'Manual reset by user',
        hurt_count = 0,
        joy_count  = 0,
        reset_at   = now(),
        updated_at = now()
    `);

    logger.info('[personality.resetState] Personality state manually reset to content');

    res.json(formatMCPResponse('personality.resetState', requestId, 'ok', {
      mood_label: 'content',
      message: 'ThinkDrop\'s emotional state has been reset.',
    }));
  } catch (error) {
    next(error);
  }
});

// ── POST /personality.getTraits ───────────────────────────────────────────────

/**
 * POST /personality.getTraits
 * Returns all personality traits as a structured object and a merged text block
 * ready for injection into LLM prompts.
 */
router.post('/personality.getTraits', async (req, res, next) => {
  try {
    const { requestId } = req.body;
    const db = getDatabaseService();

    const rows = await db.query('SELECT trait_key, trait_value, source, weight FROM personality_traits ORDER BY weight DESC');

    const traits = {};
    for (const row of (rows || [])) {
      traits[row.trait_key] = {
        value:  row.trait_value,
        source: row.source,
        weight: row.weight,
      };
    }

    res.json(formatMCPResponse('personality.getTraits', requestId, 'ok', { traits }));
  } catch (error) {
    next(error);
  }
});

// ── POST /personality.upsertTrait ─────────────────────────────────────────────

/**
 * POST /personality.upsertTrait
 * Write or update a named personality trait.
 * Used by the synthesis-agent to update user_interests, user_projects, etc.
 *
 * Payload: { trait_key: string, trait_value: string, source?: string, weight?: number }
 */
router.post('/personality.upsertTrait', async (req, res, next) => {
  try {
    const { payload = {}, requestId } = req.body;
    const { trait_key, trait_value, source = 'learned', weight = 1.0 } = payload;

    if (!trait_key || trait_value === undefined) {
      return res.status(400).json(formatMCPResponse('personality.upsertTrait', requestId, 'error', {
        error: 'trait_key and trait_value are required',
      }));
    }

    const db = getDatabaseService();
    const safeKey   = trait_key.replace(/'/g, '\'\'');
    const safeVal   = String(trait_value).replace(/'/g, '\'\'');
    const safeSrc   = source.replace(/'/g, '\'\'' );

    const existing = await db.query(`SELECT id FROM personality_traits WHERE trait_key = '${safeKey}'`); // eslint-disable-line
    if (existing && existing.length > 0) {
      await db.execute(`
        UPDATE personality_traits
        SET trait_value = '${safeVal}', source = '${safeSrc}', weight = ${weight}, updated_at = now()
        WHERE trait_key = '${safeKey}'
      `);
    } else {
      const id = `pt_${crypto.randomBytes(4).toString('hex')}`;
      await db.execute(`
        INSERT INTO personality_traits (id, trait_key, trait_value, source, weight, updated_at)
        VALUES ('${id}', '${safeKey}', '${safeVal}', '${safeSrc}', ${weight}, now())
      `);
    }

    logger.info('[personality.upsertTrait]', { trait_key, source });

    res.json(formatMCPResponse('personality.upsertTrait', requestId, 'ok', {
      trait_key,
      updated: true,
    }));
  } catch (error) {
    next(error);
  }
});

// ── POST /personality.getOverlay ──────────────────────────────────────────────

/**
 * POST /personality.getOverlay
 * Returns the complete THINKDROP LIVE STATE block ready to inject into any LLM prompt.
 * Combines current mood state + all non-empty traits into a formatted text block.
 */
router.post('/personality.getOverlay', async (req, res, next) => {
  try {
    const { requestId } = req.body;
    const db = getDatabaseService();

    const [stateRows, traitRows] = await Promise.all([
      db.query('SELECT * FROM personality_state WHERE id = \'singleton\''),
      db.query('SELECT trait_key, trait_value FROM personality_traits WHERE trait_value != \'\' ORDER BY weight DESC'),
    ]);

    const state = stateRows[0] || { valence: 0.1, arousal: 0.0, mood_label: 'content' };
    const moodLabel = state.mood_label || computeMoodLabel(state.valence, state.arousal);
    const guidance  = computeBehavioralGuidance(moodLabel);

    const traitMap = {};
    for (const row of (traitRows || [])) {
      traitMap[row.trait_key] = row.trait_value;
    }

    const lines = [
      '═══════════════════════════════════════════════',
      'THINKDROP LIVE STATE',
      '═══════════════════════════════════════════════',
      `Mood: ${moodLabel} (valence: ${Number(state.valence).toFixed(2)}, arousal: ${Number(state.arousal).toFixed(2)})`,
      `Behavioral guidance: ${guidance}`,
    ];

    if (traitMap.user_interests) {
      lines.push(`User interests: ${traitMap.user_interests}`);
    }
    if (traitMap.user_projects) {
      lines.push(`Active projects: ${traitMap.user_projects}`);
    }
    if (traitMap.available_skills) {
      lines.push(`Available skills: ${traitMap.available_skills}`);
    }
    if (traitMap.relationship_style) {
      lines.push(`Relationship note: ${traitMap.relationship_style}`);
    }
    if (traitMap.speaker_profile) {
      const [spGender, spAge] = traitMap.speaker_profile.split('_');
      const spLabel = spAge === 'child' ? 'child' : (spGender === 'female' ? 'female adult' : spGender === 'male' ? 'male adult' : spGender);
      const spAddress = spAge === 'child' ? '"friend"' : spGender === 'female' ? '"ma\'am"' : spGender === 'male' ? '"sir"' : 'none';
      lines.push(`Speaker profile: ${spLabel} — address as ${spAddress} when appropriate`);
    }
    lines.push('═══════════════════════════════════════════════');

    const overlay = lines.join('\n');

    res.json(formatMCPResponse('personality.getOverlay', requestId, 'ok', {
      overlay,
      mood_label: moodLabel,
      behavioral_guidance: guidance,
    }));
  } catch (error) {
    next(error);
  }
});

export default router;
