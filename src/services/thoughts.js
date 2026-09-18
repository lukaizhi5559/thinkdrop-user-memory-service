import { getDatabaseService } from './database.js';
import { getEmbeddingService } from './embeddings.js';
import logger from '../utils/logger.js';

// ---------------------------------------------------------------------------
// ThoughtService — persistent scored "thoughts" for the Thought/Trigger engine.
//
// A thought is a candidate observation accumulated across input modalities
// (prompt | screen_capture | memory | queue | silence). Scoring follows an
// ACT-R-style trace model: each reinforcement is a decaying trace
// {ts, w, input, srcIds} and score = SUM(w_i * (1 + ageDays_i)^-d). Semantic
// dedup is cross-modal by construction: every candidate embed is compared
// against every open thought embed; input type only changes trace weight.
//
// Embedding is stored as JSON TEXT — brute-force JS cosine at this scale.
// (Deliberately NOT FLOAT[384]: VSS LOAD hooks fire on FLOAT[] inserts — see
// database.js initVectorSearch.)
// ---------------------------------------------------------------------------

// eslint-disable-next-line quotes
const SQ = "'";

// Match/purge statuses: reinforcement candidates. 'watching' rows are live but
// deliberately excluded — a running watch must not absorb stray observations.
const MATCH_STATUSES = ['thought', 'triggered', 'awaiting_approval'];
// Display default: everything the Brain tab should show as "live".
const OPEN_STATUSES = [...MATCH_STATUSES, 'watching'];
const MATCH_SIM = parseFloat(process.env.THOUGHT_MATCH_SIM || '0.72');
const MATCH_SIM_STRONG = parseFloat(process.env.THOUGHT_MATCH_SIM_STRONG || '0.80');
// Lower floor that only applies when a shared non-generic entity corroborates
// the match — cross-modal text of the same topic can embed ~0.65–0.70 (seen
// live: 0.679 for prompt↔screen on the same subject), so binding the entity
// guard to the 0.72 floor made it unreachable.
const MATCH_SIM_ENTITY = parseFloat(process.env.THOUGHT_MATCH_SIM_ENTITY || '0.60');
const DECAY_D = parseFloat(process.env.THOUGHT_DECAY_D || '0.35');
const TTL_HOURS = parseFloat(process.env.THOUGHT_TTL_HOURS || '72');
const EXPIRED_KEEP_DAYS = 30;
const MAX_ENTITY_NAMES = 24;

// Generic entities that must NOT carry a cross-input match on their own —
// "Chrome" matching "Chrome" is coincidence, not topical reinforcement.
const GENERIC_ENTITIES = new Set([
  'browser', 'chrome', 'app', 'application', 'screen', 'window', 'page',
  'website', 'site', 'computer', 'desktop', 'internet', 'online', 'file',
  'files', 'text', 'code', 'unknown', 'other',
]);

function generateId() {
  return `th_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

function esc(s) {
  return String(s == null ? '' : s).replace(/'/g, SQ + SQ);
}

function safeJsonParse(s, fallback) {
  try { return s ? JSON.parse(s) : fallback; } catch (_) { return fallback; }
}

function cosineSim(a, b) {
  if (!a || !b || a.length !== b.length || a.length === 0) return 0;
  let dot = 0, na = 0, nb = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    na += a[i] * a[i];
    nb += b[i] * b[i];
  }
  if (na === 0 || nb === 0) return 0;
  return dot / (Math.sqrt(na) * Math.sqrt(nb));
}

function normalizeEntity(e) {
  return String(e || '').toLowerCase().trim();
}

function sharedEntityCount(a, b) {
  const setA = new Set((a || []).map(normalizeEntity).filter(e => e && !GENERIC_ENTITIES.has(e)));
  let n = 0;
  for (const e of (b || []).map(normalizeEntity)) {
    if (e && !GENERIC_ENTITIES.has(e) && setA.has(e)) n++;
  }
  return n;
}

function mergeUnique(a, b, cap) {
  const seen = new Set();
  const out = [];
  for (const v of [...(a || []), ...(b || [])]) {
    const key = typeof v === 'string' ? v.toLowerCase().trim() : JSON.stringify(v);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(v);
    if (cap && out.length >= cap) break;
  }
  return out;
}

class ThoughtService {
  constructor() {
    this.db = getDatabaseService();
    this.embeddings = getEmbeddingService();
  }

  /**
   * ACT-R trace score: score = SUM(w_i * (1 + ageDays_i)^-d).
   * Each reinforcement trace decays by a power law — old traces fade to ~0,
   * recent strong evidence dominates. No separate decay pass needed.
   */
  computeScore(reinforcements, nowMs) {
    const now = nowMs || Date.now();
    let score = 0;
    for (const tr of reinforcements || []) {
      const ageDays = Math.max(0, (now - Number(tr.ts || now)) / 86400000);
      score += Number(tr.w || 0) * Math.pow(1 + ageDays, -DECAY_D);
    }
    return score;
  }

  buildEmbedText({ summary, entityNames, actionNames }) {
    return [
      (summary || '').trim(),
      (entityNames || []).join(' '),
      (actionNames || []).join(' '),
    ].join(' | ').trim();
  }

  _rowToThought(r) {
    const reinforcements = safeJsonParse(r.reinforcements, []);
    return {
      id: r.id,
      userId: r.user_id,
      input: r.input,
      status: r.status,
      score: this.computeScore(reinforcements), // live recompute — cached column may be stale
      cachedScore: Number(r.score || 0),
      summary: r.summary,
      sources: safeJsonParse(r.sources, []),
      entityNames: safeJsonParse(r.entity_names, []),
      actionNames: safeJsonParse(r.action_names, []),
      reinforcements,
      silenceEpisode: Number(r.silence_episode || 0),
      action: safeJsonParse(r.action_json, null),
      outcomeText: r.outcome_text || null,
      createdAt: r.created_at,
      updatedAt: r.updated_at,
      triggeredAt: r.triggered_at,
      completedAt: r.completed_at,
      snoozedUntil: r.snoozed_until || null,
    };
  }

  async _embed(text) {
    try {
      if (!this.embeddings.isLoaded) return null;
      return await this.embeddings.generateEmbedding(text);
    } catch (e) {
      logger.warn('[ThoughtService] embed failed', { error: e.message });
      return null;
    }
  }

  /**
   * Upsert a candidate thought with cross-modal semantic dedup.
   *
   * payload: {
   *   input: 'prompt'|'screen_capture'|'memory'|'queue'|'silence'|'judgment',
   *   summary: string, entityNames?: string[], actionNames?: string[],
   *   sourceIds?: string[], userId?: string,
   *   traceWeight: number,            // w for the appended/initial trace
   *   silenceEpisode?: number,
   *   forceNew?: boolean              // silence escalation chains bypass match
   * }
   *
   * Returns { matched, thought, similarity }.
   */
  async upsert(payload) {
    const {
      input, summary, entityNames = [], actionNames = [], sourceIds = [],
      userId = 'local_user', traceWeight = 0.3, silenceEpisode = 0,
      forceNew = false,
    } = payload || {};

    if (!input || !summary) throw new Error('input and summary are required');

    const embedText = this.buildEmbedText({ summary, entityNames, actionNames });
    const candEmbed = await this._embed(embedText);
    const now = Date.now();

    // Load open thoughts for this user — small table, brute-force match.
    const safeUser = esc(userId);
    const openRows = await this.db.query(`
      SELECT * FROM thoughts
      WHERE user_id = '${safeUser}'
        AND status IN (${MATCH_STATUSES.map(s => `'${s}'`).join(',')})
      ORDER BY updated_at DESC
      LIMIT 500
    `);

    // Find best semantic match
    let best = null, bestSim = 0;
    if (!forceNew && candEmbed) {
      for (const r of openRows) {
        const emb = safeJsonParse(r.embedding, null);
        if (!emb) continue;
        const sim = cosineSim(candEmbed, emb);
        if (sim > bestSim) { bestSim = sim; best = r; }
      }
    }

    // Match decision — same-input needs the base floor; cross-input needs the
    // base floor, OR the entity floor + ≥1 shared non-generic entity, OR the
    // strong floor alone. (Entity-rescue tier keeps genuine cross-modal pairs
    // that embed 0.60–0.72 from splitting into duplicate thoughts.)
    let matched = false;
    if (best) {
      if (best.input === input) {
        matched = bestSim >= MATCH_SIM;
      } else {
        const ents = safeJsonParse(best.entity_names, []);
        matched =
          bestSim >= MATCH_SIM ||
          (bestSim >= MATCH_SIM_ENTITY && sharedEntityCount(ents, entityNames) >= 1) ||
          bestSim >= MATCH_SIM_STRONG;
      }
    }

    const trace = { ts: now, w: Number(traceWeight) || 0, input, srcIds: sourceIds || [] };

    if (matched && best) {
      // ── Reinforce existing row — NO duplicate insert ──────────────────────
      const existing = safeJsonParse(best.reinforcements, []);
      existing.push(trace);
      const mergedEntities = mergeUnique(safeJsonParse(best.entity_names, []), entityNames, MAX_ENTITY_NAMES);
      const mergedActions  = mergeUnique(safeJsonParse(best.action_names, []), actionNames, MAX_ENTITY_NAMES);
      const mergedSources  = mergeUnique(safeJsonParse(best.sources, []), sourceIds, 100);
      // Keep the richer summary (longer = usually more informative)
      const mergedSummary = (summary || '').length > (best.summary || '').length ? summary : best.summary;
      // Re-embed from merged text — the thought's idea re-crystallizes as
      // cross-modal evidence accumulates.
      const mergedEmbed = await this._embed(
        this.buildEmbedText({ summary: mergedSummary, entityNames: mergedEntities, actionNames: mergedActions })
      );
      const newScore = this.computeScore(existing, now);
      const safeId = esc(best.id);
      await this.db.execute(`
        UPDATE thoughts SET
          reinforcements = '${esc(JSON.stringify(existing))}',
          sources = '${esc(JSON.stringify(mergedSources))}',
          entity_names = '${esc(JSON.stringify(mergedEntities))}',
          action_names = '${esc(JSON.stringify(mergedActions))}',
          summary = '${esc(mergedSummary)}',
          ${mergedEmbed ? `embedding = '${esc(JSON.stringify(mergedEmbed))}',` : ''}
          score = ${newScore},
          silence_episode = ${Math.max(Number(best.silence_episode || 0), Number(silenceEpisode) || 0)},
          updated_at = now()
        WHERE id = '${safeId}'
      `);
      const updated = (await this.db.query(`SELECT * FROM thoughts WHERE id = '${safeId}'`))[0];
      logger.info(`[ThoughtService] Reinforced thought ${best.id} sim=${bestSim.toFixed(3)} input=${input}→${best.input} score=${newScore.toFixed(2)}`);
      return { matched: true, similarity: bestSim, thought: this._rowToThought(updated) };
    }

    // ── Insert new thought ──────────────────────────────────────────────────
    const id = generateId();
    const reinforcements = [trace];
    const score = this.computeScore(reinforcements, now);
    await this.db.execute(`
      INSERT INTO thoughts (
        id, user_id, input, status, score, summary, sources, entity_names,
        action_names, embedding, reinforcements, silence_episode,
        created_at, updated_at
      ) VALUES (
        '${id}', '${safeUser}', '${esc(input)}', 'thought', ${score},
        '${esc(summary)}', '${esc(JSON.stringify(sourceIds || []))}',
        '${esc(JSON.stringify(entityNames || []))}', '${esc(JSON.stringify(actionNames || []))}',
        ${candEmbed ? `'${esc(JSON.stringify(candEmbed))}'` : 'NULL'},
        '${esc(JSON.stringify(reinforcements))}', ${Number(silenceEpisode) || 0},
        now(), now()
      )
    `);
    logger.info(`[ThoughtService] New thought ${id} input=${input} score=${score.toFixed(2)}: ${(summary || '').slice(0, 80)}`);
    const row = (await this.db.query(`SELECT * FROM thoughts WHERE id = '${id}'`))[0];
    return { matched: false, similarity: bestSim, thought: this._rowToThought(row) };
  }

  /**
   * Append a reinforcement trace to a specific thought (T2 judgment boosts,
   * suppression penalties with negative w, approval bonuses).
   */
  async addTrace(id, { w, input = 'judgment', srcIds = [] }) {
    const safeId = esc(id);
    const rows = await this.db.query(`SELECT * FROM thoughts WHERE id = '${safeId}'`);
    if (!rows.length) return { updated: false, error: 'not_found' };
    const r = rows[0];
    const existing = safeJsonParse(r.reinforcements, []);
    existing.push({ ts: Date.now(), w: Number(w) || 0, input, srcIds: srcIds || [] });
    const newScore = this.computeScore(existing);
    await this.db.execute(`
      UPDATE thoughts SET
        reinforcements = '${esc(JSON.stringify(existing))}',
        score = ${newScore},
        updated_at = now()
      WHERE id = '${safeId}'
    `);
    return { updated: true, id, score: newScore };
  }

  /**
   * List thoughts. Default: open thoughts sorted by live score DESC.
   * options: { userId, statuses?, all?, limit?, includeExpired? }
   */
  async list(options = {}) {
    const userId = options.userId || 'local_user';
    const limit = Math.min(Number(options.limit) || 200, 500);
    const safeUser = esc(userId);
    let where = `user_id = '${safeUser}'`;
    if (options.all) {
      if (!options.includeExpired) where += ' AND status != \'expired\'';
    } else if (Array.isArray(options.statuses) && options.statuses.length) {
      where += ` AND status IN (${options.statuses.map(s => `'${esc(s)}'`).join(',')})`;
    } else {
      where += ` AND status IN (${OPEN_STATUSES.map(s => `'${s}'`).join(',')})`;
    }
    const rows = await this.db.query(`
      SELECT * FROM thoughts WHERE ${where}
      ORDER BY updated_at DESC LIMIT ${limit}
    `);
    const thoughts = rows.map(r => this._rowToThought(r));
    thoughts.sort((a, b) => b.score - a.score);
    return { thoughts, total: thoughts.length };
  }

  async get(id) {
    const rows = await this.db.query(`SELECT * FROM thoughts WHERE id = '${esc(id)}'`);
    return rows.length ? this._rowToThought(rows[0]) : null;
  }

  /**
   * Partial update. Allowed fields: status, summary, action, outcomeText,
   * silenceEpisode. Sets triggered_at/completed_at automatically on those
   * status transitions. Score is trace-derived — use addTrace to move it.
   */
  async update(id, updates = {}) {
    const safeId = esc(id);
    const sets = [];
    if (updates.status) {
      sets.push(`status = '${esc(updates.status)}'`);
      if (updates.status === 'triggered') sets.push('triggered_at = now()');
      if (updates.status === 'completed') sets.push('completed_at = now()');
    }
    if (updates.summary !== undefined) sets.push(`summary = '${esc(updates.summary)}'`);
    if (updates.action !== undefined) sets.push(`action_json = '${esc(JSON.stringify(updates.action))}'`);
    if (updates.outcomeText !== undefined) sets.push(`outcome_text = '${esc(updates.outcomeText)}'`);
    if (updates.silenceEpisode !== undefined) sets.push(`silence_episode = ${Number(updates.silenceEpisode) || 0}`);
    if (updates.snoozedUntil !== undefined) {
      sets.push(updates.snoozedUntil ? `snoozed_until = '${esc(new Date(updates.snoozedUntil).toISOString())}'` : 'snoozed_until = NULL');
    }
    if (updates.score !== undefined) sets.push(`score = ${Number(updates.score) || 0}`);
    if (!sets.length) throw new Error('No valid fields to update');
    sets.push('updated_at = now()');
    await this.db.execute(`UPDATE thoughts SET ${sets.join(', ')} WHERE id = '${safeId}'`);
    return { updated: true, id };
  }

  /**
   * Expire stale thoughts (score below floor AND no trace newer than TTL)
   * and hard-delete expired rows older than 30 days.
   * Returns { expired, deleted }.
   */
  async purge(options = {}) {
    const userId = options.userId || 'local_user';
    const ttlHours = Number(options.ttlHours) || TTL_HOURS;
    const floorScore = Number(options.floorScore) || 0.15;
    const safeUser = esc(userId);

    const openRows = await this.db.query(`
      SELECT * FROM thoughts WHERE user_id = '${safeUser}'
        AND status IN ('thought','triggered','awaiting_approval')
    `);
    const now = Date.now();
    // Negative-score grace: judged-dead thoughts fade after NEG_GRACE_MS without
    // a fresh trace (the 72h rule alone lets judgment spam keep zombies alive).
    const negGraceMs = Number(options.negGraceMs) || 3600000;
    let expired = 0;
    for (const r of openRows) {
      const t = this._rowToThought(r);
      const lastTrace = (t.reinforcements || []).reduce((m, tr) => Math.max(m, Number(tr.ts || 0)), 0);
      const staleMs = now - lastTrace;
      if ((t.score <= 0 && staleMs > negGraceMs) ||
          (t.score < floorScore && staleMs > ttlHours * 3600 * 1000)) {
        await this.db.execute(`UPDATE thoughts SET status = 'expired', updated_at = now() WHERE id = '${esc(t.id)}'`);
        expired++;
      }
    }

    const del = await this.db.query(`
      DELETE FROM thoughts WHERE status = 'expired'
        AND updated_at < CURRENT_TIMESTAMP - INTERVAL '${EXPIRED_KEEP_DAYS}' DAY
      RETURNING id
    `).catch(() => []);
    const deleted = Array.isArray(del) ? del.length : 0;
    if (expired || deleted) {
      logger.info(`[ThoughtService] Purge: ${expired} expired, ${deleted} deleted`);
    }
    return { expired, deleted };
  }
}

let _instance = null;
export function getThoughtService() {
  if (!_instance) _instance = new ThoughtService();
  return _instance;
}

export default ThoughtService;
