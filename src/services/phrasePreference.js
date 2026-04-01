import { getDatabaseService } from './database.js';
import { getEmbeddingService } from './embeddings.js';
import logger from '../utils/logger.js';

// ---------------------------------------------------------------------------
// PhrasePreferenceService — user-taught phrase→delivery mappings stored in DuckDB.
//
// When the user says something ambiguous like "shoot me a text" or "drop me a
// message when done", ThinkDrop asks once which channel they prefer, stores the
// answer here, and never asks again for semantically similar phrases.
//
// The embedding enables fuzzy matching — "ping me", "text me", "shoot me a text",
// "drop me a line" can all map to the same stored preference without exact wording.
//
// Schema: phrase_preferences(id, example_phrase, delivery, service, metadata,
//                             embedding, source, hit_count, created_at, updated_at)
//
// delivery: 'sms' | 'email' | 'slack' | 'discord' | 'push' | 'webhook'
// service:  'twilio' | 'sendgrid' | 'mailgun' | 'slack' | 'pushover' | etc.
// source:   'user_answer' (first time) | 'user_correction' (explicit override)
// ---------------------------------------------------------------------------

const SIMILARITY_THRESHOLD = 0.80; // slightly lower than intent_overrides — phrase variants are looser

const SQ = '\'';

function generateId() {
  return `pp_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

class PhrasePreferenceService {
  constructor() {
    this.db = getDatabaseService();
    this.embeddings = getEmbeddingService();
  }

  /**
   * Semantic search: find the closest stored phrase preference for a given message.
   * Returns { delivery, service, metadata, examplePhrase, similarity } or null.
   */
  async search(phrase, options = {}) {
    const { threshold = SIMILARITY_THRESHOLD } = options;
    if (!phrase) return null;

    try {
      const embedding = await this.embeddings.generateEmbedding(phrase);
      if (!embedding) return null;

      const embeddingStr = `[${embedding.join(',')}]`;

      const rows = await this.db.query(`
        SELECT id, example_phrase, delivery, service, metadata, hit_count,
               array_cosine_similarity(embedding::FLOAT[384], ${embeddingStr}::FLOAT[384]) AS similarity
        FROM phrase_preferences
        WHERE embedding IS NOT NULL
        ORDER BY similarity DESC
        LIMIT 1
      `);

      if (!rows.length) return null;

      const top = rows[0];
      const sim = Number(top.similarity);

      if (sim < threshold) {
        logger.debug(`[PhrasePreferenceService] search: top match "${top.example_phrase.slice(0, 60)}" sim=${sim.toFixed(3)} below threshold ${threshold}`);
        return null;
      }

      // Bump hit_count (fire-and-forget)
      this.db.execute(
        `UPDATE phrase_preferences SET hit_count = hit_count + 1, updated_at = now() WHERE id = '${top.id}'`
      ).catch(() => {});

      let metadata = null;
      try { metadata = top.metadata ? JSON.parse(top.metadata) : null; } catch (_e) { metadata = null; }

      logger.info(`[PhrasePreferenceService] Match: "${top.example_phrase.slice(0, 60)}" → ${top.delivery}/${top.service} (sim=${sim.toFixed(3)})`);
      return {
        delivery: top.delivery,
        service: top.service || null,
        metadata,
        examplePhrase: top.example_phrase,
        similarity: sim,
        hitCount: Number(top.hit_count),
      };
    } catch (error) {
      logger.warn(`[PhrasePreferenceService] search failed (non-fatal): ${error.message}`);
      return null;
    }
  }

  /**
   * Store or overwrite a phrase preference.
   * source='user_correction' overwrites any existing entry for the same phrase cluster.
   */
  async upsert(examplePhrase, delivery, options = {}) {
    const { service = null, metadata = null, source = 'user_answer' } = options;

    if (!examplePhrase || !delivery) throw new Error('examplePhrase and delivery are required');
    // delivery is intentionally open-ended — sms, email, push, iot, car_control,
    // desktop, calendar, payment, webhook, etc. No enum validation here.

    try {
      const embedding = await this.embeddings.generateEmbedding(examplePhrase);
      if (!embedding) throw new Error('Embedding generation failed');

      // Check for existing entry at high similarity — if correction, delete old one first
      const existing = await this.search(examplePhrase, { threshold: 0.88 });
      if (existing) {
        if (source === 'user_correction' || existing.delivery !== delivery || existing.service !== service) {
          // Remove old entry so the new preference fully replaces it
          await this.db.execute(
            `DELETE FROM phrase_preferences WHERE example_phrase = '${examplePhrase.replace(/'/g, SQ + SQ)}'`
          ).catch(() => {});
          logger.info(`[PhrasePreferenceService] Overwriting existing preference for "${examplePhrase.slice(0, 60)}"`);
        } else {
          logger.debug(`[PhrasePreferenceService] Skipping duplicate for "${examplePhrase.slice(0, 60)}"`);
          return { created: false, reason: 'near_duplicate' };
        }
      }

      const id = generateId();
      const embeddingStr = `[${embedding.join(',')}]`;
      const safePhrase    = examplePhrase.replace(/'/g, SQ + SQ);
      const safeDelivery  = delivery.replace(/'/g, SQ + SQ);
      const safeService   = service ? `'${service.replace(/'/g, SQ + SQ)}'` : 'NULL';
      const safeMeta      = metadata ? `'${JSON.stringify(metadata).replace(/'/g, SQ + SQ)}'` : 'NULL';
      const safeSource    = (source || 'user_answer').replace(/'/g, SQ + SQ);

      await this.db.execute(`
        INSERT INTO phrase_preferences (id, example_phrase, delivery, service, metadata, embedding, source, hit_count, created_at, updated_at)
        VALUES (
          '${id}',
          '${safePhrase}',
          '${safeDelivery}',
          ${safeService},
          ${safeMeta},
          ${embeddingStr}::FLOAT[384],
          '${safeSource}',
          0,
          now(),
          now()
        )
      `);

      logger.info(`[PhrasePreferenceService] Stored: "${examplePhrase.slice(0, 60)}" → ${delivery}/${service || 'unspecified'} (source: ${source})`);
      return { id, created: true };
    } catch (error) {
      logger.error('[PhrasePreferenceService] upsert failed:', error.message);
      throw error;
    }
  }

  /**
   * List all stored phrase preferences.
   */
  async list(options = {}) {
    const { limit = 100, delivery, service } = options;
    try {
      const filters = [];
      if (delivery) filters.push(`delivery = '${delivery.replace(/'/g, SQ + SQ)}'`);
      if (service)  filters.push(`service = '${service.replace(/'/g, SQ + SQ)}'`);
      const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';

      const rows = await this.db.query(`
        SELECT id, example_phrase, delivery, service, metadata, source, hit_count, created_at
        FROM phrase_preferences
        ${where}
        ORDER BY hit_count DESC, created_at DESC
        LIMIT ${limit}
      `);
      return {
        results: rows.map(r => ({
          id: r.id,
          examplePhrase: r.example_phrase,
          delivery: r.delivery,
          service: r.service,
          metadata: r.metadata ? (() => { try { return JSON.parse(r.metadata); } catch (_e) { return null; } })() : null,
          source: r.source,
          hitCount: Number(r.hit_count),
          createdAt: r.created_at,
        })),
        total: rows.length,
      };
    } catch (error) {
      logger.error('[PhrasePreferenceService] list failed:', error.message);
      return { results: [], total: 0 };
    }
  }

  /**
   * Delete a preference by id.
   */
  async delete(id) {
    try {
      await this.db.execute(`DELETE FROM phrase_preferences WHERE id = '${id.replace(/'/g, SQ + SQ)}'`);
      return { deleted: true };
    } catch (error) {
      logger.error('[PhrasePreferenceService] delete failed:', error.message);
      throw error;
    }
  }
}

let _instance = null;
export function getPhrasePreferenceService() {
  if (!_instance) _instance = new PhrasePreferenceService();
  return _instance;
}

export default PhrasePreferenceService;
