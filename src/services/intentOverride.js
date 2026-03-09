import { getDatabaseService } from './database.js';
import { getEmbeddingService } from './embeddings.js';
import logger from '../utils/logger.js';

// ---------------------------------------------------------------------------
// IntentOverrideService — learned intent corrections stored in DuckDB.
//
// When a user corrects a misclassified intent ("no, I meant go to the webpage,
// not a web search"), the original prompt + correct intent is stored here.
// parseIntent checks this BEFORE calling phi4 — so the same phrasing never
// misclassifies again.
//
// Schema: intent_overrides(id, example_prompt, correct_intent, wrong_intent,
//                          embedding, source, hit_count, created_at, updated_at)
// ---------------------------------------------------------------------------

const SIMILARITY_THRESHOLD = 0.82; // high bar — only override when very confident

function generateId() {
  return `io_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

class IntentOverrideService {
  constructor() {
    this.db = getDatabaseService();
    this.embeddings = getEmbeddingService();
  }

  /**
   * Semantic search: find the closest stored override for a given prompt.
   * Returns { correctIntent, examplePrompt, similarity } or null if no match
   * above SIMILARITY_THRESHOLD.
   */
  async search(prompt, options = {}) {
    const { threshold = SIMILARITY_THRESHOLD } = options;
    if (!prompt) return null;

    try {
      const embedding = await this.embeddings.generateEmbedding(prompt);
      if (!embedding) return null;

      const embeddingStr = `[${embedding.join(',')}]`;

      const rows = await this.db.query(`
        SELECT id, example_prompt, correct_intent, wrong_intent, hit_count,
               array_cosine_similarity(embedding::FLOAT[384], ${embeddingStr}::FLOAT[384]) AS similarity
        FROM intent_overrides
        WHERE embedding IS NOT NULL
        ORDER BY similarity DESC
        LIMIT 1
      `);

      if (!rows.length) return null;

      const top = rows[0];
      const sim = Number(top.similarity);

      if (sim < threshold) {
        logger.debug(`[IntentOverrideService] search: top match "${top.example_prompt.slice(0, 60)}" sim=${sim.toFixed(3)} below threshold ${threshold}`);
        return null;
      }

      // Bump hit_count (fire-and-forget)
      this.db.execute(
        `UPDATE intent_overrides SET hit_count = hit_count + 1, updated_at = CURRENT_TIMESTAMP WHERE id = '${top.id}'`
      ).catch(() => {});

      logger.info(`[IntentOverrideService] Match: "${top.example_prompt.slice(0, 60)}" → ${top.correct_intent} (sim=${sim.toFixed(3)})`);
      return {
        correctIntent: top.correct_intent,
        wrongIntent: top.wrong_intent,
        examplePrompt: top.example_prompt,
        similarity: sim,
        hitCount: Number(top.hit_count)
      };
    } catch (error) {
      logger.warn(`[IntentOverrideService] search failed (non-fatal): ${error.message}`);
      return null;
    }
  }

  /**
   * Store a new intent correction.
   * Skips near-duplicates (same correct_intent + similarity > 0.95 to existing).
   */
  async upsert(examplePrompt, correctIntent, options = {}) {
    const { wrongIntent = null, source = 'user_correction' } = options;

    if (!examplePrompt || !correctIntent) throw new Error('examplePrompt and correctIntent are required');

    const validIntents = ['command_automate', 'memory_retrieve', 'web_search', 'screen_intelligence', 'memory_store', 'general_query'];
    if (!validIntents.includes(correctIntent)) throw new Error(`Invalid intent: ${correctIntent}`);

    try {
      const embedding = await this.embeddings.generateEmbedding(examplePrompt);
      if (!embedding) throw new Error('Embedding generation failed');

      // Check for near-duplicate (same intent + very high similarity)
      const existing = await this.search(examplePrompt, { threshold: 0.95 });
      if (existing && existing.correctIntent === correctIntent) {
        logger.debug(`[IntentOverrideService] Skipping near-duplicate for "${examplePrompt.slice(0, 60)}"`);
        return { created: false, reason: 'near_duplicate' };
      }

      const id = generateId();
      const embeddingStr = `[${embedding.join(',')}]`;
      const safePrompt = examplePrompt.replace(/'/g, '\'\'');
      const safeIntent = correctIntent.replace(/'/g, '\'\'');
      const safeWrong = wrongIntent ? `'${wrongIntent.replace(/'/g, '\'\'')}'` : 'NULL';
      const safeSource = (source || 'user_correction').replace(/'/g, '\'\'');

      await this.db.execute(`
        INSERT INTO intent_overrides (id, example_prompt, correct_intent, wrong_intent, embedding, source, hit_count, created_at, updated_at)
        VALUES (
          '${id}',
          '${safePrompt}',
          '${safeIntent}',
          ${safeWrong},
          ${embeddingStr}::FLOAT[384],
          '${safeSource}',
          0,
          CURRENT_TIMESTAMP,
          CURRENT_TIMESTAMP
        )
      `);

      logger.info(`[IntentOverrideService] Stored correction: "${examplePrompt.slice(0, 60)}" → ${correctIntent} (wrong was: ${wrongIntent || 'unknown'})`);
      return { id, created: true };
    } catch (error) {
      logger.error('[IntentOverrideService] upsert failed:', error.message);
      throw error;
    }
  }

  /**
   * List all stored overrides.
   */
  async list(options = {}) {
    const { limit = 100, correctIntent } = options;
    try {
      const filter = correctIntent ? `WHERE correct_intent = '${correctIntent.replace(/'/g, '\'\'')}'` : '';
      const rows = await this.db.query(`
        SELECT id, example_prompt, correct_intent, wrong_intent, source, hit_count, created_at
        FROM intent_overrides
        ${filter}
        ORDER BY hit_count DESC, created_at DESC
        LIMIT ${limit}
      `);
      return {
        results: rows.map(r => ({
          id: r.id,
          examplePrompt: r.example_prompt,
          correctIntent: r.correct_intent,
          wrongIntent: r.wrong_intent,
          source: r.source,
          hitCount: Number(r.hit_count),
          createdAt: r.created_at
        })),
        total: rows.length
      };
    } catch (error) {
      logger.error('[IntentOverrideService] list failed:', error.message);
      return { results: [], total: 0 };
    }
  }

  /**
   * Delete an override by id.
   */
  async delete(id) {
    try {
      await this.db.execute(`DELETE FROM intent_overrides WHERE id = '${id.replace(/'/g, '\'\'')}'`);
      return { deleted: true };
    } catch (error) {
      logger.error('[IntentOverrideService] delete failed:', error.message);
      throw error;
    }
  }
}

let _instance = null;
export function getIntentOverrideService() {
  if (!_instance) _instance = new IntentOverrideService();
  return _instance;
}

export default IntentOverrideService;
