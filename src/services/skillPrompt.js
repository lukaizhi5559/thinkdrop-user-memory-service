import { getDatabaseService } from './database.js';
import { getEmbeddingService } from './embeddings.js';
import logger from '../utils/logger.js';

const SIMILARITY_THRESHOLD = 0.72;
// eslint-disable-next-line quotes
const SQ = "'";

function generateId() {
  return `sp_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

class SkillPromptService {
  constructor() {
    this.db = getDatabaseService();
    this.embeddings = getEmbeddingService();
  }

  /**
   * Semantic search: embed the query, return top-K skill prompt snippets above threshold.
   * Returns [] if no matches or embeddings unavailable.
   */
  async search(query, options = {}) {
    const topK = options.topK || 3;
    const minSimilarity = options.minSimilarity ?? SIMILARITY_THRESHOLD;

    try {
      if (!this.embeddings.isInitialized()) {
        logger.warn('[SkillPromptService] Embedding service not initialized — skipping search');
        return { results: [], total: 0 };
      }

      const queryEmbedding = await this.embeddings.generateEmbedding(query);
      const embeddingValues = queryEmbedding.map(v => v.toString()).join(',');
      const queryVector = `[${embeddingValues}]::FLOAT[384]`;

      const sql = `
        SELECT
          id,
          tags,
          prompt_text,
          hit_count,
          created_at,
          (1 - array_cosine_distance(embedding, ${queryVector})) as similarity
        FROM skill_prompts
        WHERE embedding IS NOT NULL
        ORDER BY array_cosine_distance(embedding, ${queryVector})
        LIMIT ${topK * 3}
      `;

      const rows = await this.db.query(sql);
      const filtered = rows.filter(r => r.similarity >= minSimilarity).slice(0, topK);

      logger.debug(`[SkillPromptService] search("${query.substring(0, 60)}") → ${filtered.length}/${rows.length} results above ${minSimilarity}`);

      // Bump hit_count for matched rows (fire-and-forget)
      for (const row of filtered) {
        this.db.execute(
          `UPDATE skill_prompts SET hit_count = hit_count + 1, updated_at = CURRENT_TIMESTAMP WHERE id = '${row.id}'`
        ).catch(() => {});
      }

      return {
        results: filtered.map(r => ({
          id: r.id,
          tags: r.tags ? r.tags.split(',').map(t => t.trim()) : [],
          promptText: r.prompt_text,
          similarity: parseFloat(r.similarity.toFixed(4)),
          hitCount: Number(r.hit_count)
        })),
        total: filtered.length
      };
    } catch (error) {
      logger.error('[SkillPromptService] search failed:', error.message);
      return { results: [], total: 0 };
    }
  }

  /**
   * Upsert a skill prompt snippet.
   * If a snippet with identical tags already exists and is highly similar (>0.92),
   * update its prompt_text instead of inserting a duplicate.
   * Returns { id, created: bool }.
   */
  async upsert(tags, promptText) {
    try {
      if (!this.embeddings.isInitialized()) {
        throw new Error('Embedding service not initialized');
      }

      const tagsStr = Array.isArray(tags) ? tags.join(', ') : (tags || '');
      const embedding = await this.embeddings.generateEmbedding(promptText);
      const embeddingValues = embedding.map(v => v.toString()).join(',');
      const queryVector = `[${embeddingValues}]::FLOAT[384]`;

      // Check for near-duplicate by cosine similarity (>0.92 = essentially the same snippet)
      const dupeCheck = await this.db.query(`
        SELECT id, (1 - array_cosine_distance(embedding, ${queryVector})) as similarity
        FROM skill_prompts
        WHERE embedding IS NOT NULL
        ORDER BY array_cosine_distance(embedding, ${queryVector})
        LIMIT 1
      `).catch(() => []);

      if (dupeCheck.length > 0 && dupeCheck[0].similarity >= 0.92) {
        const existingId = dupeCheck[0].id;
        const safeTextUpd = promptText.replace(/'/g, SQ + SQ);
        const safeTagsUpd = tagsStr.replace(/'/g, SQ + SQ);
        await this.db.execute(`
          UPDATE skill_prompts
          SET prompt_text = '${safeTextUpd}',
              tags = '${safeTagsUpd}',
              embedding = list_value(${embeddingValues}),
              updated_at = CURRENT_TIMESTAMP
          WHERE id = '${existingId}'
        `);
        logger.debug(`[SkillPromptService] Updated existing snippet ${existingId} (similarity ${dupeCheck[0].similarity.toFixed(3)})`);
        return { id: existingId, created: false };
      }

      // Insert new
      const id = generateId();
      const safeTextIns = promptText.replace(/'/g, SQ + SQ);
      const safeTagsIns = tagsStr.replace(/'/g, SQ + SQ);
      await this.db.execute(`
        INSERT INTO skill_prompts (id, tags, prompt_text, embedding, hit_count, created_at, updated_at)
        VALUES (
          '${id}',
          '${safeTagsIns}',
          '${safeTextIns}',
          list_value(${embeddingValues}),
          0,
          CURRENT_TIMESTAMP,
          CURRENT_TIMESTAMP
        )
      `);
      logger.info(`[SkillPromptService] Inserted new snippet ${id} tags=[${tagsStr}]`);
      return { id, created: true };
    } catch (error) {
      logger.error('[SkillPromptService] upsert failed:', error.message);
      throw error;
    }
  }

  /**
   * List all stored skill prompts (for admin/debug).
   */
  async list(limit = 50) {
    try {
      const rows = await this.db.query(`
        SELECT id, tags, prompt_text, hit_count, created_at, updated_at
        FROM skill_prompts
        ORDER BY hit_count DESC, created_at DESC
        LIMIT ${limit}
      `);
      return {
        results: rows.map(r => ({
          id: r.id,
          tags: r.tags ? r.tags.split(',').map(t => t.trim()) : [],
          promptText: r.prompt_text,
          hitCount: Number(r.hit_count),
          createdAt: r.created_at,
          updatedAt: r.updated_at
        })),
        total: rows.length
      };
    } catch (error) {
      logger.error('[SkillPromptService] list failed:', error.message);
      return { results: [], total: 0 };
    }
  }

  /**
   * Delete a skill prompt by id.
   */
  async delete(id) {
    try {
      await this.db.execute(`DELETE FROM skill_prompts WHERE id = '${id}'`);
      return { deleted: true };
    } catch (error) {
      logger.error('[SkillPromptService] delete failed:', error.message);
      throw error;
    }
  }
}

let _instance = null;
export function getSkillPromptService() {
  if (!_instance) _instance = new SkillPromptService();
  return _instance;
}

export default SkillPromptService;
