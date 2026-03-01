import { getDatabaseService } from './database.js';
import logger from '../utils/logger.js';

// ---------------------------------------------------------------------------
// ContextRuleService — per-site/app prompt injection rules stored in DuckDB.
//
// Rules are keyed by context_type + context_key:
//   context_type = 'site' → context_key is a hostname (e.g. 'en.wikipedia.org')
//   context_type = 'app'  → context_key is an app name (e.g. 'slack', 'excel')
//
// ThinkDrop AI writes rules via upsert() after diagnosing a failure.
// planSkills.js reads them via search() and injects into the LLM prompt.
// This keeps plan-skills.md lean — site/app quirks live in DuckDB, not the prompt.
//
// Schema: context_rules(id, context_type, context_key, rule_text, category, source, hit_count, ...)
// ---------------------------------------------------------------------------

// eslint-disable-next-line quotes
const SQ = "'";

function generateId() {
  return `cr_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

class ContextRuleService {
  constructor() {
    this.db = getDatabaseService();
  }

  /**
   * Fetch all rules matching any of the given context keys (hostnames or app names).
   * Optionally filter by context_type ('site' | 'app').
   * Returns rules ordered by hit_count DESC so highest-confidence rules come first.
   * Fast exact-match lookup — no embeddings needed.
   */
  async search(contextKeys, options = {}) {
    if (!Array.isArray(contextKeys) || contextKeys.length === 0) return { results: [] };

    const { contextType } = options; // optional filter: site | app

    try {
      const placeholders = contextKeys
        .map(k => `${SQ}${k.replace(/'/g, SQ + SQ)}${SQ}`)
        .join(', ');

      const typeFilter = contextType ? `AND context_type = '${contextType}'` : '';

      const rows = await this.db.query(`
        SELECT id, context_type, context_key, rule_text, category, source, hit_count, created_at
        FROM context_rules
        WHERE context_key IN (${placeholders}) ${typeFilter}
        ORDER BY hit_count DESC, created_at DESC
      `);

      // Bump hit_count for matched rows (fire-and-forget)
      for (const row of rows) {
        this.db.execute(
          `UPDATE context_rules SET hit_count = hit_count + 1, updated_at = CURRENT_TIMESTAMP WHERE id = '${row.id}'`
        ).catch(() => {});
      }

      logger.debug(`[ContextRuleService] search([${contextKeys.join(', ')}]) → ${rows.length} rule(s)`);
      return {
        results: rows.map(r => ({
          id: r.id,
          contextType: r.context_type,
          contextKey: r.context_key,
          ruleText: r.rule_text,
          category: r.category || 'general',
          source: r.source || 'thinkdrop_ai',
          hitCount: Number(r.hit_count)
        }))
      };
    } catch (error) {
      logger.error('[ContextRuleService] search failed:', error.message);
      return { results: [] };
    }
  }

  /**
   * Insert or update a rule for a context key.
   * Skips exact duplicates (same context_key + identical rule_text).
   */
  async upsert(contextKey, ruleText, options = {}) {
    const {
      contextType = 'site',
      category = 'general',
      source = 'thinkdrop_ai'
    } = options; // eslint-disable-line

    if (!contextKey || !ruleText) throw new Error('contextKey and ruleText are required');

    const safeKey = contextKey.replace(/'/g, SQ + SQ).toLowerCase().trim();
    const safeType = contextType === 'app' ? 'app' : 'site';
    const safeRule = ruleText.replace(/'/g, SQ + SQ);
    const safeCategory = (category || 'general').replace(/'/g, SQ + SQ);
    const safeSource = (source || 'thinkdrop_ai').replace(/'/g, SQ + SQ);

    try {
      // Check for exact duplicate
      const existing = await this.db.query(`
        SELECT id, rule_text FROM context_rules
        WHERE context_key = '${safeKey}' AND context_type = '${safeType}'
        ORDER BY hit_count DESC, created_at DESC
      `);

      for (const row of existing) {
        if (row.rule_text.trim() === ruleText.trim()) {
          logger.debug(`[ContextRuleService] Skipping duplicate rule for ${safeType}:${contextKey}`);
          return { id: row.id, created: false };
        }
      }

      const id = generateId();
      await this.db.execute(`
        INSERT INTO context_rules (id, context_type, context_key, rule_text, category, source, hit_count, created_at, updated_at)
        VALUES (
          '${id}',
          '${safeType}',
          '${safeKey}',
          '${safeRule}',
          '${safeCategory}',
          '${safeSource}',
          0,
          CURRENT_TIMESTAMP,
          CURRENT_TIMESTAMP
        )
      `);
      logger.info(`[ContextRuleService] Inserted rule ${id} for ${safeType}:${contextKey} category="${category}"`);
      return { id, created: true };
    } catch (error) {
      logger.error('[ContextRuleService] upsert failed:', error.message);
      throw error;
    }
  }

  /**
   * List all rules, optionally filtered by context_key or context_type.
   */
  async list(options = {}) {
    const { contextKey, contextType, limit = 100 } = options;
    try {
      const filters = [];
      if (contextKey) filters.push(`context_key = '${contextKey.replace(/'/g, '\'\'')}'`);
      if (contextType) filters.push(`context_type = '${contextType}'`);
      const where = filters.length > 0 ? `WHERE ${filters.join(' AND ')}` : '';

      const rows = await this.db.query(`
        SELECT id, context_type, context_key, rule_text, category, source, hit_count, created_at, updated_at
        FROM context_rules
        ${where}
        ORDER BY context_key, hit_count DESC, created_at DESC
        LIMIT ${limit}
      `);
      return {
        results: rows.map(r => ({
          id: r.id,
          contextType: r.context_type,
          contextKey: r.context_key,
          ruleText: r.rule_text,
          category: r.category,
          source: r.source,
          hitCount: Number(r.hit_count),
          createdAt: r.created_at,
          updatedAt: r.updated_at
        })),
        total: rows.length
      };
    } catch (error) {
      logger.error('[ContextRuleService] list failed:', error.message);
      return { results: [], total: 0 };
    }
  }

  /**
   * Delete a rule by id.
   */
  async delete(id) {
    try {
      await this.db.execute(`DELETE FROM context_rules WHERE id = '${id.replace(/'/g, '\'\'')}'`);
      return { deleted: true };
    } catch (error) {
      logger.error('[ContextRuleService] delete failed:', error.message);
      throw error;
    }
  }
}

let _instance = null;
export function getContextRuleService() {
  if (!_instance) _instance = new ContextRuleService();
  return _instance;
}

export default ContextRuleService;
