import { getDatabaseService } from './database.js';
import logger from '../utils/logger.js';

// ---------------------------------------------------------------------------
// ApiRuleService — per-service API contract rules for skill generation.
//
// Rules are keyed by service name (e.g. 'clicksend', 'twilio', 'gmail').
// Used by creator.agent and skillCreator to inject API-specific constraints
// into LLM prompts and to validate generated skill code.
//
// rule_type:
//   'auth'     — authentication pattern (e.g. Basic Auth requires username:key)
//   'payload'  — request body structure (e.g. messages array, not flat object)
//   'secret'   — required secrets (e.g. CLICKSEND_USERNAME must be declared)
//   'endpoint' — correct endpoint URL
//   'gotcha'   — known runtime failure pattern learned from errors
//
// code_pattern: optional regex string to detect violations in generated code.
// fix_hint:     exact correction to give the LLM on violation.
// source:       'system' (seed rules) | 'learned' (written from runtime failures)
// ---------------------------------------------------------------------------

const SQ = '\'';

function generateId() {
  return `ar_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

class ApiRuleService {
  constructor() {
    this.db = getDatabaseService();
  }

  /**
   * Fetch all rules for the given service names.
   * Returns rules ordered by hit_count DESC (highest-confidence first).
   */
  async search(services, options = {}) {
    if (!Array.isArray(services) || services.length === 0) return { results: [] };
    const { ruleType } = options;

    try {
      const placeholders = services
        .map(s => `${SQ}${s.replace(/'/g, SQ + SQ).toLowerCase()}${SQ}`)
        .join(', ');

      const typeFilter = ruleType ? `AND rule_type = '${ruleType}'` : '';

      const rows = await this.db.query(`
        SELECT id, service, rule_type, rule_text, code_pattern, fix_hint, source, hit_count, created_at
        FROM api_rules
        WHERE service IN (${placeholders}) ${typeFilter}
        ORDER BY hit_count DESC, created_at DESC
      `);

      for (const row of rows) {
        this.db.execute(
          `UPDATE api_rules SET hit_count = hit_count + 1, updated_at = CURRENT_TIMESTAMP WHERE id = '${row.id}'`
        ).catch(() => {});
      }

      logger.debug(`[ApiRuleService] search([${services.join(', ')}]) → ${rows.length} rule(s)`);
      return {
        results: rows.map(r => ({
          id: r.id,
          service: r.service,
          ruleType: r.rule_type,
          ruleText: r.rule_text,
          codePattern: r.code_pattern || null,
          fixHint: r.fix_hint || null,
          source: r.source || 'system',
          hitCount: Number(r.hit_count),
        }))
      };
    } catch (error) {
      logger.error('[ApiRuleService] search failed:', error.message);
      return { results: [] };
    }
  }

  /**
   * Insert or update a rule for a service.
   * Skips exact duplicates (same service + identical rule_text).
   */
  async upsert(service, ruleType, ruleText, options = {}) {
    const {
      codePattern = null,
      fixHint = null,
      source = 'system',
    } = options;

    if (!service || !ruleType || !ruleText) {
      throw new Error('service, ruleType, and ruleText are required');
    }

    const safeService     = service.replace(/'/g, SQ + SQ).toLowerCase().trim();
    const safeRuleType    = ruleType.replace(/'/g, SQ + SQ);
    const safeRuleText    = ruleText.replace(/'/g, SQ + SQ);
    const safeCodePattern = codePattern ? codePattern.replace(/'/g, SQ + SQ) : null;
    const safeFixHint     = fixHint ? fixHint.replace(/'/g, SQ + SQ) : null;
    const safeSource      = (source || 'system').replace(/'/g, SQ + SQ);

    try {
      const existing = await this.db.query(`
        SELECT id, rule_text FROM api_rules
        WHERE service = '${safeService}' AND rule_type = '${safeRuleType}'
        ORDER BY hit_count DESC, created_at DESC
      `);

      for (const row of existing) {
        if (row.rule_text.trim() === ruleText.trim()) {
          logger.debug(`[ApiRuleService] Skipping duplicate rule for ${service}:${ruleType}`);
          return { id: row.id, created: false };
        }
      }

      const id = generateId();
      const patternVal  = safeCodePattern ? `'${safeCodePattern}'` : 'NULL';
      const fixHintVal  = safeFixHint     ? `'${safeFixHint}'`     : 'NULL';

      await this.db.execute(`
        INSERT INTO api_rules (id, service, rule_type, rule_text, code_pattern, fix_hint, source, hit_count, created_at, updated_at)
        VALUES (
          '${id}',
          '${safeService}',
          '${safeRuleType}',
          '${safeRuleText}',
          ${patternVal},
          ${fixHintVal},
          '${safeSource}',
          0,
          CURRENT_TIMESTAMP,
          CURRENT_TIMESTAMP
        )
      `);
      logger.info(`[ApiRuleService] Inserted rule ${id} for ${service}:${ruleType}`);
      return { id, created: true };
    } catch (error) {
      logger.error('[ApiRuleService] upsert failed:', error.message);
      throw error;
    }
  }

  /**
   * List all rules, optionally filtered.
   */
  async list(options = {}) {
    const { service, ruleType, source, limit = 200 } = options;
    try {
      const filters = [];
      if (service)  filters.push(`service = '${service.replace(/'/g, '\'\'')}'`);
      if (ruleType) filters.push(`rule_type = '${ruleType.replace(/'/g, '\'\'')}'`);
      if (source)   filters.push(`source = '${source.replace(/'/g, '\'\'')}'`);
      const where = filters.length > 0 ? `WHERE ${filters.join(' AND ')}` : '';

      const rows = await this.db.query(`
        SELECT id, service, rule_type, rule_text, code_pattern, fix_hint, source, hit_count, created_at, updated_at
        FROM api_rules
        ${where}
        ORDER BY service, hit_count DESC, created_at DESC
        LIMIT ${limit}
      `);
      return {
        results: rows.map(r => ({
          id: r.id,
          service: r.service,
          ruleType: r.rule_type,
          ruleText: r.rule_text,
          codePattern: r.code_pattern,
          fixHint: r.fix_hint,
          source: r.source,
          hitCount: Number(r.hit_count),
          createdAt: r.created_at,
          updatedAt: r.updated_at,
        })),
        total: rows.length,
      };
    } catch (error) {
      logger.error('[ApiRuleService] list failed:', error.message);
      return { results: [], total: 0 };
    }
  }

  /**
   * Delete a rule by id.
   */
  async delete(id) {
    try {
      await this.db.execute(`DELETE FROM api_rules WHERE id = '${id.replace(/'/g, '\'\'')}'`);
      return { deleted: true };
    } catch (error) {
      logger.error('[ApiRuleService] delete failed:', error.message);
      throw error;
    }
  }
}

let _instance = null;
export function getApiRuleService() {
  if (!_instance) _instance = new ApiRuleService();
  return _instance;
}

export default ApiRuleService;
