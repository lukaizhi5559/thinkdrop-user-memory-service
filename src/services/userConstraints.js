import { getDatabaseService } from './database.js';
import { createHash } from 'crypto';
import logger from '../utils/logger.js';

function hashPin(pin) {
  return createHash('sha256').update(String(pin).toUpperCase().trim()).digest('hex');
}

// ---------------------------------------------------------------------------
// UserConstraintsService — hard/soft rules that restrict ThinkDrop's
// autonomous actions, stored in DuckDB.
//
// severity = 'hard' → abort immediately + surface ASK_USER
// severity = 'soft' → show a confirmation warning before proceeding
//
// blocks: JSON array of action glob patterns covered by this rule,
//   e.g. ["signup.*", "browser.act.*", "gmail.*"].
//   If blocks is empty/null the rule is checked against ALL actions.
//
// Schema: user_constraints(id, scope, rule, blocks, severity, ...)
// ---------------------------------------------------------------------------

const SQ = "'";

function generateId() {
  return `uc_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

class UserConstraintsService {
  constructor() {
    this.db = getDatabaseService();
  }

  /**
   * Add a new constraint.
   * opts.scope    — 'global' | service name | 'action:<verb>'
   * opts.blocks   — array of glob patterns   (optional)
   * opts.severity — 'hard' | 'soft'
   */
  async add(rule, opts = {}) {
    const { scope = 'global', blocks = null, severity = 'hard', pin = null } = opts;
    if (!rule) throw new Error('rule is required');

    const safeRule     = String(rule).replace(/'/g, SQ + SQ);
    const safeScope    = String(scope).replace(/'/g, SQ + SQ);
    const safeSeverity = severity === 'soft' ? 'soft' : 'hard';
    const blocksJson   = blocks
      ? `'${JSON.stringify(blocks).replace(/'/g, SQ + SQ)}'`
      : 'NULL';
    const pinHash    = pin ? hashPin(pin) : null;
    const pinHashSql = pinHash ? `'${pinHash}'` : 'NULL';

    try {
      const id = generateId();
      await this.db.execute(`
        INSERT INTO user_constraints (id, scope, rule, blocks, severity, override_pin, created_at, updated_at)
        VALUES (
          '${id}',
          '${safeScope}',
          '${safeRule}',
          ${blocksJson},
          '${safeSeverity}',
          ${pinHashSql},
          now(),
          now()
        )
      `);
      logger.debug(`[UserConstraintsService] added constraint id="${id}" scope="${scope}" severity="${safeSeverity}" pin=${pin ? 'yes' : 'no'}`);
      return { id, scope, rule, severity: safeSeverity, pinProtected: Boolean(pin) };
    } catch (error) {
      logger.error('[UserConstraintsService] add failed:', error.message);
      throw error;
    }
  }

  /**
   * Check whether any constraint blocks a given set of action patterns or a plain
   * text user message.
   *
   * actionPatterns: string[] — e.g. ['gmail.login', 'browser.act', 'signup.google']
   * opts.scope:   optional scope string to narrow the search
   * opts.message: optional raw user message — used for text-keyword matching against
   *   stored rule text when no actionPatterns match.  This is the primary check path
   *   for command_automate requests where the exact skill isn't known yet.
   *
   * Returns:
   *   { blocked: bool, hardBlocks: string[], softWarnings: string[], allMatched: ConstraintRow[] }
   */
  async check(actionPatterns = [], opts = {}) {
    const { scope = null, message = null, pinAttempt = null } = opts;
    const pinAttemptHash = pinAttempt ? hashPin(pinAttempt) : null;
    const scopeFilter = scope
      ? `AND scope IN ('global', '${String(scope).replace(/'/g, SQ + SQ)}')`
      : '';

    try {
      const rows = await this.db.query(`
        SELECT * FROM user_constraints
        WHERE 1=1 ${scopeFilter}
        ORDER BY severity DESC, created_at ASC
      `);

      const matched = [];

      // Pre-compute keywords from the raw message for text-level matching
      const msgKeywords = message
        ? message.toLowerCase().split(/\W+/).filter(w => w.length > 2)
        : [];

      for (const row of rows) {
        let blockPatterns = [];
        try {
          blockPatterns = row.blocks ? JSON.parse(row.blocks) : [];
        } catch (_) {}

        // 1. Pattern-based match: passed actionPatterns vs stored block globs
        const patternMatch =
          blockPatterns.length === 0 ||
          (actionPatterns.length > 0 &&
            actionPatterns.some(ap =>
              blockPatterns.some(bp => {
                // Simple glob: 'signup.*' → /^signup\..*$/
                const re = new RegExp(
                  '^' + bp.replace(/\./g, '\\.').replace(/\*/g, '.*') + '$',
                  'i'
                );
                return re.test(ap);
              })
            ));

        // 2. Text-based match: keywords from the user's message vs the stored
        //    rule text (e.g. user says "delete files" → rule says "never delete").
        //    Extract content words from the rule and check overlap with message.
        const textMatch = msgKeywords.length > 0 && (() => {
          const ruleWords = row.rule.toLowerCase().split(/\W+/).filter(w => w.length > 2);
          // Count overlapping significant words (exclude stop-words)
          const STOP = new Set(['the','and','from','that','this','with','not','let','can',
            'you','for','are','was','its','all','any','but','our','they','been','have','has',
            'will','would','could','should','never','dont','also','just','make','sure']);
          const ruleContent = ruleWords.filter(w => !STOP.has(w));
          const msgContent  = msgKeywords.filter(w => !STOP.has(w));
          const overlap = ruleContent.filter(w => msgContent.includes(w));
          return overlap.length >= 1;
        })();

        const appliesToAction = patternMatch || textMatch;

        if (appliesToAction) {
          matched.push({
            id:          row.id,
            scope:       row.scope,
            rule:        row.rule,
            severity:    row.severity,
            blocks:      blockPatterns,
            overridePin: row.override_pin || null,
          });
        }
      }

      // Separate hard blocks: if a constraint has a PIN, check the attempt
      const hardMatched    = matched.filter(m => m.severity === 'hard');
      const unlockedByPin  = new Set();
      const pinProtectedRules = [];

      for (const m of hardMatched) {
        if (m.overridePin) {
          if (pinAttemptHash && pinAttemptHash === m.overridePin) {
            // Correct PIN supplied — this constraint is bypassed
            unlockedByPin.add(m.id);
            logger.info(`[UserConstraintsService] PIN override accepted for constraint id="${m.id}"`);
          } else {
            // PIN required but wrong or missing
            pinProtectedRules.push(m.rule);
          }
        }
      }

      const effectiveHardBlocks = hardMatched
        .filter(m => !unlockedByPin.has(m.id))
        .map(m => m.rule);

      return {
        blocked:            effectiveHardBlocks.length > 0,
        hardBlocks:         effectiveHardBlocks,
        pinProtectedBlocks: pinProtectedRules,  // hard blocks that have a PIN (wrong/missing)
        softWarnings:       matched.filter(m => m.severity === 'soft').map(m => m.rule),
        allMatched:         matched,
      };
    } catch (error) {
      logger.error('[UserConstraintsService] check failed:', error.message);
      return { blocked: false, hardBlocks: [], softWarnings: [], allMatched: [] };
    }
  }

  /**
   * List all constraints, optionally filtered by scope.
   */
  async list(opts = {}) {
    const { scope } = opts;
    const scopeFilter = scope
      ? `WHERE scope = '${String(scope).replace(/'/g, SQ + SQ)}'`
      : '';
    try {
      const rows = await this.db.query(
        `SELECT * FROM user_constraints ${scopeFilter} ORDER BY severity DESC, created_at ASC`
      );
      return rows.map(r => ({
        id:           r.id,
        scope:        r.scope,
        rule:         r.rule,
        severity:     r.severity,
        pinProtected: Boolean(r.override_pin),
        blocks:       (() => { try { return r.blocks ? JSON.parse(r.blocks) : []; } catch (_) { return []; } })(),
      }));
    } catch (error) {
      logger.error('[UserConstraintsService] list failed:', error.message);
      return [];
    }
  }

  /**
   * Delete a constraint by id.
   */
  async remove(id) {
    if (!id) throw new Error('id is required');
    const safeId = String(id).replace(/'/g, SQ + SQ);
    try {
      await this.db.execute(
        `DELETE FROM user_constraints WHERE id = '${safeId}'`
      );
      logger.debug(`[UserConstraintsService] removed id="${safeId}"`);
      return { deleted: true, id: safeId };
    } catch (error) {
      logger.error('[UserConstraintsService] remove failed:', error.message);
      throw error;
    }
  }
}

let _instance = null;

export function getUserConstraintsService() {
  if (!_instance) _instance = new UserConstraintsService();
  return _instance;
}
