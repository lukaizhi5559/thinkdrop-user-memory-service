import { getDatabaseService } from './database.js';
import logger from '../utils/logger.js';
import path from 'path';
import os from 'os';

const SQ = '\x27';
const SKILLS_BASE_DIR = path.join(os.homedir(), '.thinkdrop', 'skills');

const VALID_EXEC_TYPES = ['node', 'shell'];
const SKILL_NAME_PATTERN = /^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)+$/;
const REQUIRED_FRONTMATTER = ['name', 'description', 'exec_path', 'exec_type'];

function generateId() {
  return `skill_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

/**
 * Parse YAML-style frontmatter from a skill contract .md file.
 * Returns { name, description, version, exec_path, exec_type } or throws on invalid.
 */
function parseFrontmatter(contractMd) {
  const fmMatch = contractMd.match(/^---\s*\n([\s\S]*?)\n---/);
  if (!fmMatch) {
    throw new Error('Skill contract is missing frontmatter (--- block at top of file)');
  }

  const fm = {};
  for (const line of fmMatch[1].split('\n')) {
    const colonIdx = line.indexOf(':');
    if (colonIdx === -1) continue;
    const key = line.slice(0, colonIdx).trim();
    const value = line.slice(colonIdx + 1).trim().replace(/^['"]|['"]$/g, '');
    if (key) fm[key] = value;
  }

  return fm;
}

/**
 * Validate a skill contract and return parsed fields.
 * Throws with a human-readable message on any validation failure.
 */
function validateContract(contractMd) {
  const fm = parseFrontmatter(contractMd);

  for (const field of REQUIRED_FRONTMATTER) {
    if (!fm[field]) {
      throw new Error(`Skill contract frontmatter is missing required field: "${field}"`);
    }
  }

  if (!SKILL_NAME_PATTERN.test(fm.name)) {
    throw new Error(
      `Skill name "${fm.name}" is invalid. Must be dot-notation with at least 2 parts, lowercase letters/numbers only (e.g. "check.weather.daily")`
    );
  }

  if (!VALID_EXEC_TYPES.includes(fm.exec_type)) {
    throw new Error(`exec_type "${fm.exec_type}" is not valid. Must be one of: ${VALID_EXEC_TYPES.join(', ')}`);
  }

  // Resolve ~ in exec_path
  const resolvedPath = fm.exec_path.startsWith('~/')
    ? path.join(os.homedir(), fm.exec_path.slice(2))
    : fm.exec_path;

  // Security: exec_path must be inside ~/.thinkdrop/skills/
  if (!resolvedPath.startsWith(SKILLS_BASE_DIR)) {
    throw new Error(
      `exec_path ${fm.exec_path} is outside the allowed skills directory (${SKILLS_BASE_DIR}). Skills must reside inside ~/.thinkdrop/skills/`
    );
  }

  // Cross-field consistency: exec_type and exec_path must agree on skill type.
  // Rule: exec_type:node → exec_path must be a .cjs/.js file (not .md)
  //       exec_type:shell → exec_path must be a .md file (not .cjs/.js)
  if (fm.exec_type === 'node' && resolvedPath.endsWith('.md')) {
    throw new Error(
      `Invalid skill contract: exec_type 'node' requires a .cjs exec_path, but got "${fm.exec_path}". ` +
      'Contract/shell skills must use exec_type: shell.'
    );
  }
  if ((resolvedPath.endsWith('.cjs') || resolvedPath.endsWith('.js')) && fm.exec_type !== 'node') {
    throw new Error(
      `Invalid skill contract: exec_path "${fm.exec_path}" (JS file) requires exec_type: node, but got "${fm.exec_type}".`
    );
  }

  // ── Contract body quality checks ──────────────────────────────────────────
  // These run after frontmatter validation so the body is always extracted
  // from a known-valid contract structure.
  const fmBlock = contractMd.match(/^---\s*\n[\s\S]*?\n---/);
  const bodyText = fmBlock ? contractMd.slice(fmBlock[0].length) : contractMd;

  // 1. Truncation guard: odd number of ``` fences means the LLM was cut off
  //    mid-code-block (e.g. synthesize hit maxTokens mid-curl-command).
  const fenceCount = (bodyText.match(/^```/mg) || []).length;
  if (fenceCount % 2 !== 0) {
    throw new Error(
      `Skill contract appears truncated — odd number of code fences (${fenceCount}). ` +
      'Increase synthesize maxTokens and regenerate the skill.'
    );
  }

  // 2. Minimum body length — catches empty or near-empty generations.
  if (bodyText.trim().length < 50) {
    throw new Error(
      `Skill contract body is too short (${bodyText.trim().length} chars). ` +
      'The contract was likely truncated or failed to generate.'
    );
  }

  // 3. Forbidden OAuth token-file pattern — prevents poisoned contractMd from
  //    being stored and later injected as "CRITICAL RULES" into the planner.
  if (/\.thinkdrop\/tokens\//.test(bodyText)) {
    throw new Error(
      'Skill contract contains a forbidden OAuth token file read pattern ' +
      '(~/.thinkdrop/tokens/). Use the pre-injected $<PROVIDER>_ACCESS_TOKEN ' +
      'env var instead. Fix the ## Auth section and reinstall.'
    );
  }
  // ── End contract body quality checks ──────────────────────────────────────

  return {
    name: fm.name,
    description: fm.description,
    version: fm.version || '1.0.0',
    exec_path: resolvedPath,
    exec_type: fm.exec_type
  };
}

class SkillRegistryService {
  constructor() {
    this.db = getDatabaseService();
  }

  /**
   * Install a skill from its contract .md content.
   * Validates the contract, rejects invalid or insecure skills.
   * Returns { id, name, created: bool }.
   */
  async install(contractMd) {
    const parsed = validateContract(contractMd);
    const { name, description, exec_path, exec_type } = parsed;

    const safe = (s) => s.replace(/'/g, SQ + SQ);

    // Check if already installed — update if so
    const existing = await this.db.query(
      `SELECT id FROM installed_skills WHERE name = '${safe(name)}'`
    );

    if (existing.length > 0) {
      const id = existing[0].id;
      await this.db.execute(`
        UPDATE installed_skills
        SET description  = '${safe(description)}',
            contract_md  = '${safe(contractMd)}',
            exec_path    = '${safe(exec_path)}',
            exec_type    = '${safe(exec_type)}',
            enabled      = true,
            updated_at   = now()
        WHERE id = '${id}'
      `);
      logger.info(`[SkillRegistry] Updated skill: ${name} (${id})`);
      await this._upsertHealth(name, 'ok', null);
      return { id, name, created: false };
    }

    const id = generateId();
    await this.db.execute(`
      INSERT INTO installed_skills (id, name, description, contract_md, exec_path, exec_type, enabled, installed_at, updated_at)
      VALUES (
        '${id}',
        '${safe(name)}',
        '${safe(description)}',
        '${safe(contractMd)}',
        '${safe(exec_path)}',
        '${safe(exec_type)}',
        true,
        now(),
        now()
      )
    `);
    logger.info(`[SkillRegistry] Installed new skill: ${name} (${id})`);
    await this._upsertHealth(name, 'ok', null);
    return { id, name, created: true };
  }

  /**
   * Remove a skill by name. Returns { deleted: bool }.
   */
  async remove(name) {
    const safe = (s) => s.replace(/'/g, SQ + SQ);
    const existing = await this.db.query(
      `SELECT id FROM installed_skills WHERE name = '${safe(name)}'`
    );
    if (existing.length === 0) {
      return { deleted: false, reason: `No skill named '${name}' is installed` };
    }
    // Remove health record first (FK constraint)
    await this.db.execute(`DELETE FROM skill_health WHERE skill_name = '${safe(name)}'`).catch(() => {});
    await this.db.execute(`DELETE FROM installed_skills WHERE name = '${safe(name)}'`);
    logger.info(`[SkillRegistry] Removed skill: ${name}`);
    return { deleted: true };
  }

  /**
   * List all installed skills (lightweight — no contract_md).
   */
  async list(enabledOnly = false) {
    const where = enabledOnly ? 'WHERE enabled = true' : '';
    const rows = await this.db.query(`
      SELECT id, name, description, exec_path, exec_type, enabled, installed_at, updated_at
      FROM installed_skills
      ${where}
      ORDER BY name ASC
    `);
    return {
      results: rows.map(r => ({
        id: r.id,
        name: r.name,
        description: r.description,
        execPath: r.exec_path,
        execType: r.exec_type,
        enabled: Boolean(r.enabled),
        installedAt: r.installed_at,
        updatedAt: r.updated_at
      })),
      total: rows.length
    };
  }

  /**
   * Get a single skill by name, including its full contract_md.
   */
  async get(name) {
    const safe = (s) => s.replace(/'/g, SQ + SQ);
    const rows = await this.db.query(
      `SELECT * FROM installed_skills WHERE name = '${safe(name)}' LIMIT 1`
    );
    if (rows.length === 0) return null;
    const r = rows[0];
    return {
      id: r.id,
      name: r.name,
      description: r.description,
      contractMd: r.contract_md,
      execPath: r.exec_path,
      execType: r.exec_type,
      enabled: Boolean(r.enabled),
      installedAt: r.installed_at,
      updatedAt: r.updated_at
    };
  }

  /**
   * Get all enabled skill names + descriptions (used by parseSkill for fast lookup).
   * Returns a plain array of { name, description } — no contract bodies.
   */
  async listNames() {
    const rows = await this.db.query(`
      SELECT name, description, exec_type, exec_path FROM installed_skills WHERE enabled = true ORDER BY name ASC
    `);
    return rows.map(r => ({ name: r.name, description: r.description, execType: r.exec_type, execPath: r.exec_path }));
  }

  /**
   * Upsert a skill directly by fields — no contractMd required.
   * Used by the installSkill node after the build pipeline writes the .cjs file.
   * Returns { id, name, created: bool }.
   */
  async upsert({ name, description, execPath, execType = 'node', enabled = true, contractMd = '' }) {
    if (!name || !execPath) throw new Error('upsert requires name and execPath');

    const resolvedPath = execPath.startsWith('~/')
      ? path.join(os.homedir(), execPath.slice(2))
      : execPath;

    if (!resolvedPath.startsWith(SKILLS_BASE_DIR)) {
      throw new Error(`execPath ${execPath} is outside ${SKILLS_BASE_DIR}`);
    }

    const safe = (s) => String(s).replace(/'/g, SQ + SQ);
    const existing = await this.db.query(
      `SELECT id FROM installed_skills WHERE name = '${safe(name)}'`
    );

    if (existing.length > 0) {
      const id = existing[0].id;
      await this.db.execute(`
        UPDATE installed_skills
        SET description = '${safe(description || '')}',
            exec_path   = '${safe(resolvedPath)}',
            exec_type   = '${safe(execType)}',
            contract_md = '${safe(contractMd || '')}',
            enabled     = ${enabled ? 'true' : 'false'},
            updated_at  = now()
        WHERE id = '${id}'
      `);
      logger.info(`[SkillRegistry] Upserted (updated) skill: ${name}`);
      return { id, name, created: false };
    }

    const id = generateId();
    await this.db.execute(`
      INSERT INTO installed_skills (id, name, description, contract_md, exec_path, exec_type, enabled, installed_at, updated_at)
      VALUES (
        '${id}',
        '${safe(name)}',
        '${safe(description || '')}',
        '${safe(contractMd || '')}',
        '${safe(resolvedPath)}',
        '${safe(execType)}',
        ${enabled ? 'true' : 'false'},
        now(),
        now()
      )
    `);
    logger.info(`[SkillRegistry] Upserted (inserted) skill: ${name}`);
    return { id, name, created: true };
  }

  /**
   * Enable or disable a skill by name.
   */
  async setEnabled(name, enabled) {
    const safe = (s) => s.replace(/'/g, SQ + SQ);
    await this.db.execute(`
      UPDATE installed_skills
      SET enabled = ${enabled ? 'true' : 'false'}, updated_at = now()
      WHERE name = '${safe(name)}'
    `);
    return { name, enabled };
  }

  // ── skill_health helpers ───────────────────────────────────────────────────

  /**
   * Internal helper — upsert a health record. status is 'ok'|'invalid'|'repaired'|'unvalidated'.
   */
  async _upsertHealth(skillName, status, errors, autoRepaired = false) {
    const safe = (s) => String(s || '').replace(/'/g, SQ + SQ);
    const errText = errors ? safe(typeof errors === 'object' ? JSON.stringify(errors) : String(errors)) : '';
    try {
      await this.db.execute(`
        INSERT INTO skill_health (skill_name, status, errors, last_checked_at, auto_repaired)
        VALUES ('${safe(skillName)}', '${safe(status)}', '${errText}', now(), ${autoRepaired})
        ON CONFLICT (skill_name) DO UPDATE SET
          status = '${safe(status)}',
          errors = '${errText}',
          last_checked_at = now(),
          auto_repaired = ${autoRepaired}
      `);
    } catch (e) {
      // Graceful fallback if ON CONFLICT syntax varies
      await this.db.execute(`DELETE FROM skill_health WHERE skill_name = '${safe(skillName)}'`).catch(() => {});
      await this.db.execute(`
        INSERT INTO skill_health (skill_name, status, errors, last_checked_at, auto_repaired)
        VALUES ('${safe(skillName)}', '${safe(status)}', '${errText}', now(), ${autoRepaired})
      `);
    }
  }

  /**
   * Upsert a health record for a skill (called by skill.review agent).
   */
  async healthUpsert({ skillName, status, errors, autoRepaired = false }) {
    if (!skillName || !status) throw new Error('healthUpsert requires skillName and status');
    await this._upsertHealth(skillName, status, errors, autoRepaired);
    return { skillName, status };
  }

  /**
   * Get health record for a single skill.
   */
  async healthGet(skillName) {
    const safe = (s) => String(s).replace(/'/g, SQ + SQ);
    const rows = await this.db.query(
      `SELECT * FROM skill_health WHERE skill_name = '${safe(skillName)}' LIMIT 1`
    );
    if (rows.length === 0) return null;
    const r = rows[0];
    return {
      skillName: r.skill_name,
      status: r.status,
      errors: r.errors || null,
      lastCheckedAt: r.last_checked_at,
      autoRepaired: Boolean(r.auto_repaired)
    };
  }

  /**
   * List skills with non-'ok' health status (the unhealthy set).
   * Pass { all: true } to return all health records.
   */
  async healthList({ all = false } = {}) {
    const where = all ? '' : 'WHERE h.status != \'ok\'';
    const rows = await this.db.query(`
      SELECT h.skill_name, h.status, h.errors, h.last_checked_at, h.auto_repaired,
             s.exec_type, s.exec_path, s.enabled
      FROM skill_health h
      LEFT JOIN installed_skills s ON s.name = h.skill_name
      ${where}
      ORDER BY h.last_checked_at DESC
    `);
    return {
      results: rows.map(r => ({
        skillName: r.skill_name,
        status: r.status,
        errors: r.errors || null,
        lastCheckedAt: r.last_checked_at,
        autoRepaired: Boolean(r.auto_repaired),
        execType: r.exec_type,
        execPath: r.exec_path,
        enabled: Boolean(r.enabled)
      })),
      total: rows.length
    };
  }
}

let _instance = null;
export function getSkillRegistryService() {
  if (!_instance) _instance = new SkillRegistryService();
  return _instance;
}

export default SkillRegistryService;
