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
            updated_at   = CURRENT_TIMESTAMP
        WHERE id = '${id}'
      `);
      logger.info(`[SkillRegistry] Updated skill: ${name} (${id})`);
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
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
      )
    `);
    logger.info(`[SkillRegistry] Installed new skill: ${name} (${id})`);
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
      SELECT name, description FROM installed_skills WHERE enabled = true ORDER BY name ASC
    `);
    return rows.map(r => ({ name: r.name, description: r.description }));
  }

  /**
   * Enable or disable a skill by name.
   */
  async setEnabled(name, enabled) {
    const safe = (s) => s.replace(/'/g, SQ + SQ);
    await this.db.execute(`
      UPDATE installed_skills
      SET enabled = ${enabled ? 'true' : 'false'}, updated_at = CURRENT_TIMESTAMP
      WHERE name = '${safe(name)}'
    `);
    return { name, enabled };
  }
}

let _instance = null;
export function getSkillRegistryService() {
  if (!_instance) _instance = new SkillRegistryService();
  return _instance;
}

export default SkillRegistryService;
