import express from 'express';
import duckdb from 'duckdb';
import os from 'os';
import path from 'path';
import fs from 'fs';
import { promisify } from 'util';

const router = express.Router();

// IMPORTANT: Do NOT import withDb from @thinkdrop/agents-db here.
//
// @thinkdrop/agents-db pulls in its own duckdb-async@0.10.2 which depends on
// duckdb@0.10.2 — a completely different native binary from the duckdb@1.4.4
// already loaded by the user-memory service.  Loading two different DuckDB
// native addons in the same Node.js process corrupts shared C++ global state
// and produces the "unique_ptr that is NULL" crash within ~3 minutes.
//
// Fix: open agents.db using the service's own `duckdb` import (v1.4.4),
// which is the binary already loaded — no second binary, no crash.

const AGENTS_DB_PATH = path.join(os.homedir(), '.thinkdrop', 'agents.db');
const AGENTS_DIR = path.join(os.homedir(), '.thinkdrop', 'agents');

/**
 * Open agents.db, run callback(all), then close — never holds the lock between requests.
 * Using open-query-close per request so the file lock is released after every call,
 * preventing the user-memory-service from blocking withDb() in other processes.
 */
async function withAgentsDb(callback) {
  return new Promise((resolve, reject) => {
    const db = new duckdb.Database(AGENTS_DB_PATH, (openErr) => {
      if (openErr) return reject(openErr);
      const conn = db.connect();
      const all = promisify(conn.all.bind(conn));
      const run = promisify(conn.run.bind(conn));
      Promise.resolve()
        .then(() => callback({ all, run }))
        .then(result => {
          db.close(() => resolve(result));
        })
        .catch(err => {
          try { db.close(() => {}); } catch (_) { /* best-effort close on error */ }
          reject(err);
        });
    });
  });
}

/**
 * Parse a YAML-style frontmatter block from an .agent.md file.
 * Returns a flat object of key→value string pairs.
 */
function _parseFrontmatter(content) {
  const fmMatch = content.match(/^---\s*\n([\s\S]*?)\n---/);
  if (!fmMatch) return {};
  const fm = {};
  for (const line of fmMatch[1].split('\n')) {
    const colonIdx = line.indexOf(':');
    if (colonIdx === -1) continue;
    const key = line.slice(0, colonIdx).trim();
    const value = line.slice(colonIdx + 1).trim().replace(/^['"]|['"]$/g, '');
    if (key && !key.startsWith(' ') && !key.startsWith('-')) fm[key] = value;
  }
  return fm;
}

/**
 * Silent fallback: read agent metadata from ~/.thinkdrop/agents/*.agent.md files.
 * Used when agents.db is locked/unavailable. Returns same shape as DB query.
 */
function _readAgentsFromDisk() {
  const agents = [];
  try {
    if (!fs.existsSync(AGENTS_DIR)) return agents;
    const files = fs.readdirSync(AGENTS_DIR).filter(f => f.endsWith('.agent.md'));
    for (const file of files) {
      try {
        const content = fs.readFileSync(path.join(AGENTS_DIR, file), 'utf8');
        const fm = _parseFrontmatter(content);
        if (!fm.id || !fm.service) continue;
        // Parse capabilities list from frontmatter (indented YAML list items)
        const capMatch = content.match(/^capabilities:\s*\n((?:[ \t]+-[^\n]*\n?)*)/m);
        const capabilities = capMatch
          ? capMatch[1].split('\n').map(l => l.replace(/^\s*-\s*/, '').trim()).filter(Boolean)
          : [];
        agents.push({
          id: fm.id,
          type: fm.type || 'browser',
          service: fm.service,
          cli_tool: fm.cli_tool || null,
          capabilities,
        });
      } catch (_) { /* skip unreadable file */ }
    }
  } catch (_) { /* skip if dir unreadable */ }
  return agents;
}

/**
 * POST /agent.list
 * Returns all healthy registered browser/CLI agents from agents.db.
 * Used by planSkillsV2 to inject REGISTERED AGENTS context into the LLM planner prompt.
 * Body: { payload: {}, context, requestId }
 */
router.post('/agent.list', async (req, res) => {
  try {
    const rows = await withAgentsDb(({ all }) =>
      all('SELECT id, type, service, cli_tool, capabilities FROM agents WHERE status = \'healthy\' ORDER BY id')
    );
    const agents = (rows || []).map(r => ({
      id: r.id,
      type: r.type,
      service: r.service,
      cli_tool: r.cli_tool || null,
      capabilities: (() => {
        try { return r.capabilities ? JSON.parse(r.capabilities) : []; } catch { return []; }
      })(),
    }));
    return res.json({ status: 'ok', action: 'agent.list', data: agents });
  } catch (err) {
    console.warn('[agentRegistry] agents.db unavailable, falling back to disk:', err.message.slice(0, 120));
    // Silent disk fallback — read *.agent.md files instead of returning empty
    const diskAgents = _readAgentsFromDisk();
    if (diskAgents.length > 0) {
      console.log(`[agentRegistry] Disk fallback loaded ${diskAgents.length} agent(s) from ~/.thinkdrop/agents/`);
    }
    return res.json({ status: 'ok', action: 'agent.list', data: diskAgents });
  }
});

/**
 * POST /agent.update
 * Update an agent's status (and optionally failure_log) in agents.db.
 * Body: { payload: { id, status, failureLog? } }
 */
router.post('/agent.update', async (req, res) => {
  const body = req.body?.payload || req.body || {};
  const { id, status, failureLog } = body;
  if (!id || !status) {
    return res.status(400).json({ status: 'error', error: 'id and status are required' });
  }
  try {
    await withAgentsDb(({ run }) => {
      const sql = failureLog !== undefined
        ? 'UPDATE agents SET status = ?, failure_log = ?, last_validated = CURRENT_TIMESTAMP WHERE id = ?'
        : 'UPDATE agents SET status = ?, last_validated = CURRENT_TIMESTAMP WHERE id = ?';
      const params = failureLog !== undefined ? [status, failureLog, id] : [status, id];
      return run(sql, ...params);
    });
    return res.json({ status: 'ok', action: 'agent.update', data: { id, status } });
  } catch (err) {
    console.error('[agentRegistry] Failed to update agent:', err.message);
    return res.status(500).json({ status: 'error', error: err.message });
  }
});

export default router;
