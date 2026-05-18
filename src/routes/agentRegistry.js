import express from 'express';
import path from 'path';
import os from 'os';
import duckdb from 'duckdb';

const router = express.Router();

const AGENTS_DB_PATH = path.join(os.homedir(), '.thinkdrop', 'agents.db');

let _conn = null;

async function getAgentsConn() {
  if (_conn) return _conn;
  try {
    const raw = await new Promise((resolve, reject) => {
      const db = new duckdb.Database(AGENTS_DB_PATH, { access_mode: 'READ_ONLY' }, (err) => {
        if (err) reject(err); else resolve(db);
      });
    });
    _conn = {
      all: (sql, ...p) => new Promise((res, rej) => {
        raw.all(sql, ...p, (e, rows) => { if (e) rej(e); else res(rows); });
      }),
    };
  } catch {
    return null;
  }
  return _conn;
}

/**
 * POST /agent.list
 * Returns all healthy registered browser/CLI agents from agents.db.
 * Used by planSkillsV2 to inject REGISTERED AGENTS context into the LLM planner prompt.
 * Body: { payload: {}, context, requestId }
 */
router.post('/agent.list', async (req, res) => {
  try {
    const db = await getAgentsConn();
    if (!db) {
      return res.json({ status: 'ok', action: 'agent.list', data: [] });
    }
    const rows = await db.all(
      'SELECT id, type, service, cli_tool, capabilities FROM agents WHERE status = \'healthy\' ORDER BY id'
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
    return res.json({ status: 'ok', action: 'agent.list', data: [] });
  }
});

export default router;
