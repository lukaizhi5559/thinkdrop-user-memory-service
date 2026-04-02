import express from 'express';
import { getDatabaseService } from '../services/database.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();

function db() {
  return getDatabaseService();
}

/**
 * POST /pending_tasks.create
 * Register a new long-running task record.
 * Body: { payload: { id, original_prompt, sub_prompt, intent, step_order, plan_context,
 *                    completion_signal, completion_arg, session_id, user_id? }, requestId }
 */
router.post('/pending_tasks.create', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const {
      id, original_prompt, sub_prompt, intent, step_order,
      plan_context, completion_signal, completion_arg,
      session_id, user_id = 'default',
    } = payload || {};

    if (!id || !original_prompt || !sub_prompt || !intent || step_order == null) {
      return res.status(400).json({ error: 'Missing required fields: id, original_prompt, sub_prompt, intent, step_order' });
    }

    await db().run(
      `INSERT INTO pending_tasks
        (id, original_prompt, sub_prompt, intent, step_order, plan_context,
         status, completion_signal, completion_arg, session_id, user_id)
       VALUES (?, ?, ?, ?, ?, ?, 'running', ?, ?, ?, ?)`,
      [
        id, original_prompt, sub_prompt, intent, step_order,
        plan_context || null, completion_signal || 'waitForContent',
        completion_arg || '', session_id || null, user_id,
      ]
    );

    res.json(formatMCPResponse('pending_tasks.create', requestId, 'ok', { id }));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /pending_tasks.list
 * List tasks by status or fetch a single task by id.
 * Body: { payload: { status?: string, id?: string, limit?: number }, requestId }
 */
router.post('/pending_tasks.list', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const { status, id, limit = 100 } = payload || {};

    let tasks;
    if (id) {
      tasks = await db().all(
        'SELECT * FROM pending_tasks WHERE id = ?',
        [id]
      );
    } else if (status) {
      tasks = await db().all(
        'SELECT * FROM pending_tasks WHERE status = ? ORDER BY started_at DESC LIMIT ?',
        [status, limit]
      );
    } else {
      tasks = await db().all(
        'SELECT * FROM pending_tasks ORDER BY started_at DESC LIMIT ?',
        [limit]
      );
    }

    res.json(formatMCPResponse('pending_tasks.list', requestId, 'ok', { tasks: tasks || [] }));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /pending_tasks.update
 * Update status/result for a task by id.
 * Body: { payload: { id, status?, result?, error_text?, completed_at? }, requestId }
 */
router.post('/pending_tasks.update', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const { id, status, result, error_text, completed_at } = payload || {};

    if (!id) {
      return res.status(400).json({ error: 'Missing required field: id' });
    }

    const setClauses = [];
    const values = [];

    if (status !== undefined)       { setClauses.push('status = ?');       values.push(status); }
    if (result !== undefined)        { setClauses.push('result = ?');        values.push(result); }
    if (error_text !== undefined)    { setClauses.push('error_text = ?');    values.push(error_text); }
    if (completed_at !== undefined)  { setClauses.push('completed_at = ?');  values.push(completed_at); }

    if (setClauses.length === 0) {
      return res.status(400).json({ error: 'No fields to update' });
    }

    values.push(id);
    await db().run(
      `UPDATE pending_tasks SET ${setClauses.join(', ')} WHERE id = ?`,
      values
    );

    res.json(formatMCPResponse('pending_tasks.update', requestId, 'ok', { id }));
  } catch (error) {
    next(error);
  }
});

/**
 * POST /pending_tasks.cancel
 * Cancel a task by id — sets status to 'cancelled'.
 * Body: { payload: { id }, requestId }
 */
router.post('/pending_tasks.cancel', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const { id } = payload || {};

    if (!id) {
      return res.status(400).json({ error: 'Missing required field: id' });
    }

    await db().run(
      `UPDATE pending_tasks SET status = 'cancelled', completed_at = CURRENT_TIMESTAMP WHERE id = ?`,
      [id]
    );

    res.json(formatMCPResponse('pending_tasks.cancel', requestId, 'ok', { id, status: 'cancelled' }));
  } catch (error) {
    next(error);
  }
});

export default router;
