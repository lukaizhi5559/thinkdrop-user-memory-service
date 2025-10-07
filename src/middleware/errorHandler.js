import logger from '../utils/logger.js';

export function errorHandler(err, req, res, next) {
  logger.error('Request error', {
    error: err.message,
    stack: err.stack,
    action: req.body?.action,
    requestId: req.body?.requestId
  });

  // Determine error code
  let code = 'INTERNAL_ERROR';
  let status = 500;

  if (err.message.includes('not found')) {
    code = 'NOT_FOUND';
    status = 404;
  } else if (err.message.includes('Invalid') || err.message.includes('validation')) {
    code = 'INVALID_REQUEST';
    status = 400;
  } else if (err.message.includes('Database') || err.message.includes('database')) {
    code = 'DATABASE_ERROR';
    status = 500;
  } else if (err.message.includes('Embedding') || err.message.includes('embedding')) {
    code = 'EMBEDDING_FAILED';
    status = 500;
  }

  res.status(status).json({
    version: 'mcp.v1',
    service: 'user-memory',
    action: req.body?.action || 'unknown',
    requestId: req.body?.requestId || null,
    status: 'error',
    data: null,
    error: {
      code,
      message: err.message
    }
  });
}

export default errorHandler;
