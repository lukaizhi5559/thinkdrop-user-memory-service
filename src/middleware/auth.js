import logger from '../utils/logger.js';

// Callers across the services send `Bearer $MCP_USER_MEMORY_API_KEY` and the
// root .env defines that name — honor API_KEY too, but without this fallback
// every request 401s (observed: episodic.search, /audio.segment, memory.search
// all locked out when only MCP_USER_MEMORY_API_KEY is set).
const API_KEYS = (process.env.API_KEY || process.env.MCP_USER_MEMORY_API_KEY || '').split(',').filter(k => k.length > 0);

export function authMiddleware(req, res, next) {
  const authHeader = req.headers.authorization;

  if (!authHeader) {
    logger.warn('Missing authorization header', { ip: req.ip, path: req.path, method: req.method });
    return res.status(401).json({
      version: 'mcp.v1',
      status: 'error',
      error: {
        code: 'UNAUTHORIZED',
        message: 'Missing authorization header'
      }
    });
  }

  const token = authHeader.replace('Bearer ', '');

  if (!API_KEYS.includes(token)) {
    logger.warn('Invalid API key', { ip: req.ip, path: req.path, method: req.method });
    return res.status(401).json({
      version: 'mcp.v1',
      status: 'error',
      error: {
        code: 'UNAUTHORIZED',
        message: 'Invalid API key'
      }
    });
  }

  next();
}

export default authMiddleware;
