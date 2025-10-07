import logger from '../utils/logger.js';

const API_KEYS = (process.env.API_KEY || '').split(',').filter(k => k.length > 0);

export function authMiddleware(req, res, next) {
  const authHeader = req.headers.authorization;

  if (!authHeader) {
    logger.warn('Missing authorization header', { ip: req.ip });
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
    logger.warn('Invalid API key', { ip: req.ip });
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
