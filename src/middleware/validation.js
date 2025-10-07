import logger from '../utils/logger.js';

/**
 * Validate MCP request envelope
 */
export function validateMCPRequest(req, res, next) {
  const { version, service, action, payload } = req.body;

  if (!version || version !== 'mcp.v1') {
    return res.status(400).json({
      version: 'mcp.v1',
      status: 'error',
      error: {
        code: 'INVALID_REQUEST',
        message: 'Invalid or missing MCP version'
      }
    });
  }

  if (!service || service !== 'user-memory') {
    return res.status(400).json({
      version: 'mcp.v1',
      status: 'error',
      error: {
        code: 'INVALID_REQUEST',
        message: 'Invalid or missing service name'
      }
    });
  }

  if (!action) {
    return res.status(400).json({
      version: 'mcp.v1',
      status: 'error',
      error: {
        code: 'INVALID_REQUEST',
        message: 'Missing action'
      }
    });
  }

  if (!payload) {
    return res.status(400).json({
      version: 'mcp.v1',
      status: 'error',
      error: {
        code: 'INVALID_REQUEST',
        message: 'Missing payload'
      }
    });
  }

  next();
}

/**
 * Validate payload size
 */
export function validatePayloadSize(req, res, next) {
  const maxSize = 1024 * 1024; // 1MB
  const contentLength = parseInt(req.headers['content-length'] || '0');

  if (contentLength > maxSize) {
    logger.warn('Payload too large', { contentLength, maxSize });
    return res.status(413).json({
      version: 'mcp.v1',
      status: 'error',
      error: {
        code: 'PAYLOAD_TOO_LARGE',
        message: 'Payload exceeds maximum size of 1MB'
      }
    });
  }

  next();
}

export default { validateMCPRequest, validatePayloadSize };
