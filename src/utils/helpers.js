import crypto from 'crypto';

/**
 * Generate a unique memory ID
 * Format: mem_[timestamp]_[random]
 */
export function generateMemoryId() {
  const timestamp = Date.now();
  const random = crypto.randomBytes(4).toString('hex');
  return `mem_${timestamp}_${random}`;
}

/**
 * Validate memory text
 */
export function validateMemoryText(text) {
  if (!text || typeof text !== 'string') {
    throw new Error('Memory text must be a non-empty string');
  }
  if (text.length > 10000) {
    throw new Error('Memory text exceeds maximum length of 10,000 characters');
  }
  return text.trim();
}

/**
 * Parse metadata safely
 */
export function parseMetadata(metadata) {
  if (!metadata) return {};
  if (typeof metadata === 'object') return metadata;
  try {
    return JSON.parse(metadata);
  } catch (error) {
    return {};
  }
}

/**
 * Format MCP response
 */
export function formatMCPResponse(action, requestId, status, data, error = null, metrics = {}) {
  return {
    version: 'mcp.v1',
    service: 'user-memory',
    action,
    requestId,
    status,
    data,
    error,
    metrics
  };
}

/**
 * Extract user ID from context
 */
export function extractUserId(context) {
  return context?.userId || 'default_user';
}

/**
 * Validate entity structure
 */
export function validateEntity(entity) {
  if (!entity || typeof entity !== 'object') {
    return false;
  }
  return !!(entity.type && entity.value);
}

/**
 * Normalize entities array
 */
export function normalizeEntities(entities) {
  if (!Array.isArray(entities)) return [];
  return entities.filter(validateEntity).slice(0, 100); // Max 100 entities
}
