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
 * Generate a random string
 */
export function randomString(length = 8) {
  return crypto.randomBytes(Math.ceil(length / 2)).toString('hex').slice(0, length);
}

/**
 * Calculate cosine similarity between two vectors
 */
export function cosineSimilarity(vecA, vecB) {
  if (!vecA || !vecB || vecA.length !== vecB.length) {
    throw new Error('Invalid vectors for similarity calculation');
  }

  let dotProduct = 0;
  let normA = 0;
  let normB = 0;

  for (let i = 0; i < vecA.length; i++) {
    dotProduct += vecA[i] * vecB[i];
    normA += vecA[i] * vecA[i];
    normB += vecB[i] * vecB[i];
  }

  const denominator = Math.sqrt(normA) * Math.sqrt(normB);
  return denominator === 0 ? 0 : dotProduct / denominator;
}

/**
 * Sanitize SQL input (basic protection)
 */
export function sanitizeSqlInput(input) {
  if (typeof input !== 'string') return input;
  return input.replace(/[;\-\-]/g, '');
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
