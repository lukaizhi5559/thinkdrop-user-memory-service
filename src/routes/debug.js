import express from 'express';
import { getEmbeddingService } from '../services/embeddings.js';
import { formatMCPResponse } from '../utils/helpers.js';

const router = express.Router();

/**
 * POST /memory.debug-embedding
 * Test embedding generation for a given text
 */
router.post('/memory.debug-embedding', async (req, res, next) => {
  try {
    const { payload, requestId } = req.body;
    const { text } = payload;
    
    if (!text) {
      throw new Error('Missing required field: text');
    }
    
    console.log('🔧 [DEBUG] Testing embedding generation...');
    
    const embeddingService = getEmbeddingService();
    
    // Generate embedding
    const embedding = await embeddingService.generateEmbedding(text);
    
    // Calculate embedding statistics
    const mean = embedding.reduce((sum, val) => sum + val, 0) / embedding.length;
    const variance = embedding.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / embedding.length;
    const stdDev = Math.sqrt(variance);
    const norm = Math.sqrt(embedding.reduce((sum, val) => sum + val * val, 0));
    
    const result = {
      success: true,
      text,
      embedding: {
        dimensions: embedding.length,
        sample: embedding.slice(0, 10), // First 10 values
        statistics: {
          mean: mean.toFixed(6),
          stdDev: stdDev.toFixed(6),
          norm: norm.toFixed(6),
          min: Math.min(...embedding).toFixed(6),
          max: Math.max(...embedding).toFixed(6)
        }
      },
      modelInfo: embeddingService.getModelInfo()
    };
    
    res.json(formatMCPResponse('memory.debug-embedding', requestId, 'ok', result));
  } catch (error) {
    next(error);
  }
});

export default router;
