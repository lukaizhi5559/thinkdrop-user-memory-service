import express from 'express';
import { getEmbeddingService } from '../services/embeddings.js';
import { getDatabaseService } from '../services/database.js';

const router = express.Router();

/**
 * POST /memory.health-check
 * Comprehensive health check for memory service
 */
router.post('/memory.health-check', async (req, res, next) => {
  try {
    const { requestId } = req.body;
    
    console.log('🏥 [HEALTH] Running health check...');
    
    const embeddingService = getEmbeddingService();
    const db = getDatabaseService();
    
    // 1. Check embedding service
    const embeddingStatus = {
      initialized: embeddingService.isInitialized(),
      modelInfo: embeddingService.getModelInfo()
    };
    
    // 2. Check database
    const totalMemories = await db.query('SELECT COUNT(*) as count FROM memory');
    const withEmbeddings = await db.query('SELECT COUNT(*) as count FROM memory WHERE embedding IS NOT NULL');
    const withoutEmbeddings = await db.query('SELECT COUNT(*) as count FROM memory WHERE embedding IS NULL');
    
    // Convert BigInt to Number
    const totalCount = Number(totalMemories[0].count);
    const withEmbeddingsCount = Number(withEmbeddings[0].count);
    const withoutEmbeddingsCount = Number(withoutEmbeddings[0].count);
    
    const embeddingCoverage = totalCount > 0
      ? (withEmbeddingsCount / totalCount * 100).toFixed(1)
      : '0.0';
    
    // 3. Sample a memory with embedding
    const sampleMemory = await db.query(`
      SELECT id, source_text, created_at,
             CASE WHEN embedding IS NULL THEN 'NULL' ELSE 'HAS_EMBEDDING' END as embedding_status
      FROM memory 
      WHERE embedding IS NOT NULL
      ORDER BY created_at DESC
      LIMIT 1
    `);
    
    // 4. Check recent activity
    const recentMemories = await db.query(`
      SELECT COUNT(*) as count 
      FROM memory 
      WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
    `);
    const recentCount = Number(recentMemories[0].count);
    
    const result = {
      status: 'healthy',
      timestamp: new Date().toISOString(),
      embedding: embeddingStatus,
      database: {
        totalMemories: totalCount,
        withEmbeddings: withEmbeddingsCount,
        withoutEmbeddings: withoutEmbeddingsCount,
        embeddingCoverage: embeddingCoverage + '%',
        recentActivity: recentCount
      },
      sample: sampleMemory[0] || null,
      warnings: []
    };
    
    // Add warnings
    if (!embeddingStatus.initialized) {
      result.warnings.push('Embedding service not initialized');
      result.status = 'degraded';
    }
    
    if (withoutEmbeddingsCount > 0) {
      result.warnings.push(`${withoutEmbeddingsCount} memories without embeddings`);
    }
    
    if (parseFloat(embeddingCoverage) < 100) {
      result.warnings.push(`Embedding coverage is ${embeddingCoverage}%, should be 100%`);
    }
    
    res.json({
      version: 'mcp.v1',
      service: 'user-memory',
      action: 'memory.health-check',
      requestId,
      status: 'ok',
      data: result
    });
  } catch (error) {
    next(error);
  }
});

export default router;
