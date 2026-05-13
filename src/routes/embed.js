import express from 'express';
import { getEmbeddingService } from '../services/embeddings.js';

const router = express.Router();

router.post('/memory.embed', async (req, res, next) => {
  try {
    const { payload } = req.body;
    const texts = payload?.texts;

    if (!Array.isArray(texts) || texts.length === 0) {
      return res.status(400).json({
        version: 'mcp.v1',
        status: 'error',
        error: { code: 'BAD_REQUEST', message: 'payload.texts must be a non-empty array of strings' }
      });
    }

    const embeddings = getEmbeddingService();
    if (!embeddings.isInitialized()) {
      return res.status(503).json({
        version: 'mcp.v1',
        status: 'error',
        error: { code: 'SERVICE_UNAVAILABLE', message: 'Embedding model not initialized' }
      });
    }

    const vectors = await embeddings.generateEmbeddings(texts);

    res.json({
      version: 'mcp.v1',
      status: 'ok',
      result: { embeddings: vectors, count: vectors.length }
    });
  } catch (error) {
    next(error);
  }
});

export default router;
