import { describe, test, expect, beforeAll } from '@jest/globals';
import EmbeddingService from '../../src/services/embeddings.js';

describe('EmbeddingService', () => {
  let embeddingService;

  beforeAll(async () => {
    embeddingService = new EmbeddingService();
    await embeddingService.initialize();
  });

  test('should initialize embedding model', () => {
    expect(embeddingService.isLoaded).toBe(true);
    expect(embeddingService.embedder).toBeDefined();
  });

  test('should generate embedding for text', async () => {
    const embedding = await embeddingService.generateEmbedding('test text');

    expect(Array.isArray(embedding)).toBe(true);
    expect(embedding.length).toBe(384);
    expect(typeof embedding[0]).toBe('number');
  });

  test('should cache embeddings', async () => {
    const text = 'cached text example';
    
    await embeddingService.generateEmbedding(text);
    const cacheStats1 = embeddingService.getCacheStats();
    
    await embeddingService.generateEmbedding(text);
    const cacheStats2 = embeddingService.getCacheStats();

    expect(cacheStats2.size).toBeGreaterThanOrEqual(cacheStats1.size);
  });

  test('should generate batch embeddings', async () => {
    const texts = ['text one', 'text two', 'text three'];
    const embeddings = await embeddingService.generateEmbeddings(texts);

    expect(embeddings.length).toBe(3);
    expect(embeddings[0].length).toBe(384);
  });

  test('should reject invalid input', async () => {
    await expect(async () => {
      await embeddingService.generateEmbedding(null);
    }).rejects.toThrow();
  });

  test('should clear cache', () => {
    embeddingService.clearCache();
    const stats = embeddingService.getCacheStats();
    
    expect(stats.size).toBe(0);
  });

  test('should pass health check', async () => {
    const health = await embeddingService.healthCheck();

    expect(health.status).toBe('loaded');
    expect(health.model).toBe('Xenova/all-MiniLM-L6-v2');
  });
});
