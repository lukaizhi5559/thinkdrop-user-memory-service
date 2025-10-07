import { describe, test, expect, beforeAll, afterAll } from '@jest/globals';
import DatabaseService from '../../src/services/database.js';

describe('DatabaseService', () => {
  let db;

  beforeAll(async () => {
    db = new DatabaseService(':memory:');
    await db.initialize();
  });

  afterAll(async () => {
    await db.close();
  });

  test('should initialize database successfully', () => {
    expect(db.isInitialized).toBe(true);
    expect(db.db).toBeDefined();
    expect(db.connection).toBeDefined();
  });

  test('should create tables on initialization', async () => {
    const tables = await db.query(`
      SELECT name FROM sqlite_master 
      WHERE type='table' AND name IN ('memory', 'memory_entities')
    `);
    
    expect(tables.length).toBe(2);
  });

  test('should execute INSERT query', async () => {
    const result = await db.execute(`
      INSERT INTO memory (id, user_id, source_text)
      VALUES ('test_001', 'user_123', 'Test memory')
    `);

    expect(result.success).toBe(true);
  });

  test('should query data', async () => {
    const results = await db.query(`
      SELECT * FROM memory WHERE id = 'test_001'
    `);

    expect(results.length).toBe(1);
    expect(results[0].id).toBe('test_001');
    expect(results[0].source_text).toBe('Test memory');
  });

  test('should get database statistics', async () => {
    const stats = await db.getStats();

    expect(stats.totalMemories).toBeGreaterThan(0);
    expect(stats.totalEntities).toBeDefined();
  });

  test('should perform health check', async () => {
    const health = await db.healthCheck();

    expect(health.status).toBe('connected');
    expect(health.database).toBe(':memory:');
  });

  test('should handle query errors gracefully', async () => {
    await expect(async () => {
      await db.query('SELECT * FROM nonexistent_table');
    }).rejects.toThrow();
  });

  test('should create indexes', async () => {
    const indexes = await db.query(`
      SELECT name FROM sqlite_master 
      WHERE type='index' AND name LIKE 'idx_memory%'
    `);

    expect(indexes.length).toBeGreaterThan(0);
  });
});
