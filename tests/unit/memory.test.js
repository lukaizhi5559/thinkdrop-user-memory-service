import { describe, test, expect, beforeAll, afterAll } from '@jest/globals';
import { getMemoryService } from '../../src/services/memory.js';
import { getDatabaseService } from '../../src/services/database.js';

describe('MemoryService', () => {
  let memoryService;
  let db;

  beforeAll(async () => {
    process.env.DB_PATH = ':memory:';
    db = getDatabaseService();
    await db.initialize();
    memoryService = getMemoryService();
  });

  afterAll(async () => {
    await db.close();
  });

  test('should store a memory', async () => {
    const result = await memoryService.storeMemory({
      text: 'Test memory',
      entities: [
        { type: 'person', value: 'John', entity_type: 'PERSON' }
      ]
    }, { userId: 'test_user' });

    expect(result.stored).toBe(true);
    expect(result.memoryId).toBeDefined();
    expect(result.entities).toBe(1);
  });

  test('should search memories', async () => {
    // Store a memory first
    await memoryService.storeMemory({
      text: 'Meeting with Alice tomorrow',
      entities: [
        { type: 'person', value: 'Alice', entity_type: 'PERSON' }
      ]
    }, { userId: 'test_user' });

    // Search for it
    const result = await memoryService.searchMemories(
      'meeting Alice',
      { limit: 10 },
      { userId: 'test_user' }
    );

    expect(result.results).toBeDefined();
    expect(result.total).toBeGreaterThan(0);
  });

  test('should retrieve memory by ID', async () => {
    const stored = await memoryService.storeMemory({
      text: 'Test retrieval',
    }, { userId: 'test_user' });

    const retrieved = await memoryService.retrieveMemory(
      stored.memoryId,
      { userId: 'test_user' }
    );

    expect(retrieved.id).toBe(stored.memoryId);
    expect(retrieved.text).toBe('Test retrieval');
  });

  test('should classify conversational queries', () => {
    const result1 = memoryService.classifyConversationalQuery('what did I say first?');
    expect(result1.isConversational).toBe(true);
    expect(result1.classification).toBe('POSITIONAL');

    const result2 = memoryService.classifyConversationalQuery('what is the weather?');
    expect(result2.isConversational).toBe(false);
  });
});
