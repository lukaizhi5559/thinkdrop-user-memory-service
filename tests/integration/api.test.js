import { describe, test, expect, beforeAll, afterAll } from '@jest/globals';
import request from 'supertest';
import app from '../../src/server.js';
import { getDatabaseService } from '../../src/services/database.js';

describe('UserMemory API Integration Tests', () => {
  let db;
  const API_KEY = 'k7F9qLp3XzR2vH8sT1mN4bC0yW6uJ5eQG4tY9bH2wQ6nM1vS8xR3cL5pZ0kF7uDe';
  let testMemoryId;

  beforeAll(async () => {
    process.env.NODE_ENV = 'test';
    process.env.API_KEY = API_KEY;
    process.env.DB_PATH = ':memory:';
    process.env.LOG_LEVEL = 'error';
    
    db = getDatabaseService();
    await db.initialize();
  });

  afterAll(async () => {
    await db.close();
  });

  describe('Health & Capabilities', () => {
    test('GET /service.health - should return service health', async () => {
      const response = await request(app)
        .get('/service.health')
        .expect(200);

      expect(response.body.service).toBe('user-memory');
      expect(response.body.status).toBe('up');
      expect(response.body.database).toBe('connected');
    });

    test('GET /service.capabilities - should return service capabilities', async () => {
      const response = await request(app)
        .get('/service.capabilities')
        .expect(200);

      expect(response.body.service).toBe('user-memory');
      expect(response.body.capabilities.actions).toHaveLength(7);
      expect(response.body.capabilities.features).toContain('semantic-search');
    });
  });

  describe('Authentication', () => {
    test('should reject requests without API key', async () => {
      const response = await request(app)
        .post('/memory.store')
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.store',
          payload: { text: 'test' }
        })
        .expect(401);

      expect(response.body.error.code).toBe('UNAUTHORIZED');
    });

    test('should reject requests with invalid API key', async () => {
      const response = await request(app)
        .post('/memory.store')
        .set('Authorization', 'Bearer invalid-key')
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.store',
          payload: { text: 'test' }
        })
        .expect(401);

      expect(response.body.error.code).toBe('UNAUTHORIZED');
    });
  });

  describe('MCP Protocol Validation', () => {
    test('should reject invalid MCP version', async () => {
      const response = await request(app)
        .post('/memory.store')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'invalid',
          service: 'user-memory',
          action: 'memory.store',
          payload: { text: 'test' }
        })
        .expect(400);

      expect(response.body.error.code).toBe('INVALID_REQUEST');
    });

    test('should reject invalid service name', async () => {
      const response = await request(app)
        .post('/memory.store')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'wrong-service',
          action: 'memory.store',
          payload: { text: 'test' }
        })
        .expect(400);

      expect(response.body.error.code).toBe('INVALID_REQUEST');
    });
  });

  describe('POST /memory.store', () => {
    test('should store a memory successfully', async () => {
      const response = await request(app)
        .post('/memory.store')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.store',
          requestId: 'req-001',
          context: { userId: 'test_user' },
          payload: {
            text: 'I have a meeting with Dr. Smith next Tuesday at 3pm',
            entities: [
              { type: 'person', value: 'Dr. Smith', entity_type: 'PERSON' },
              { type: 'datetime', value: 'next Tuesday at 3pm', entity_type: 'DATE' }
            ],
            metadata: {
              category: 'appointment',
              tags: ['medical']
            }
          }
        })
        .expect(200);

      expect(response.body.status).toBe('ok');
      expect(response.body.data.stored).toBe(true);
      expect(response.body.data.memoryId).toBeDefined();
      expect(response.body.data.entities).toBe(2);
      
      testMemoryId = response.body.data.memoryId;
    });

    test('should reject memory without text', async () => {
      const response = await request(app)
        .post('/memory.store')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.store',
          requestId: 'req-002',
          context: { userId: 'test_user' },
          payload: {
            entities: []
          }
        })
        .expect(500);

      expect(response.body.status).toBe('error');
    });

    test('should store memory with screenshot', async () => {
      const response = await request(app)
        .post('/memory.store')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.store',
          requestId: 'req-003',
          context: { userId: 'test_user' },
          payload: {
            text: 'Screenshot of documentation page',
            screenshot: '/screenshots/doc_page.png',
            extractedText: 'API Documentation for UserMemory Service'
          }
        })
        .expect(200);

      expect(response.body.data.stored).toBe(true);
    });
  });

  describe('POST /memory.search', () => {
    test('should search memories semantically', async () => {
      const response = await request(app)
        .post('/memory.search')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.search',
          requestId: 'req-004',
          context: { userId: 'test_user' },
          payload: {
            query: 'doctor appointment',
            limit: 10,
            minSimilarity: 0.3
          }
        })
        .expect(200);

      expect(response.body.status).toBe('ok');
      expect(response.body.data.results).toBeDefined();
      expect(Array.isArray(response.body.data.results)).toBe(true);
      expect(response.body.data.query).toBe('doctor appointment');
    });

    test('should filter search by session', async () => {
      const response = await request(app)
        .post('/memory.search')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.search',
          requestId: 'req-005',
          context: { userId: 'test_user' },
          payload: {
            query: 'meeting',
            sessionId: 'session_123',
            limit: 5
          }
        })
        .expect(200);

      expect(response.body.status).toBe('ok');
    });

    test('should reject search without query', async () => {
      const response = await request(app)
        .post('/memory.search')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.search',
          requestId: 'req-006',
          context: { userId: 'test_user' },
          payload: {
            limit: 10
          }
        })
        .expect(500);

      expect(response.body.status).toBe('error');
    });
  });

  describe('POST /memory.retrieve', () => {
    test('should retrieve memory by ID', async () => {
      const response = await request(app)
        .post('/memory.retrieve')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.retrieve',
          requestId: 'req-007',
          context: { userId: 'test_user' },
          payload: {
            memoryId: testMemoryId
          }
        })
        .expect(200);

      expect(response.body.status).toBe('ok');
      expect(response.body.data.memory.id).toBe(testMemoryId);
      expect(response.body.data.memory.text).toContain('Dr. Smith');
    });

    test('should return error for non-existent memory', async () => {
      const response = await request(app)
        .post('/memory.retrieve')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.retrieve',
          requestId: 'req-008',
          context: { userId: 'test_user' },
          payload: {
            memoryId: 'mem_nonexistent_123'
          }
        })
        .expect(404);

      expect(response.body.status).toBe('error');
      expect(response.body.error.code).toBe('NOT_FOUND');
    });
  });

  describe('POST /memory.update', () => {
    test('should update memory text', async () => {
      const response = await request(app)
        .post('/memory.update')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.update',
          requestId: 'req-009',
          context: { userId: 'test_user' },
          payload: {
            memoryId: testMemoryId,
            updates: {
              text: 'I have a meeting with Dr. Smith next Wednesday at 3pm',
              entities: [
                { type: 'person', value: 'Dr. Smith', entity_type: 'PERSON' },
                { type: 'datetime', value: 'next Wednesday at 3pm', entity_type: 'DATE' }
              ]
            }
          }
        })
        .expect(200);

      expect(response.body.status).toBe('ok');
      expect(response.body.data.updated).toBe(true);
      expect(response.body.data.embedding).toBe(true);
    });

    test('should reject update without memoryId', async () => {
      const response = await request(app)
        .post('/memory.update')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.update',
          requestId: 'req-010',
          context: { userId: 'test_user' },
          payload: {
            updates: { text: 'new text' }
          }
        })
        .expect(500);

      expect(response.body.status).toBe('error');
    });
  });

  describe('POST /memory.list', () => {
    test('should list memories with pagination', async () => {
      const response = await request(app)
        .post('/memory.list')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.list',
          requestId: 'req-011',
          context: { userId: 'test_user' },
          payload: {
            limit: 10,
            offset: 0,
            sortBy: 'created_at',
            sortOrder: 'DESC'
          }
        })
        .expect(200);

      expect(response.body.status).toBe('ok');
      expect(response.body.data.memories).toBeDefined();
      expect(Array.isArray(response.body.data.memories)).toBe(true);
      expect(response.body.data.total).toBeGreaterThan(0);
    });

    test('should filter memories by type', async () => {
      const response = await request(app)
        .post('/memory.list')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.list',
          requestId: 'req-012',
          context: { userId: 'test_user' },
          payload: {
            filters: {
              type: 'user_memory'
            },
            limit: 25
          }
        })
        .expect(200);

      expect(response.body.status).toBe('ok');
    });
  });

  describe('POST /memory.classify-conversational-query', () => {
    test('should classify positional query', async () => {
      const response = await request(app)
        .post('/memory.classify-conversational-query')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.classify-conversational-query',
          requestId: 'req-013',
          context: { userId: 'test_user' },
          payload: {
            query: 'what did I say first?'
          }
        })
        .expect(200);

      expect(response.body.status).toBe('ok');
      expect(response.body.data.isConversational).toBe(true);
      expect(response.body.data.classification).toBe('POSITIONAL');
      expect(response.body.data.confidence).toBeGreaterThan(0.9);
    });

    test('should classify topical query', async () => {
      const response = await request(app)
        .post('/memory.classify-conversational-query')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.classify-conversational-query',
          requestId: 'req-014',
          context: { userId: 'test_user' },
          payload: {
            query: 'what topics did we discuss?'
          }
        })
        .expect(200);

      expect(response.body.data.isConversational).toBe(true);
      expect(response.body.data.classification).toBe('TOPICAL');
    });

    test('should classify overview query', async () => {
      const response = await request(app)
        .post('/memory.classify-conversational-query')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.classify-conversational-query',
          requestId: 'req-015',
          context: { userId: 'test_user' },
          payload: {
            query: 'summarize our conversation'
          }
        })
        .expect(200);

      expect(response.body.data.isConversational).toBe(true);
      expect(response.body.data.classification).toBe('OVERVIEW');
    });

    test('should classify non-conversational query', async () => {
      const response = await request(app)
        .post('/memory.classify-conversational-query')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.classify-conversational-query',
          requestId: 'req-016',
          context: { userId: 'test_user' },
          payload: {
            query: 'what is the weather today?'
          }
        })
        .expect(200);

      expect(response.body.data.isConversational).toBe(false);
      expect(response.body.data.classification).toBe('GENERAL');
    });
  });

  describe('POST /memory.delete', () => {
    test('should delete memory by ID', async () => {
      const response = await request(app)
        .post('/memory.delete')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.delete',
          requestId: 'req-017',
          context: { userId: 'test_user' },
          payload: {
            memoryId: testMemoryId
          }
        })
        .expect(200);

      expect(response.body.status).toBe('ok');
      expect(response.body.data.deleted).toBe(true);
    });

    test('should reject delete without memoryId', async () => {
      const response = await request(app)
        .post('/memory.delete')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.delete',
          requestId: 'req-018',
          context: { userId: 'test_user' },
          payload: {}
        })
        .expect(500);

      expect(response.body.status).toBe('error');
    });
  });

  describe('Metrics', () => {
    test('should include metrics in response', async () => {
      const response = await request(app)
        .post('/memory.store')
        .set('Authorization', `Bearer ${API_KEY}`)
        .send({
          version: 'mcp.v1',
          service: 'user-memory',
          action: 'memory.store',
          requestId: 'req-019',
          context: { userId: 'test_user' },
          payload: {
            text: 'Test memory for metrics'
          }
        })
        .expect(200);

      expect(response.body.metrics).toBeDefined();
      expect(response.body.metrics.elapsedMs).toBeDefined();
      expect(typeof response.body.metrics.elapsedMs).toBe('number');
    });
  });
});
