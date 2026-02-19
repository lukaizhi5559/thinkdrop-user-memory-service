import { describe, test, expect } from '@jest/globals';
import {
  generateMemoryId,
  validateMemoryText,
  parseMetadata,
  formatMCPResponse,
  extractUserId,
  validateEntity,
  normalizeEntities
} from '../../src/utils/helpers.js';

describe('Helper Functions', () => {
  describe('generateMemoryId', () => {
    test('should generate valid memory ID', () => {
      const id = generateMemoryId();
      
      expect(id).toMatch(/^mem_\d+_[a-f0-9]+$/);
    });

    test('should generate unique IDs', () => {
      const id1 = generateMemoryId();
      const id2 = generateMemoryId();
      
      expect(id1).not.toBe(id2);
    });
  });

  describe('validateMemoryText', () => {
    test('should validate and trim valid text', () => {
      const text = validateMemoryText('  valid text  ');
      
      expect(text).toBe('valid text');
    });

    test('should reject empty text', () => {
      expect(() => {
        validateMemoryText('');
      }).toThrow('Memory text must be a non-empty string');
    });

    test('should reject non-string input', () => {
      expect(() => {
        validateMemoryText(123);
      }).toThrow();
    });

    test('should reject text exceeding max length', () => {
      const longText = 'a'.repeat(10001);
      
      expect(() => {
        validateMemoryText(longText);
      }).toThrow('exceeds maximum length');
    });
  });

  describe('parseMetadata', () => {
    test('should parse JSON string', () => {
      const metadata = parseMetadata('{"key": "value"}');
      
      expect(metadata).toEqual({ key: 'value' });
    });

    test('should return object as-is', () => {
      const obj = { key: 'value' };
      const metadata = parseMetadata(obj);
      
      expect(metadata).toEqual(obj);
    });

    test('should return empty object for invalid JSON', () => {
      const metadata = parseMetadata('invalid json');
      
      expect(metadata).toEqual({});
    });

    test('should return empty object for null', () => {
      const metadata = parseMetadata(null);
      
      expect(metadata).toEqual({});
    });
  });

  describe('formatMCPResponse', () => {
    test('should format MCP response correctly', () => {
      const response = formatMCPResponse(
        'memory.store',
        'req-123',
        'ok',
        { memoryId: 'mem_123' },
        null,
        { elapsedMs: 42 }
      );

      expect(response.version).toBe('mcp.v1');
      expect(response.service).toBe('user-memory');
      expect(response.action).toBe('memory.store');
      expect(response.requestId).toBe('req-123');
      expect(response.status).toBe('ok');
      expect(response.data).toEqual({ memoryId: 'mem_123' });
      expect(response.metrics).toEqual({ elapsedMs: 42 });
    });

    test('should include error in response', () => {
      const response = formatMCPResponse(
        'memory.store',
        'req-456',
        'error',
        null,
        { code: 'ERROR', message: 'Failed' }
      );

      expect(response.status).toBe('error');
      expect(response.error).toEqual({ code: 'ERROR', message: 'Failed' });
    });
  });

  describe('extractUserId', () => {
    test('should extract userId from context', () => {
      const userId = extractUserId({ userId: 'user_123' });
      
      expect(userId).toBe('user_123');
    });

    test('should return default for missing context', () => {
      const userId = extractUserId(null);
      
      expect(userId).toBe('default_user');
    });

    test('should return default for empty context', () => {
      const userId = extractUserId({});
      
      expect(userId).toBe('default_user');
    });
  });

  describe('validateEntity', () => {
    test('should validate valid entity', () => {
      const isValid = validateEntity({
        type: 'person',
        value: 'John Doe'
      });
      
      expect(isValid).toBe(true);
    });

    test('should reject entity without type', () => {
      const isValid = validateEntity({
        value: 'John Doe'
      });
      
      expect(isValid).toBe(false);
    });

    test('should reject entity without value', () => {
      const isValid = validateEntity({
        type: 'person'
      });
      
      expect(isValid).toBe(false);
    });

    test('should reject non-object entity', () => {
      const isValid = validateEntity('not an object');
      
      expect(isValid).toBe(false);
    });
  });

  describe('normalizeEntities', () => {
    test('should normalize valid entities', () => {
      const entities = normalizeEntities([
        { type: 'person', value: 'John' },
        { type: 'location', value: 'NYC' }
      ]);
      
      expect(entities.length).toBe(2);
    });

    test('should filter out invalid entities', () => {
      const entities = normalizeEntities([
        { type: 'person', value: 'John' },
        { value: 'invalid' },
        { type: 'location', value: 'NYC' }
      ]);
      
      expect(entities.length).toBe(2);
    });

    test('should limit to 100 entities', () => {
      const manyEntities = Array(150).fill({ type: 'test', value: 'test' });
      const entities = normalizeEntities(manyEntities);
      
      expect(entities.length).toBe(100);
    });

    test('should return empty array for non-array input', () => {
      const entities = normalizeEntities('not an array');
      
      expect(entities).toEqual([]);
    });
  });
});
