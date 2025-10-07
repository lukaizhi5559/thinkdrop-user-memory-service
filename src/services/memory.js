import { getDatabaseService } from './database.js';
import { getEmbeddingService } from './embeddings.js';
import { 
  generateMemoryId, 
  validateMemoryText, 
  parseMetadata, 
  normalizeEntities,
  extractUserId 
} from '../utils/helpers.js';
import logger from '../utils/logger.js';

class MemoryService {
  constructor() {
    this.db = getDatabaseService();
    this.embeddings = getEmbeddingService();
  }

  /**
   * Store a new memory
   */
  async storeMemory(data, context = {}) {
    const startTime = Date.now();
    
    try {
      // Validate input
      const text = validateMemoryText(data.text);
      const userId = extractUserId(context) || data.userId || 'default_user';
      const memoryId = generateMemoryId();

      // Parse metadata
      const metadata = parseMetadata(data.metadata);
      const metadataJson = JSON.stringify(metadata);

      // Normalize entities
      const entities = normalizeEntities(data.entities || []);

      // Generate embedding
      let embedding = null;
      let embeddingGenerated = false;
      try {
        const embeddingArray = await this.embeddings.generateEmbedding(text);
        embedding = embeddingArray;
        embeddingGenerated = true;
      } catch (error) {
        logger.warn('Failed to generate embedding, storing without it', { 
          error: error.message 
        });
      }

      // Prepare SQL with embedding
      let sql;
      if (embedding) {
        // Use list_value to ensure proper type
        const embeddingValues = embedding.map(v => v.toString()).join(',');
        sql = `
          INSERT INTO memory (
            id, user_id, type, primary_intent, requires_memory_access,
            suggested_response, source_text, metadata, screenshot, 
            extracted_text, embedding, created_at, updated_at
          ) VALUES (
            '${memoryId}', '${userId}', '${data.type || 'user_memory'}',
            ${data.primary_intent ? `'${data.primary_intent}'` : 'NULL'},
            ${data.requires_memory_access || false},
            ${data.suggested_response ? `'${data.suggested_response.replace(/'/g, "''")}'` : 'NULL'},
            '${text.replace(/'/g, "''")}',
            '${metadataJson.replace(/'/g, "''")}',
            ${data.screenshot ? `'${data.screenshot}'` : 'NULL'},
            ${data.extractedText ? `'${data.extractedText.replace(/'/g, "''")}'` : 'NULL'},
            list_value(${embeddingValues}),
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
          )
        `;
      } else {
        sql = `
          INSERT INTO memory (
            id, user_id, type, primary_intent, requires_memory_access,
            suggested_response, source_text, metadata, screenshot, 
            extracted_text, created_at, updated_at
          ) VALUES (
            '${memoryId}', '${userId}', '${data.type || 'user_memory'}',
            ${data.primary_intent ? `'${data.primary_intent}'` : 'NULL'},
            ${data.requires_memory_access || false},
            ${data.suggested_response ? `'${data.suggested_response.replace(/'/g, "''")}'` : 'NULL'},
            '${text.replace(/'/g, "''")}',
            '${metadataJson.replace(/'/g, "''")}',
            ${data.screenshot ? `'${data.screenshot}'` : 'NULL'},
            ${data.extractedText ? `'${data.extractedText.replace(/'/g, "''")}'` : 'NULL'},
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
          )
        `;
      }

      await this.db.execute(sql);

      // Store entities
      for (const entity of entities) {
        try {
          const entityId = `ent_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
          const normalizedValue = entity.normalized || entity.value;
          const entitySql = `
            INSERT INTO memory_entities (id, memory_id, entity, type, entity_type, normalized_value)
            VALUES (
              '${entityId}',
              '${memoryId}',
              '${entity.value.replace(/'/g, "''")}',
              '${entity.type}',
              '${entity.entity_type || entity.type}',
              '${normalizedValue.replace(/'/g, "''")}'
            )
          `;
          await this.db.execute(entitySql);
        } catch (error) {
          logger.error('Failed to store entity', { 
            memoryId,
            entity, 
            error: error.message
          });
          // Don't throw - continue with other entities
        }
      }

      const elapsedMs = Date.now() - startTime;

      logger.info('Memory stored successfully', {
        memoryId,
        userId,
        entitiesCount: entities.length,
        hasEmbedding: embeddingGenerated,
        elapsedMs
      });

      return {
        memoryId,
        stored: true,
        embedding: embeddingGenerated,
        entities: entities.length,
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      logger.error('Failed to store memory', { error: error.message });
      throw error;
    }
  }

  /**
   * Search memories using semantic similarity
   */
  async searchMemories(query, options = {}, context = {}) {
    const startTime = Date.now();

    try {
      const userId = extractUserId(context) || options.userId || 'default_user';
      const limit = options.limit || 25;
      const offset = options.offset || 0;
      const minSimilarity = options.minSimilarity || parseFloat(process.env.MIN_SIMILARITY_THRESHOLD) || 0.3;

      // Generate query embedding
      const queryEmbedding = await this.embeddings.generateEmbedding(query);
      
      // Build WHERE clause
      let whereConditions = [`user_id = '${userId}'`];
      
      if (options.filters) {
        if (options.filters.type) {
          whereConditions.push(`type = '${options.filters.type}'`);
        }
        if (options.filters.sessionId) {
          whereConditions.push(`metadata LIKE '%"sessionId":"${options.filters.sessionId}"%'`);
        }
      }

      if (options.sessionId) {
        whereConditions.push(`metadata LIKE '%"sessionId":"${options.sessionId}"%'`);
      }

      const whereClause = whereConditions.length > 0 
        ? `WHERE ${whereConditions.join(' AND ')} AND embedding IS NOT NULL` 
        : 'WHERE embedding IS NOT NULL';

      // Search with similarity - use list_value to create proper FLOAT[384] type
      const embeddingValues = queryEmbedding.map(v => v.toString()).join(',');
      const sql = `
        SELECT 
          id,
          source_text,
          metadata,
          screenshot,
          extracted_text,
          created_at,
          list_cosine_similarity(embedding, list_value(${embeddingValues})) as similarity
        FROM memory
        ${whereClause}
        ORDER BY list_cosine_similarity(embedding, list_value(${embeddingValues})) DESC
        LIMIT ${limit}
        OFFSET ${offset}
      `;

      const results = await this.db.query(sql);

      // Filter by minimum similarity and fetch entities for each result
      const filteredResults = results.filter(r => r.similarity >= minSimilarity);
      
      const enrichedResults = await Promise.all(
        filteredResults.map(async (result) => {
          const entitiesSql = `
            SELECT entity, type, entity_type
            FROM memory_entities
            WHERE memory_id = '${result.id}'
          `;
          const entities = await this.db.query(entitiesSql);

          return {
            id: result.id,
            text: result.source_text,
            similarity: result.similarity,
            entities: entities.map(e => ({
              type: e.type,
              value: e.entity,
              entity_type: e.entity_type
            })),
            metadata: parseMetadata(result.metadata),
            screenshot: result.screenshot,
            extractedText: result.extracted_text,
            created_at: result.created_at
          };
        })
      );

      const elapsedMs = Date.now() - startTime;

      logger.info('Memory search completed', {
        query,
        resultsCount: enrichedResults.length,
        elapsedMs
      });

      return {
        results: enrichedResults,
        total: enrichedResults.length,
        query,
        elapsedMs
      };
    } catch (error) {
      logger.error('Memory search failed', { error: error.message });
      throw error;
    }
  }

  /**
   * Retrieve memory by ID
   */
  async retrieveMemory(memoryId, context = {}) {
    try {
      const userId = extractUserId(context);

      const sql = `
        SELECT * FROM memory
        WHERE id = '${memoryId}' AND user_id = '${userId}'
      `;

      const results = await this.db.query(sql);

      if (results.length === 0) {
        throw new Error('Memory not found');
      }

      const memory = results[0];

      // Fetch entities
      const entitiesSql = `
        SELECT entity, type, entity_type
        FROM memory_entities
        WHERE memory_id = '${memoryId}'
      `;
      const entities = await this.db.query(entitiesSql);

      return {
        id: memory.id,
        text: memory.source_text,
        entities: entities.map(e => ({
          type: e.type,
          value: e.entity,
          entity_type: e.entity_type
        })),
        metadata: parseMetadata(memory.metadata),
        screenshot: memory.screenshot,
        extractedText: memory.extracted_text,
        created_at: memory.created_at,
        updated_at: memory.updated_at
      };
    } catch (error) {
      logger.error('Failed to retrieve memory', { memoryId, error: error.message });
      throw error;
    }
  }

  /**
   * Update memory
   */
  async updateMemory(memoryId, updates, context = {}) {
    const startTime = Date.now();

    try {
      const userId = extractUserId(context);

      // Verify memory exists
      const existing = await this.retrieveMemory(memoryId, context);

      const updateFields = [];
      let regenerateEmbedding = false;

      if (updates.text) {
        const text = validateMemoryText(updates.text);
        updateFields.push(`source_text = '${text.replace(/'/g, "''")}'`);
        regenerateEmbedding = true;
      }

      if (updates.metadata) {
        const metadataJson = JSON.stringify(updates.metadata);
        updateFields.push(`metadata = '${metadataJson.replace(/'/g, "''")}'`);
      }

      if (updates.screenshot !== undefined) {
        updateFields.push(`screenshot = ${updates.screenshot ? `'${updates.screenshot}'` : 'NULL'}`);
      }

      if (updates.extractedText !== undefined) {
        updateFields.push(`extracted_text = ${updates.extractedText ? `'${updates.extractedText.replace(/'/g, "''")}'` : 'NULL'}`);
      }

      updateFields.push('updated_at = CURRENT_TIMESTAMP');

      // Generate embedding first if needed
      let embedding = null;
      if (regenerateEmbedding) {
        embedding = await this.embeddings.generateEmbedding(updates.text);
      }

      // Use a different approach - delete and insert to avoid DuckDB UPDATE constraint bug
      const deleteSQL = `DELETE FROM memory WHERE id = '${memoryId}' AND user_id = '${userId}'`;
      
      // Get current data first including embedding
      const currentMemorySQL = `SELECT * FROM memory WHERE id = '${memoryId}' AND user_id = '${userId}'`;
      const currentMemoryResult = await this.db.query(currentMemorySQL);
      const currentMemory = currentMemoryResult[0];
      
      // Delete old record
      await this.db.execute(deleteSQL);
      
      // Prepare embedding value using list_value
      let embeddingValue;
      if (embedding) {
        const embeddingValues = embedding.map(v => v.toString()).join(',');
        embeddingValue = `list_value(${embeddingValues})`;
      } else if (currentMemory.embedding) {
        const embeddingValues = currentMemory.embedding.map(v => v.toString()).join(',');
        embeddingValue = `list_value(${embeddingValues})`;
      } else {
        embeddingValue = 'NULL';
      }
      
      // Insert updated record
      const insertSQL = `
        INSERT INTO memory (
          id, user_id, source_text, embedding, metadata, screenshot, 
          extracted_text, type, created_at, updated_at
        ) VALUES (
          '${memoryId}',
          '${userId}',
          '${updates.text ? validateMemoryText(updates.text).replace(/'/g, "''") : currentMemory.source_text.replace(/'/g, "''")}',
          ${embeddingValue},
          '${updates.metadata ? JSON.stringify(updates.metadata).replace(/'/g, "''") : (currentMemory.metadata || '{}').replace(/'/g, "''")}',
          ${updates.screenshot !== undefined ? (updates.screenshot ? `'${updates.screenshot}'` : 'NULL') : (currentMemory.screenshot ? `'${currentMemory.screenshot}'` : 'NULL')},
          ${updates.extractedText !== undefined ? (updates.extractedText ? `'${updates.extractedText.replace(/'/g, "''")}'` : 'NULL') : (currentMemory.extracted_text ? `'${currentMemory.extracted_text.replace(/'/g, "''")}'` : 'NULL')},
          'user_memory',
          '${new Date(currentMemory.created_at).toISOString().slice(0, 19).replace('T', ' ')}',
          CURRENT_TIMESTAMP
        )
      `;
      
      await this.db.execute(insertSQL);

      // Update entities if provided
      if (updates.entities) {
        // Delete old entities
        await this.db.execute(`DELETE FROM memory_entities WHERE memory_id = '${memoryId}'`);

        // Insert new entities
        const entities = normalizeEntities(updates.entities);
        for (const entity of entities) {
          const entityId = `ent_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
          const normalizedValue = entity.normalized || entity.value;
          const entitySql = `
            INSERT INTO memory_entities (id, memory_id, entity, type, entity_type, normalized_value)
            VALUES (
              '${entityId}',
              '${memoryId}',
              '${entity.value.replace(/'/g, "''")}',
              '${entity.type}',
              '${entity.entity_type || entity.type}',
              '${normalizedValue.replace(/'/g, "''")}'
            )
          `;
          await this.db.execute(entitySql);
        }
      }

      const elapsedMs = Date.now() - startTime;

      logger.info('Memory updated successfully', {
        memoryId,
        fieldsUpdated: updateFields.length,
        elapsedMs
      });

      return {
        memoryId,
        updated: true,
        embedding: regenerateEmbedding,
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      logger.error('Failed to update memory', { memoryId, error: error.message });
      throw error;
    }
  }

  /**
   * Delete memory
   */
  async deleteMemory(memoryId, context = {}) {
    try {
      const userId = extractUserId(context);

      // Delete entities first
      await this.db.execute(`DELETE FROM memory_entities WHERE memory_id = '${memoryId}'`);

      // Delete memory
      const sql = `
        DELETE FROM memory
        WHERE id = '${memoryId}' AND user_id = '${userId}'
      `;

      await this.db.execute(sql);

      logger.info('Memory deleted successfully', { memoryId });

      return {
        memoryId,
        deleted: true
      };
    } catch (error) {
      logger.error('Failed to delete memory', { memoryId, error: error.message });
      throw error;
    }
  }

  /**
   * List memories with pagination
   */
  async listMemories(options = {}, context = {}) {
    try {
      const userId = extractUserId(context);
      const limit = options.limit || 25;
      const offset = options.offset || 0;
      const sortBy = options.sortBy || 'created_at';
      const sortOrder = options.sortOrder || 'DESC';

      // Build WHERE clause
      let whereConditions = [`user_id = '${userId}'`];

      if (options.filters) {
        if (options.filters.type) {
          whereConditions.push(`type = '${options.filters.type}'`);
        }
        if (options.filters.sessionId) {
          whereConditions.push(`metadata LIKE '%"sessionId":"${options.filters.sessionId}"%'`);
        }
      }

      const whereClause = whereConditions.length > 0 
        ? `WHERE ${whereConditions.join(' AND ')}` 
        : '';

      // Get total count
      const countSql = `SELECT COUNT(*) as total FROM memory ${whereClause}`;
      const countResult = await this.db.query(countSql);
      const total = Number(countResult[0]?.total) || 0;

      // Get memories
      const sql = `
        SELECT id, source_text, metadata, created_at, updated_at
        FROM memory
        ${whereClause}
        ORDER BY ${sortBy} ${sortOrder}
        LIMIT ${limit}
        OFFSET ${offset}
      `;

      const results = await this.db.query(sql);

      // Fetch entities for each result
      const memories = await Promise.all(
        results.map(async (result) => {
          const entitiesSql = `
            SELECT entity, type, entity_type
            FROM memory_entities
            WHERE memory_id = '${result.id}'
          `;
          const entities = await this.db.query(entitiesSql);

          return {
            id: result.id,
            text: result.source_text,
            entities: entities.map(e => ({
              type: e.type,
              value: e.entity
            })),
            metadata: parseMetadata(result.metadata),
            created_at: result.created_at,
            updated_at: result.updated_at
          };
        })
      );

      return {
        memories,
        total,
        limit,
        offset,
        hasMore: offset + limit < total
      };
    } catch (error) {
      logger.error('Failed to list memories', { error: error.message });
      throw error;
    }
  }

  /**
   * Classify if query is conversational
   */
  classifyConversationalQuery(query, options = {}) {
    const queryLower = query.toLowerCase();

    // Positional patterns
    const positionalPatterns = [
      /\b(first|last|earlier|previous|initial|latest)\b/i,
      /\b(what did (i|we) (say|ask|discuss|mention))\b/i,
      /\b(beginning|start|end)\b/i
    ];

    // Topical patterns
    const topicalPatterns = [
      /\b(what (did|have) (we|i) (discuss|talk about|cover))\b/i,
      /\b(topics (we|i) (discussed|covered|mentioned))\b/i,
      /\b(what topics (did|have) (we|i) (discuss|cover|talk about))\b/i,
      /\b(conversation about)\b/i
    ];

    // Overview patterns
    const overviewPatterns = [
      /\b(summarize|recap|sum up|overview)\b/i,
      /\b(what (did|have) (we|i) (chat|talk) about)\b/i
    ];

    let classification = 'GENERAL';
    let confidence = 0.5;
    let reasoning = 'No specific conversation patterns detected';

    if (positionalPatterns.some(p => p.test(queryLower))) {
      classification = 'POSITIONAL';
      confidence = 0.95;
      reasoning = 'Query contains positional reference to conversation history';
    } else if (topicalPatterns.some(p => p.test(queryLower))) {
      classification = 'TOPICAL';
      confidence = 0.90;
      reasoning = 'Query asks about conversation topics';
    } else if (overviewPatterns.some(p => p.test(queryLower))) {
      classification = 'OVERVIEW';
      confidence = 0.85;
      reasoning = 'Query requests conversation summary';
    }

    const isConversational = classification !== 'GENERAL';

    return {
      isConversational,
      confidence,
      classification,
      reasoning
    };
  }
}

// Singleton instance
let memoryInstance = null;

export function getMemoryService() {
  if (!memoryInstance) {
    memoryInstance = new MemoryService();
  }
  return memoryInstance;
}

export default MemoryService;
