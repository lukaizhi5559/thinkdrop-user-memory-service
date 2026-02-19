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
    const timings = {};
    
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

      console.log(`💾 [MEMORY-STORE] Storing memory for user: ${userId}`);
      console.log(`📝 [MEMORY-STORE] Text: "${text.substring(0, 100)}..."`);
      
      // Generate embedding (REQUIRED - don't store without it)
      let embedding;
      try {
        if (!this.embeddings.isInitialized()) {
          throw new Error('Embedding service not initialized');
        }
        
        const embeddingStart = Date.now();
        embedding = await this.embeddings.generateEmbedding(text);
        timings.embedding = Date.now() - embeddingStart;
        
        // Validate embedding
        if (!Array.isArray(embedding) || embedding.length !== 384) {
          throw new Error(`Invalid embedding: expected 384-dim array, got ${typeof embedding} with length ${embedding?.length}`);
        }
        
        console.log(`✅ [MEMORY-STORE] Embedding generated: ${embedding.length} dimensions`);
      } catch (embeddingError) {
        console.error('❌ [MEMORY-STORE] Embedding generation failed:', embeddingError.message);
        
        // CRITICAL: Don't store memory without embedding
        throw new Error(`Cannot store memory without embedding: ${embeddingError.message}`);
      }

      // Prepare SQL with embedding (always present now)
      const embeddingValues = embedding.map(v => v.toString()).join(',');
      const sql = `
        INSERT INTO memory (
          id, user_id, type, source_text, metadata, screenshot, 
          extracted_text, embedding, created_at, updated_at
        ) VALUES (
          '${memoryId}', '${userId}', '${data.type || 'user_memory'}',
          '${text.replace(/'/g, '\'\'')}',
          '${metadataJson.replace(/'/g, '\'\'')}',
          ${data.screenshot ? `'${data.screenshot}'` : 'NULL'},
          ${data.extractedText ? `'${data.extractedText.replace(/'/g, '\'\'')}'` : 'NULL'},
          list_value(${embeddingValues}),
          CURRENT_TIMESTAMP,
          CURRENT_TIMESTAMP
        )
      `;

      const dbStart = Date.now();
      await this.db.execute(sql);
      timings.dbInsert = Date.now() - dbStart;

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
              '${entity.value.replace(/'/g, '\'\'')}',
              '${entity.type}',
              '${entity.entity_type || entity.type}',
              '${normalizedValue.replace(/'/g, '\'\'')}'
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

      timings.total = Date.now() - startTime;

      console.log(`✅ [MEMORY-STORE] Memory stored successfully in ${timings.total}ms`);
      console.log(`📊 [MEMORY-STORE] Memory ID: ${memoryId}`);

      logger.info('⏱️  Memory stored successfully', {
        memoryId,
        userId,
        entitiesCount: entities.length,
        hasEmbedding: true,
        embeddingDimensions: embedding.length,
        timings
      });

      return {
        memoryId,
        stored: true,
        embedding: embedding.slice(0, 5), // Return first 5 values for verification
        embeddingDimensions: embedding.length,
        entities: entities.length,
        timestamp: new Date().toISOString(),
        timings
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
    const timings = {};

    try {
      const userId = extractUserId(context) || options.userId || 'default_user';
      const limit = options.limit || 25;
      const offset = options.offset || 0;
      const minSimilarity = options.minSimilarity || parseFloat(process.env.MIN_SIMILARITY_THRESHOLD) || 0.3;
      const maxAgeDays = options.maxAgeDays || parseInt(process.env.MAX_AGE_DAYS) || 30;

      console.log('🔍 [MEMORY-SEARCH] Starting search...');
      console.log(`📝 [MEMORY-SEARCH] Query: "${query}"`);
      console.log(`🎯 [MEMORY-SEARCH] Min similarity: ${minSimilarity}`);
      console.log(`👤 [MEMORY-SEARCH] User ID: ${userId}`);

      // Generate query embedding
      let queryEmbedding;
      try {
        if (!this.embeddings.isInitialized()) {
          throw new Error('Embedding service not initialized');
        }
        
        const embeddingStart = Date.now();
        queryEmbedding = await this.embeddings.generateEmbedding(query);
        timings.embedding = Date.now() - embeddingStart;
        console.log(`✅ [MEMORY-SEARCH] Query embedding generated: ${queryEmbedding.length} dimensions`);
      } catch (embeddingError) {
        console.error('❌ [MEMORY-SEARCH] Failed to generate query embedding:', embeddingError.message);
        throw new Error(`Cannot search without query embedding: ${embeddingError.message}`);
      }
      
      // Build WHERE clause with maxAge filter
      let whereConditions = [`user_id = '${userId}'`];
      
      // Add maxAge filter to reduce search space
      if (maxAgeDays > 0) {
        whereConditions.push(`created_at >= CURRENT_TIMESTAMP - INTERVAL '${maxAgeDays}' DAY`);
      }
      
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

      // Search with similarity using array_cosine_distance (HNSW-accelerated when vss is loaded)
      // cosine_distance = 0 means identical, 2 means opposite; similarity = 1 - distance
      const embeddingValues = queryEmbedding.map(v => v.toString()).join(',');
      const queryVector = `[${embeddingValues}]::FLOAT[384]`;
      const sql = `
        SELECT 
          id,
          source_text,
          metadata,
          screenshot,
          extracted_text,
          created_at,
          (1 - array_cosine_distance(embedding, ${queryVector})) as similarity
        FROM memory
        ${whereClause}
        ORDER BY array_cosine_distance(embedding, ${queryVector})
        LIMIT ${limit}
        OFFSET ${offset}
      `;

      console.log('🔍 [MEMORY-SEARCH] Executing search query...');
      
      const dbStart = Date.now();
      const results = await this.db.query(sql);
      timings.dbQuery = Date.now() - dbStart;

      // Filter by minimum similarity and fetch entities for each result
      const filteredResults = results.filter(r => r.similarity >= minSimilarity);
      
      const duration = Date.now() - startTime;
      
      console.log(`✅ [MEMORY-SEARCH] Found ${filteredResults.length} results in ${duration}ms`);
      
      // Log results for debugging
      if (filteredResults.length > 0) {
        console.log('📊 [MEMORY-SEARCH] Top results:');
        filteredResults.slice(0, 3).forEach((result, idx) => {
          console.log(`  ${idx + 1}. "${result.source_text.substring(0, 60)}..." (similarity: ${result.similarity.toFixed(3)})`);
        });
      } else {
        console.warn('⚠️ [MEMORY-SEARCH] No results found. Checking database...');
        
        // Debug: Check if any memories exist
        const totalMemories = await this.db.query(`SELECT COUNT(*) as count FROM memory WHERE user_id = '${userId}'`);
        const withEmbeddings = await this.db.query(`SELECT COUNT(*) as count FROM memory WHERE user_id = '${userId}' AND embedding IS NOT NULL`);
        
        console.log('📊 [MEMORY-SEARCH] Debug info:');
        console.log(`  Total memories for user: ${totalMemories[0].count}`);
        console.log(`  Memories with embeddings: ${withEmbeddings[0].count}`);
        console.log(`  Similarity threshold: ${minSimilarity}`);
        
        if (withEmbeddings[0].count > 0) {
          // Check highest similarity
          const bestMatch = await this.db.query(`
            SELECT source_text, (1 - array_cosine_distance(embedding, ${queryVector})) as similarity
            FROM memory
            WHERE user_id = '${userId}' AND embedding IS NOT NULL
            ORDER BY array_cosine_distance(embedding, ${queryVector})
            LIMIT 1
          `);
          
          if (bestMatch.length > 0) {
            console.log(`  Best match similarity: ${bestMatch[0].similarity.toFixed(3)}`);
            console.log(`  Best match text: "${bestMatch[0].source_text.substring(0, 60)}..."`);
            
            if (bestMatch[0].similarity < minSimilarity) {
              console.warn(`⚠️ [MEMORY-SEARCH] Best match (${bestMatch[0].similarity.toFixed(3)}) below threshold (${minSimilarity})`);
              console.warn('💡 [MEMORY-SEARCH] Consider lowering minSimilarity or improving query');
            }
          }
        }
      }
      
      const enrichStart = Date.now();
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
      timings.enrichment = Date.now() - enrichStart;

      timings.total = Date.now() - startTime;

      logger.info('⏱️  Memory search completed', {
        query,
        resultsCount: enrichedResults.length,
        maxAgeDays,
        timings
      });

      return {
        results: enrichedResults,
        total: enrichedResults.length,
        query,
        timings
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

      // Get current memory data (verifies it exists and we need it for the update)
      const currentMemorySQL = `SELECT * FROM memory WHERE id = '${memoryId}' AND user_id = '${userId}'`;
      const currentMemoryResult = await this.db.query(currentMemorySQL);
      
      if (!currentMemoryResult || currentMemoryResult.length === 0) {
        throw new Error(`Memory not found: ${memoryId}`);
      }
      
      const currentMemory = currentMemoryResult[0];

      let regenerateEmbedding = false;

      if (updates.text) {
        validateMemoryText(updates.text);
        regenerateEmbedding = true;
      }

      // Generate embedding first if needed
      let embedding = null;
      if (regenerateEmbedding) {
        embedding = await this.embeddings.generateEmbedding(updates.text);
      }

      // Use a different approach - delete and insert to avoid DuckDB UPDATE constraint bug
      const deleteSQL = `DELETE FROM memory WHERE id = '${memoryId}' AND user_id = '${userId}'`;
      
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
          '${updates.text ? validateMemoryText(updates.text).replace(/'/g, '\'\'') : currentMemory.source_text.replace(/'/g, '\'\'')}',
          ${embeddingValue},
          '${updates.metadata ? JSON.stringify(updates.metadata).replace(/'/g, '\'\'') : (currentMemory.metadata || '{}').replace(/'/g, '\'\'')}',
          ${updates.screenshot !== undefined ? (updates.screenshot ? `'${updates.screenshot}'` : 'NULL') : (currentMemory.screenshot ? `'${currentMemory.screenshot}'` : 'NULL')},
          ${updates.extractedText !== undefined ? (updates.extractedText ? `'${updates.extractedText.replace(/'/g, '\'\'')}'` : 'NULL') : (currentMemory.extracted_text ? `'${currentMemory.extracted_text.replace(/'/g, '\'\'')}'` : 'NULL')},
          '${currentMemory.type || 'user_memory'}',
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
              '${entity.value.replace(/'/g, '\'\'')}',
              '${entity.type}',
              '${entity.entity_type || entity.type}',
              '${normalizedValue.replace(/'/g, '\'\'')}'
            )
          `;
          await this.db.execute(entitySql);
        }
      }

      const elapsedMs = Date.now() - startTime;

      logger.info('Memory updated successfully', {
        memoryId,
        regeneratedEmbedding: regenerateEmbedding,
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
   * Get the most recent screen capture OCR text if it's fresh enough.
   * Used by external services (e.g., screen_intelligence) to avoid redundant OCR.
   * 
   * @param {Object} options
   * @param {number} options.maxAgeSeconds - Max age in seconds (default: 10)
   * @param {string} options.userId - User ID filter
   * @returns {Object|null} Recent OCR data or null if none fresh enough
   */
  async getRecentOcr(options = {}, context = {}) {
    try {
      // Screen captures are stored by the monitor under MONITOR_USER_ID (default: 'local_user')
      const userId = options.userId || process.env.MONITOR_USER_ID || 'local_user';
      const maxAgeSeconds = options.maxAgeSeconds || 10;

      const sql = `
        SELECT 
          id,
          source_text,
          extracted_text,
          metadata,
          created_at
        FROM memory
        WHERE type = 'screen_capture'
          AND user_id = '${userId}'
          AND created_at >= CURRENT_TIMESTAMP - INTERVAL '${maxAgeSeconds}' SECOND
        ORDER BY created_at DESC
        LIMIT 1
      `;

      const results = await this.db.query(sql);

      if (!results || results.length === 0) {
        return null;
      }

      const row = results[0];
      const metadata = parseMetadata(row.metadata);

      return {
        id: row.id,
        text: row.extracted_text || row.source_text,
        sourceText: row.source_text,
        appName: metadata.appName || 'unknown',
        windowTitle: metadata.windowTitle || 'unknown',
        url: metadata.url || null,
        files: metadata.files || [],
        codeSnippets: metadata.codeSnippets || [],
        ocrConfidence: metadata.ocrConfidence || null,
        capturedAt: metadata.capturedAt || row.created_at,
        ageMs: Date.now() - new Date(row.created_at).getTime()
      };
    } catch (error) {
      logger.error('Failed to get recent OCR', { error: error.message });
      throw error;
    }
  }

  /**
   * Classify if query is conversational (context-aware)
   */
  classifyConversationalQuery(query, options = {}) {
    const queryLower = query.toLowerCase();
    const context = options.context || {};
    
    // Check if conversation context exists
    const hasSessionContext = !!(context.sessionId || options.sessionId);
    const hasMessageHistory = context.messageCount > 0 || context.hasHistory;
    const hasConversationContext = hasSessionContext && hasMessageHistory;

    // Conversational pronouns that reference shared context
    const conversationalPronouns = /\b(we|our|you said|i said|you mentioned|you told|i asked|we discussed|we talked|we covered)\b/i;
    
    // Anaphoric references (pointing back to previous discourse)
    const anaphoricReferences = /\b(that|this|it|those|these)\b.*\b(we|you|i)\b/i;
    const demonstratives = /\b(that thing|this topic|those points|these ideas)\b/i;
    
    // Temporal references WITH conversational context
    const temporalConversational = /\b(earlier|before|previously|just now|a moment ago|you just)\b.*\b(said|mentioned|told|discussed|explained)\b/i;
    const temporalConversationalReverse = /\b(said|mentioned|told|discussed|explained)\b.*\b(earlier|before|previously|just now|a moment ago)\b/i;
    
    // Positional patterns - ONLY with conversational pronouns
    const positionalWithContext = [
      /\b(first|last|initial|latest)\b.*\b(we|you|i)\b.*\b(said|mentioned|discussed|talked)\b/i,
      /\b(what did (i|we|you))\b.*\b(say|ask|discuss|mention|talk about)\b/i,
      /\b(beginning|start|end)\b.*\b(of (our|the) (conversation|discussion|chat))\b/i,
      /\b(go back to|return to|back to)\b.*\b(what (we|you|i))\b/i
    ];

    // Topical patterns - ONLY about OUR conversation
    const topicalWithContext = [
      /\b(what (did|have) (we|i|you))\b.*\b(discuss|talk about|cover|chat about)\b/i,
      /\b(topics|things|points|issues)\b.*\b((we|i|you) (discussed|covered|mentioned|talked about))\b/i,
      /\b(our (conversation|discussion|chat))\b.*\b(about|regarding|concerning)\b/i
    ];

    // Overview patterns - ONLY about THIS conversation
    const overviewWithContext = [
      /\b(summarize|recap|sum up|overview of)\b.*\b(our|this|the)\b.*\b(conversation|discussion|chat)\b/i,
      /\b(what (did|have) (we|i|you))\b.*\b(chat|talk)\b.*\babout\b/i,
      /\b(give me (a|an))\b.*\b(summary|recap|overview)\b.*\b(of (our|this|the) (conversation|discussion))\b/i
    ];

    // Discourse markers indicating reference to previous statements
    const discourseMarkers = /\b(like (you|i) (said|mentioned)|as (you|i) (mentioned|said|explained)|you were saying|i was saying)\b/i;

    let classification = 'GENERAL';
    let confidence = 0.5;
    let reasoning = 'No conversational context or patterns detected';

    // If no conversation context exists, be very strict
    if (!hasConversationContext) {
      // Only classify as conversational if there are VERY strong signals
      if (discourseMarkers.test(queryLower) || 
          conversationalPronouns.test(queryLower) && (temporalConversational.test(queryLower) || temporalConversationalReverse.test(queryLower))) {
        classification = 'POSITIONAL';
        confidence = 0.70;
        reasoning = 'Strong conversational markers present, but no session context available';
      } else {
        confidence = 0.95;
        reasoning = 'No conversation context exists - treating as general query';
      }
    } else {
      // We have conversation context - check patterns
      
      // Check for discourse markers first (strongest signal)
      if (discourseMarkers.test(queryLower)) {
        classification = 'POSITIONAL';
        confidence = 0.98;
        reasoning = 'Explicit discourse marker referencing previous conversation';
      }
      // Positional with context
      else if (positionalWithContext.some(p => p.test(queryLower)) || 
               (temporalConversational.test(queryLower) || temporalConversationalReverse.test(queryLower))) {
        classification = 'POSITIONAL';
        confidence = 0.95;
        reasoning = 'Positional reference to conversation history with context';
      }
      // Topical with context
      else if (topicalWithContext.some(p => p.test(queryLower))) {
        classification = 'TOPICAL';
        confidence = 0.92;
        reasoning = 'Query asks about topics discussed in our conversation';
      }
      // Overview with context
      else if (overviewWithContext.some(p => p.test(queryLower))) {
        classification = 'OVERVIEW';
        confidence = 0.90;
        reasoning = 'Query requests summary of our conversation';
      }
      // Anaphoric references with conversational pronouns
      else if ((anaphoricReferences.test(queryLower) || demonstratives.test(queryLower)) && 
               conversationalPronouns.test(queryLower)) {
        classification = 'POSITIONAL';
        confidence = 0.85;
        reasoning = 'Anaphoric reference to previous conversation content';
      }
      // Has conversation context but no strong patterns
      else if (conversationalPronouns.test(queryLower)) {
        classification = 'GENERAL';
        confidence = 0.60;
        reasoning = 'Conversational pronouns present but no clear reference to conversation history';
      }
    }

    const isConversational = classification !== 'GENERAL';

    return {
      isConversational,
      confidence,
      classification,
      reasoning,
      contextInfo: {
        hasSessionContext,
        hasMessageHistory,
        hasConversationContext
      }
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
