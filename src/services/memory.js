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

// Row threshold above which a transient in-memory HNSW is used instead of brute-force.
const HNSW_THRESHOLD = 20000;
// How long (ms) to reuse a cached transient HNSW before rebuilding.
const HNSW_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes
// SQL single-quote literal used to escape quotes as ''.
const SQ = '\'';

class MemoryService {
  constructor() {
    this.db = getDatabaseService();
    this.embeddings = getEmbeddingService();
    // Transient in-memory HNSW cache (only used when row count >= HNSW_THRESHOLD)
    this._hnswCache = null; // { db, connection, builtAt, rowCount }
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
          now(),
          now()
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
      const maxAgeDays = options.maxAgeDays != null ? options.maxAgeDays : (parseInt(process.env.MAX_AGE_DAYS) || 365);
      const startDate = options.startDate || null;
      const endDate = options.endDate || null;

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
      
      // Build WHERE clause with date filters
      let whereConditions = [`user_id = '${userId}'`];
      
      // Explicit date range takes priority over maxAgeDays
      if (startDate) {
        whereConditions.push(`created_at >= '${startDate}'`);
      } else if (maxAgeDays > 0) {
        whereConditions.push(`created_at >= CURRENT_TIMESTAMP - INTERVAL '${maxAgeDays}' DAY`);
      }
      if (endDate) {
        whereConditions.push(`created_at <= '${endDate}'`);
      }
      
      if (options.filters) {
        if (options.filters.type) {
          const types = Array.isArray(options.filters.type)
            ? options.filters.type
            : [options.filters.type];
          whereConditions.push(`type IN (${types.map(t => `'${t}'`).join(', ')})`);
        }
        if (options.filters.excludeTypes) {
          const excludeTypes = Array.isArray(options.filters.excludeTypes)
            ? options.filters.excludeTypes
            : [options.filters.excludeTypes];
          for (const excludeType of excludeTypes) {
            whereConditions.push(`type != '${excludeType}'`);
          }
        }
        if (options.filters.sessionId) {
          whereConditions.push(`metadata LIKE '%"sessionId":"${options.filters.sessionId}"%'`);
        }
      }

      // ── Episodic memory split ──────────────────────────────────────────────
      // Screen captures live in episodic_memory, not memory. Default exclude
      // them here so callers don't get stale rows before migration.
      const excludeScreenCapture = !(
        options.filters?.type === 'screen_capture' ||
        (Array.isArray(options.filters?.type) && options.filters.type.includes('screen_capture'))
      );
      if (excludeScreenCapture) {
        whereConditions.push('type != \'screen_capture\'');
      }

      if (options.sessionId) {
        whereConditions.push(`metadata LIKE '%"sessionId":"${options.sessionId}"%'`);
      }

      const whereClause = whereConditions.length > 0 
        ? `WHERE ${whereConditions.join(' AND ')} AND embedding IS NOT NULL` 
        : 'WHERE embedding IS NOT NULL';

      // Legacy date-range screen capture dedup path — kept for backwards compat
      // but screen_capture results should now come from episodic.search.
      const isScreenCaptureDateRange = (startDate || endDate) && !excludeScreenCapture;

      const embeddingValues = queryEmbedding.map(v => v.toString()).join(',');
      const queryVector = `list_value(${embeddingValues})::FLOAT[384]`;

      let results;
      const dbStart = Date.now();

      if (isScreenCaptureDateRange) {
        console.log('🔍 [MEMORY-SEARCH] Using screen-capture date-range dedup');
        const dedupSql = `
          SELECT 
            MIN(id) as id,
            type,
            MIN(source_text) as source_text,
            MIN(metadata) as metadata,
            MIN(screenshot) as screenshot,
            MIN(extracted_text) as extracted_text,
            MIN(created_at) as created_at,
            NULL as similarity,
            NULL as final_score
          FROM memory
          ${whereClause}
          GROUP BY 
            type,
            json_extract_string(metadata, '$.appName'),
            json_extract_string(metadata, '$.windowTitle'),
            date_trunc('hour', created_at)
          ORDER BY MIN(created_at) DESC
          LIMIT ${limit}
          OFFSET ${offset}
        `;
        results = await this.db.query(dedupSql);
      } else {
        // Ensure VSS is loaded before issuing any array_cosine_distance() query.
        // VSS is intentionally NOT loaded at startup (see database.js initVectorSearch).
        // This call goes through _enqueue() so it is fully serialized — no screen
        // monitor INSERT can be in flight at the same time.  After the first call
        // this is a fast boolean check (no DB round-trip).
        await this.db.ensureVssLoaded();

        // Threshold-based search:
        //   < HNSW_THRESHOLD rows → brute-force array_cosine_distance() (~25–55 ms)
        //   >= HNSW_THRESHOLD rows → transient in-memory HNSW (cached 5 min, never on persistent DB)

        // Count rows eligible for this search (whereClause already includes embedding IS NOT NULL)
        const eligibleCountResult = await this.db.query(
          `SELECT COUNT(*) as count FROM memory ${whereClause}`
        );
        const eligibleCount = Number(eligibleCountResult[0]?.count || 0);

        console.log(`🔍 [MEMORY-SEARCH] Eligible rows: ${eligibleCount}, threshold: ${HNSW_THRESHOLD}`);

        if (eligibleCount < HNSW_THRESHOLD) {
          // ── Brute-force path ──────────────────────────────────────────────────
          console.log('🔍 [MEMORY-SEARCH] Using brute-force search');
          const sql = `
            SELECT 
              id,
              type,
              source_text,
              metadata,
              screenshot,
              extracted_text,
              created_at,
              (1 - array_cosine_distance(embedding, ${queryVector})) as similarity,
              (
                0.7 * (1 - array_cosine_distance(embedding, ${queryVector})) +
                0.3 * CASE 
                  WHEN type = 'personal_profile' THEN 1.0
                  ELSE 1.0 / (1 + ln(1 + GREATEST(DATEDIFF('day', created_at, CURRENT_TIMESTAMP), 0)))
                END
              ) as final_score
            FROM memory
            ${whereClause}
            ORDER BY final_score DESC
            LIMIT ${limit}
            OFFSET ${offset}
          `;
          results = await this.db.query(sql);
        } else {
          // ── Transient in-memory HNSW path ─────────────────────────────────────
          // Build (or reuse) a cached in-memory DuckDB instance with HNSW index.
          // This is completely separate from the persistent DB so CHECKPOINT on
          // the main DB never touches the HNSW graph.
          console.log('🔍 [MEMORY-SEARCH] Using transient in-memory HNSW');
          results = await this._searchWithTransientHnsw(
            queryVector, whereClause, limit, offset
          );
        }
      }

      timings.dbQuery = Date.now() - dbStart;

      // ── BM25 + entity fusion ─────────────────────────────────────────────────
      // For non-dedup searches, boost vector results that also match keywords or
      // named entities, and pull in high-scoring keyword matches that vector search
      // might have missed (e.g. rare names, exact file paths).
      if (!isScreenCaptureDateRange) {
        results = await this._fuseWithBm25AndEntities(results, query, userId, whereClause, limit, minSimilarity);
      }

      // Filter by minimum similarity and fetch entities for each result
      const filteredResults = isScreenCaptureDateRange
        ? results
        : results.filter(r => r.similarity >= minSimilarity);
      
      const duration = Date.now() - startTime;
      
      console.log(`✅ [MEMORY-SEARCH] Found ${filteredResults.length} results in ${duration}ms`);
      
      // Log results for debugging
      if (filteredResults.length > 0) {
        console.log('📊 [MEMORY-SEARCH] Top results:');
        filteredResults.slice(0, 3).forEach((result, idx) => {
          const simLabel = result.similarity != null
            ? `(similarity: ${result.similarity.toFixed(3)})`
            : '(deduped screen capture)';
          console.log(`  ${idx + 1}. "${result.source_text.substring(0, 60)}..." ${simLabel}`);
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
            type: result.type || 'user_memory',
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
   * Fuse vector search results with BM25 keyword matches and entity matches.
   * Returns a re-ranked list of up to `limit` results.
   * If the FTS extension is unavailable, returns the original vector results.
   */
  async _fuseWithBm25AndEntities(vectorResults, query, userId, whereClause, limit, _minSimilarity) {
    if (!vectorResults || vectorResults.length === 0) return vectorResults;

    try {
      await this.db.ensureFtsLoaded();
    } catch (e) {
      console.warn('🔍 [MEMORY-SEARCH] BM25 fusion disabled: FTS not available');
      return vectorResults;
    }

    // Remove embedding IS NOT NULL constraint from BM25 query — text is enough.
    const bm25Where = whereClause.replace(/AND embedding IS NOT NULL/g, '').trim();
    const escapedQuery = query.replace(/\\/g, '\\\\').replace(/'/g, SQ + SQ);

    let bm25Results = [];
    try {
      const bm25Sql = `
        SELECT m.id, m.type, m.source_text, m.metadata, m.screenshot, m.extracted_text, m.created_at,
               score_bm25.score as bm25_score
        FROM memory m
        INNER JOIN (
          SELECT id, COALESCE(fts_main_memory.match_bm25(id, '${escapedQuery}'), 0) as score
          FROM memory
        ) score_bm25 ON m.id = score_bm25.id AND score_bm25.score > 0
        ${bm25Where}
        ORDER BY score_bm25.score DESC
        LIMIT ${limit * 3}
      `;
      bm25Results = await this.db.query(bm25Sql);
    } catch (e) {
      console.warn('🔍 [MEMORY-SEARCH] BM25 query failed:', e.message);
    }

    // Entity match: memory IDs whose stored entities match any query term
    const terms = query
      .split(/\s+/)
      .map(t => t.replace(/[^a-zA-Z0-9]/g, ''))
      .filter(t => t.length > 2);
    let entityIds = new Set();
    if (terms.length > 0) {
      try {
        const entitySql = `
          SELECT DISTINCT me.memory_id as id
          FROM memory_entities me
          WHERE ${terms.map(t => `me.entity ILIKE '%${t.replace(/'/g, SQ + SQ)}%'`).join(' OR ')}
        `;
        const entityResults = await this.db.query(entitySql);
        entityIds = new Set(entityResults.map(r => r.id));
      } catch (e) {
        console.warn('🔍 [MEMORY-SEARCH] Entity query failed:', e.message);
      }
    }

    // Normalize BM25 scores to [0, 1]
    const bm25Scores = bm25Results.map(r => r.bm25_score || 0).filter(s => s > 0);
    const maxBm25 = bm25Scores.length > 0 ? Math.max(...bm25Scores) : 0;
    const minBm25 = bm25Scores.length > 0 ? Math.min(...bm25Scores) : 0;

    const vectorMap = new Map(vectorResults.map(r => [r.id, r]));
    const bm25Map = new Map(bm25Results.map(r => [r.id, r]));
    const allIds = new Set([...vectorMap.keys(), ...bm25Map.keys()]);

    const nowMs = Date.now();
    const scored = [];
    for (const id of allIds) {
      const vec = vectorMap.get(id);
      const bm25 = bm25Map.get(id);
      const vectorScore = vec?.similarity || 0;
      const bm25Raw = bm25?.bm25_score || 0;
      const bm25Norm = bm25Raw > 0 && maxBm25 > minBm25
        ? (bm25Raw - minBm25) / (maxBm25 - minBm25)
        : 0;
      const entityBoost = entityIds.has(id) ? 0.1 : 0;

      // Recency boost (already baked into brute-force final_score, but re-apply
      // here for HNSW and BM25-only rows so final ranking is consistent).
      const createdAt = new Date(vec?.created_at || bm25?.created_at || 0);
      const ageDays = Math.max(0, Math.floor((nowMs - createdAt.getTime()) / 86400000));
      const recency = vec?.type === 'personal_profile' || bm25?.type === 'personal_profile'
        ? 1.0
        : 1.0 / (1 + Math.log(1 + ageDays));

      const finalScore = 0.5 * vectorScore + 0.25 * bm25Norm + 0.15 * recency + entityBoost;
      const row = vec || bm25;
      scored.push({ ...row, similarity: vectorScore, final_score: finalScore, bm25_score: bm25Raw });
    }

    scored.sort((a, b) => b.final_score - a.final_score);
    return scored.slice(0, limit);
  }

  /**
   * Search the episodic_memory table (screen captures / activity log).
   * Supports date-range dedup by app + window + hour and keyword filters.
   */
  async searchEpisodicMemories(query, options = {}, context = {}) {
    const startTime = Date.now();
    const timings = {};

    try {
      const userId = extractUserId(context) || options.userId || 'default_user';
      const limit = options.limit || 25;
      const offset = options.offset || 0;
      const maxAgeDays = options.maxAgeDays != null ? options.maxAgeDays : (parseInt(process.env.MAX_AGE_DAYS) || 365);
      const startDate = options.startDate || null;
      const endDate = options.endDate || null;

      // Build WHERE clause for episodic_memory
      let whereConditions = [`user_id = '${userId}'`];
      if (startDate) {
        whereConditions.push(`created_at >= '${startDate}'`);
      } else if (maxAgeDays > 0) {
        whereConditions.push(`created_at >= CURRENT_TIMESTAMP - INTERVAL '${maxAgeDays}' DAY`);
      }
      if (endDate) {
        whereConditions.push(`created_at <= '${endDate}'`);
      }
      if (options.filters?.type) {
        const types = Array.isArray(options.filters.type) ? options.filters.type : [options.filters.type];
        whereConditions.push(`type IN (${types.map(t => `'${t}'`).join(', ')})`);
      }
      if (options.filters?.excludeTypes) {
        const excludeTypes = Array.isArray(options.filters.excludeTypes)
          ? options.filters.excludeTypes
          : [options.filters.excludeTypes];
        for (const excludeType of excludeTypes) {
          whereConditions.push(`type != '${excludeType}'`);
        }
      }
      if (options.filters?.appName) {
        const escapedAppName = options.filters.appName.replace(/'/g, SQ + SQ);
        whereConditions.push(`json_extract_string(metadata, '$.appName') = '${escapedAppName}'`);
      }
      if (options.filters?.excludeOverlay) {
        whereConditions.push('json_extract_string(metadata, \'$.overlayTainted\') IS NULL');
      }
      if (options.filters?.sessionId) {
        whereConditions.push(`metadata LIKE '%"sessionId":"${options.filters.sessionId}"%'`);
      }
      if (options.sessionId) {
        whereConditions.push(`metadata LIKE '%"sessionId":"${options.sessionId}"%'`);
      }

      const whereClause = whereConditions.length > 0
        ? `WHERE ${whereConditions.join(' AND ')}`
        : '';

      const dedup = options.dedup !== false;
      const dbStart = Date.now();
      let results;

      // Load FTS extension and create episodic index if needed. This is a
      // no-op after the first call. If FTS is unavailable, keyword ranking
      // is skipped but the date/app filter still works.
      let bm25Enabled = false;
      const trimmedQuery = (query || '').trim();
      if (trimmedQuery) {
        try {
          await this.db.ensureFtsLoaded();
          bm25Enabled = this.db.isFtsEnabled();
        } catch (e) {
          console.warn('🔍 [EPISODIC-SEARCH] FTS not available for keyword ranking:', e.message);
        }
      }
      const escapedQuery = trimmedQuery.replace(/\\/g, '\\\\').replace(/'/g, SQ + SQ);
      const bm25Join = bm25Enabled && trimmedQuery
        ? `LEFT JOIN (
          SELECT id, COALESCE(fts_main_episodic_memory.match_bm25(id, '${escapedQuery}'), 0) as score
          FROM episodic_memory
        ) score_bm25 ON episodic_memory.id = score_bm25.id`
        : '';

      if (dedup) {
        const sql = `
          SELECT 
            MIN(episodic_memory.id) as id,
            episodic_memory.type,
            MIN(episodic_memory.source_text) as source_text,
            MIN(episodic_memory.metadata) as metadata,
            MIN(episodic_memory.screenshot) as screenshot,
            MIN(episodic_memory.extracted_text) as extracted_text,
            MIN(episodic_memory.created_at) as created_at,
            NULL as similarity,
            NULL as final_score,
            MAX(COALESCE(score_bm25.score, 0)) as bm25_score
          FROM episodic_memory
          ${bm25Join}
          ${whereClause}
          GROUP BY 
            episodic_memory.type,
            json_extract_string(episodic_memory.metadata, '$.appName'),
            json_extract_string(episodic_memory.metadata, '$.windowTitle'),
            date_trunc('hour', episodic_memory.created_at)
          ORDER BY MAX(COALESCE(score_bm25.score, 0)) DESC, MIN(episodic_memory.created_at) DESC
          LIMIT ${limit}
          OFFSET ${offset}
        `;
        results = await this.db.query(sql);
      } else {
        const sql = `
          SELECT 
            episodic_memory.id,
            episodic_memory.type,
            episodic_memory.source_text,
            episodic_memory.metadata,
            episodic_memory.screenshot,
            episodic_memory.extracted_text,
            episodic_memory.created_at,
            NULL as similarity,
            NULL as final_score,
            COALESCE(score_bm25.score, 0) as bm25_score
          FROM episodic_memory
          ${bm25Join}
          ${whereClause}
          ORDER BY COALESCE(score_bm25.score, 0) DESC, episodic_memory.created_at DESC
          LIMIT ${limit}
          OFFSET ${offset}
        `;
        results = await this.db.query(sql);
      }
      timings.dbQuery = Date.now() - dbStart;

      const enrichStart = Date.now();
      const enrichedResults = await Promise.all(
        results.map(async (result) => {
          const entitiesSql = `
            SELECT entity, type, entity_type
            FROM episodic_entities
            WHERE memory_id = '${result.id}'
          `;
          const entities = await this.db.query(entitiesSql);
          return {
            id: result.id,
            type: result.type || 'screen_capture',
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

      logger.info('⏱️  Episodic memory search completed', {
        query,
        resultsCount: enrichedResults.length,
        timings
      });

      return {
        results: enrichedResults,
        total: enrichedResults.length,
        query,
        timings
      };
    } catch (error) {
      logger.error('Episodic memory search failed', { error: error.message });
      throw error;
    }
  }

  /**
   * Build (or reuse cached) a transient in-memory DuckDB instance with HNSW.
   * Called only when row count >= HNSW_THRESHOLD.
   * The in-memory DB is entirely separate from the persistent DB so CHECKPOINT
   * on the main DB never touches the HNSW graph — eliminating the crash path.
   */
  async _searchWithTransientHnsw(queryVector, whereClause, limit, offset) {
    const duckdbModule = await import('duckdb');
    const duckdb = duckdbModule.default;

    const now = Date.now();
    const totalCountResult = await this.db.query(
      'SELECT COUNT(*) as count FROM memory WHERE embedding IS NOT NULL'
    );
    const rowCount = Number(totalCountResult[0]?.count || 0);

    const cacheValid = this._hnswCache &&
      (now - this._hnswCache.builtAt) < HNSW_CACHE_TTL_MS &&
      this._hnswCache.rowCount === rowCount;

    if (!cacheValid) {
      if (this._hnswCache) {
        try { this._hnswCache.connection.close(); } catch (e) { /* ignore */ }
        try { this._hnswCache.db.close(); } catch (e) { /* ignore */ }
        this._hnswCache = null;
      }

      logger.info('Building transient in-memory HNSW index', { rowCount });
      const buildStart = Date.now();

      const memDb = new duckdb.Database(':memory:');
      const memConn = memDb.connect();
      const memRun = (sql) => new Promise((res, rej) =>
        memConn.run(sql, (e) => (e ? rej(e) : res()))
      );
      const memAll = (sql) => new Promise((res, rej) =>
        memConn.all(sql, (e, r) => (e ? rej(e) : res(r)))
      );

      await memRun('LOAD vss');
      await memRun('CREATE TABLE mem_search (row_id TEXT, embedding FLOAT[384])');

      const rows = await this.db.query(
        'SELECT id, embedding FROM memory WHERE embedding IS NOT NULL'
      );

      if (rows.length > 0) {
        const batchSize = 500;
        for (let i = 0; i < rows.length; i += batchSize) {
          const batch = rows.slice(i, i + batchSize);
          const vals = batch.map((r) => {
            const floats = Array.isArray(r.embedding)
              ? r.embedding
              : Object.values(r.embedding);
            const safeId = r.id.replace(/'/g, '\'\'');
            return `('${safeId}', list_value(${floats.join(',')}))`;
          }).join(',\n');
          await memRun(`INSERT INTO mem_search VALUES ${vals}`);
        }
        await memRun(
          'CREATE INDEX mem_hnsw ON mem_search USING HNSW (embedding) WITH (metric=\'cosine\')'
        );
      }

      this._hnswCache = {
        db: memDb,
        connection: memConn,
        all: memAll,
        builtAt: now,
        rowCount
      };
      logger.info('Transient HNSW built', {
        rowCount,
        buildTimeMs: Date.now() - buildStart
      });
    }

    const memAll = this._hnswCache.all;

    const hnswSql = `
      SELECT row_id as id, (1 - array_cosine_distance(embedding, ${queryVector})) as similarity
      FROM mem_search
      ORDER BY array_cosine_distance(embedding, ${queryVector})
      LIMIT ${limit + offset + 100}
    `;
    const hnswResults = await memAll(hnswSql);
    const simMap = Object.fromEntries(hnswResults.map((r) => [r.id, r.similarity]));

    if (hnswResults.length === 0) return [];

    const candidateIds = hnswResults.map((r) => r.id);
    const idList = candidateIds.map((id) => `'${id.replace(/'/g, '\'\'')}'`).join(',');
    const fullRows = await this.db.query(`
      SELECT id, type, source_text, metadata, screenshot, extracted_text, created_at
      FROM memory
      WHERE id IN (${idList})
    `);

    const nowMs = Date.now();
    const scored = fullRows.map((r) => {
      const ageDays = Math.max(
        0,
        Math.floor((nowMs - new Date(r.created_at).getTime()) / 86400000)
      );
      const recency = r.type === 'personal_profile' ? 1.0 : 1.0 / (1 + Math.log(1 + ageDays));
      const finalScore = 0.7 * (simMap[r.id] || 0) + 0.3 * recency;
      return { ...r, similarity: simMap[r.id], finalScore };
    });

    scored.sort((a, b) => b.finalScore - a.finalScore);
    return scored.slice(offset, offset + limit);
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
          now()
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
  async getRecentOcr(options = {}) {
    try {
      // Screen captures are stored by the monitor under MONITOR_USER_ID (default: 'local_user')
      const userId = options.userId || process.env.MONITOR_USER_ID || 'local_user';
      const maxAgeSeconds = options.maxAgeSeconds || 3;
      const appName = options.appName || null;
      // preferAppName: when set, sort captures from this app first within the time window.
      // Used to prefer the app the user was looking at before opening the ThinkDrop overlay,
      // even if another app was captured more recently (e.g. Devin captured 8s after Chrome).
      const preferAppName = options.preferAppName || null;

      // When appName is provided, filter by it so we never return a different app's
      // stale capture (e.g. Devin capture while Chrome is active).
      const appNameClause = appName
        ? `AND json_extract_string(metadata, '$.appName') = '${appName.replace(/'/g, '\'\'')}'`
        : '';

      const preferClause = preferAppName
        ? `CASE WHEN json_extract_string(metadata, '$.appName') = '${preferAppName.replace(/'/g, '\'\'')}' THEN 0 ELSE 1 END,`
        : '';

      const sql = `
        SELECT 
          id,
          source_text,
          extracted_text,
          metadata,
          created_at
        FROM episodic_memory
        WHERE type = 'screen_capture'
          AND user_id = '${userId}'
          AND created_at >= CURRENT_TIMESTAMP - INTERVAL '${maxAgeSeconds}' SECOND
          AND json_extract_string(metadata, '$.overlayTainted') IS NULL
          ${appNameClause}
        ORDER BY ${preferClause} created_at DESC
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
        category: metadata.category || 'other',
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
