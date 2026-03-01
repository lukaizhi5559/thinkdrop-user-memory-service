import duckdb from 'duckdb';
import { promisify } from 'util';
import path from 'path';
import fs from 'fs';
import logger from '../utils/logger.js';

class DatabaseService {
  constructor(dbPath) {
    this.dbPath = dbPath || process.env.DB_PATH || './data/user_memory.duckdb';
    this.db = null;
    this.connection = null;
    this.isInitialized = false;
  }

  /**
   * Initialize database connection and create tables
   */
  async initialize() {
    if (this.isInitialized) {
      logger.info('Database already initialized');
      return;
    }

    // Ensure data directory exists
    const dir = path.dirname(this.dbPath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
      logger.info(`Created database directory: ${dir}`);
    }

    const MAX_RETRIES = 5;
    const RETRY_DELAY_MS = 3000;

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        await this._tryConnect();
        return;
      } catch (err) {
        const isLockError = err.message && err.message.includes('Could not set lock');
        if (isLockError && attempt < MAX_RETRIES) {
          logger.warn(`Database locked by another process (attempt ${attempt}/${MAX_RETRIES}). Retrying in ${RETRY_DELAY_MS / 1000}s...`, {
            error: err.message.split('\n')[0]
          });
          await new Promise(r => setTimeout(r, RETRY_DELAY_MS * attempt));
        } else {
          logger.error('Failed to initialize database after retries', { error: err.message });
          throw err;
        }
      }
    }
  }

  async _tryConnect() {
    return new Promise((resolve, reject) => {
      try {
        this.db = new duckdb.Database(this.dbPath, async (err) => {
          if (err) {
            reject(err);
            return;
          }

          try {
            // Create connection
            this.connection = this.db.connect();

            // Promisify connection methods
            this.run = promisify(this.connection.run.bind(this.connection));
            this.all = promisify(this.connection.all.bind(this.connection));

            // Create tables
            await this.createTables();

            // Load VSS extension for HNSW vector indexing
            await this.initVectorSearch();

            this.isInitialized = true;
            logger.info(`Database initialized successfully: ${this.dbPath}`);
            resolve();
          } catch (error) {
            reject(error);
          }
        });
      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * Create database tables
   */
  async createTables() {
    try {
      // Create memory table
      logger.info('Creating memory table...');
      await this.run(`
        CREATE TABLE IF NOT EXISTS memory (
          id TEXT PRIMARY KEY,
          user_id TEXT,
          type TEXT DEFAULT 'user_memory',
          source_text TEXT,
          metadata TEXT,
          screenshot TEXT,
          extracted_text TEXT,
          embedding FLOAT[384],
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      logger.info('Memory table created');

      // Create single-column indexes for memory table
      await this.run('CREATE INDEX IF NOT EXISTS idx_memory_user_id ON memory(user_id)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_memory_type ON memory(type)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_memory_created_at ON memory(created_at)');
      
      // Create composite indexes for common query patterns
      await this.run('CREATE INDEX IF NOT EXISTS idx_memory_user_created ON memory(user_id, created_at DESC)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_memory_user_type ON memory(user_id, type)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_memory_user_type_created ON memory(user_id, type, created_at DESC)');
      
      logger.info('Memory table indexes created (including composite indexes)');

      // Create memory_entities table
      logger.info('Creating memory_entities table...');
      await this.run(`
        CREATE TABLE IF NOT EXISTS memory_entities (
          id TEXT PRIMARY KEY,
          memory_id TEXT NOT NULL,
          entity TEXT NOT NULL,
          type TEXT,
          entity_type TEXT,
          normalized_value TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      logger.info('Memory_entities table created');

      // Create indexes for memory_entities table
      await this.run('CREATE INDEX IF NOT EXISTS idx_memory_entities_memory_id ON memory_entities(memory_id)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_memory_entities_entity ON memory_entities(entity)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_memory_entities_type ON memory_entities(type)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_memory_entities_entity_type ON memory_entities(entity_type)');
      logger.info('Memory_entities table indexes created');

      // Create skill_prompts table for RAG-based dynamic skill prompt injection
      logger.info('Creating skill_prompts table...');
      await this.run(`
        CREATE TABLE IF NOT EXISTS skill_prompts (
          id TEXT PRIMARY KEY,
          tags TEXT,
          prompt_text TEXT NOT NULL,
          embedding FLOAT[384],
          hit_count INTEGER DEFAULT 0,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      await this.run('CREATE INDEX IF NOT EXISTS idx_skill_prompts_tags ON skill_prompts(tags)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_skill_prompts_created_at ON skill_prompts(created_at)');
      logger.info('Skill_prompts table created');

      // Create context_rules table for per-site/app prompt injection
      // context_type: 'site' (hostname) | 'app' (app name e.g. 'slack', 'excel')
      // context_key:  hostname (e.g. 'en.wikipedia.org') OR app name (e.g. 'slack')
      logger.info('Creating context_rules table...');
      await this.run(`
        CREATE TABLE IF NOT EXISTS context_rules (
          id TEXT PRIMARY KEY,
          context_type TEXT NOT NULL DEFAULT 'site',
          context_key TEXT NOT NULL,
          rule_text TEXT NOT NULL,
          category TEXT DEFAULT 'general',
          source TEXT DEFAULT 'thinkdrop_ai',
          hit_count INTEGER DEFAULT 0,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      await this.run('CREATE INDEX IF NOT EXISTS idx_context_rules_key ON context_rules(context_key)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_context_rules_type ON context_rules(context_type)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_context_rules_category ON context_rules(category)');
      logger.info('Context_rules table created');

      // Create installed_skills table for the skill extension system
      logger.info('Creating installed_skills table...');
      await this.run(`
        CREATE TABLE IF NOT EXISTS installed_skills (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          description TEXT NOT NULL,
          contract_md TEXT NOT NULL,
          exec_path TEXT NOT NULL,
          exec_type TEXT NOT NULL DEFAULT 'node',
          enabled BOOLEAN DEFAULT true,
          installed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      await this.run('CREATE INDEX IF NOT EXISTS idx_installed_skills_name ON installed_skills(name)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_installed_skills_enabled ON installed_skills(enabled)');
      logger.info('Installed_skills table created');

      logger.info('Database tables created successfully');
    } catch (error) {
      logger.error('Failed to create tables', { error: error.message, stack: error.stack });
      throw error;
    }
  }

  /**
   * Initialize the VSS extension and create HNSW index for vector similarity search.
   * The index is rebuilt on each startup (safe approach — experimental persistence
   * has WAL recovery issues that could corrupt data on crash).
   */
  async initVectorSearch() {
    try {
      // Install and load the VSS extension
      await this.run('INSTALL vss');
      await this.run('LOAD vss');
      logger.info('VSS extension loaded');

      // Drop existing HNSW index if it exists (rebuild fresh on each startup)
      try {
        await this.run('DROP INDEX IF EXISTS idx_memory_embedding_hnsw');
      } catch (e) {
        // Index may not exist yet — that's fine
      }

      // Check if there are any records with embeddings to index
      const result = await this.all(
        'SELECT COUNT(*) as count FROM memory WHERE embedding IS NOT NULL'
      );
      const count = Number(result[0]?.count || 0);

      if (count > 0) {
        // Create HNSW index with cosine distance metric
        const startTime = Date.now();
        await this.run(
          'CREATE INDEX idx_memory_embedding_hnsw ON memory USING HNSW (embedding) WITH (metric = \'cosine\')'
        );
        const elapsed = Date.now() - startTime;
        logger.info('HNSW vector index created', { records: count, buildTimeMs: elapsed });
      } else {
        logger.info('HNSW vector index skipped — no embeddings yet');
      }

      this.vssEnabled = true;
    } catch (error) {
      // VSS extension may not be available — fall back to brute-force search
      logger.warn('VSS extension not available, falling back to brute-force vector search', {
        error: error.message
      });
      this.vssEnabled = false;
    }
  }

  /**
   * Rebuild the HNSW index (call after bulk inserts or purges).
   */
  async rebuildHnswIndex() {
    if (!this.vssEnabled) return;
    try {
      await this.run('DROP INDEX IF EXISTS idx_memory_embedding_hnsw');
      const result = await this.all(
        'SELECT COUNT(*) as count FROM memory WHERE embedding IS NOT NULL'
      );
      const count = Number(result[0]?.count || 0);
      if (count > 0) {
        const startTime = Date.now();
        await this.run(
          'CREATE INDEX idx_memory_embedding_hnsw ON memory USING HNSW (embedding) WITH (metric = \'cosine\')'
        );
        logger.info('HNSW vector index rebuilt', { records: count, buildTimeMs: Date.now() - startTime });
      }
    } catch (error) {
      logger.error('Failed to rebuild HNSW index', { error: error.message });
    }
  }

  /**
   * Compact the HNSW index (prune deleted entries).
   */
  async compactHnswIndex() {
    if (!this.vssEnabled) return;
    try {
      await this.run('PRAGMA hnsw_compact_index(\'idx_memory_embedding_hnsw\')');
      logger.info('HNSW index compacted');
    } catch (error) {
      logger.warn('Failed to compact HNSW index', { error: error.message });
    }
  }

  /**
   * Execute a query and return all results
   */
  async query(sql, params = []) {
    if (!this.isInitialized) {
      await this.initialize();
    }

    try {
      const results = await this.all(sql, ...params);
      return results;
    } catch (error) {
      logger.error('Database query failed', { sql, error: error.message });
      throw error;
    }
  }

  /**
   * Execute a statement (INSERT, UPDATE, DELETE)
   */
  async execute(sql, params = []) {
    if (!this.isInitialized) {
      await this.initialize();
    }

    try {
      await this.run(sql, ...params);
      return { success: true };
    } catch (error) {
      logger.error('Database execution failed', { sql, error: error.message });
      throw error;
    }
  }

  /**
   * Get database statistics
   */
  async getStats() {
    const totalMemories = await this.query('SELECT COUNT(*) as count FROM memory');
    const totalEntities = await this.query('SELECT COUNT(*) as count FROM memory_entities');
    
    return {
      totalMemories: totalMemories[0]?.count || 0,
      totalEntities: totalEntities[0]?.count || 0
    };
  }

  /**
   * Health check
   */
  async healthCheck() {
    try {
      await this.query('SELECT 1');
      return { status: 'connected', database: this.dbPath };
    } catch (error) {
      return { status: 'disconnected', error: error.message };
    }
  }

  /**
   * Close database connection
   */
  async close() {
    if (this.connection) {
      try {
        // Checkpoint WAL to persist changes
        await this.run('CHECKPOINT');
        logger.info('Database checkpointed');
      } catch (error) {
        logger.warn('Failed to checkpoint database', { error: error.message });
      }
      this.connection.close();
      this.connection = null;
    }
    if (this.db) {
      this.db.close();
      this.db = null;
    }
    this.isInitialized = false;
    logger.info('Database connection closed');
  }
}

// Singleton instance
let dbInstance = null;

export function getDatabaseService() {
  if (!dbInstance) {
    dbInstance = new DatabaseService();
  }
  return dbInstance;
}

export default DatabaseService;
