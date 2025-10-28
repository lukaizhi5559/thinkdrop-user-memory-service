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

    return new Promise((resolve, reject) => {
      try {
        // Ensure data directory exists
        const dir = path.dirname(this.dbPath);
        if (!fs.existsSync(dir)) {
          fs.mkdirSync(dir, { recursive: true });
          logger.info(`Created database directory: ${dir}`);
        }

        // Create database instance with callback
        this.db = new duckdb.Database(this.dbPath, async (err) => {
          if (err) {
            logger.error('Failed to create database', { error: err.message });
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

            this.isInitialized = true;
            logger.info(`Database initialized successfully: ${this.dbPath}`);
            resolve();
          } catch (error) {
            logger.error('Failed to initialize database', { error: error.message });
            reject(error);
          }
        });
      } catch (error) {
        logger.error('Failed to initialize database', { error: error.message });
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
          primary_intent TEXT,
          requires_memory_access BOOLEAN DEFAULT FALSE,
          suggested_response TEXT,
          source_text TEXT,
          metadata TEXT,
          screenshot TEXT,
          extracted_text TEXT,
          embedding FLOAT[384],
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          synced_to_backend BOOLEAN DEFAULT FALSE,
          backend_memory_id TEXT,
          sync_attempts INTEGER DEFAULT 0,
          last_sync_attempt TIMESTAMP
        )
      `);
      logger.info('Memory table created');

      // Create single-column indexes for memory table
      await this.run('CREATE INDEX IF NOT EXISTS idx_memory_user_id ON memory(user_id)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_memory_type ON memory(type)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_memory_created_at ON memory(created_at)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_memory_primary_intent ON memory(primary_intent)');
      
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

      logger.info('Database tables created successfully');
    } catch (error) {
      logger.error('Failed to create tables', { error: error.message, stack: error.stack });
      throw error;
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
