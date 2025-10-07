import { getDatabaseService } from '../src/services/database.js';
import logger from '../src/utils/logger.js';
import dotenv from 'dotenv';

dotenv.config();

async function migrateDatabase() {
  try {
    logger.info('Starting database migration...');

    const db = getDatabaseService();
    await db.initialize();

    // Check if memory_entities table exists (DuckDB syntax)
    let tables = [];
    try {
      tables = await db.query(`
        SELECT table_name as name 
        FROM information_schema.tables 
        WHERE table_schema = 'main' AND table_name = 'memory_entities'
      `);
    } catch (error) {
      logger.warn('Could not check for existing tables, will create anyway', { error: error.message });
    }

    if (tables.length === 0) {
      logger.info('memory_entities table not found. Creating...');
      
      // Create memory_entities table
      await db.execute(`
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

      // Create indexes
      await db.execute('CREATE INDEX IF NOT EXISTS idx_memory_entities_memory_id ON memory_entities(memory_id)');
      await db.execute('CREATE INDEX IF NOT EXISTS idx_memory_entities_entity ON memory_entities(entity)');
      await db.execute('CREATE INDEX IF NOT EXISTS idx_memory_entities_type ON memory_entities(type)');
      await db.execute('CREATE INDEX IF NOT EXISTS idx_memory_entities_entity_type ON memory_entities(entity_type)');

      logger.info('✅ memory_entities table created successfully');
    } else {
      logger.info('✅ memory_entities table already exists');
      
      // Check if normalized_value column exists (DuckDB syntax)
      const columns = await db.query(`
        SELECT column_name as name 
        FROM information_schema.columns 
        WHERE table_name = 'memory_entities' AND table_schema = 'main'
      `);
      
      const hasNormalizedValue = columns.some(col => col.name === 'normalized_value');
      
      if (!hasNormalizedValue) {
        logger.info('Adding normalized_value column...');
        await db.execute(`
          ALTER TABLE memory_entities ADD COLUMN normalized_value TEXT
        `);
        logger.info('✅ normalized_value column added');
      }
    }

    // Verify table structure
    const stats = await db.getStats();
    logger.info('Database migration completed successfully', {
      totalMemories: stats.totalMemories,
      totalEntities: stats.totalEntities
    });

    // List all tables (DuckDB syntax)
    const allTables = await db.query(`
      SELECT table_name as name 
      FROM information_schema.tables 
      WHERE table_schema = 'main' 
      ORDER BY table_name
    `);
    
    logger.info('Database tables:', {
      tables: allTables.map(t => t.name)
    });

    await db.close();
    process.exit(0);
  } catch (error) {
    logger.error('Database migration failed', { error: error.message, stack: error.stack });
    process.exit(1);
  }
}

migrateDatabase();
