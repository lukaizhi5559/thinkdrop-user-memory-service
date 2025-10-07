import { getDatabaseService } from '../src/services/database.js';
import logger from '../src/utils/logger.js';
import dotenv from 'dotenv';

dotenv.config();

async function initializeDatabase() {
  try {
    logger.info('Starting database initialization...');

    const db = getDatabaseService();
    await db.initialize();

    const stats = await db.getStats();
    logger.info('Database initialized successfully', stats);

    await db.close();
    process.exit(0);
  } catch (error) {
    logger.error('Database initialization failed', { error: error.message });
    process.exit(1);
  }
}

initializeDatabase();
