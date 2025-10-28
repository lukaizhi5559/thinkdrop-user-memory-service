import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import dotenv from 'dotenv';
import { getDatabaseService } from './services/database.js';
import { getEmbeddingService } from './services/embeddings.js';
import { getMetrics } from './middleware/metrics.js';
import logger from './utils/logger.js';

// Import middleware
import authMiddleware from './middleware/auth.js';
import { validateMCPRequest, validatePayloadSize } from './middleware/validation.js';
import metricsMiddleware from './middleware/metrics.js';
import errorHandler from './middleware/errorHandler.js';

// Import routes
import storeRoute from './routes/store.js';
import searchRoute from './routes/search.js';
import retrieveRoute from './routes/retrieve.js';
import updateRoute from './routes/update.js';
import deleteRoute from './routes/delete.js';
import listRoute from './routes/list.js';
import classifyRoute from './routes/classify.js';
import debugRoute from './routes/debug.js';
import healthRoute from './routes/health.js';

// Load environment variables
dotenv.config();

const app = express();
const PORT = process.env.PORT || 3001;
const HOST = process.env.HOST || '0.0.0.0';

// Middleware
app.use(helmet());
app.use(cors({
  origin: (process.env.ALLOWED_ORIGINS || '').split(','),
  credentials: true
}));
app.use(express.json({ limit: '1mb' }));
app.use(validatePayloadSize);
app.use(metricsMiddleware);

// Health check endpoint (no auth required)
app.get('/service.health', async (req, res) => {
  try {
    const db = getDatabaseService();
    const embeddings = getEmbeddingService();
    
    const dbHealth = await db.healthCheck();
    const embeddingHealth = await embeddings.healthCheck();
    const dbStats = await db.getStats();
    const metrics = getMetrics();
    const cacheStats = embeddings.getCacheStats();

    res.json({
      service: 'user-memory',
      version: '1.0.0',
      status: 'up',
      uptime: process.uptime(),
      database: dbHealth.status,
      embeddingModel: embeddingHealth.status,
      metrics: {
        totalMemories: Number(dbStats.totalMemories),
        requestCount: metrics.requestCount,
        errorRate: parseFloat(metrics.errorRate),
        avgResponseTime: metrics.avgResponseTime,
        embeddingCache: {
          size: cacheStats.size,
          maxSize: cacheStats.maxSize,
          hitRate: cacheStats.hitRate,
          hits: cacheStats.hits,
          misses: cacheStats.misses,
          totalRequests: cacheStats.totalRequests
        }
      }
    });
  } catch (error) {
    logger.error('Health check failed', { error: error.message });
    res.status(503).json({
      service: 'user-memory',
      version: '1.0.0',
      status: 'degraded',
      error: error.message
    });
  }
});

// Capabilities endpoint (no auth required)
app.get('/service.capabilities', (req, res) => {
  res.json({
    service: 'user-memory',
    version: '1.0.0',
    capabilities: {
      actions: [
        {
          name: 'memory.store',
          description: 'Store a new memory with entities and embeddings',
          inputSchema: {
            text: 'string (required)',
            entities: 'array (optional)',
            metadata: 'object (optional)',
            screenshot: 'string (optional)',
            extractedText: 'string (optional)',
            sessionId: 'string (optional)',
            userId: 'string (optional)'
          }
        },
        {
          name: 'memory.search',
          description: 'Semantic search across memories',
          inputSchema: {
            query: 'string (required)',
            limit: 'number (optional, default: 25)',
            offset: 'number (optional, default: 0)',
            filters: 'object (optional)',
            minSimilarity: 'number (optional, default: 0.3)'
          }
        },
        {
          name: 'memory.retrieve',
          description: 'Retrieve specific memory by ID',
          inputSchema: {
            memoryId: 'string (optional)',
            filters: 'object (optional)',
            limit: 'number (optional, default: 25)'
          }
        },
        {
          name: 'memory.update',
          description: 'Update existing memory',
          inputSchema: {
            memoryId: 'string (required)',
            updates: 'object (required)'
          }
        },
        {
          name: 'memory.delete',
          description: 'Delete memory by ID',
          inputSchema: {
            memoryId: 'string (required)'
          }
        },
        {
          name: 'memory.list',
          description: 'List memories with pagination',
          inputSchema: {
            limit: 'number (optional, default: 25)',
            offset: 'number (optional, default: 0)',
            filters: 'object (optional)',
            sortBy: 'string (optional, default: created_at)',
            sortOrder: 'string (optional, default: DESC)'
          }
        },
        {
          name: 'memory.classify-conversational-query',
          description: 'Classify if query references conversation history',
          inputSchema: {
            query: 'string (required)',
            sessionId: 'string (optional)'
          }
        },
        {
          name: 'memory.debug-embedding',
          description: 'Test embedding generation and view statistics',
          inputSchema: {
            text: 'string (required)'
          }
        },
        {
          name: 'memory.health-check',
          description: 'Comprehensive health check for memory service',
          inputSchema: {}
        }
      ],
      features: [
        'semantic-search',
        'entity-extraction',
        'embeddings',
        'cross-session-search',
        'screenshot-support',
        'conversational-context'
      ]
    }
  });
});

// Apply auth and validation to all MCP routes
app.use(authMiddleware);
app.use(validateMCPRequest);

// MCP action routes
app.use(storeRoute);
app.use(searchRoute);
app.use(retrieveRoute);
app.use(updateRoute);
app.use(deleteRoute);
app.use(listRoute);
app.use(classifyRoute);
app.use(debugRoute);
app.use(healthRoute);

// Error handler (must be last)
app.use(errorHandler);

// Initialize services and start server
async function startServer() {
  try {
    logger.info('Initializing services...');

    // Initialize database
    const db = getDatabaseService();
    await db.initialize();
    logger.info('Database service initialized');

    // Initialize embedding model
    const embeddings = getEmbeddingService();
    await embeddings.initialize();
    logger.info('Embedding service initialized');

    // Start server
    app.listen(PORT, HOST, () => {
      const serviceUrl = `http://${HOST}:${PORT}`;
      
      // Print formatted startup message
      console.log('\n🧠 User Memory MCP Service is running');
      console.log(`   URL: ${serviceUrl}`);
      console.log(`   Health: ${serviceUrl}/service.health`);
      console.log(`   Capabilities: ${serviceUrl}/service.capabilities`);
      console.log('\n📊 Available Actions:');
      console.log('   - POST /memory.store');
      console.log('   - POST /memory.search');
      console.log('   - POST /memory.retrieve');
      console.log('   - POST /memory.update');
      console.log('   - POST /memory.delete');
      console.log('   - POST /memory.list');
      console.log('   - POST /memory.classify-conversational-query\n');
      
      logger.info(`UserMemory service running on ${serviceUrl}`);
      logger.info(`Environment: ${process.env.NODE_ENV || 'development'}`);
      logger.info(`Database: ${process.env.DB_PATH || './data/user_memory.duckdb'}`);
    });
  } catch (error) {
    logger.error('Failed to start server', { error: error.message, stack: error.stack });
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGTERM', async () => {
  logger.info('SIGTERM received, shutting down gracefully...');
  const db = getDatabaseService();
  await db.close();
  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.info('SIGINT received, shutting down gracefully...');
  const db = getDatabaseService();
  await db.close();
  process.exit(0);
});

// Start the server only if not in test mode
if (process.env.NODE_ENV !== 'test') {
  startServer();
}

export default app;
