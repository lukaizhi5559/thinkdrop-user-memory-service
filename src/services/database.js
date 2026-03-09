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

      // Create api_rules table for per-service API contract rules used by skill generation pipeline
      // service:   npm/API service name (e.g. 'clicksend', 'twilio', 'stripe', 'gmail')
      // rule_type: 'auth' | 'payload' | 'secret' | 'endpoint' | 'gotcha'
      // code_pattern: optional regex string to detect violations in generated skill code
      // fix_hint:     exact correction to give the LLM when violation is found
      // source:       'system' (seed rules) | 'learned' (written from runtime failures)
      logger.info('Creating api_rules table...');
      await this.run(`
        CREATE TABLE IF NOT EXISTS api_rules (
          id TEXT PRIMARY KEY,
          service TEXT NOT NULL,
          rule_type TEXT NOT NULL DEFAULT 'gotcha',
          rule_text TEXT NOT NULL,
          code_pattern TEXT,
          fix_hint TEXT,
          source TEXT DEFAULT 'system',
          hit_count INTEGER DEFAULT 0,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      await this.run('CREATE INDEX IF NOT EXISTS idx_api_rules_service ON api_rules(service)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_api_rules_type ON api_rules(rule_type)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_api_rules_source ON api_rules(source)');
      logger.info('Api_rules table created');
      await this.seedApiRules();

      // Create intent_overrides table for user-corrected intent learning.
      // When the user corrects a wrong classification ("no, I meant go to the webpage"),
      // we store the original phrase + correct intent here and check it before phi4
      // on future prompts — so the same phrasing never misclassifies again.
      logger.info('Creating intent_overrides table...');
      await this.run(`
        CREATE TABLE IF NOT EXISTS intent_overrides (
          id TEXT PRIMARY KEY,
          example_prompt TEXT NOT NULL,
          correct_intent TEXT NOT NULL,
          wrong_intent TEXT,
          embedding FLOAT[384],
          source TEXT DEFAULT 'user_correction',
          hit_count INTEGER DEFAULT 0,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      await this.run('CREATE INDEX IF NOT EXISTS idx_intent_overrides_intent ON intent_overrides(correct_intent)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_intent_overrides_created ON intent_overrides(created_at)');
      logger.info('Intent_overrides table created');

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
   * Seed api_rules with known API contract rules on first boot (idempotent).
   * Only inserts rules that don't already exist for a given service+rule_type+text combo.
   */
  async seedApiRules() {
    const SQ = '\'';
    const rules = [
      // ── ClickSend ────────────────────────────────────────────────────────────
      { service: 'clicksend', rule_type: 'auth',
        rule_text: 'ClickSend Basic Auth MUST use both username AND API key: Buffer.from(secrets.CLICKSEND_USERNAME + ":" + secrets.CLICKSEND_API_KEY).toString("base64"). Empty username (":" + key) always returns 401 invalid_request.',
        code_pattern: 'Buffer\\.from\\s*\\([`\'"]+:\\s*[`\'"]*\\$?\\{?\\s*\\w*(?:API_KEY|api_key)',
        fix_hint: 'Replace Buffer.from(":" + key) with Buffer.from(secrets.CLICKSEND_USERNAME + ":" + secrets.CLICKSEND_API_KEY).toString("base64")' },
      { service: 'clicksend', rule_type: 'secret',
        rule_text: 'ClickSend requires TWO secrets: CLICKSEND_USERNAME (account email) AND CLICKSEND_API_KEY. Both must be in the Skill Interface secrets list.',
        code_pattern: null,
        fix_hint: 'Add CLICKSEND_USERNAME and CLICKSEND_API_KEY to secrets in plan.md Skill Interface.' },
      { service: 'clicksend', rule_type: 'payload',
        rule_text: 'ClickSend SMS POST /v3/sms/send requires: { messages: [{ to, body, source }] }. Flat { to, message } format returns invalid_request.',
        code_pattern: '"to"\\s*:\\s*(?:secrets|RECIPIENT|phone)(?!.*messages\\s*:)',
        fix_hint: 'Use: JSON.stringify({ messages: [{ to: secrets.RECIPIENT_PHONE_NUMBER, body: text, source: "thinkdrop" }] })' },
      { service: 'clicksend', rule_type: 'endpoint',
        rule_text: 'ClickSend SMS endpoint: POST https://rest.clicksend.com/v3/sms/send with Content-Type: application/json and Authorization: Basic <base64>.',
        code_pattern: null,
        fix_hint: 'hostname: "rest.clicksend.com", path: "/v3/sms/send", method: "POST"' },
      { service: 'clicksend', rule_type: 'gotcha',
        rule_text: 'npm package "clicksend" is TypeScript-only — require("clicksend") throws at runtime. Use Node.js built-in https to call the REST API directly.',
        code_pattern: 'require\\s*\\([\'"]{1}clicksend[\'"]{1}\\)',
        fix_hint: 'Remove require("clicksend"). Call https://rest.clicksend.com/v3/sms/send directly with https.request().' },
      // ── Twilio ───────────────────────────────────────────────────────────────
      { service: 'twilio', rule_type: 'auth',
        rule_text: 'Twilio uses HTTP Basic Auth: Buffer.from(secrets.TWILIO_ACCOUNT_SID + ":" + secrets.TWILIO_AUTH_TOKEN).toString("base64"). SID is username, Auth Token is password.',
        code_pattern: null,
        fix_hint: '"Basic " + Buffer.from(secrets.TWILIO_ACCOUNT_SID + ":" + secrets.TWILIO_AUTH_TOKEN).toString("base64")' },
      { service: 'twilio', rule_type: 'secret',
        rule_text: 'Twilio requires: TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER (sender). All three must be in the secrets list.',
        code_pattern: null,
        fix_hint: 'Declare TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER in secrets.' },
      { service: 'twilio', rule_type: 'payload',
        rule_text: 'Twilio SMS POST /2010-04-01/Accounts/{SID}/Messages.json requires Content-Type: application/x-www-form-urlencoded body with To, From, Body fields.',
        code_pattern: null,
        fix_hint: 'Use new URLSearchParams({ To, From, Body }).toString() with Content-Type: application/x-www-form-urlencoded.' },
      // ── Gmail ────────────────────────────────────────────────────────────────
      { service: 'gmail', rule_type: 'auth',
        rule_text: 'Gmail API uses OAuth2. Required secrets: GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REDIRECT_URI, GOOGLE_REFRESH_TOKEN. Use googleapis (ships compiled JS).',
        code_pattern: null,
        fix_hint: 'const { google } = require("googleapis"); const auth = new google.auth.OAuth2(clientId, secret, redirect); auth.setCredentials({ refresh_token });' },
      { service: 'gmail', rule_type: 'secret',
        rule_text: 'Gmail requires four OAuth secrets: GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REDIRECT_URI, GOOGLE_REFRESH_TOKEN.',
        code_pattern: null,
        fix_hint: 'Add all four to secrets list.' },
      { service: 'gmail', rule_type: 'gotcha',
        rule_text: 'gmail-cli and mail-cli do not exist as real npm packages. Use the googleapis package for Gmail API access.',
        code_pattern: 'gmail-cli|mail-cli|execSync.*gmail|spawn.*gmail',
        fix_hint: 'Replace with: const { google } = require("googleapis").' },
      // ── Stripe ───────────────────────────────────────────────────────────────
      { service: 'stripe', rule_type: 'auth',
        rule_text: 'Stripe uses Bearer auth: Authorization: "Bearer " + secrets.STRIPE_SECRET_KEY. The stripe npm package ships compiled CommonJS.',
        code_pattern: null,
        fix_hint: 'require("stripe")(secrets.STRIPE_SECRET_KEY) or https with Authorization: "Bearer " + secrets.STRIPE_SECRET_KEY' },
      { service: 'stripe', rule_type: 'secret',
        rule_text: 'Stripe requires STRIPE_SECRET_KEY (sk_live_... or sk_test_...). For webhooks also declare STRIPE_WEBHOOK_SECRET.',
        code_pattern: null,
        fix_hint: 'Declare STRIPE_SECRET_KEY in secrets.' },
      // ── SendGrid ─────────────────────────────────────────────────────────────
      { service: 'sendgrid', rule_type: 'auth',
        rule_text: 'SendGrid uses Bearer auth: Authorization: "Bearer " + secrets.SENDGRID_API_KEY. Keys start with "SG.".',
        code_pattern: null,
        fix_hint: '{ "Authorization": "Bearer " + secrets.SENDGRID_API_KEY, "Content-Type": "application/json" }' },
      { service: 'sendgrid', rule_type: 'payload',
        rule_text: 'SendGrid POST /v3/mail/send payload: { personalizations:[{to:[{email}]}], from:{email}, subject, content:[{type:"text/plain",value}] }.',
        code_pattern: null,
        fix_hint: 'Payload: { personalizations:[{to:[{email:secrets.TO_EMAIL}]}], from:{email:secrets.FROM_EMAIL}, subject, content:[{type:"text/plain",value}] }' },
      // ── Slack ────────────────────────────────────────────────────────────────
      { service: 'slack', rule_type: 'auth',
        rule_text: 'Slack Web API uses Bearer token: Authorization: "Bearer " + secrets.SLACK_BOT_TOKEN. @slack/web-api ships compiled CommonJS.',
        code_pattern: null,
        fix_hint: 'require("@slack/web-api") WebClient(secrets.SLACK_BOT_TOKEN) or https Authorization: "Bearer " + secrets.SLACK_BOT_TOKEN' },
      { service: 'slack', rule_type: 'secret',
        rule_text: 'Slack requires SLACK_BOT_TOKEN (xoxb-...) and SLACK_CHANNEL_ID for posting.',
        code_pattern: null,
        fix_hint: 'Declare SLACK_BOT_TOKEN and SLACK_CHANNEL_ID in secrets.' },
      // ── GitHub ───────────────────────────────────────────────────────────────
      { service: 'github', rule_type: 'auth',
        rule_text: 'GitHub API: Authorization: "Bearer " + secrets.GITHUB_TOKEN. Personal tokens start with "ghp_", fine-grained with "github_pat_".',
        code_pattern: null,
        fix_hint: '{ "Authorization": "Bearer " + secrets.GITHUB_TOKEN, "Accept": "application/vnd.github.v3+json" }' },
      // ── OpenAI ───────────────────────────────────────────────────────────────
      { service: 'openai', rule_type: 'auth',
        rule_text: 'OpenAI API: Authorization: "Bearer " + secrets.OPENAI_API_KEY. Keys start with "sk-".',
        code_pattern: null,
        fix_hint: '{ "Authorization": "Bearer " + secrets.OPENAI_API_KEY, "Content-Type": "application/json" }' },
    ];

    let seeded = 0;
    for (const rule of rules) {
      try {
        const safeService  = rule.service.replace(/'/g, SQ + SQ);
        const safeType     = rule.rule_type.replace(/'/g, SQ + SQ);
        const safeText     = rule.rule_text.replace(/'/g, SQ + SQ);
        const existing     = await this.all(
          `SELECT id FROM api_rules WHERE service = '${safeService}' AND rule_type = '${safeType}' AND rule_text = '${safeText}'`
        );
        if (existing && existing.length > 0) continue;

        const id          = `ar_seed_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
        const patternVal  = rule.code_pattern ? `'${rule.code_pattern.replace(/'/g, SQ + SQ)}'` : 'NULL';
        const fixHintVal  = rule.fix_hint     ? `'${rule.fix_hint.replace(/'/g, SQ + SQ)}'`     : 'NULL';
        await this.run(`
          INSERT INTO api_rules (id, service, rule_type, rule_text, code_pattern, fix_hint, source, hit_count, created_at, updated_at)
          VALUES ('${id}','${safeService}','${safeType}','${safeText}',${patternVal},${fixHintVal},'system',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
        `);
        seeded++;
      } catch (e) {
        logger.warn(`[seedApiRules] Failed to seed ${rule.service}:${rule.rule_type}`, { error: e.message });
      }
    }
    if (seeded > 0) logger.info(`[seedApiRules] Seeded ${seeded} api_rules`);
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
