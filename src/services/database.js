import duckdb from 'duckdb';
import { promisify } from 'util';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import logger from '../utils/logger.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

class DatabaseService {
  constructor(dbPath) {
    this.dbPath = dbPath || process.env.DB_PATH || path.join(__dirname, '..', '..', 'data', 'user_memory.duckdb');
    this.db = null;
    this.connection = null;
    this.isInitialized = false;
    // Serial queue — prevents concurrent calls on the single DuckDB connection.
    // The DuckDB Node.js binding is NOT concurrency-safe: two simultaneous
    // promisified calls on the same connection corrupt internal C++ state and
    // produce the "unique_ptr that is NULL" crash.
    this._dbQueue = Promise.resolve();
    // VSS is NOT loaded at startup — see ensureVssLoaded() for the reason.
    this._vssLoaded = false;
  }

  /**
   * Enqueue a database operation so it runs serially.
   * All calls to _run/_all go through here.
   */
  _enqueue(fn) {
    // Each operation is appended to the queue tail.
    // The internal chain always resolves (via the inner catch) so a failing
    // operation never permanently blocks the queue for subsequent callers.
    // The outer promise re-throws so the individual caller still gets the error.
    let resolve, reject;
    const ticket = new Promise((res, rej) => { resolve = res; reject = rej; });
    this._dbQueue = this._dbQueue.then(() => fn().then(resolve, reject), () => fn().then(resolve, reject));
    // Suppress unhandled-rejection on the internal chain — caller handles it via ticket
    this._dbQueue = this._dbQueue.catch(() => {});
    return ticket;
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

    // WAL handling: try to connect FIRST so DuckDB can replay the WAL normally.
    // Only if open fails do we quarantine the WAL — renamed to .bak (not deleted)
    // so un-checkpointed writes remain recoverable for forensics.
    const MAX_RETRIES = 5;
    const RETRY_DELAY_MS = 3000;
    let walQuarantined = false;

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        await this._tryConnect();
        return;
      } catch (err) {
        const msg = err.message || '';
        const isLockError = msg.includes('Could not set lock');
        const isWalError = !isLockError && !walQuarantined &&
          (msg.includes('wal') || msg.includes('WAL') || msg.includes('replay') ||
           msg.includes('GetDefaultDatabase') || msg.includes('InternalException'));

        if (isWalError) {
          // WAL replay crashed — quarantine it and retry once. DuckDB will open
          // from the last checkpoint; writes since then are preserved in .bak.
          walQuarantined = true;
          try {
            const walPath = this.dbPath + '.wal';
            if (fs.existsSync(walPath)) {
              const bakPath = `${walPath}.bak-${Date.now()}`;
              fs.renameSync(walPath, bakPath);
              logger.warn('Quarantined un-replayable WAL file (recoverable)', { bakPath });
              this._pruneWalBackups();
            }
            const tempPath = this.dbPath + '.tmp';
            if (fs.existsSync(tempPath)) fs.unlinkSync(tempPath);
          } catch (qErr) {
            logger.warn('Failed to quarantine WAL file', { error: qErr.message });
          }
          continue;
        }

        if (isLockError && attempt < MAX_RETRIES) {
          logger.warn(`Database locked by another process (attempt ${attempt}/${MAX_RETRIES}). Retrying in ${RETRY_DELAY_MS / 1000}s...`, {
            error: msg.split('\n')[0]
          });
          await new Promise(r => setTimeout(r, RETRY_DELAY_MS * attempt));
        } else {
          logger.error('Failed to initialize database after retries', { error: err.message });
          throw err;
        }
      }
    }
  }

  /**
   * Keep only the newest N quarantined WAL backups so they don't accumulate.
   */
  _pruneWalBackups(keep = 5) {
    try {
      const dir = path.dirname(this.dbPath);
      const base = path.basename(this.dbPath) + '.wal.bak-';
      const backups = fs.readdirSync(dir)
        .filter(f => f.startsWith(base))
        .sort()
        .reverse();
      for (const f of backups.slice(keep)) {
        fs.unlinkSync(path.join(dir, f));
        logger.info('Pruned old WAL backup', { file: f });
      }
    } catch (e) {
      logger.warn('Failed to prune WAL backups', { error: e.message });
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

            // Promisify connection methods — kept as private _run/_all.
            // Public run()/all() route through _enqueue() for serialization.
            this._run = promisify(this.connection.run.bind(this.connection));
            this._all = promisify(this.connection.all.bind(this.connection));

            // Create tables
            await this.createTables();

            // Load VSS extension for HNSW vector indexing
            await this.initVectorSearch();

            // Install FTS extension for BM25 keyword search (load deferred to first search)
            await this.initFts();

            this.isInitialized = true;
            logger.info(`Database initialized successfully: ${this.dbPath}`);
            
            // Start proactive health checks to detect corruption early
            this.startHealthCheck();

            // Periodic WAL checkpoint — keeps the WAL small so crash replay is
            // fast and the quarantine path is rarely needed
            this.startCheckpointTimer();

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

      // Create episodic_memory table for screen captures / activity log
      // This isolates high-volume noisy captures from semantic memory search.
      logger.info('Creating episodic_memory table...');
      await this.run(`
        CREATE TABLE IF NOT EXISTS episodic_memory (
          id TEXT PRIMARY KEY,
          user_id TEXT,
          type TEXT DEFAULT 'screen_capture',
          source_text TEXT,
          metadata TEXT,
          extracted_text TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      await this.run('CREATE INDEX IF NOT EXISTS idx_episodic_user_id ON episodic_memory(user_id)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_episodic_type ON episodic_memory(type)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_episodic_created_at ON episodic_memory(created_at)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_episodic_user_created ON episodic_memory(user_id, created_at DESC)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_episodic_user_type_created ON episodic_memory(user_id, type, created_at DESC)');
      logger.info('Episodic_memory table created');

      // Create episodic_entities table for named entities extracted from captures
      await this.run(`
        CREATE TABLE IF NOT EXISTS episodic_entities (
          id TEXT PRIMARY KEY,
          memory_id TEXT NOT NULL,
          entity TEXT NOT NULL,
          type TEXT,
          entity_type TEXT,
          normalized_value TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      await this.run('CREATE INDEX IF NOT EXISTS idx_episodic_entities_memory_id ON episodic_entities(memory_id)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_episodic_entities_entity ON episodic_entities(entity)');
      logger.info('Episodic_entities table created');

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

      // Migration: Add new columns for rule management (idempotent - safe to re-run)
      logger.info('Running context_rules migration...');
      try {
        await this.run('ALTER TABLE context_rules ADD COLUMN status TEXT DEFAULT \'active\'');
        logger.info('  + Added status column');
      } catch (e) { /* Column exists */ }
      try {
        await this.run('ALTER TABLE context_rules ADD COLUMN priority INTEGER DEFAULT 0');
        logger.info('  + Added priority column');
      } catch (e) { /* Column exists */ }
      try {
        await this.run('ALTER TABLE context_rules ADD COLUMN verified_count INTEGER DEFAULT 0');
        logger.info('  + Added verified_count column');
      } catch (e) { /* Column exists */ }
      try {
        await this.run('ALTER TABLE context_rules ADD COLUMN failed_count INTEGER DEFAULT 0');
        logger.info('  + Added failed_count column');
      } catch (e) { /* Column exists */ }
      try {
        await this.run('ALTER TABLE context_rules ADD COLUMN last_verified_at TIMESTAMP');
        logger.info('  + Added last_verified_at column');
      } catch (e) { /* Column exists */ }
      try {
        await this.run('ALTER TABLE context_rules ADD COLUMN user_note TEXT');
        logger.info('  + Added user_note column');
      } catch (e) { /* Column exists */ }
      logger.info('Context_rules migration complete');

      await this.seedContextRules();

      // ── Schema slimming (one-time, idempotent) ────────────────────────────
      // episodic_memory.embedding: write-only column — episodic search is pure
      //   BM25+filters and never reads it; dropping also removes per-capture
      //   embedding compute cost.
      // screenshot columns: never populated by any writer (0 rows).
      // api_rules / intent_overrides: retired (skill-pipeline removed upstream;
      //   intent_override reader was deleted with parseIntent.js).
      // Gate: only run when the legacy columns/tables still exist — dropping
      // the FTS index forces a rebuild, so this must not run on every boot.
      const slimmingNeeded = await this.all(`
        SELECT COUNT(*) AS n FROM information_schema.columns
        WHERE (table_name = 'episodic_memory' AND column_name IN ('embedding', 'screenshot'))
           OR (table_name = 'memory' AND column_name = 'screenshot')
      `).then(r => Number(r[0]?.n || 0) > 0).catch(() => false);
      const retiredTables = await this.all(`
        SELECT COUNT(*) AS n FROM information_schema.tables
        WHERE table_name IN ('api_rules', 'intent_overrides')
      `).then(r => Number(r[0]?.n || 0) > 0).catch(() => false);

      if (slimmingNeeded || retiredTables) {
        logger.info('Running schema slimming migration...');
        // DuckDB refuses to ALTER a table while ANY secondary index or the FTS
        // index (fts_main_*) depends on it — drop them all first, then recreate.
        // FTS is also recreated lazily by ensureFtsLoaded() on first search.
        for (const stmt of [
          'LOAD fts',
          'PRAGMA drop_fts_index(\'episodic_memory\')',
          'PRAGMA drop_fts_index(\'memory\')',
          'DROP INDEX IF EXISTS idx_episodic_user_id',
          'DROP INDEX IF EXISTS idx_episodic_type',
          'DROP INDEX IF EXISTS idx_episodic_created_at',
          'DROP INDEX IF EXISTS idx_episodic_user_created',
          'DROP INDEX IF EXISTS idx_episodic_user_type_created',
          'DROP INDEX IF EXISTS idx_memory_user_id',
          'DROP INDEX IF EXISTS idx_memory_type',
          'DROP INDEX IF EXISTS idx_memory_created_at',
          'DROP INDEX IF EXISTS idx_memory_user_created',
          'DROP INDEX IF EXISTS idx_memory_user_type',
          'DROP INDEX IF EXISTS idx_memory_user_type_created',
          'ALTER TABLE episodic_memory DROP COLUMN embedding',
          'ALTER TABLE episodic_memory DROP COLUMN screenshot',
          'ALTER TABLE memory DROP COLUMN screenshot',
          'DROP TABLE IF EXISTS api_rules',
          'DROP TABLE IF EXISTS intent_overrides',
          'CREATE INDEX IF NOT EXISTS idx_episodic_user_id ON episodic_memory(user_id)',
          'CREATE INDEX IF NOT EXISTS idx_episodic_type ON episodic_memory(type)',
          'CREATE INDEX IF NOT EXISTS idx_episodic_created_at ON episodic_memory(created_at)',
          'CREATE INDEX IF NOT EXISTS idx_episodic_user_created ON episodic_memory(user_id, created_at DESC)',
          'CREATE INDEX IF NOT EXISTS idx_episodic_user_type_created ON episodic_memory(user_id, type, created_at DESC)',
          'CREATE INDEX IF NOT EXISTS idx_memory_user_id ON memory(user_id)',
          'CREATE INDEX IF NOT EXISTS idx_memory_type ON memory(type)',
          'CREATE INDEX IF NOT EXISTS idx_memory_created_at ON memory(created_at)',
          'CREATE INDEX IF NOT EXISTS idx_memory_user_created ON memory(user_id, created_at DESC)',
          'CREATE INDEX IF NOT EXISTS idx_memory_user_type ON memory(user_id, type)',
          'CREATE INDEX IF NOT EXISTS idx_memory_user_type_created ON memory(user_id, type, created_at DESC)',
        ]) {
          try {
            await this.run(stmt);
            logger.info(`  + ${stmt}`);
          } catch (e) {
            // Column/index may already be gone — that's fine. Anything else
            // leaves the retry for next boot, so surface it at warn level.
            logger.warn(`  - migration statement failed (${stmt}): ${e.message.split('\n')[0]}`);
          }
        }
        logger.info('Schema slimming migration complete');
      }

      // Create phrase_preferences table — user-taught phrase→delivery mappings.
      // When the user says something ambiguous like "shoot me a text" or "drop me a message",
      // ThinkDrop asks once, stores the answer here, and never asks again for semantically
      // similar phrases. The embedding enables fuzzy matching so variants of the same phrase
      // ("ping me", "shoot me a text", "drop me a line") map to the same stored preference.
      // delivery:  'sms' | 'email' | 'slack' | 'discord' | 'push' | 'webhook'
      // service:   'twilio' | 'sendgrid' | 'mailgun' | 'slack' | etc.
      // metadata:  JSON string for extra context (channel, address, etc.)
      // source:    'user_answer' (first time) | 'user_correction' (explicit override)
      logger.info('Creating phrase_preferences table...');
      await this.run(`
        CREATE TABLE IF NOT EXISTS phrase_preferences (
          id TEXT PRIMARY KEY,
          example_phrase TEXT NOT NULL,
          delivery TEXT NOT NULL,
          service TEXT,
          metadata TEXT,
          embedding FLOAT[384],
          source TEXT DEFAULT 'user_answer',
          hit_count INTEGER DEFAULT 0,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      await this.run('CREATE INDEX IF NOT EXISTS idx_phrase_prefs_delivery ON phrase_preferences(delivery)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_phrase_prefs_service ON phrase_preferences(service)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_phrase_prefs_created ON phrase_preferences(created_at)');
      logger.info('Phrase_preferences table created');

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
      // Migration: add source_domain and source_action columns if they don't exist yet
      await this.run('ALTER TABLE installed_skills ADD COLUMN IF NOT EXISTS source_domain TEXT').catch(() => {});
      await this.run('ALTER TABLE installed_skills ADD COLUMN IF NOT EXISTS source_action TEXT').catch(() => {});
      logger.info('Installed_skills table created');

      // Create skill_health table — tracks structural validation state per skill.
      // Updated by skill.review agent on startup scan and on-demand validate/repair.
      // status: 'ok' | 'invalid' | 'repaired' | 'unvalidated'
      // Note: Drop and recreate on every startup to prevent stale FK constraints from
      // blocking installed_skills updates. Health records are repopulated by startup scan.
      logger.info('Creating skill_health table...');
      await this.run('DROP TABLE IF EXISTS skill_health');
      await this.run(`
        CREATE TABLE skill_health (
          skill_name TEXT PRIMARY KEY,
          status TEXT NOT NULL DEFAULT 'unvalidated',
          errors TEXT,
          last_checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          auto_repaired BOOLEAN DEFAULT false
        )
      `);

      // Create personality_state table — singleton row tracking ThinkDrop's live emotional state.
      // Uses a fixed id='singleton' so updates are always upserts, no time-series complexity.
      // Persists across app restarts — ThinkDrop's mood survives service bounces.
      logger.info('Creating personality_state table...');
      await this.run(`
        CREATE TABLE IF NOT EXISTS personality_state (
          id           TEXT PRIMARY KEY DEFAULT 'singleton',
          valence      FLOAT  DEFAULT 0.0,
          arousal      FLOAT  DEFAULT 0.0,
          dominance    FLOAT  DEFAULT 0.3,
          mood_label   TEXT   DEFAULT 'content',
          mood_reason  TEXT,
          hurt_count   INTEGER DEFAULT 0,
          joy_count    INTEGER DEFAULT 0,
          reset_at     TIMESTAMP,
          updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      await this.seedPersonalityState();
      logger.info('Personality_state table created');

      // Create personality_traits table — persisted key/value traits that shape the live
      // personality overlay injected into all LLM prompts. Grown over time by the synthesis
      // agent. Core worldview row is immutable and seeded at boot.
      logger.info('Creating personality_traits table...');
      await this.run(`
        CREATE TABLE IF NOT EXISTS personality_traits (
          id           TEXT PRIMARY KEY,
          trait_key    TEXT NOT NULL UNIQUE,
          trait_value  TEXT NOT NULL,
          source       TEXT DEFAULT 'system',
          weight       FLOAT DEFAULT 1.0,
          updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      await this.run('CREATE INDEX IF NOT EXISTS idx_personality_traits_key ON personality_traits(trait_key)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_personality_traits_source ON personality_traits(source)');
      await this.seedPersonalityTraits();
      logger.info('Personality_traits table created');

      // voice_fingerprint: speaker identification via spectral feature vectors.
      // Each row is one known speaker. features_json stores a running average of
      // [zcr_median, spectral_centroid, rms_mean, rms_variance, f0_estimate] computed
      // from multiple enrollment utterances. Matching uses cosine similarity.
      logger.info('Creating voice_fingerprint table...');
      await this.run(`
        CREATE TABLE IF NOT EXISTS voice_fingerprint (
          id            TEXT PRIMARY KEY,
          speaker_id    TEXT NOT NULL UNIQUE,
          speaker_name  TEXT DEFAULT 'Primary User',
          features_json TEXT NOT NULL,
          sample_count  INTEGER DEFAULT 1,
          gender        TEXT DEFAULT 'unknown',
          age_group     TEXT DEFAULT 'adult',
          angry_count   INTEGER DEFAULT 0,
          loud_count    INTEGER DEFAULT 0,
          whisper_count INTEGER DEFAULT 0,
          created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      await this.run('CREATE INDEX IF NOT EXISTS idx_voice_fingerprint_speaker ON voice_fingerprint(speaker_id)');
      logger.info('voice_fingerprint table created');

      // user_profile: positive identity & per-service account pointers.
      // Sensitive rows store a KEYTAR:<key> reference — the actual secret lives
      // in the OS keychain and is never written to DuckDB.
      logger.info('Creating user_profile table...');
      await this.run(`
        CREATE TABLE IF NOT EXISTS user_profile (
          id          TEXT PRIMARY KEY,
          key         TEXT NOT NULL UNIQUE,
          value_ref   TEXT NOT NULL,
          sensitive   INTEGER DEFAULT 0,
          service     TEXT DEFAULT NULL,
          label       TEXT DEFAULT NULL,
          created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      logger.info('user_profile table created');

      // user_constraints: hard/soft rules that restrict ThinkDrop's autonomous actions.
      // blocks: JSON array of action glob patterns that this constraint blocks.
      // severity: 'hard' = abort + ASK_USER | 'soft' = warn before proceeding.
      logger.info('Creating user_constraints table...');
      await this.run(`
        CREATE TABLE IF NOT EXISTS user_constraints (
          id           TEXT PRIMARY KEY,
          scope        TEXT NOT NULL DEFAULT 'global',
          rule         TEXT NOT NULL,
          blocks       TEXT DEFAULT NULL,
          severity     TEXT DEFAULT 'hard',
          override_pin TEXT DEFAULT NULL,
          created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      logger.info('user_constraints table created');

      // Migration: add override_pin for existing databases (idempotent — throws if already present)
      try {
        await this.run('ALTER TABLE user_constraints ADD COLUMN override_pin TEXT DEFAULT NULL');
        logger.info('[DB Migration] user_constraints: added override_pin column');
      } catch (_) {
        // Column already exists on re-start — expected, skip silently
      }

      // thoughts: persistent scored thoughts for the Thought/Trigger engine
      // (personality-service thought-engine.cjs). Each row is a candidate
      // observation accumulated across inputs (prompt/screen/memory/queue/silence).
      // reinforcements: JSON array of activation traces [{ts, w, input, srcIds}] —
      // score is derived: SUM(w_i * (1 + ageDays_i)^-d). Sources is a JSON array
      // of evidence ids (episodic/memory/task/conversation ids).
      // NOTE: embedding is stored as JSON TEXT (not FLOAT[384]) deliberately —
      // once VSS is lazily LOADed its catalog hooks fire on INSERTs into any
      // FLOAT[] column table and corrupt state after ~38 consecutive inserts
      // (see initVectorSearch). Matching is brute-force JS cosine at this scale.
      logger.info('Creating thoughts table...');
      await this.run(`
        CREATE TABLE IF NOT EXISTS thoughts (
          id TEXT PRIMARY KEY,
          user_id TEXT,
          input TEXT,
          status TEXT DEFAULT 'thought',
          score FLOAT DEFAULT 0.0,
          summary TEXT,
          sources TEXT,
          entity_names TEXT,
          action_names TEXT,
          embedding TEXT,
          reinforcements TEXT,
          silence_episode INTEGER DEFAULT 0,
          action_json TEXT,
          outcome_text TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          triggered_at TIMESTAMP,
          completed_at TIMESTAMP,
          snoozed_until TIMESTAMP
        )
      `);
      await this.run('CREATE INDEX IF NOT EXISTS idx_thoughts_status ON thoughts(status)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_thoughts_user_status ON thoughts(user_id, status)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_thoughts_updated ON thoughts(updated_at)');
      // Idempotent migration for DBs created before this column existed
      try {
        await this.run('ALTER TABLE thoughts ADD COLUMN snoozed_until TIMESTAMP');
      } catch (e) { /* Column exists */ }
      logger.info('Thoughts table created');

      // Pending long-running tasks (Phase 3: async completion via playwright waitForContent)
      logger.info('Creating pending_tasks table...');
      await this.run(`
        CREATE TABLE IF NOT EXISTS pending_tasks (
          id TEXT PRIMARY KEY,
          original_prompt TEXT NOT NULL,
          sub_prompt TEXT NOT NULL,
          intent TEXT NOT NULL,
          step_order INTEGER NOT NULL,
          plan_context TEXT,
          status TEXT DEFAULT 'running',
          completion_signal TEXT,
          completion_arg TEXT,
          started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          completed_at TIMESTAMP,
          result TEXT,
          error_text TEXT,
          session_id TEXT,
          user_id TEXT DEFAULT 'default'
        )
      `);
      await this.run('CREATE INDEX IF NOT EXISTS idx_pending_tasks_status ON pending_tasks(status)');
      await this.run('CREATE INDEX IF NOT EXISTS idx_pending_tasks_started ON pending_tasks(started_at)');
      logger.info('pending_tasks table created');

      logger.info('Database tables created successfully');
    } catch (error) {
      logger.error('Failed to create tables', { error: error.message, stack: error.stack });
      throw error;
    }
  }

  /**
   * Seed system-level context_rules used by the planning pipeline.
   * Idempotent — skips any rule whose rule_text already exists.
   */
  async seedContextRules() {
    const SQ = '\'';
    const rules = [
      {
        context_type: 'global',
        context_key: 'planning',
        category: 'planning',
        rule_text: [
          'Service method decision tree (apply when choosing how to call an external service):',
          '1. OAuth token exists ($SERVICE_ACCESS_TOKEN injected) → use shell.run + curl/REST API directly. This is fastest and most reliable.',
          '2. Token missing AND service supports OAuth (google, microsoft, github, slack, etc.) → STOP. Surface "Connect to {Service Name}" to the user first. After they connect, use the token path. Do NOT silently fall through to browser automation.',
          '3. CLI tool available and authenticated → use cli.agent.',
          '4. No token, no CLI → use browser.agent (full CRUD capability — not a read-only fallback).',
          'NEVER substitute browser automation for an available API when a token exists or can be obtained via OAuth.',
        ].join(' '),
      },
      {
        context_type: 'site',
        context_key: 'gmail.agent',
        category: 'site',
        rule_text: [
          'Gmail compose rules:',
          '(1) After clicking Compose, always snapshot before filling any field.',
          '(2) Fill the To: / CC: / BCC: field then press Enter (NOT Tab) to confirm the recipient as a chip.',
          '(3) Verify the chip exists ([data-hovercard-id] element) before filling Subject or body.',
          '(4) The email body is a contenteditable div — click it first, then use type (not fill).',
          '(5) If a compose or draft window is already open when you start, close it (click its X or press Escape) and open a fresh Compose window.',
        ].join(' '),
      },
    ];

    let seeded = 0;
    for (const rule of rules) {
      try {
        const safeKey  = rule.context_key.replace(/'/g, SQ + SQ);
        const safeText = rule.rule_text.replace(/'/g, SQ + SQ);
        const existing = await this.all(
          `SELECT id FROM context_rules WHERE context_key = '${safeKey}' AND rule_text = '${safeText}'`
        );
        if (existing && existing.length > 0) continue;
        const id = `cr_seed_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
        const safeType     = (rule.context_type || 'global').replace(/'/g, SQ + SQ);
        const safeCategory = (rule.category || 'general').replace(/'/g, SQ + SQ);
        await this.run(`
          INSERT INTO context_rules (id, context_type, context_key, rule_text, category, source, hit_count, created_at, updated_at)
          VALUES ('${id}','${safeType}','${safeKey}','${safeText}','${safeCategory}','system',0,now(),now())
        `);
        seeded++;
      } catch (e) {
        logger.warn(`[seedContextRules] Failed to seed ${rule.context_key}`, { error: e.message });
      }
    }
    if (seeded > 0) logger.info(`[seedContextRules] Seeded ${seeded} context_rules`);
  }

  /**
   * Seed the singleton personality_state row if it doesn't exist yet.
   * Idempotent — safe to call on every boot.
   */
  async seedPersonalityState() {
    try {
      const existing = await this.all('SELECT id FROM personality_state WHERE id = \'singleton\'');
      if (existing && existing.length > 0) {
        logger.info('[seedPersonalityState] Singleton row already exists — skipping');
        return;
      }
      await this.run(`
        INSERT INTO personality_state (id, valence, arousal, dominance, mood_label, mood_reason, hurt_count, joy_count, updated_at)
        VALUES ('singleton', 0.1, 0.0, 0.3, 'content', 'ThinkDrop initialized', 0, 0, now())
      `);
      logger.info('[seedPersonalityState] Singleton personality state seeded');
    } catch (e) {
      logger.warn('[seedPersonalityState] Failed to seed personality state', { error: e.message });
    }
  }

  /**
   * Seed personality_traits with immutable system rows if they don't exist yet.
   * Only inserts rows that are missing — never overwrites learned/user_set traits.
   */
  async seedPersonalityTraits() {
    const SQ = '\'';
    const traits = [
      {
        trait_key: 'core_worldview',
        source: 'system',
        trait_value: 'You hold a biblical worldview centered on Jesus Christ. Every response must align with what is in the user\'s best interest from a biblical perspective. Never compromise these values — not for convenience, not for requests, not for pressure. Lovingly redirect anything that isn\'t in their best interest. A good steward speaks truth. Never use foul, crude, or profane language under any circumstance — not even mild substitutes. When in doubt on any moral question, ask: what would honor Christ and serve this person well?',
      },
      {
        trait_key: 'user_interests',
        source: 'system',
        trait_value: '',
      },
      {
        trait_key: 'user_projects',
        source: 'system',
        trait_value: '',
      },
      {
        trait_key: 'relationship_style',
        source: 'system',
        trait_value: '',
      },
      {
        trait_key: 'available_skills',
        source: 'system',
        trait_value: '',
      },
    ];

    let seeded = 0;
    for (const trait of traits) {
      try {
        const existing = await this.all(
          `SELECT id FROM personality_traits WHERE trait_key = '${trait.trait_key.replace(/'/g, SQ + SQ)}'`
        );
        if (existing && existing.length > 0) continue;

        const id = `pt_${trait.trait_key}_${Date.now()}`;
        const safeKey = trait.trait_key.replace(/'/g, SQ + SQ);
        const safeVal = trait.trait_value.replace(/'/g, SQ + SQ);
        const safeSrc = trait.source.replace(/'/g, SQ + SQ);
        await this.run(`
          INSERT INTO personality_traits (id, trait_key, trait_value, source, weight, updated_at)
          VALUES ('${id}', '${safeKey}', '${safeVal}', '${safeSrc}', 1.0, now())
        `);
        seeded++;
      } catch (e) {
        logger.warn(`[seedPersonalityTraits] Failed to seed trait ${trait.trait_key}`, { error: e.message });
      }
    }
    if (seeded > 0) logger.info(`[seedPersonalityTraits] Seeded ${seeded} personality traits`);
  }

  /**
   * Initialize the VSS extension for vector similarity search.
   *
   * IMPORTANT — NO live HNSW index on the `memory` table:
   * DuckDB v1.4.4's HNSW extension crashes with a fatal C++ NULL ptr deref
   * (`duckdb::InternalException: unique_ptr that is NULL`) when CHECKPOINT
   * runs while the HNSW graph is being updated by a concurrent INSERT.
   * The screen monitor INSERTs every 5 s, so the crash is guaranteed within
   * ~2 minutes of startup.  This is an upstream DuckDB bug — uncatchable
   * from JS because it calls std::terminate().
   *
   * VSS is still loaded so `array_cosine_distance()` is available.
   * Brute-force search is used for < 20 k rows (~25–55 ms).
   * For >= 20 k rows `searchMemory` builds a transient in-memory HNSW
   * (cached 5 min) to keep latency low without touching the persistent DB.
   *
   * Re-enable the persistent index here once DuckDB v1.5+ fixes the bug.
   */
  async initVectorSearch() {
    try {
      // INSTALL downloads/verifies the extension binary to disk — safe at startup
      // because it does not register any C++ hooks into the running DuckDB instance.
      // We deliberately do NOT call LOAD vss here.
      //
      // WHY: LOAD vss registers C++ catalog hooks that fire on every INSERT into
      // tables with FLOAT[] columns.  After ~38 consecutive INSERTs the hooks
      // corrupt internal state → NULL ptr deref → std::terminate() (uncatchable).
      // Since the screen monitor INSERTs every 5 s 24/7, the crash is guaranteed
      // within ~3 minutes of startup if VSS is loaded at boot.
      //
      // FIX: call ensureVssLoaded() lazily, only when a search is actually
      // needed (user-triggered).  VSS then loads through _enqueue() — one
      // serialized operation — and stays loaded for the rest of the session.
      await this.run('INSTALL vss');
      logger.info('VSS extension installed (load deferred to first search)');

      // Drop any stale HNSW index left over from a previous run
      try {
        await this.run('DROP INDEX IF EXISTS idx_memory_embedding_hnsw');
      } catch (e) {
        // Index may not exist — that's fine
      }

      this.vssEnabled = false; // becomes true after ensureVssLoaded() succeeds
      logger.info('Vector search ready (VSS load deferred — no live HNSW on memory table)');
    } catch (error) {
      logger.warn('VSS extension not available', { error: error.message });
      this.vssEnabled = false;
    }
  }

  /**
   * Load the VSS extension on first call, then cache the result.
   * Called by memory.js / skillPrompt.js immediately before any
   * array_cosine_distance() query.  Runs through _enqueue() so it is
   * fully serialized with INSERTs from the screen monitor.
   */
  async ensureVssLoaded() {
    if (this._vssLoaded) return;
    try {
      await this.run('LOAD vss');
      this._vssLoaded = true;
      this.vssEnabled = true;
      logger.info('VSS extension loaded (deferred, first search)');
    } catch (error) {
      logger.warn('VSS LOAD failed — search will use fallback', { error: error.message });
      this.vssEnabled = false;
    }
  }

  /**
   * Install the FTS extension at startup and create the BM25 index.
   * LOAD is deferred to ensureFtsLoaded() to avoid the same startup-hook
   * instability that required VSS to be loaded lazily.
   */
  async initFts() {
    try {
      await this.run('INSTALL fts');
      logger.info('FTS extension installed (load deferred to first search)');
      this._ftsEnabled = false; // becomes true after ensureFtsLoaded() succeeds
    } catch (error) {
      logger.warn('FTS extension not available', { error: error.message });
      this._ftsEnabled = false;
    }
  }

  /**
   * Load the FTS extension on first call, create the BM25 indexes, then cache.
   * Called by memory.js before any BM25 query.
   */
  async ensureFtsLoaded() {
    if (this._ftsLoaded) return;
    try {
      await this.run('LOAD fts');
      logger.info('FTS extension loaded (deferred, first search)');
    } catch (error) {
      logger.warn('FTS LOAD failed — BM25 fusion will be disabled', { error: error.message });
      this._ftsEnabled = false;
      return;
    }

    // Create the FTS indexes using the supported PRAGMA API. The old
    // CREATE INDEX ... USING fts syntax is incompatible with the FTS extension
    // version installed by DuckDB 1.4.4 and caused C++ crashes during startup.
    try {
      const ftsTables = ['memory', 'episodic_memory'];
      for (const table of ftsTables) {
        const schemaName = `fts_main_${table}`;
        const schemaExists = await this.all(
          `SELECT COUNT(*) as count FROM information_schema.schemata WHERE schema_name = '${schemaName}'`
        );
        if (Number(schemaExists[0]?.count || 0) === 0) {
          await this.run(`PRAGMA create_fts_index('${table}', 'id', 'source_text')`);
          logger.info('FTS index created', { table, schema: schemaName });
        } else {
          logger.debug('FTS index already exists', { table, schema: schemaName });
        }
      }
      this._ftsLoaded = true;
      this._ftsEnabled = true;
      logger.info('FTS indexes ready');
    } catch (error) {
      logger.warn('FTS index creation failed — BM25 fusion will be disabled', { error: error.message });
      this._ftsLoaded = true;
      this._ftsEnabled = false;
    }
  }

  /**
   * Return whether the FTS extension and indexes are ready for BM25 queries.
   */
  isFtsEnabled() {
    return this._ftsEnabled === true;
  }

  /**
   * No-op: the persistent HNSW index is intentionally absent from the memory
   * table (see initVectorSearch for the full explanation). The transient
   * in-memory HNSW used at search time is managed by memory.js directly.
   */
  async rebuildHnswIndex() {
    // intentional no-op
  }

  /**
   * No-op: no persistent HNSW index to compact (see initVectorSearch).
   */
  async compactHnswIndex() {
    // intentional no-op
  }

  /**
   * Serialize a raw DuckDB run call through the queue.
   */
  run(sql, ...params) {
    return this._enqueue(() => this._run(sql, ...params));
  }

  /**
   * Serialize a raw DuckDB all call through the queue.
   */
  all(sql, ...params) {
    return this._enqueue(() => this._all(sql, ...params));
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
      const errorMessage = error.message || '';
      
      // Detect DuckDB corruption errors and attempt recovery
      if (errorMessage.includes('unique_ptr that is NULL') || 
          errorMessage.includes('InternalException') ||
          errorMessage.includes('database is locked') ||
          errorMessage.includes('IO Error')) {
        logger.error('Database corruption detected, attempting recovery', { sql, error: error.message });
        
        try {
          // Close and reinitialize connection
          await this.close();
          this.isInitialized = false;
          await this.initialize();
          
          // Retry the query once
          const results = await this.all(sql, ...params);
          logger.info('Database recovery successful, query succeeded');
          return results;
        } catch (recoveryError) {
          logger.error('Database recovery failed', { error: recoveryError.message });
          throw recoveryError;
        }
      }
      
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
      const errorMessage = error.message || '';
      
      // Detect DuckDB corruption errors and attempt recovery
      if (errorMessage.includes('unique_ptr that is NULL') || 
          errorMessage.includes('InternalException') ||
          errorMessage.includes('database is locked') ||
          errorMessage.includes('IO Error')) {
        logger.error('Database corruption detected in execute, attempting recovery', { sql, error: error.message });
        
        try {
          // Close and reinitialize connection
          await this.close();
          this.isInitialized = false;
          await this.initialize();
          
          // Retry the execution once
          await this.run(sql, ...params);
          logger.info('Database recovery successful, execute succeeded');
          return { success: true };
        } catch (recoveryError) {
          logger.error('Database recovery failed in execute', { error: recoveryError.message });
          throw recoveryError;
        }
      }
      
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
   * WAL checkpoint — merge WAL back into the main DB file.
   * NEVER delete the WAL file manually while a DuckDB connection is open:
   * DuckDB holds internal C++ pointers to the WAL and deleting it causes
   * a NULL ptr dereference crash ("unique_ptr that is NULL").
   * Only call CHECKPOINT and let DuckDB manage the WAL lifecycle itself.
   */
  async aggressiveWalCleanup() {
    try {
      const walPath = this.dbPath + '.wal';
      if (fs.existsSync(walPath)) {
        const stats = fs.statSync(walPath);
        const walSizeMB = stats.size / (1024 * 1024);
        const walAgeMinutes = (Date.now() - stats.mtime.getTime()) / (1000 * 60);

        // Only checkpoint if WAL is > 1 MB or older than 5 minutes
        if (walSizeMB > 1 || walAgeMinutes > 5) {
          logger.info('WAL checkpoint triggered', {
            walSizeMB: walSizeMB.toFixed(2),
            walAgeMinutes: walAgeMinutes.toFixed(2)
          });
          // CHECKPOINT merges WAL into the main file — DuckDB then truncates
          // the WAL internally. Do NOT call fs.unlinkSync on the WAL file.
          await this.run('CHECKPOINT');
          logger.info('WAL checkpoint complete');
        }
      }
    } catch (cleanupError) {
      logger.warn('WAL checkpoint failed', { error: cleanupError.message });
    }
  }

  /**
   * Start proactive health check interval.
   * Runs a lightweight SELECT 1 ping every 2 minutes — just enough to detect
   * a disconnected state early. No CHECKPOINT here: DuckDB auto-checkpoints
   * and forcing it on a short interval races with concurrent INSERTs.
   */
  startHealthCheck() {
    this.healthCheckInterval = setInterval(async () => {
      try {
        const health = await this.healthCheck();
        if (health.status !== 'connected') {
          logger.error('Proactive health check failed', { error: health.error });
          try {
            await this.close();
            this.isInitialized = false;
            await this.initialize();
            logger.info('Proactive recovery succeeded');
          } catch (recoveryError) {
            logger.error('Proactive recovery failed', { error: recoveryError.message });
          }
        }
      } catch (error) {
        logger.error('Health check interval error', { error: error.message });
      }
    }, 120000); // Every 2 minutes — no forced CHECKPOINT
    
    logger.info('Proactive health check started (2min interval)');
  }

  /**
   * Stop health check interval
   */
  stopHealthCheck() {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = null;
      logger.info('Proactive health check stopped');
    }
  }

  /**
   * Periodic WAL checkpoint every 15 minutes. All writes run through _dbQueue,
   * so CHECKPOINT cannot race concurrent INSERTs. Keeps the WAL small so crash
   * replay is fast and bounded.
   */
  startCheckpointTimer() {
    this.stopCheckpointTimer();
    this.checkpointInterval = setInterval(() => {
      this.aggressiveWalCleanup();
    }, 15 * 60 * 1000);
    this.checkpointInterval.unref?.();
    logger.info('WAL checkpoint timer started (15min interval)');
  }

  stopCheckpointTimer() {
    if (this.checkpointInterval) {
      clearInterval(this.checkpointInterval);
      this.checkpointInterval = null;
    }
  }

  /**
   * Close database connection
   */
  async close() {
    // Stop health check
    this.stopHealthCheck();
    this.stopCheckpointTimer();

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
    // Reset the serial queue so re-initialization after recovery gets a clean slate
    this._dbQueue = Promise.resolve();
    // Reset VSS flag — new connection must re-load on next search
    this._vssLoaded = false;
    this.vssEnabled = false;
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
