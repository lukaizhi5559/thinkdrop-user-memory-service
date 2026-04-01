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
        await this.run(`ALTER TABLE user_constraints ADD COLUMN override_pin TEXT DEFAULT NULL`);
        logger.info('[DB Migration] user_constraints: added override_pin column');
      } catch (_) {
        // Column already exists on re-start — expected, skip silently
      }

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
      // ── Endpoint / Docs URLs ─────────────────────────────────────────────────
      // These are loaded by planSkills at runtime to inject exact API docs URLs
      // into the skill generation prompt. Stored in DB so they can grow without
      // code deploys — any new service can be added via api_rule.upsert.
      // rule_text = the docs URL. fix_hint = short description of what the URL covers.
      // SMS / Voice
      { service: 'twilio',      rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://www.twilio.com/docs/sms/api/message-resource#create-a-message-resource',
        fix_hint: 'Twilio SMS send message REST API' },
      { service: 'clicksend',   rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://developers.clicksend.com/docs/rest/v3/#send-sms',
        fix_hint: 'ClickSend SMS REST API v3' },
      { service: 'vonage',      rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://developer.vonage.com/api/sms',
        fix_hint: 'Vonage SMS API' },
      { service: 'sinch',       rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://developers.sinch.com/docs/sms/api-reference',
        fix_hint: 'Sinch SMS API reference' },
      { service: 'messagebird', rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://developers.messagebird.com/api/sms-messaging/',
        fix_hint: 'MessageBird SMS API' },
      { service: 'plivo',       rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://www.plivo.com/docs/sms/api/message/',
        fix_hint: 'Plivo SMS message API' },
      // Email
      { service: 'mailgun',     rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://documentation.mailgun.com/en/latest/api-sending.html',
        fix_hint: 'Mailgun email sending API' },
      { service: 'sendgrid',    rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://docs.sendgrid.com/api-reference/mail-send/mail-send',
        fix_hint: 'SendGrid mail send API' },
      { service: 'ses',         rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://docs.aws.amazon.com/ses/latest/APIReference/API_SendEmail.html',
        fix_hint: 'AWS SES SendEmail API' },
      { service: 'postmark',    rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://postmarkapp.com/developer/api/email-api',
        fix_hint: 'Postmark email API' },
      { service: 'resend',      rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://resend.com/docs/api-reference/emails/send-email',
        fix_hint: 'Resend email API' },
      // Push notifications
      { service: 'pushover',    rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://pushover.net/api',
        fix_hint: 'Pushover push notification API' },
      { service: 'pushbullet',  rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://docs.pushbullet.com/#create-push',
        fix_hint: 'Pushbullet create push API' },
      { service: 'onesignal',   rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://documentation.onesignal.com/reference/create-notification',
        fix_hint: 'OneSignal create notification API' },
      { service: 'firebase',    rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://firebase.google.com/docs/cloud-messaging/send-message',
        fix_hint: 'Firebase Cloud Messaging send API' },
      { service: 'gotify',      rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://gotify.net/docs/msgother',
        fix_hint: 'Gotify message push API' },
      { service: 'ntfy',        rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://docs.ntfy.sh/publish/',
        fix_hint: 'ntfy.sh publish notification API' },
      // Chat / Collaboration
      { service: 'slack',       rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://api.slack.com/messaging/webhooks',
        fix_hint: 'Slack incoming webhooks API' },
      { service: 'discord',     rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://discord.com/developers/docs/resources/webhook#execute-webhook',
        fix_hint: 'Discord execute webhook API' },
      { service: 'telegram',    rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://core.telegram.org/bots/api#sendmessage',
        fix_hint: 'Telegram Bot API sendMessage' },
      { service: 'whatsapp',    rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://developers.facebook.com/docs/whatsapp/cloud-api/messages',
        fix_hint: 'WhatsApp Cloud API send messages' },
      { service: 'teams',       rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/connectors-using',
        fix_hint: 'Microsoft Teams incoming webhook connector' },
      { service: 'mattermost',  rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://api.mattermost.com/#tag/posts/operation/CreatePost',
        fix_hint: 'Mattermost create post API' },
      // Automation / Webhooks
      { service: 'zapier',      rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://zapier.com/developer/documentation/v2/rest-hooks/',
        fix_hint: 'Zapier REST hooks API' },
      { service: 'make',        rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://www.make.com/en/help/tools/webhooks',
        fix_hint: 'Make (Integromat) webhooks' },
      { service: 'n8n',         rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://docs.n8n.io/integrations/builtin/core-nodes/n8n-nodes-base.webhook/',
        fix_hint: 'n8n webhook node docs' },
      { service: 'ifttt',       rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://ifttt.com/maker_webhooks',
        fix_hint: 'IFTTT Maker webhooks' },
      // Calendar
      { service: 'googlecalendar', rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://developers.google.com/calendar/api/v3/reference/events/insert',
        fix_hint: 'Google Calendar events insert API' },
      // Payment
      { service: 'stripe',      rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://stripe.com/docs/api/payment_intents/create',
        fix_hint: 'Stripe create PaymentIntent API' },
      // DevOps / Project management
      { service: 'github',      rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://docs.github.com/en/rest/issues/issues#create-an-issue',
        fix_hint: 'GitHub REST API create issue' },
      { service: 'gitlab',      rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://docs.gitlab.com/ee/api/issues.html#new-issue',
        fix_hint: 'GitLab Issues API create issue' },
      { service: 'linear',      rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://developers.linear.app/docs/graphql/working-with-the-graphql-api',
        fix_hint: 'Linear GraphQL API' },
      { service: 'jira',        rule_type: 'endpoint', code_pattern: null,
        rule_text: 'https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-post',
        fix_hint: 'Jira REST API create issue' },
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
          VALUES ('${id}','${safeService}','${safeType}','${safeText}',${patternVal},${fixHintVal},'system',0,now(),now())
        `);
        seeded++;
      } catch (e) {
        logger.warn(`[seedApiRules] Failed to seed ${rule.service}:${rule.rule_type}`, { error: e.message });
      }
    }
    if (seeded > 0) logger.info(`[seedApiRules] Seeded ${seeded} api_rules`);
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
