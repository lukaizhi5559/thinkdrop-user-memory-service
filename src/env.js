// env.js — load .env BEFORE every other module in this service.
//
// Imported first by server.js: ESM evaluates imports depth-first in source
// order, so sibling modules that capture process.env at module scope
// (middleware/auth.js API_KEYS, monitor/audioJournal.js config) only see the
// populated env when this file runs ahead of them. Calling dotenv.config() in
// server.js's body is too late — imports are already evaluated by then.
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, '..', '.env') });
// Secondary: project-root .env supplies cross-service keys (MCP_PHI4_API_KEY
// etc.) monitorService needs to call other MCP services — comms-graph does the
// same. override:false keeps the service-local file authoritative.
dotenv.config({ path: path.join(__dirname, '..', '..', '..', '.env'), override: false });
