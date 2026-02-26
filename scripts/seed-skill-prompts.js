/* eslint-disable quotes */
/**
 * Seed initial skill_prompts entries into DuckDB.
 * Run once: node scripts/seed-skill-prompts.js
 * Safe to re-run — upsert deduplicates by cosine similarity.
 */
import { getSkillPromptService } from '../src/services/skillPrompt.js';
import { getDatabaseService } from '../src/services/database.js';
import { getEmbeddingService } from '../src/services/embeddings.js';
import logger from '../src/utils/logger.js';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.join(__dirname, '..', '.env') });

const SEEDS = [
  {
    tags: ['github', 'git', 'pull request', 'shell.run'],
    promptText: `To create a GitHub pull request: use shell.run bash to (1) safely switch or create the branch with "git checkout <branch> 2>/dev/null || git checkout -b <branch>", commit, and push; (2) retrieve the GitHub token from macOS keychain with "security find-internet-password -s github.com -w" and POST to https://api.github.com/repos/<owner>/<repo>/pulls with JSON body {title, body, head, base}. NEVER use browser.act for GitHub — the browser has no login session. Print the returned html_url using python3.`
  },
  {
    tags: ['github', 'git', 'pr comment', 'review', 'shell.run'],
    promptText: `To review a GitHub PR and post a comment: use shell.run bash to (1) fetch changed files via GET https://api.github.com/repos/<owner>/<repo>/pulls/<number>/files with the GitHub token from keychain ("security find-internet-password -s github.com -w"); (2) use synthesize to draft a review comment; (3) POST the comment to https://api.github.com/repos/<owner>/<repo>/issues/<number>/comments with body {"body":"<comment text>"}. GitHub Issues API handles PR comments — use /issues/<number>/comments not /pulls. NEVER use browser.act for any GitHub interaction.`
  },
  {
    tags: ['git', 'branch', 'push', 'shell.run'],
    promptText: `To create a git branch and push it: use a single shell.run bash step with "git checkout <branch> 2>/dev/null || git checkout -b <branch> && git add -A && git commit -m '<message>' --allow-empty && git push -u origin <branch>". Always use "git checkout <branch> 2>/dev/null || git checkout -b <branch>" to safely switch-or-create — never bare "git checkout -b" which fails if the branch already exists.`
  },
  {
    tags: ['slack', 'message', 'shell.run'],
    promptText: `To send a Slack message via API: use shell.run bash with curl POST to https://slack.com/api/chat.postMessage with Authorization header "Bearer <token>". Retrieve the Slack token from macOS keychain: "security find-generic-password -s slack.com -w". Pass channel and text as JSON body. NEVER use browser.act for Slack — use osascript activate + CMD+K shortcut only for native Slack app desktop navigation, not for sending messages.`
  },
  {
    tags: ['gmail', 'email', 'browser.act', 'shell.run'],
    promptText: `To send a Gmail email: use browser.act navigate to https://mail.google.com with a sessionId, then browser.act smartFill with {to, subject, body, sessionId} — smartFill auto-discovers the compose fields. Do NOT use individual type/fillField steps for Gmail; smartFill handles contenteditable compose areas. After smartFill, send with keyboard Meta+Enter (not a click). If the Gmail page is not already open, open it first with navigate.`
  },
  {
    tags: ['file', 'shell.run', 'convert', 'image', 'compress'],
    promptText: `For file operations (convert, compress, rename, move, resize image): use shell.run bash with standard CLI tools — ffmpeg for video/audio, ImageMagick (convert) for images, python3 for data files, zip/tar for archives. Always use the full absolute path for cwd and file arguments. Chain operations in a single bash -c string when possible. Print the output path at the end so the user knows where the result is.`
  },
  {
    tags: ['browser.act', 'navigate', 'scrape', 'search'],
    promptText: `For public web browsing, reading pages, or scraping content: use browser.act navigate to open the URL (with a sessionId), then browser.act getPageText to extract the text content. For search engines, navigate to the search URL directly (e.g. https://www.google.com/search?q=<query>) rather than typing into a search box. browser.act is only appropriate for unauthenticated pages — for any service requiring login, use curl with a token from keychain instead.`
  }
];

async function seed() {
  try {
    logger.info('Starting skill_prompts seeding...');

    const db = getDatabaseService();
    await db.initialize();

    const embeddings = getEmbeddingService();
    await embeddings.initialize();

    const svc = getSkillPromptService();

    let created = 0;
    let updated = 0;

    for (const seed of SEEDS) {
      const result = await svc.upsert(seed.tags, seed.promptText);
      if (result.created) {
        created++;
        logger.info(`✅ Created: ${result.id} [${seed.tags.join(', ')}]`);
      } else {
        updated++;
        logger.info(`♻️  Updated: ${result.id} [${seed.tags.join(', ')}]`);
      }
    }

    logger.info(`\nSeed complete: ${created} created, ${updated} updated (${SEEDS.length} total)`);

    await db.close();
    process.exit(0);
  } catch (error) {
    logger.error('Seed failed:', error.message, error.stack);
    process.exit(1);
  }
}

seed();
