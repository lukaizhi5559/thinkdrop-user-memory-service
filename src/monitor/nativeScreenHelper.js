import { execFile } from 'child_process';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import logger from '../utils/logger.js';

// nativeScreenHelper.js — td-screen-helper lifecycle + one-shot capture.
//
// The helper is its own TCC client ("ThinkDrop Screen Helper" in Screen
// Recording settings) — unlike `screencapture`, whose grant belongs to the
// launcher tree and can never be toggled for "ThinkDrop". Lazily built with
// swiftc + ad-hoc codesign on first use (native/td-screen-helper.swift →
// native/bin/td-screen-helper.app). stderr carries JSON lifecycle events so
// denial is a typed failure, not a stderr regex.

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const HELPER_DIR = path.join(__dirname, '..', '..', 'native');
const HELPER_APP = path.join(HELPER_DIR, 'bin', 'td-screen-helper.app');
const HELPER_BIN = path.join(HELPER_APP, 'Contents', 'MacOS', 'td-screen-helper');
const HELPER_RAW = path.join(HELPER_DIR, 'bin', 'td-screen-helper');
const HELPER_SRC = path.join(HELPER_DIR, 'td-screen-helper.swift');
const HELPER_PLIST = path.join(HELPER_DIR, 'Info.plist');

const CAPTURE_TIMEOUT_MS = 15000;
const MAX_PNG_BYTES = 128 * 1024 * 1024; // retina PNGs run ~5-15MB

let _buildPromise = null;

async function ensureBuilt() {
  if (fs.existsSync(HELPER_BIN) && fs.existsSync(HELPER_SRC)
      && fs.statSync(HELPER_BIN).mtimeMs > fs.statSync(HELPER_SRC).mtimeMs) {
    return HELPER_BIN;
  }
  if (process.platform !== 'darwin') throw new Error('td-screen-helper is macOS-only');
  if (!fs.existsSync(HELPER_SRC)) throw new Error(`helper source missing: ${HELPER_SRC}`);
  logger.info('[NativeScreenHelper] building td-screen-helper (swiftc -O, one-time)');
  await fs.promises.mkdir(path.dirname(HELPER_BIN), { recursive: true });
  await fs.promises.copyFile(HELPER_PLIST, path.join(HELPER_APP, 'Contents', 'Info.plist'));
  await new Promise((resolve, reject) => {
    execFile('swiftc', ['-O', HELPER_SRC, '-o', HELPER_RAW], { timeout: 120000 },
      (err, _so, se) => err ? reject(new Error(`swiftc: ${se || err.message}`)) : resolve());
  });
  await fs.promises.copyFile(HELPER_RAW, HELPER_BIN);
  await new Promise((resolve, reject) => {
    execFile('codesign', ['-s', '-', '--force', '--deep', HELPER_APP], { timeout: 30000 },
      (err, _so, se) => err ? reject(new Error(`codesign: ${se || err.message}`)) : resolve());
  });
  logger.info('[NativeScreenHelper] td-screen-helper built + signed (.app)');
  return HELPER_BIN;
}

function parseEvents(stderr) {
  const events = [];
  for (const line of String(stderr || '').split('\n')) {
    const t = line.trim();
    if (!t.startsWith('{')) continue;
    try { events.push(JSON.parse(t)); } catch (_) {}
  }
  return events;
}

/**
 * Capture one PNG frame of the display. Returns Buffer on success; throws a
 * typed error on failure — err.denied=true when TCC refused the helper.
 */
export async function capturePng({ displayIndex = 0 } = {}) {
  if (!_buildPromise) _buildPromise = ensureBuilt();
  const bin = await _buildPromise;
  const args = ['--display', String(displayIndex)];
  return new Promise((resolve, reject) => {
    execFile(bin, args, {
      timeout: CAPTURE_TIMEOUT_MS,
      maxBuffer: MAX_PNG_BYTES,
      encoding: 'buffer',
    }, (err, stdout, stderr) => {
      const events = parseEvents(stderr);
      const denied = events.find(e => e.event === 'denied');
      if (!err && stdout && stdout.length > 0) return resolve(stdout);
      const e = new Error(denied?.msg
        || events.find(e => e.event === 'error')?.msg
        || err?.message || 'td-screen-helper capture failed');
      if (denied || err?.code === 2) e.denied = true;
      reject(e);
    });
  });
}
