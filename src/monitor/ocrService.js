import Tesseract from 'tesseract.js';
import crypto from 'crypto';
import logger from '../utils/logger.js';

// --- OCR Text Processing Helpers ---

function cleanOcrText(raw) {
  return raw
    .replace(/\s+/g, ' ')
    .replace(/[^\x20-\x7E]/g, '')
    .trim();
}

function extractFileNames(text) {
  const regex = /(?:[a-zA-Z0-9/_-]+\.){1}(?:js|jsx|ts|tsx|json|md|png)/g;
  return [...new Set(text.match(regex) || [])];
}

function extractCodeSnippets(text) {
  const lines = text.split('\n');
  return lines.filter(line =>
    /^(export|import|function|const|let|var)\b/.test(line.trim())
  );
}

/**
 * Filter out gibberish OCR fragments.
 * Uses multiple strategies to remove noise from icons, avatars, UI chrome.
 * Replaces gibberish with --- delimiters so LLMs see context boundaries.
 * Preserves timestamps and meaningful date/time references.
 */
function filterGibberish(text) {
  const DELIMITER = ' --- ';
  const PLACEHOLDER_PREFIX = 'XXTIMESTAMPXX';

  // Step 0: Extract and protect timestamps before filtering
  // Matches: ThuFeb19 12:01AM, Mon Jan 5 3:45PM, 2026-02-19, 12:01AM, 11:30PM, December 23rd 2019, etc.
  const timestampPatterns = [
    /(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[A-Za-z]*\d{1,2}\s+\d{1,2}:\d{2}\s*[AP]M/gi,
    /(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[A-Za-z]*\s*\d{1,2}\s+\d{1,2}:\d{2}\s*[AP]M/gi,
    /\d{1,2}:\d{2}\s*[AP]M/gi,
    /\d{4}-\d{2}-\d{2}/g,
    /(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}/gi,
    /(?:Dec|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov)\s+\d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}/gi
  ];
  const placeholderMap = {};
  let placeholderIdx = 0;
  // Deduplicate: longer matches first to avoid partial replacements
  const allMatches = [];
  for (const pattern of timestampPatterns) {
    let match;
    const re = new RegExp(pattern.source, pattern.flags);
    while ((match = re.exec(text)) !== null) {
      allMatches.push({ value: match[0], index: match.index, length: match[0].length });
    }
  }
  // Sort by length descending so longer matches get replaced first
  allMatches.sort((a, b) => b.length - a.length);
  const replacedRanges = [];
  for (const m of allMatches) {
    // Skip if this range overlaps with an already-replaced range
    const overlaps = replacedRanges.some(r =>
      m.index < r.index + r.length && m.index + m.length > r.index
    );
    if (overlaps) continue;
    const placeholder = `${PLACEHOLDER_PREFIX}${placeholderIdx}`;
    placeholderMap[placeholder] = m.value;
    text = text.replace(m.value, placeholder);
    replacedRanges.push(m);
    placeholderIdx++;
  }

  // Step A: Replace sequences of isolated 1-2 char words with delimiter
  text = text.replace(/(?:\b\S{1,2}\b[\s.,;:!?-]*){4,}/g, DELIMITER);

  // Step B: Remove unknown ALL-CAPS nonsense words (3+ chars)
  const knownAcronyms = new Set([
    'API', 'URL', 'HTTP', 'HTTPS', 'HTML', 'CSS', 'JSON', 'SQL', 'OCR',
    'POST', 'GET', 'PUT', 'DELETE', 'MCP', 'NPM', 'CLI', 'IDE', 'GUI',
    'RAM', 'CPU', 'GPU', 'SSD', 'USB', 'PDF', 'PNG', 'JPG', 'SVG',
    'DOM', 'SSH', 'FTP', 'DNS', 'TCP', 'UDP', 'AWS', 'GCP', 'ENV',
    'DM', 'PM', 'AM', 'EST', 'UTC', 'PST', 'CST'
  ]);
  text = text.replace(/\b[A-Z]{3,}\b/g, match =>
    knownAcronyms.has(match) ? match : ''
  );

  // Step C: Replace punctuation-heavy fragments with delimiter
  text = text.replace(/(?:[^a-zA-Z0-9\s]{1,2}\s*){3,}/g, DELIMITER);

  // Step D: Sliding window — replace gibberish phrases with delimiter
  const words = text.split(/\s+/).filter(w => w.length > 0);
  const windowSize = 6;
  const gibberishRanges = [];

  for (let i = 0; i <= words.length - windowSize; i++) {
    const window = words.slice(i, i + windowSize);
    // Don't count timestamp placeholders as tiny words
    const tinyCount = window.filter(w =>
      !w.startsWith(PLACEHOLDER_PREFIX) && w.replace(/[^a-zA-Z]/g, '').length <= 2
    ).length;
    if (tinyCount >= 4) {
      gibberishRanges.push([i, i + windowSize]);
    }
  }

  if (gibberishRanges.length > 0) {
    const merged = [gibberishRanges[0]];
    for (let i = 1; i < gibberishRanges.length; i++) {
      const last = merged[merged.length - 1];
      if (gibberishRanges[i][0] <= last[1]) {
        last[1] = Math.max(last[1], gibberishRanges[i][1]);
      } else {
        merged.push(gibberishRanges[i]);
      }
    }
    const removeSet = new Set();
    for (const [start, end] of merged) {
      for (let i = start; i < end && i < words.length; i++) {
        removeSet.add(i);
      }
    }
    // Replace gibberish with delimiter, but never remove timestamp placeholders
    let lastWasRemoved = false;
    const filtered = [];
    for (let i = 0; i < words.length; i++) {
      if (removeSet.has(i) && !words[i].startsWith(PLACEHOLDER_PREFIX)) {
        if (!lastWasRemoved) filtered.push('---');
        lastWasRemoved = true;
      } else {
        filtered.push(words[i]);
        lastWasRemoved = false;
      }
    }
    text = filtered.join(' ');
  }

  // Step E: Replace trailing short-word gibberish with delimiter
  text = text.replace(/(?:\s+\S{1,3}){3,}\s*$/, DELIMITER);

  // Step F: Replace lone symbols with delimiter (but not underscores in placeholders)
  text = text.replace(/\s+[^a-zA-Z0-9_]\s+/g, DELIMITER);

  // Step G: Restore preserved timestamps
  for (const [placeholder, ts] of Object.entries(placeholderMap)) {
    text = text.replace(new RegExp(placeholder, 'g'), ts);
  }

  // Step H: Collapse multiple consecutive delimiters into one
  text = text.replace(/(?:\s*---\s*)+/g, ' --- ');

  // Step I: Remove leading/trailing delimiters and collapse whitespace
  text = text.replace(/^\s*---\s*/, '').replace(/\s*---\s*$/, '');
  text = text.replace(/\s+/g, ' ').trim();

  return text;
}

function additionalCleanup(text) {
  // Remove square-bracketed tags
  text = text.replace(/\[[^\]]+\]/g, '');
  // Remove timestamps (e.g., "2026-02-09 at 2.47.44 PM")
  text = text.replace(/\d{4}-\d{2}-\d{2} at \d{1,2}\.\d{2}\.\d{2} [AP]M/g, '');
  // Remove session/id hashes
  text = text.replace(/[A-Z0-9]{8,}/g, '');
  // Remove emoji and miscellaneous symbols
  text = text.replace(/[\u2190-\u21FF\u2300-\u27BF\u2600-\u26FF\u2700-\u27BF\u2B50-\u2BFF\ud83c-\ud83e][\ufe0f]*/g, '');
  // Remove excessive whitespace and blank lines
  text = text.replace(/\s+/g, ' ').replace(/(\r?\n){2,}/g, '\n');
  return text.trim();
}

function isValidFileName(name) {
  const forbiddenPattern = new RegExp('[<>:"/\\\\|?*\\x00-\\x1F]'); // eslint-disable-line no-control-regex
  return (
    typeof name === 'string' &&
    !forbiddenPattern.test(name) &&
    name.trim().length > 0 &&
    name.length < 256 &&
    /\.[a-zA-Z0-9]+$/.test(name)
  );
}

function dedupeAndValidateFiles(files) {
  const uniqueFiles = Array.from(
    new Set(files.map(f => f.toLowerCase()))
  ).map(lower =>
    files.find(f => f.toLowerCase() === lower)
  );
  return uniqueFiles.filter(f => f !== undefined && isValidFileName(f));
}

class OCRService {
  constructor() {
    this.worker = null;
    this.isInitialized = false;
    this.lastTextHash = null;
  }

  /**
   * Initialize the Tesseract worker.
   */
  async initialize() {
    if (this.isInitialized) return;

    try {
      logger.info('Initializing Tesseract OCR worker...');
      this.worker = await Tesseract.createWorker('eng', 1, {
        logger: () => {} // suppress tesseract logs
      });
      this.isInitialized = true;
      logger.info('Tesseract OCR worker initialized');
    } catch (error) {
      logger.error('Failed to initialize Tesseract OCR', { error: error.message });
      throw error;
    }
  }

  /**
   * Run OCR on a screenshot buffer.
   * Returns the extracted text.
   */
  async extractText(imageBuffer) {
    if (!this.isInitialized) {
      await this.initialize();
    }

    try {
      const startTime = Date.now();
      const { data } = await this.worker.recognize(imageBuffer);
      const elapsed = Date.now() - startTime;

      // Clean up OCR text: remove excessive whitespace, empty lines
      const cleanedText = data.text
        .split('\n')
        .map(line => line.trim())
        .filter(line => line.length > 0)
        .join('\n')
        .trim();

      logger.debug('OCR completed', {
        elapsed,
        confidence: data.confidence,
        textLength: cleanedText.length
      });

      return {
        text: cleanedText,
        confidence: data.confidence,
        elapsed
      };
    } catch (error) {
      logger.error('OCR extraction failed', { error: error.message });
      return { text: '', confidence: 0, elapsed: 0 };
    }
  }

  /**
   * Check if OCR text is different from the last capture.
   * Uses SHA-256 hash comparison.
   * Returns { isDifferent: boolean, hash: string }
   */
  checkTextChanged(text) {
    const hash = crypto.createHash('sha256').update(text).digest('hex');
    const isDifferent = hash !== this.lastTextHash;
    this.lastTextHash = hash;
    return { isDifferent, hash };
  }

  /**
   * Process raw OCR text through the full cleanup pipeline.
   * Returns structured output with cleaned text, extracted files, and code snippets.
   */
  static processOcrOutput(rawOcrText) {
    // Step 1: Normalize and sanitize
    const cleaned = cleanOcrText(rawOcrText);

    // Step 2: Extract files and code
    const files = extractFileNames(cleaned);
    const code = extractCodeSnippets(cleaned);

    // Step 3: Remove duplicates and validate files
    const validFiles = dedupeAndValidateFiles(files);

    // Step 4: Redact files and code snippets from cleaned text
    let redactedText = cleaned;
    validFiles.forEach(file => {
      const escaped = file.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');
      redactedText = redactedText.replace(new RegExp(escaped, 'g'), '');
    });
    code.forEach(snippet => {
      const snippetTrimmed = snippet.trim();
      if (snippetTrimmed) {
        const escaped = snippetTrimmed.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');
        redactedText = redactedText.replace(new RegExp(escaped, 'g'), '');
      }
    });

    const additionalCleaned = additionalCleanup(redactedText);

    // Step 5: Filter out gibberish OCR noise (random letters from icons, avatars, UI chrome)
    const filtered = filterGibberish(additionalCleaned);

    return {
      files: validFiles,
      codeSnippets: code,
      cleanedText: cleaned,
      redactedText,
      additionalCleanedText: additionalCleaned,
      filteredText: filtered
    };
  }

  /**
   * Summarize OCR text for embedding generation.
   * Uses cleaned text for better semantic search quality.
   */
  static summarizeForEmbedding(appName, windowTitle, processedOcr, maxLength = 2000) {
    const prefix = `${appName}: ${windowTitle}`;
    const text = processedOcr.filteredText || processedOcr.additionalCleanedText || processedOcr.cleanedText || '';

    const truncated = text.length > maxLength
      ? text.substring(0, maxLength) + '...'
      : text;

    return `${prefix}\n${truncated}`;
  }

  /**
   * Shut down the Tesseract worker.
   */
  async terminate() {
    if (this.worker) {
      await this.worker.terminate();
      this.worker = null;
      this.isInitialized = false;
      logger.info('Tesseract OCR worker terminated');
    }
  }
}

let instance = null;

export function getOCRService() {
  if (!instance) {
    instance = new OCRService();
  }
  return instance;
}

export default OCRService;
