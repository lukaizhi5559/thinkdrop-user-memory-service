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
  // Broad set of file extensions to detect
  // Note: 'env' excluded to avoid matching 'process.env' (JS property access)
  const extensions = 'js|jsx|ts|tsx|json|md|png|pdf|csv|xlsx|xls|doc|docx|ppt|pptx|html|htm|css|py|go|rb|java|c|cpp|h|sh|bash|yaml|yml|toml|log|txt|zip|tar|gz|mp3|mp4|wav|jpg|jpeg|gif|bmp|webp|svg|ico|tiff|heic|sql|xml|ini|cfg|conf|lock|map';

  // Pattern 1: Standard filenames (e.g., server.js, cbs-report-caleb.pdf)
  const standardRegex = new RegExp(`[a-zA-Z0-9][a-zA-Z0-9._/-]*\\.(?:${extensions})\\b`, 'gi');
  const standardMatches = text.match(standardRegex) || [];

  // Pattern 2: Reconstruct OCR-broken filenames with ellipsis
  // Finder truncates: "cbs-report-caleb.pdf" → "cbs-rep...caleb.pdf"
  // OCR reads fragments: "cbs-rep" ... noise ... "caleb.pdf" (or just "caleb")
  // Heuristic: a prefix ending with "-" or a hyphenated word near a .ext suffix
  const extRegex = new RegExp(`([a-zA-Z0-9][a-zA-Z0-9_-]*\\.(?:${extensions}))`, 'gi');
  const prefixRegex = /([a-zA-Z0-9]+-[a-zA-Z0-9-]*[a-zA-Z0-9]|[a-zA-Z0-9]+-)(?=\s)/g;
  const fragmentMatches = [];

  // Find all truncated prefixes (words ending with - or containing -)
  const prefixes = [];
  let pm;
  while ((pm = prefixRegex.exec(text)) !== null) {
    prefixes.push({ value: pm[1], index: pm.index, end: pm.index + pm[0].length });
  }

  // Find all .ext suffixes
  const suffixes = [];
  let sm;
  while ((sm = extRegex.exec(text)) !== null) {
    suffixes.push({ value: sm[1], index: sm.index });
  }

  // Pair each prefix with ALL following suffixes that share the same prefix pattern
  // e.g., "cbs-rep" pairs with "chloe.pdf", "istian.pdf" (both are cbs-report-*.pdf)
  const usedSuffixes = new Set();
  for (const pre of prefixes) {
    for (const suf of suffixes) {
      if (usedSuffixes.has(suf.index)) continue;
      const gap = suf.index - pre.end;
      if (gap > 0 && gap < 200) {
        fragmentMatches.push(`${pre.value}...${suf.value}`);
        usedSuffixes.add(suf.index);
      }
    }
  }

  // Pattern 3: Filenames without extensions near file-listing context
  // e.g., "quarter-two-report Jan 26, 2025" or "quarter-t...rt-caleb Jan 26"
  const contextualRegex = /([a-zA-Z][a-zA-Z0-9]+-[a-zA-Z0-9-]+[a-zA-Z0-9])\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s/gi;
  const contextualMatches = [];
  let ctxm;
  while ((ctxm = contextualRegex.exec(text)) !== null) {
    contextualMatches.push(ctxm[1]);
  }

  return [...new Set([...standardMatches, ...fragmentMatches, ...contextualMatches])];
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
 * Preserves timestamps, file metadata, and meaningful content.
 */
function filterGibberish(text) {
  const DELIMITER = ' --- ';
  const PLACEHOLDER_PREFIX = 'XXTIMESTAMPXX';
  const PROTECT_PREFIX = 'XXPROTECTXX';

  // Step 0: Protect timestamps before filtering
  const timestampPatterns = [
    /(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[A-Za-z]*\d{1,2}\s+\d{1,2}:\d{2}\s*[AP]M/gi,
    /(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[A-Za-z]*\s*\d{1,2}\s+\d{1,2}:\d{2}\s*[AP]M/gi,
    /\d{1,2}:\d{2}\s*[AP]M/gi,
    /\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2})?/g,
    /(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}/gi,
    /(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}(?:\s+at\s+\d{1,2}:\d{2}\s*[AP]M)?/gi
  ];
  const placeholderMap = {};
  let placeholderIdx = 0;
  const allMatches = [];
  for (const pattern of timestampPatterns) {
    let match;
    const re = new RegExp(pattern.source, pattern.flags);
    while ((match = re.exec(text)) !== null) {
      allMatches.push({ value: match[0], index: match.index, length: match[0].length });
    }
  }
  allMatches.sort((a, b) => b.length - a.length);
  const replacedRanges = [];
  for (const m of allMatches) {
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

  // Step 0b: Common English words + tech/OS terms that should never be removed
  // This set is used to distinguish real words from OCR fragments
  // Includes: common English words (3-8 chars), OS/UI terms, tech acronyms, file metadata
  const protectedWords = new Set([
    // Common English (high-frequency short words)
    'the', 'and', 'for', 'are', 'but', 'not', 'you', 'all', 'can', 'had', 'her', 'was', 'one',
    'our', 'out', 'has', 'his', 'how', 'its', 'may', 'new', 'now', 'old', 'see', 'way', 'who',
    'did', 'get', 'let', 'say', 'she', 'too', 'use', 'him', 'man', 'run', 'set', 'try', 'ask',
    'own', 'put', 'big', 'end', 'few', 'got', 'why', 'yes', 'yet', 'ago', 'add', 'also', 'back',
    'been', 'call', 'came', 'come', 'each', 'find', 'from', 'give', 'good', 'have', 'help',
    'here', 'high', 'home', 'just', 'keep', 'know', 'last', 'left', 'life', 'like', 'line',
    'list', 'long', 'look', 'made', 'make', 'many', 'more', 'most', 'much', 'must', 'name',
    'next', 'only', 'open', 'over', 'part', 'play', 'read', 'real', 'same', 'show', 'side',
    'some', 'such', 'sure', 'take', 'tell', 'text', 'than', 'that', 'them', 'then', 'they',
    'this', 'time', 'turn', 'used', 'very', 'want', 'well', 'went', 'what', 'when', 'will',
    'with', 'word', 'work', 'year', 'your', 'about', 'after', 'again', 'being', 'below',
    'between', 'both', 'build', 'check', 'close', 'could', 'every', 'first', 'found', 'great',
    'group', 'house', 'large', 'later', 'learn', 'never', 'night', 'number', 'order', 'other',
    'place', 'point', 'right', 'shall', 'since', 'small', 'start', 'state', 'still', 'story',
    'think', 'three', 'under', 'until', 'using', 'water', 'where', 'which', 'while', 'world',
    'would', 'write', 'young', 'before', 'change', 'create', 'delete', 'during', 'follow',
    'memory', 'screen', 'search', 'server', 'should', 'system', 'update', 'window',
    // Prepositions, articles, conjunctions
    'at', 'of', 'in', 'to', 'or', 'an', 'is', 'it', 'on', 'by', 'no', 'if', 'do', 'so', 'up',
    'as', 'be', 'he', 'we', 'me',
    // Units and file metadata
    'KB', 'MB', 'GB', 'TB', 'PDF', 'PNG', 'JPG', 'SVG', 'ZIP', 'CSV', 'DOC', 'XLS',
    'Document', 'Image', 'Folder', 'Network', 'Shared', 'Tags', 'Modified', 'Created',
    'Information',
    // macOS UI
    'File', 'Edit', 'View', 'Go', 'Name', 'Size', 'Kind', 'Date', 'Type', 'Window', 'Help',
    'AirDrop', 'Recents', 'Desktop', 'Documents', 'Downloads', 'Applications',
    'iCloud', 'Drive', 'Locations', 'Favorites', 'Finder', 'Safari', 'Chrome', 'Warp',
    'Markup', 'More', 'Show',
    // Logging / dev
    'info', 'level', 'message', 'error', 'warn', 'debug', 'confidence', 'threshold',
    'capture', 'stored', 'Screen', 'Diff', 'main', 'true', 'false', 'null',
    // Tech acronyms
    'AI', 'ML', 'SDK', 'LLM', 'RAG', 'NLP',
    'POST', 'GET', 'PUT', 'DELETE', 'MCP', 'API', 'URL', 'HTTP', 'HTTPS',
    'HTML', 'CSS', 'JSON', 'SQL', 'OCR', 'NPM', 'CLI', 'IDE', 'GUI',
    'RAM', 'CPU', 'GPU', 'SSD', 'USB', 'DOM', 'SSH', 'FTP', 'DNS',
    'TCP', 'UDP', 'AWS', 'GCP', 'ENV', 'PM', 'AM', 'EST', 'UTC', 'PST', 'CST',
    // Names that commonly appear in filenames
    'misc', 'data', 'test', 'config', 'index', 'setup'

  ]);
  // Case-insensitive lookup
  const protectedLower = new Set([...protectedWords].map(w => w.toLowerCase()));

  // Step A: Replace sequences of 4+ consecutive single-letter words (clear OCR noise)
  text = text.replace(/(?:\b[a-zA-Z]\b[\s.,;:!?-]*){4,}/g, DELIMITER);

  // Step B: Replace punctuation-heavy fragments with delimiter (3+ consecutive symbol groups)
  text = text.replace(/(?:[^a-zA-Z0-9\s]{2,}\s*){3,}/g, DELIMITER);

  // Step C: Sliding window — flag windows dense with short/nonsense words
  const words = text.split(/\s+/).filter(w => w.length > 0);
  const windowSize = 6;
  const gibberishRanges = [];

  function isNonsenseWord(w) {
    if (w.startsWith(PLACEHOLDER_PREFIX) || w.startsWith(PROTECT_PREFIX)) return false;
    if (w === '---') return false;
    // Strip punctuation for dictionary lookup
    const clean = w.replace(/[^a-zA-Z0-9-]/g, '');
    if (protectedLower.has(clean.toLowerCase())) return false;
    // Numbers (file sizes, dates) are not gibberish
    if (/^\d+$/.test(w)) return false;
    // Words with digits mixed in are likely real (e.g., "0.15", "3001")
    if (/\d/.test(w) && w.length >= 3) return false;
    // Filenames with extensions are not gibberish
    if (/\.[a-zA-Z]{2,4}$/.test(w)) return false;
    // Hyphenated compound words are likely real (e.g., "quarter-two-report")
    if (/-/.test(w) && w.length >= 5) return false;
    const alpha = w.replace(/[^a-zA-Z]/g, '');
    if (alpha.length === 0) return false;
    // 1-2 letter words not in dictionary = likely noise
    if (alpha.length <= 2) return true;
    // 3+ letter words: use structural heuristics (not dictionary)
    // Real English words have vowels and follow consonant-vowel patterns
    const vowels = alpha.replace(/[^aeiouAEIOU]/g, '').length;
    // No vowels at all in 3+ chars = gibberish (e.g., "rbd", "Hg", "Rn")
    if (vowels === 0) return true;
    // 3-4 char words with <25% vowels = likely gibberish (e.g., "vfba" = 25%, borderline)
    if (alpha.length <= 4 && vowels / alpha.length < 0.2) return true;
    // Check for impossible consonant clusters (3+ consonants in a row at start/end)
    if (/^[^aeiouAEIOU]{3,}/i.test(alpha) && alpha.length <= 5) return true;
    if (/[^aeiouAEIOU]{4,}$/i.test(alpha)) return true;
    return false;
  }

  for (let i = 0; i <= words.length - windowSize; i++) {
    const window = words.slice(i, i + windowSize);
    const nonsenseCount = window.filter(isNonsenseWord).length;
    // 4 out of 6 short nonsense words = likely gibberish
    if (nonsenseCount >= 4) {
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
    let lastWasRemoved = false;
    const filtered = [];
    for (let i = 0; i < words.length; i++) {
      const w = words[i];
      // Never remove timestamp placeholders or protected words
      if (removeSet.has(i) && !w.startsWith(PLACEHOLDER_PREFIX) && !protectedLower.has(w.replace(/[^a-zA-Z0-9-]/g, '').toLowerCase())) {
        if (!lastWasRemoved) filtered.push('---');
        lastWasRemoved = true;
      } else {
        filtered.push(w);
        lastWasRemoved = false;
      }
    }
    text = filtered.join(' ');
  }

  // Step D: Remove individual nonsense words that survived the sliding window
  // These are isolated OCR artifacts like "vfba", "inder", "Barf", "rere", "Genej"
  // Only remove if they're clearly not real words (no vowels, or very short + no context)
  {
    const cleanedWords = text.split(/\s+/).filter(w => w.length > 0);
    const result = [];
    let lastWasRemoved = false;
    for (let i = 0; i < cleanedWords.length; i++) {
      const w = cleanedWords[i];
      if (w === '---' || w.startsWith(PLACEHOLDER_PREFIX)) {
        result.push(w);
        lastWasRemoved = false;
        continue;
      }
      const clean = w.replace(/[^a-zA-Z0-9-]/g, '');
      if (protectedLower.has(clean.toLowerCase())) {
        result.push(w);
        lastWasRemoved = false;
        continue;
      }
      const alpha = w.replace(/[^a-zA-Z]/g, '');
      // Skip non-alpha tokens (numbers, punctuation, timestamps)
      if (alpha.length === 0) {
        result.push(w);
        lastWasRemoved = false;
        continue;
      }
      // Protect filenames, hyphenated words, words with digits
      if (/\.[a-zA-Z]{2,4}$/.test(w) || (/-/.test(w) && w.length >= 5) || (/\d/.test(w) && w.length >= 3)) {
        result.push(w);
        lastWasRemoved = false;
        continue;
      }
      // Use same structural heuristics as sliding window
      const vowels = alpha.replace(/[^aeiouAEIOU]/g, '').length;
      const isLikelyNonsense = (
        (alpha.length <= 2) ||
        (alpha.length >= 3 && vowels === 0) ||
        (alpha.length <= 4 && vowels / alpha.length < 0.2) ||
        (/^[^aeiouAEIOU]{3,}/i.test(alpha) && alpha.length <= 5) ||
        /[^aeiouAEIOU]{4,}$/i.test(alpha)
      );
      if (isLikelyNonsense) {
        if (!lastWasRemoved) result.push('---');
        lastWasRemoved = true;
      } else {
        result.push(w);
        lastWasRemoved = false;
      }
    }
    text = result.join(' ');
  }

  // Step E: Restore preserved timestamps
  for (const [placeholder, ts] of Object.entries(placeholderMap)) {
    text = text.replace(new RegExp(placeholder, 'g'), ts);
  }

  // Step E: Collapse multiple consecutive delimiters into one
  text = text.replace(/(?:\s*---\s*)+/g, ' --- ');

  // Step F: Remove leading/trailing delimiters and collapse whitespace
  text = text.replace(/^\s*---\s*/, '').replace(/\s*---\s*$/, '');
  text = text.replace(/\s+/g, ' ').trim();

  return text;
}

function additionalCleanup(text) {
  // Remove square-bracketed log tags like [INFO], [DEBUG] etc.
  text = text.replace(/\[(?:INFO|DEBUG|WARN|ERROR|TRACE)\]/gi, '');
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
    // Must have an extension OR be a hyphenated name (contextual file detection)
    (/\.[a-zA-Z0-9]+$/.test(name) || /^[a-zA-Z0-9]+-[a-zA-Z0-9-]+[a-zA-Z0-9]$/.test(name))
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
