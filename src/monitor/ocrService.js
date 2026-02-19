import Tesseract from 'tesseract.js';
import crypto from 'crypto';
import logger from '../utils/logger.js';

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
   * Summarize OCR text for embedding generation.
   * Truncates to a reasonable length and removes noise.
   */
  static summarizeForEmbedding(appName, windowTitle, ocrText, maxLength = 2000) {
    // Build a structured summary for better semantic search
    const prefix = `${appName}: ${windowTitle}`;

    // Take the first N characters of OCR text
    const truncatedOCR = ocrText.length > maxLength
      ? ocrText.substring(0, maxLength) + '...'
      : ocrText;

    return `${prefix}\n${truncatedOCR}`;
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
