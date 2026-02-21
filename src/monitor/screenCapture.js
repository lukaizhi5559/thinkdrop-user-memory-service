import screenshot from 'screenshot-desktop';
import pixelmatch from 'pixelmatch';
import { PNG } from 'pngjs';
import crypto from 'crypto';
import logger from '../utils/logger.js';

class ScreenCaptureService {
  constructor() {
    this.lastScreenshotBuffer = null;
    this.lastScreenshotPNG = null;
    this.diffThreshold = parseFloat(process.env.SCREEN_CAPTURE_DIFF_THRESHOLD || '0.15');
  }

  /**
   * Take a screenshot and return the raw PNG buffer.
   */
  async capture() {
    try {
      const buffer = await screenshot({ format: 'png' });
      return buffer;
    } catch (error) {
      // Screen locked or no display available — skip silently
      if (error.message && error.message.includes('No displays detected')) {
        logger.debug('Screenshot skipped: no displays available (screen may be locked)');
        return null;
      }
      logger.error('Screenshot capture failed', { error: error.message });
      return null;
    }
  }

  /**
   * Parse a PNG buffer into pixel data for comparison.
   */
  parsePNG(buffer) {
    return new Promise((resolve, reject) => {
      const png = new PNG();
      png.parse(buffer, (err, data) => {
        if (err) reject(err);
        else resolve(data);
      });
    });
  }

  /**
   * Compare current screenshot to the previous one.
   * Returns the diff ratio (0.0 = identical, 1.0 = completely different).
   */
  async computeDiff(currentBuffer) {
    if (!this.lastScreenshotBuffer) {
      // First screenshot — always counts as changed
      this.lastScreenshotBuffer = currentBuffer;
      this.lastScreenshotPNG = await this.parsePNG(currentBuffer);
      return 1.0;
    }

    try {
      const currentPNG = await this.parsePNG(currentBuffer);

      // If dimensions changed (e.g., window resize), treat as fully different
      if (currentPNG.width !== this.lastScreenshotPNG.width ||
          currentPNG.height !== this.lastScreenshotPNG.height) {
        this.lastScreenshotBuffer = currentBuffer;
        this.lastScreenshotPNG = currentPNG;
        return 1.0;
      }

      const { width, height } = currentPNG;
      const totalPixels = width * height;

      const diffPixels = pixelmatch(
        this.lastScreenshotPNG.data,
        currentPNG.data,
        null, // no output diff image
        width,
        height,
        { threshold: 0.1 }
      );

      const diffRatio = diffPixels / totalPixels;

      // Update last screenshot
      this.lastScreenshotBuffer = currentBuffer;
      this.lastScreenshotPNG = currentPNG;

      return diffRatio;
    } catch (error) {
      logger.error('Pixel diff computation failed', { error: error.message });
      // On error, assume changed to avoid missing captures
      this.lastScreenshotBuffer = currentBuffer;
      try {
        this.lastScreenshotPNG = await this.parsePNG(currentBuffer);
      } catch (e) {
        // ignore
      }
      return 1.0;
    }
  }

  /**
   * Check if the screen has changed enough to warrant a new capture.
   * Returns { changed: boolean, diffRatio: number, buffer: Buffer }
   */
  async captureIfChanged() {
    const buffer = await this.capture();
    if (!buffer) {
      return { changed: false, diffRatio: 0, buffer: null };
    }

    const diffRatio = await this.computeDiff(buffer);
    const changed = diffRatio > this.diffThreshold;

    return { changed, diffRatio, buffer };
  }

  /**
   * Generate a hash of text content for dedup.
   */
  static hashText(text) {
    return crypto.createHash('sha256').update(text).digest('hex');
  }
}

let instance = null;

export function getScreenCaptureService() {
  if (!instance) {
    instance = new ScreenCaptureService();
  }
  return instance;
}

export default ScreenCaptureService;
