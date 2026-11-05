import screenshot from 'screenshot-desktop';
import pixelmatch from 'pixelmatch';
import { PNG } from 'pngjs';
import crypto from 'crypto';
import logger from '../utils/logger.js';

const DIFF_GRID_W = 480;

class ScreenCaptureService {
  constructor() {
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
   * Downsample decoded RGBA to a fixed comparison grid via stride sampling.
   * Full-retina pixelmatch on ~5MP was a large share of per-tick CPU; a
   * ~480px grid preserves ratio semantics while cutting compare work ~35x.
   */
  _downsampleForDiff(png) {
    const { width, height, data } = png;
    if (width <= DIFF_GRID_W) return { data, width, height };
    const scale = width / DIFF_GRID_W;
    const tw = DIFF_GRID_W;
    const th = Math.max(1, Math.round(height / scale));
    const out = Buffer.alloc(tw * th * 4);
    for (let ty = 0; ty < th; ty++) {
      const sy = Math.min(height - 1, Math.floor(ty * scale));
      for (let tx = 0; tx < tw; tx++) {
        const sx = Math.min(width - 1, Math.floor(tx * scale));
        const si = (sy * width + sx) * 4;
        const di = (ty * tw + tx) * 4;
        out[di] = data[si]; out[di + 1] = data[si + 1];
        out[di + 2] = data[si + 2]; out[di + 3] = 255;
      }
    }
    return { data: out, width: tw, height: th };
  }

  /**
   * Compare current screenshot to the previous one.
   * Returns the diff ratio (0.0 = identical, 1.0 = completely different).
   */
  async computeDiff(currentBuffer) {
    if (!this.lastScreenshotPNG) {
      // First screenshot — always counts as changed
      this.lastScreenshotPNG = this._downsampleForDiff(await this.parsePNG(currentBuffer));
      return 1.0;
    }

    try {
      const currentPNG = this._downsampleForDiff(await this.parsePNG(currentBuffer));

      // If dimensions changed (e.g., window resize), treat as fully different
      if (currentPNG.width !== this.lastScreenshotPNG.width ||
          currentPNG.height !== this.lastScreenshotPNG.height) {
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
      this.lastScreenshotPNG = currentPNG;

      return diffRatio;
    } catch (error) {
      logger.error('Pixel diff computation failed', { error: error.message });
      // On error, assume changed to avoid missing captures
      try {
        this.lastScreenshotPNG = this._downsampleForDiff(await this.parsePNG(currentBuffer));
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
