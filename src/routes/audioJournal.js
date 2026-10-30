// audioJournal routes — ingest for ambient audio journaling.
//   POST /audio.context — is-playing context from the extension media probe
//     ({tabId, url, title, mediaTitle, state}) relayed via ext.gateway
//   POST /audio.segment — transcript segment from voice-service ambient
//     capture ({text, ts?, energy?})
import express from 'express';
import { getAudioJournal } from '../monitor/audioJournal.js';

const router = express.Router();

router.post('/audio.context', (req, res) => {
  try {
    getAudioJournal().handleContext(req.body || {});
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

router.post('/audio.segment', (req, res) => {
  try {
    getAudioJournal().handleSegment(req.body || {});
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Live capture state — journal.status planning tool + manual bring-up checks.
router.get('/audio.status', (req, res) => {
  try {
    res.json(getAudioJournal().status());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
