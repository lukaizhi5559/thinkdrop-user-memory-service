// ocrStructure.js
// Pure, testable function for restructuring LiteParser OCR word fragments
// into clean structured rows. Copied from command-service/ocrOverlayStructure.cjs
// so user-memory-service can store structured rows in memory metadata without
// a cross-service dependency.
//
// No dependencies — operates on the textItems array shape returned by LiteParser:
// [{ text, x, y, width, height, confidence }]
//
// Exports: structureOcrOverlayItems

'use strict';

// ---------------------------------------------------------------------------
// Noise filters
// ---------------------------------------------------------------------------
const _PUNCT_ONLY_RE = /^[^\w]+$/;
const _ICON_CHARS_RE = /^[®©™»>•‹›←→↑↓✓✕✗✔✘★☆◇◆■□●○]+$/;
const _BUTTON_LABELS = /^(create|save|cancel|done|submit|ok|confirm|delete|remove|close|apply|next|back|edit|add|update|send|post|publish|share|select|choose|done|finish|continue|retry|try again|yes|no|accept|reject|decline)$/i;
const _INPUT_LABELS = /^(search|enter|name|title|description|email|password|username|label|find|what|where|when)$/i;
const _MENU_WORDS = new Set([
  'edit', 'details', 'remove', 'from', 'profile', 'delete', 'make', 'private', 'invite',
  'collaborators', 'exclude', 'your', 'taste', 'move', 'to', 'folder', 'share', 'open',
  'in', 'desktop', 'app', 'create', 'playlist', 'songs', 'episodes', 'blend', 'combine',
  'tastes', 'into', 'a', 'organize', 'playlists', 'cancel', 'update', 'add', 'new',
]);

// ---------------------------------------------------------------------------
// structureOcrOverlayItems — cluster word fragments into structured rows
// ---------------------------------------------------------------------------
// Input:  items[] = [{ text, x, y, width, height, confidence }]
// Output: rows[]  = [{ id, type, text, description, x, y, width, height }]
//
// Types: heading | row-item-link | row-item-description | input-field | button | divider
//
export function structureOcrOverlayItems(items, opts = {}) {
  if (!items || items.length === 0) return [];

  // ── Step 1: Filter noise ────────────────────────────────────────────────
  const filtered = [];
  for (const item of items) {
    const text = (item.text || '').trim();
    if (text.length === 0) continue;
    const isIconChar = _ICON_CHARS_RE.test(text) ||
      (text.length === 1 && !/[a-z0-9]/i.test(text));
    const isPunctOnly = _PUNCT_ONLY_RE.test(text);
    if (text.length <= 3 && (item.confidence || 1.0) < 0.6) continue;
    if (isIconChar) {
      filtered.push({ ...item, text, _isIconCandidate: true });
      continue;
    }
    if (isPunctOnly && text.length > 3) continue;
    filtered.push({ ...item, text });
  }
  if (filtered.length === 0) return [];

  // ── Step 1b: Filter the left icon column ────────────────────────────────
  filtered.sort((a, b) => (a.x || 0) - (b.x || 0));
  let iconColumnCutoff = Infinity;
  for (let i = 0; i < filtered.length; i++) {
    if (i + 1 < filtered.length) {
      const curr = filtered[i];
      const next = filtered[i + 1];
      const gap = (next.x || 0) - ((curr.x || 0) + (curr.width || 0));
      if (gap > 20) {
        iconColumnCutoff = (curr.x || 0) + (curr.width || 0) + 10;
        break;
      }
    }
  }
  const iconFiltered = [];
  for (const item of filtered) {
    const text = item.text || '';
    const right = (item.x || 0) + (item.width || 0);
    const isInIconColumn = (item.x || 0) < iconColumnCutoff && right < iconColumnCutoff;
    const hasTextToRight = filtered.some(other =>
      other !== item &&
      (other.x || 0) > right &&
      Math.abs((other.y || 0) - (item.y || 0)) < 40
    );
    if (item._isIconCandidate) {
      iconFiltered.push(item);
      continue;
    }
    if (isInIconColumn && hasTextToRight && text.length <= 3 && !_MENU_WORDS.has(text.toLowerCase())) {
      continue;
    }
    iconFiltered.push(item);
  }

  // ── Step 2: Cluster into rows by vertical overlap ───────────────────────
  iconFiltered.sort((a, b) => (a.y || 0) - (b.y || 0));

  const rows = [];
  let currentRow = [iconFiltered[0]];
  let currentRowY = iconFiltered[0].y || 0;
  let currentRowH = iconFiltered[0].height || 0;

  for (let i = 1; i < iconFiltered.length; i++) {
    const item = iconFiltered[i];
    const itemY = item.y || 0;
    const itemH = item.height || 0;
    const threshold = Math.max(currentRowH, itemH) * 0.6;
    if (Math.abs(itemY - currentRowY) > threshold) {
      rows.push(currentRow);
      currentRow = [item];
      currentRowY = itemY;
      currentRowH = itemH;
    } else {
      currentRow.push(item);
      if (itemH > currentRowH) {
        currentRowH = itemH;
      }
    }
  }
  rows.push(currentRow);

  // ── Step 2b: Split rows with large horizontal gaps ──────────────────────
  const splitRows = [];
  for (const rowItems of rows) {
    if (rowItems.length <= 1) {
      splitRows.push(rowItems);
      continue;
    }
    const sorted = [...rowItems].sort((a, b) => (a.x || 0) - (b.x || 0));
    const subRows = [[sorted[0]]];
    for (let j = 1; j < sorted.length; j++) {
      const prev = sorted[j - 1];
      const curr = sorted[j];
      const prevRight = (prev.x || 0) + (prev.width || 0);
      const currLeft = curr.x || 0;
      const hGap = currLeft - prevRight;
      const refHeight = Math.max(prev.height || 0, curr.height || 0);
      if (hGap > refHeight * 2) {
        subRows.push([curr]);
      } else {
        subRows[subRows.length - 1].push(curr);
      }
    }
    for (const sub of subRows) {
      splitRows.push(sub);
    }
  }

  // ── Step 3: Within each row, sort by x and join text ────────────────────
  const _ACTION_PREFIXES = ['Edit', 'Remove', 'Delete', 'Make', 'Move', 'Share', 'Invite',
    'Exclude', 'Open', 'Create', 'Add', 'Update', 'Send', 'Post', 'Publish', 'Select',
    'Choose', 'Save', 'Cancel', 'Close', 'Apply', 'Submit'];
  function segmentMergedWords(text) {
    if (!text || text.length < 5 || /\s/.test(text)) return text;
    for (const prefix of _ACTION_PREFIXES) {
      if (text.toLowerCase().startsWith(prefix.toLowerCase()) && text.length > prefix.length) {
        const rest = text.slice(prefix.length);
        if (/^[A-Z]/.test(rest) || _MENU_WORDS.has(rest.toLowerCase())) {
          return prefix + ' ' + rest;
        }
      }
    }
    return text;
  }
  const rowObjs = splitRows.map(rowItems => {
    rowItems.sort((a, b) => (a.x || 0) - (b.x || 0));
    const textParts = rowItems.filter(i => (i.confidence || 1.0) >= 0.5).map(i => i.text);
    let text = textParts.join(' ').replace(/\s+/g, ' ').trim();
    text = textParts.map(segmentMergedWords).join(' ').replace(/\s+/g, ' ').trim();
    const x = Math.min(...rowItems.map(i => i.x || 0));
    const y = Math.min(...rowItems.map(i => i.y || 0));
    const right = Math.max(...rowItems.map(i => (i.x || 0) + (i.width || 0)));
    const bottom = Math.max(...rowItems.map(i => (i.y || 0) + (i.height || 0)));
    const height = bottom - y;
    const itemHeights = rowItems.map(i => i.height || 0).filter(h => h > 0).sort((a, b) => a - b);
    const medianItemHeight = itemHeights.length > 0
      ? itemHeights[Math.floor(itemHeights.length / 2)]
      : (height || 10);
    return {
      text,
      x: Math.round(x),
      y: Math.round(y),
      width: Math.round(right - x),
      height: Math.round(height),
      _medianItemHeight: Math.round(medianItemHeight),
      _itemCount: rowItems.length,
    };
  });

  // ── Step 4: Compute median font height across all rows for heading detection ───
  const fontHeights = rowObjs.map(r => r._medianItemHeight).filter(h => h > 0).sort((a, b) => a - b);
  const medianHeight = fontHeights.length > 0 ? fontHeights[Math.floor(fontHeights.length / 2)] : 10;

  // ── Step 5: Classify each row (first pass — no description detection yet) ──
  const classified = rowObjs.map((row, idx) => {
    const text = row.text;
    const textLower = text.toLowerCase();
    const textLen = text.length;
    const h = row._medianItemHeight;
    const isLargeFont = h > medianHeight * 1.4;

    if (row._isIconCandidate || (textLen <= 2 && !/[a-z0-9]/i.test(text))) {
      return { ...row, type: 'icon' };
    }
    if (textLen <= 2 && _PUNCT_ONLY_RE.test(text)) {
      return { ...row, type: 'divider' };
    }
    if (idx === 0 && isLargeFont && textLen < 50) {
      return { ...row, type: 'heading' };
    }
    if (isLargeFont && textLen < 40 && textLen > 2) {
      return { ...row, type: 'heading' };
    }
    if (textLen > 3 && textLen < 60 && /[?]/.test(text)) {
      return { ...row, type: 'input-field' };
    }
    if (textLen > 2 && textLen < 50 && row._itemCount <= 3) {
      const words = textLower.split(/\s+/);
      if (words.some(w => _INPUT_LABELS.test(w))) {
        return { ...row, type: 'input-field' };
      }
    }
    return { ...row, type: 'row-item-link' };
  });

  // ── Step 5b: Second pass — detect descriptions by y-gap to previous LINK ──
  const _REAL_SINGLE_CHARS = new Set(['a', 'i', 'o']);
  for (let i = 1; i < classified.length; i++) {
    const row = classified[i];
    if (row.type !== 'row-item-link') continue;
    let prevIdx = i - 1;
    while (prevIdx >= 0) {
      const pt = classified[prevIdx].type;
      const ptLen = classified[prevIdx].text.length;
      const isNoise = ptLen <= 2 && !_REAL_SINGLE_CHARS.has(classified[prevIdx].text.toLowerCase());
      if (pt === 'heading' || pt === 'divider' || isNoise) {
        prevIdx--;
      } else {
        break;
      }
    }
    if (prevIdx < 0) continue;
    const prev = classified[prevIdx];
    if (prev.type !== 'row-item-link') continue;
    const yGap = row.y - prev.y;
    const gapThreshold = prev._medianItemHeight * 2.5;
    if (yGap < gapThreshold && row.text.length > 10 && row.text.length < 100) {
      classified[i].type = 'row-item-description';
    }
  }

  // ── Step 5c: Classify buttons based on full overlay context ─────────────
  const _hasInputs = classified.some(r => r.type === 'input-field');
  for (let i = 0; i < classified.length; i++) {
    const row = classified[i];
    if (row.type !== 'row-item-link') continue;
    const textLen = row.text.length;
    const textLower = row.text.toLowerCase();
    if (_hasInputs && textLen >= 2 && textLen <= 30 && _BUTTON_LABELS.test(textLower)) {
      classified[i].type = 'button';
      continue;
    }
    if (i + 1 < classified.length) {
      const next = classified[i + 1];
      if (next.type === 'row-item-link' &&
          Math.abs(next.y - row.y) < 10 &&
          textLen >= 2 && textLen <= 20 && _BUTTON_LABELS.test(textLower) &&
          next.text.length >= 2 && next.text.length <= 20 && _BUTTON_LABELS.test(next.text.toLowerCase())) {
        classified[i].type = 'button';
        classified[i + 1].type = 'button';
      }
    }
  }

  // ── Step 5d: Button-rescue pass ──
  if (_hasInputs) {
    const _rescued = [];
    for (let i = 0; i < classified.length; i++) {
      const row = classified[i];
      if (row.type !== 'row-item-link') { _rescued.push(row); continue; }
      const _words = row.text.split(/\s+/);
      const _buttonWord = _words.find(w => _BUTTON_LABELS.test(w.toLowerCase()));
      if (!_buttonWord || _words.length <= 1) {
        _rescued.push(row);
        continue;
      }
      _rescued.push({
        ...row,
        type: 'button',
        text: _buttonWord,
        description: row.description || null,
      });
      const _remaining = _words.filter(w => w !== _buttonWord).join(' ').trim();
      if (_remaining.length > 0) {
        _rescued.push({
          ...row,
          text: _remaining,
          type: 'row-item-link',
          description: null,
        });
      }
    }
    classified.length = 0;
    classified.push(..._rescued);
  }

  // ── Step 6: Pair descriptions with parent links ─────────────────────────
  const final = [];
  for (let i = 0; i < classified.length; i++) {
    const row = classified[i];
    if (row.text.length <= 2 && !_REAL_SINGLE_CHARS.has(row.text.toLowerCase()) && row.type !== 'heading') {
      continue;
    }
    if (row.type === 'row-item-description' && final.length > 0) {
      const last = final[final.length - 1];
      if (last.type === 'row-item-link' && !last.description && last.text.length > 2) {
        last.description = row.text;
        continue;
      }
    }
    if (row.type === 'divider') {
      continue;
    }
    final.push({ ...row, description: row.description || null });
  }

  // ── Step 7: Assign sequential IDs and clean up internal fields ──────────
  let id = 1;
  return final.map(row => {
    const { _medianItemHeight, _itemCount, ...clean } = row;
    return { id: id++, ...clean };
  });
}

export default { structureOcrOverlayItems };
