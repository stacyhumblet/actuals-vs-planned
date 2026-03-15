const DATA_SHEET_ID  = '1j165dsa1a-DDapOCgyBLrJQ_UBa4LzCWdWez4_obLD0';
const ACTUALS_TAB    = 'Actuals_DummyData';
const PLANNED_GID    = 1195627887;   // tab pointed to in the project URL
const CACHE_KEY      = 'avp_data_v2';
const CACHE_TTL      = 21600; // 6 hours

// ── Entry point ────────────────────────────────────────────────────────────────
function doGet() {
  try {
    const data = getAvpData();
    return ContentService
      .createTextOutput(JSON.stringify(data))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (e) {
    return ContentService
      .createTextOutput(JSON.stringify({ error: e.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

// ── Called from client ─────────────────────────────────────────────────────────
function getAvpData() {
  const cache  = CacheService.getScriptCache();
  const chunks = getChunks_(cache);
  if (chunks) return JSON.parse(chunks);

  const ss = SpreadsheetApp.openById(DATA_SHEET_ID);

  // ── Actuals ─────────────────────────────────────────────────────────────────
  const actualsSheet = ss.getSheetByName(ACTUALS_TAB);
  if (!actualsSheet) throw new Error('Tab not found: ' + ACTUALS_TAB);
  const actualsVals    = actualsSheet.getDataRange().getValues();
  const actualsHeaders = actualsVals[0].map(String);
  const actualsRows    = [];

  for (let i = 1; i < actualsVals.length; i++) {
    const row = {};
    actualsHeaders.forEach((h, j) => {
      const v = actualsVals[i][j];
      row[h]  = (v !== undefined && v !== null) ? String(v) : '';
    });
    if (row['Resource Name'] && row['Resource Name'].trim()) actualsRows.push(row);
  }

  // ── Planned — try fetching by GID, fall back to synthetic ─────────────────
  let plannedRows    = null;
  let plannedHeaders = null;

  try {
    const url  = 'https://docs.google.com/spreadsheets/d/' + DATA_SHEET_ID +
                 '/export?format=csv&gid=' + PLANNED_GID;
    const resp = UrlFetchApp.fetch(url, {
      headers: { Authorization: 'Bearer ' + ScriptApp.getOAuthToken() },
      muteHttpExceptions: true,
      followRedirects: true
    });

    if (resp.getResponseCode() === 200) {
      const parsed = parseCSV_(resp.getContentText());
      // Accept if it has at least 2 columns and 1 data row
      if (parsed.headers.length >= 2 && parsed.rows.length > 0) {
        plannedHeaders = parsed.headers;
        plannedRows    = parsed.rows;
      }
    }
  } catch (e) {
    console.log('Planned tab fetch failed (will use synthetic):', e);
  }

  // ── Synthetic planned data ──────────────────────────────────────────────────
  // If no real planned tab, generate per-resource-per-period planned hours by
  // applying a deterministic scaling factor to each resource's actual hours.
  // Factor is derived from a simple hash of the resource name so it's stable
  // across cache refreshes but varies across resources (0.75–1.30 range).
  if (!plannedRows) {
    plannedHeaders = ['Resource Name', 'Year of Worklog', 'Month of Worklog', 'Planned Hours'];
    const resMonthAgg = {};

    actualsRows.forEach(r => {
      const res   = r['Resource Name'] || '';
      const year  = r['Year of Worklog'] || '';
      const month = r['Month of Worklog'] || '';
      const hrs   = parseFloat(r['Worklog Hours']) || 0;
      const key   = res + '|||' + year + '|||' + month;
      if (!resMonthAgg[key]) resMonthAgg[key] = { res, year, month, actualHrs: 0 };
      resMonthAgg[key].actualHrs += hrs;
    });

    plannedRows = Object.values(resMonthAgg).map(e => {
      const factor = syntheticFactor_(e.res, e.year, e.month);
      return {
        'Resource Name':    e.res,
        'Year of Worklog':  e.year,
        'Month of Worklog': e.month,
        'Planned Hours':    String(Math.round(e.actualHrs * factor))
      };
    });
  }

  const result = { actualsRows, actualsHeaders, plannedRows, plannedHeaders };
  putChunks_(cache, JSON.stringify(result));
  return result;
}

// ── Stable pseudo-random factor per resource+period (range 0.75–1.30) ─────────
function syntheticFactor_(res, year, month) {
  let h = 0;
  const s = res + year + month;
  for (let i = 0; i < s.length; i++) {
    h = ((h << 5) - h + s.charCodeAt(i)) | 0;
  }
  // Map to [0,1] then scale to [0.75, 1.30]
  const t = (Math.abs(h) % 10000) / 10000;
  return 0.75 + t * 0.55;
}

// ── CSV parser ─────────────────────────────────────────────────────────────────
function parseCSV_(csv) {
  const lines = csv.replace(/\r/g, '').split('\n').filter(l => l.trim());
  if (lines.length < 2) return { headers: [], rows: [] };
  const headers = splitLine_(lines[0]);
  const rows    = lines.slice(1).map(line => {
    const vals = splitLine_(line);
    const row  = {};
    headers.forEach((h, j) => { row[h] = vals[j] !== undefined ? vals[j] : ''; });
    return row;
  });
  return { headers, rows };
}

function splitLine_(line) {
  const out = [];
  let cur = '', inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      if (inQ && line[i + 1] === '"') { cur += '"'; i++; }
      else inQ = !inQ;
    } else if (c === ',' && !inQ) {
      out.push(cur); cur = '';
    } else {
      cur += c;
    }
  }
  out.push(cur);
  return out;
}

// ── Cache helpers ──────────────────────────────────────────────────────────────
function putChunks_(cache, json) {
  try {
    const CHUNK = 90000;
    const total = Math.ceil(json.length / CHUNK);
    const pairs = { '__avp_chunks__': String(total) };
    for (let i = 0; i < total; i++) {
      pairs[CACHE_KEY + '_' + i] = json.slice(i * CHUNK, (i + 1) * CHUNK);
    }
    cache.putAll(pairs, CACHE_TTL);
  } catch (e) { console.log('Cache write failed:', e); }
}

function getChunks_(cache) {
  try {
    const meta = cache.get('__avp_chunks__');
    if (!meta) return null;
    const total  = parseInt(meta);
    const keys   = Array.from({ length: total }, (_, i) => CACHE_KEY + '_' + i);
    const stored = cache.getAll(keys);
    if (Object.keys(stored).length !== total) return null;
    return keys.map(k => stored[k]).join('');
  } catch (e) { return null; }
}

// ── Utilities ─────────────────────────────────────────────────────────────────
function clearAvpCache() {
  const cache = CacheService.getScriptCache();
  cache.remove('__avp_chunks__');
  Logger.log('Cache cleared.');
}

function warmCache() {
  clearAvpCache();
  getAvpData();
  Logger.log('Cache warmed at ' + new Date().toLocaleString());
}

function createWarmCacheTrigger() {
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === 'warmCache')
    .forEach(t => ScriptApp.deleteTrigger(t));
  ScriptApp.newTrigger('warmCache').timeBased().everyHours(4).create();
  Logger.log('Warm-cache trigger created.');
}

function testDataAccess() {
  clearAvpCache();
  const data = getAvpData();
  Logger.log('Actuals rows: '  + data.actualsRows.length);
  Logger.log('Planned rows: '  + data.plannedRows.length);
  Logger.log('Planned headers: ' + data.plannedHeaders.join(' | '));
  Logger.log('Actuals headers: ' + data.actualsHeaders.join(' | '));
}
