require('dotenv').config();
const axios        = require('axios');
const { google }   = require('googleapis');

// ── Environment ───────────────────────────────────────────────────────────────

const HUBSPOT_API_KEY        = process.env.HUBSPOT_API_KEY;
const GOOGLE_SERVICE_ACCOUNT = process.env.GOOGLE_SERVICE_ACCOUNT;
const GOOGLE_SHEET_ID        = process.env.GOOGLE_SHEET_ID;
const SHEET_TAB              = process.env.SHEET_TAB || 'Active Hires';

if (!HUBSPOT_API_KEY)        { console.error('ERROR: HUBSPOT_API_KEY not set');        process.exit(1); }
if (!GOOGLE_SERVICE_ACCOUNT) { console.error('ERROR: GOOGLE_SERVICE_ACCOUNT not set'); process.exit(1); }
if (!GOOGLE_SHEET_ID)        { console.error('ERROR: GOOGLE_SHEET_ID not set');        process.exit(1); }

// ── Constants ─────────────────────────────────────────────────────────────────

const HS_BASE    = 'https://api.hubapi.com';
const HS_HEADERS = { Authorization: `Bearer ${HUBSPOT_API_KEY}`, 'Content-Type': 'application/json' };

// Exact column header names in the Google Sheet
const COL_DEAL_ID    = 'HubSpot Deal ID';
const COL_CLIENT_FEE = 'Normalized+Discount Current Client Fee (Output)';
const COL_SALARY     = 'Current Candidate Salary (Output)';

// HubSpot deal properties to update
const PROP_CLIENT_FEE = 'amount_charged';
const PROP_SALARY     = 'salary_to_pay';

const MAX_RETRIES = 3;
const BATCH_SIZE  = 50;

// Test mode — leave empty to process all deals in the sheet
const TEST_DEAL_IDS = ['15112278621', '15777186084', '16076238937'];

// ── Utilities ─────────────────────────────────────────────────────────────────

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function withRetry(fn, label) {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (attempt === MAX_RETRIES) throw err;
      const isRateLimit = err.response?.status === 429;
      const wait = isRateLimit ? 10000 : attempt * 1000;
      console.warn(`  [${label}] attempt ${attempt} failed — retrying in ${wait}ms…`);
      await sleep(wait);
    }
  }
}

function parseNumber(raw) {
  if (raw === null || raw === undefined || raw === '') return null;
  const cleaned = String(raw).replace(/[$,\s]/g, '');
  const n = parseFloat(cleaned);
  return isNaN(n) ? null : n;
}

// ── Google Sheets ─────────────────────────────────────────────────────────────

async function fetchSheetRows() {
  const credentials = JSON.parse(GOOGLE_SERVICE_ACCOUNT);
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });

  const sheets = google.sheets({ version: 'v4', auth });
  const res    = await sheets.spreadsheets.values.get({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: SHEET_TAB,
  });

  return res.data.values || [];
}

function parseRows(rows) {
  if (rows.length < 2) return [];

  const headers   = rows[0].map(h => h.trim());
  const dealIdIdx = headers.indexOf(COL_DEAL_ID);
  const clientIdx = headers.indexOf(COL_CLIENT_FEE);
  const salaryIdx = headers.indexOf(COL_SALARY);

  if (dealIdIdx === -1) throw new Error(`Column "${COL_DEAL_ID}" not found in sheet`);
  if (clientIdx === -1) throw new Error(`Column "${COL_CLIENT_FEE}" not found in sheet`);
  if (salaryIdx === -1) throw new Error(`Column "${COL_SALARY}" not found in sheet`);

  const records = [];
  for (let i = 1; i < rows.length; i++) {
    const row    = rows[i];
    const dealId = row[dealIdIdx]?.trim();
    if (!dealId) continue;

    records.push({
      dealId,
      clientFee: parseNumber(row[clientIdx]),
      salary:    parseNumber(row[salaryIdx]),
    });
  }

  return records;
}

// ── HubSpot ───────────────────────────────────────────────────────────────────

async function updateDeals(records) {
  let updated = 0, skipped = 0, errors = 0;

  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);

    const results = await Promise.allSettled(
      batch.map(async ({ dealId, clientFee, salary }) => {
        if (clientFee === null && salary === null) {
          console.log(`  [deal ${dealId}] no values in sheet — skipping`);
          return false;
        }

        const properties = {};
        if (clientFee !== null) properties[PROP_CLIENT_FEE] = clientFee;
        if (salary    !== null) properties[PROP_SALARY]     = salary;

        await withRetry(
          () => axios.patch(
            `${HS_BASE}/crm/v3/objects/deals/${dealId}`,
            { properties },
            { headers: HS_HEADERS }
          ),
          `updateDeal(${dealId})`
        );

        console.log(`  [deal ${dealId}] ${PROP_CLIENT_FEE}: ${clientFee}  ${PROP_SALARY}: ${salary}`);
        return true;
      })
    );

    for (const res of results) {
      if (res.status === 'fulfilled') {
        res.value ? updated++ : skipped++;
      } else {
        console.error(`  ERROR: ${res.reason?.response?.data?.message || res.reason?.message}`);
        errors++;
      }
    }

    if (i + BATCH_SIZE < records.length) await sleep(500);
  }

  return { updated, skipped, errors };
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`[${new Date().toISOString()}] Starting deal financial sync`);
  console.log(`Sheet: ${GOOGLE_SHEET_ID}  Tab: ${SHEET_TAB}`);

  console.log('Reading Google Sheet…');
  let rows;
  try {
    rows = await fetchSheetRows();
  } catch (err) {
    console.error('Failed to read sheet:', err.message);
    process.exit(1);
  }

  let records;
  try {
    records = parseRows(rows);
  } catch (err) {
    console.error('Failed to parse sheet:', err.message);
    process.exit(1);
  }

  if (TEST_DEAL_IDS.length > 0) {
    records = records.filter(r => TEST_DEAL_IDS.includes(r.dealId));
    console.log(`TEST MODE — restricting to ${TEST_DEAL_IDS.length} deals: ${TEST_DEAL_IDS.join(', ')}`);
  }

  console.log(`${records.length} deals found in sheet`);

  if (records.length === 0) {
    console.log('Nothing to update.');
    return;
  }

  const { updated, skipped, errors } = await updateDeals(records);
  console.log(`\n[${new Date().toISOString()}] Done.  Updated: ${updated}  Skipped: ${skipped}  Errors: ${errors}`);

  if (errors > 0) process.exit(1);
}

main();
