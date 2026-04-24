require('dotenv').config();
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const API_KEY = process.env.HUBSPOT_API_KEY;
if (!API_KEY) {
  console.error('ERROR: HUBSPOT_API_KEY is not set');
  process.exit(1);
}

const BASE = 'https://api.hubapi.com';
const HEADERS = { Authorization: `Bearer ${API_KEY}`, 'Content-Type': 'application/json' };

// ─── Configure these to match your HubSpot account ───────────────────────────
// Find your pipeline ID: HubSpot → Settings → CRM → Deals → Pipelines,
// then hover the pipeline name and copy the ID from the URL.
const CLIENT_SUCCESS_PIPELINE = 'client_success'; // ← replace with your pipeline ID
const EXCLUDED_STAGES         = ['closedlost', 'buyout']; // deal stages to ignore
const DEAL_PROPERTY           = 'margin__price___salary_';
const COMPANY_PROPERTY        = 'total_mrr';
// ─────────────────────────────────────────────────────────────────────────────

// ─── Test mode ────────────────────────────────────────────────────────────────
// List specific company IDs to process only those companies.
// Clear this array (leave it as []) to run against all companies.
const TEST_COMPANY_IDS = [
  '51643462676',
  '30123644727',
  '25513480478',
];
// ─────────────────────────────────────────────────────────────────────────────

const BATCH_SIZE      = 10;
const BATCH_DELAY_MS  = 500;
const MAX_RETRIES     = 3;
const PROGRESS_FILE   = path.join(__dirname, 'progress.json');

// ── Progress helpers ──────────────────────────────────────────────────────────

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      const data = JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
      return new Set(data.processed || []);
    }
  } catch {
    console.warn('Could not load progress file — starting fresh');
  }
  return new Set();
}

function saveProgress(processed) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({ processed: [...processed] }, null, 2));
}

// ── Retry wrapper ─────────────────────────────────────────────────────────────

async function withRetry(fn, label) {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (attempt === MAX_RETRIES) throw err;
      const wait = attempt * 1000;
      console.warn(`    [${label}] attempt ${attempt} failed — retrying in ${wait}ms…`);
      await sleep(wait);
    }
  }
}

// ── API calls ─────────────────────────────────────────────────────────────────

async function fetchAllCompanies() {
  const companies = [];
  let after;

  while (true) {
    const params = { limit: 100, properties: 'name' };
    if (after) params.after = after;

    const res = await withRetry(
      () => axios.get(`${BASE}/crm/v3/objects/companies`, { headers: HEADERS, params }),
      'fetchAllCompanies'
    );
    companies.push(...res.data.results);

    if (res.data.paging?.next?.after) {
      after = res.data.paging.next.after;
    } else {
      break;
    }
  }

  return companies;
}

async function fetchQualifyingDeals(companyId) {
  const deals = [];
  let after;

  while (true) {
    const params = { limit: 100 };
    if (after) params.after = after;

    const res = await withRetry(
      () => axios.get(
        `${BASE}/crm/v3/objects/companies/${companyId}/associations/deals`,
        { headers: HEADERS, params }
      ),
      `associations(${companyId})`
    );

    const dealIds = res.data.results.map((r) => r.id);

    if (dealIds.length > 0) {
      const batchRes = await withRetry(
        () => axios.post(
          `${BASE}/crm/v3/objects/deals/batch/read`,
          {
            inputs: dealIds.map((id) => ({ id })),
            properties: ['dealstage', 'pipeline', DEAL_PROPERTY],
          },
          { headers: HEADERS }
        ),
        `batchRead(${companyId})`
      );

      const qualifying = batchRes.data.results.filter(
        (d) =>
          d.properties.pipeline === CLIENT_SUCCESS_PIPELINE &&
          !EXCLUDED_STAGES.includes(d.properties.dealstage)
      );
      deals.push(...qualifying);
    }

    if (res.data.paging?.next?.after) {
      after = res.data.paging.next.after;
    } else {
      break;
    }
  }

  return deals;
}

async function updateCompanyMRR(companyId, value) {
  await withRetry(
    () => axios.patch(
      `${BASE}/crm/v3/objects/companies/${companyId}`,
      { properties: { [COMPANY_PROPERTY]: value } },
      { headers: HEADERS }
    ),
    `updateCompany(${companyId})`
  );
}

// ── Utilities ─────────────────────────────────────────────────────────────────

function sumDealField(deals) {
  return deals.reduce((total, deal) => {
    const raw = deal.properties[DEAL_PROPERTY];
    const value = raw !== null && raw !== undefined && raw !== '' ? parseFloat(raw) : 0;
    return total + (isNaN(value) ? 0 : value);
  }, 0);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`[${new Date().toISOString()}] Starting HubSpot MRR sync`);
  console.log(`Pipeline filter: "${CLIENT_SUCCESS_PIPELINE}"  |  Excluding stages: ${EXCLUDED_STAGES.join(', ')}`);
  if (TEST_COMPANY_IDS.length > 0) {
    console.log(`TEST MODE — restricting to ${TEST_COMPANY_IDS.length} companies: ${TEST_COMPANY_IDS.join(', ')}`);
  }

  const processed = loadProgress();
  if (processed.size > 0) {
    console.log(`Resuming — ${processed.size} companies already processed`);
  }

  let companies;
  try {
    companies = await fetchAllCompanies();
  } catch (err) {
    console.error('Failed to fetch companies:', err.response?.data || err.message);
    process.exit(1);
  }

  const pool = TEST_COMPANY_IDS.length > 0
    ? companies.filter((c) => TEST_COMPANY_IDS.includes(c.id))
    : companies;
  const remaining = pool.filter((c) => !processed.has(c.id));
  console.log(`${companies.length} companies total, ${pool.length} in scope, ${remaining.length} remaining to process`);

  let updated = 0;
  let skipped = 0;
  let errors  = 0;

  const totalBatches = Math.ceil(remaining.length / BATCH_SIZE);

  for (let i = 0; i < remaining.length; i += BATCH_SIZE) {
    const batch     = remaining.slice(i, i + BATCH_SIZE);
    const batchNum  = Math.floor(i / BATCH_SIZE) + 1;
    const rangeEnd  = Math.min(i + BATCH_SIZE, remaining.length);

    console.log(`\nBatch ${batchNum}/${totalBatches}  (companies ${i + 1}–${rangeEnd})`);

    for (const company of batch) {
      const companyId   = company.id;
      const companyName = company.properties?.name || companyId;

      try {
        const deals = await fetchQualifyingDeals(companyId);

        if (deals.length === 0) {
          console.log(`  [${companyName}] no qualifying deals — skipping`);
          skipped++;
        } else {
          const total = sumDealField(deals);
          await updateCompanyMRR(companyId, total);
          console.log(`  [${companyName}] ${deals.length} deal(s) → ${COMPANY_PROPERTY} = ${total}`);
          updated++;
        }

        processed.add(companyId);
        saveProgress(processed);
      } catch (err) {
        console.error(
          `  [${companyName}] ERROR after ${MAX_RETRIES} retries:`,
          err.response?.data?.message || err.message
        );
        errors++;
      }
    }

    if (i + BATCH_SIZE < remaining.length) {
      await sleep(BATCH_DELAY_MS);
    }
  }

  console.log(
    `\n[${new Date().toISOString()}] Done.  Updated: ${updated}  Skipped: ${skipped}  Errors: ${errors}`
  );

  if (errors === 0 && fs.existsSync(PROGRESS_FILE)) {
    fs.unlinkSync(PROGRESS_FILE);
    console.log('Progress file cleared.');
  }

  if (errors > 0) process.exit(1);
}

main();
