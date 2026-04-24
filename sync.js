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

// ─── Pipeline 1: Client Success ───────────────────────────────────────────────
const CLIENT_SUCCESS_PIPELINE = '826172857';
const CLIENT_SUCCESS_EXCLUDED = ['1223751309', '1271488125'];

// ─── Pipeline 2: MOF ─────────────────────────────────────────────────────────
const MOF_PIPELINE        = '12344141';
const MOF_INCLUDED_STAGES = ['66160700', '72205400', '72362554'];

// ─── Properties ───────────────────────────────────────────────────────────────
const DEAL_PROPERTY         = 'margin__price___salary_';
const MRR_PROPERTY          = 'total_mrr';
const ACTIVE_DEALS_PROPERTY = 'number_of_active_deals';

// ─── Active company list ──────────────────────────────────────────────────────
const ACTIVE_LIST_ID = '5410';

// ─── Test mode ────────────────────────────────────────────────────────────────
const TEST_COMPANY_IDS = [];

// ─── Performance ──────────────────────────────────────────────────────────────
// Lower concurrency keeps us under HubSpot's 100 requests/10s rate limit.
const CONCURRENCY    = 5;
const BATCH_DELAY_MS = 1000;
const MAX_RETRIES    = 3;
const PROGRESS_FILE  = path.join(__dirname, 'progress.json');

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
// Waits longer when HubSpot returns a rate limit error specifically.

async function withRetry(fn, label) {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (attempt === MAX_RETRIES) throw err;

      const isRateLimit =
        err.response?.status === 429 ||
        err.response?.data?.message?.includes('ten_secondly_rolling') ||
        err.response?.data?.message?.includes('secondly');

      const wait = isRateLimit ? 10000 : attempt * 1000;
      console.warn(`    [${label}] attempt ${attempt} failed — retrying in ${wait / 1000}s… ${isRateLimit ? '(rate limit)' : ''}`);
      await sleep(wait);
    }
  }
}

// ── Deal filter ───────────────────────────────────────────────────────────────

function isQualifyingDeal(deal) {
  const { pipeline, dealstage } = deal.properties;
  if (pipeline === CLIENT_SUCCESS_PIPELINE) return !CLIENT_SUCCESS_EXCLUDED.includes(dealstage);
  if (pipeline === MOF_PIPELINE) return MOF_INCLUDED_STAGES.includes(dealstage);
  return false;
}

// ── API calls ─────────────────────────────────────────────────────────────────

async function fetchActiveCompanies() {
  const companies = [];
  let after;

  while (true) {
    const params = { limit: 100 };
    if (after) params.after = after;

    const res = await withRetry(
      () => axios.get(
        `${BASE}/crm/v3/lists/${ACTIVE_LIST_ID}/memberships`,
        { headers: HEADERS, params }
      ),
      'fetchActiveCompanies'
    );

    const ids = res.data.results.map((r) => r.recordId || r.id);

    if (ids.length > 0) {
      const batchRes = await withRetry(
        () => axios.post(
          `${BASE}/crm/v3/objects/companies/batch/read`,
          { inputs: ids.map((id) => ({ id })), properties: ['name'] },
          { headers: HEADERS }
        ),
        'batchReadCompanies'
      );
      companies.push(...batchRes.data.results);
    }

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
      deals.push(...batchRes.data.results.filter(isQualifyingDeal));
    }

    if (res.data.paging?.next?.after) {
      after = res.data.paging.next.after;
    } else {
      break;
    }
  }

  return deals;
}

async function updateCompany(companyId, mrr, dealCount) {
  await withRetry(
    () => axios.patch(
      `${BASE}/crm/v3/objects/companies/${companyId}`,
      {
        properties: {
          [MRR_PROPERTY]:          mrr,
          [ACTIVE_DEALS_PROPERTY]: dealCount,
        },
      },
      { headers: HEADERS }
    ),
    `updateCompany(${companyId})`
  );
}

// ── Process a single company ──────────────────────────────────────────────────

async function processCompany(company, processed) {
  const companyId   = company.id;
  const companyName = company.properties?.name || companyId;

  try {
    const deals = await fetchQualifyingDeals(companyId);

    if (deals.length === 0) {
      console.log(`  [${companyName}] no qualifying deals — skipping`);
      processed.add(companyId);
      saveProgress(processed);
      return { status: 'skipped' };
    }

    const total = Math.round(
      deals.reduce((sum, deal) => {
        const raw = deal.properties[DEAL_PROPERTY];
        const val = raw !== null && raw !== undefined && raw !== '' ? parseFloat(raw) : 0;
        return sum + (isNaN(val) ? 0 : val);
      }, 0) * 100
    ) / 100;

    await updateCompany(companyId, total, deals.length);
    console.log(`  [${companyName}] ${deals.length} deal(s) → ${MRR_PROPERTY} = ${total}, ${ACTIVE_DEALS_PROPERTY} = ${deals.length}`);

    processed.add(companyId);
    saveProgress(processed);
    return { status: 'updated' };
  } catch (err) {
    console.error(
      `  [${companyName}] ERROR after ${MAX_RETRIES} retries:`,
      err.response?.data?.message || err.message
    );
    return { status: 'error' };
  }
}

// ── Utilities ─────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`[${new Date().toISOString()}] Starting HubSpot MRR sync`);
  console.log(`Active list ID: ${ACTIVE_LIST_ID}`);
  console.log(`Client Success pipeline: ${CLIENT_SUCCESS_PIPELINE}  |  Excluded stages: ${CLIENT_SUCCESS_EXCLUDED.join(', ')}`);
  console.log(`MOF pipeline: ${MOF_PIPELINE}  |  Included stages: ${MOF_INCLUDED_STAGES.join(', ')}`);
  console.log(`Concurrency: ${CONCURRENCY} companies at a time`);
  if (TEST_COMPANY_IDS.length > 0) {
    console.log(`TEST MODE — restricting to ${TEST_COMPANY_IDS.length} companies: ${TEST_COMPANY_IDS.join(', ')}`);
  }

  const processed = loadProgress();
  if (processed.size > 0) {
    console.log(`Resuming — ${processed.size} companies already processed`);
  }

  let companies;
  try {
    companies = await fetchActiveCompanies();
  } catch (err) {
    console.error('Failed to fetch active company list:', err.response?.data || err.message);
    process.exit(1);
  }

  const pool      = TEST_COMPANY_IDS.length > 0
    ? companies.filter((c) => TEST_COMPANY_IDS.includes(c.id))
    : companies;
  const remaining = pool.filter((c) => !processed.has(c.id));

  console.log(`${companies.length} companies in active list, ${remaining.length} remaining to process`);

  let updated = 0;
  let skipped = 0;
  let errors  = 0;

  const totalBatches = Math.ceil(remaining.length / CONCURRENCY);

  for (let i = 0; i < remaining.length; i += CONCURRENCY) {
    const batch    = remaining.slice(i, i + CONCURRENCY);
    const batchNum = Math.floor(i / CONCURRENCY) + 1;
    const rangeEnd = Math.min(i + CONCURRENCY, remaining.length);

    console.log(`\nBatch ${batchNum}/${totalBatches}  (companies ${i + 1}–${rangeEnd})`);

    const results = await Promise.allSettled(
      batch.map((company) => processCompany(company, processed))
    );

    for (const result of results) {
      const value = result.status === 'fulfilled' ? result.value : { status: 'error' };
      if (value.status === 'updated') updated++;
      else if (value.status === 'skipped') skipped++;
      else errors++;
    }

    if (i + CONCURRENCY < remaining.length) {
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
