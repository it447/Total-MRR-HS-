require('dotenv').config();
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const API_KEY = process.env.HUBSPOT_API_KEY;
if (!API_KEY) {
  console.error('ERROR: HUBSPOT_API_KEY is not set');
  process.exit(1);
}

// ─── HubSpot ──────────────────────────────────────────────────────────────────
const BASE         = 'https://api.hubapi.com';
const HEADERS      = { Authorization: `Bearer ${API_KEY}`, 'Content-Type': 'application/json' };
const HS_PORTAL_ID = '22650739';

// ─── Asana ────────────────────────────────────────────────────────────────────
const ASANA_KEY        = process.env.ASANA_API_KEY;
const ASANA_PROJECT_ID = process.env.ASANA_PROJECT_ID || '1214241148472876';
const ASANA_BASE       = 'https://app.asana.com/api/1.0';
const ASANA_HEADERS    = { Authorization: `Bearer ${ASANA_KEY}`, 'Content-Type': 'application/json' };

// ─── Pipeline 1: Client Success ───────────────────────────────────────────────
const CLIENT_SUCCESS_PIPELINE = '826172857';
const CLIENT_SUCCESS_EXCLUDED = ['1223751309', '1271488125'];

// ─── Pipeline 2: MOF ─────────────────────────────────────────────────────────
const MOF_PIPELINE        = '12344141';
const MOF_INCLUDED_STAGES = ['66160700', '72205400', '72362554'];

// ─── HubSpot properties ───────────────────────────────────────────────────────
const DEAL_PROPERTY         = 'margin__price___salary_';
const MRR_PROPERTY          = 'total_mrr';
const ACTIVE_DEALS_PROPERTY = 'number_of_active_deals';

// ─── Asana field name → HubSpot property ─────────────────────────────────────
const ASANA_FIELD_MAP = {
  'MRR':            'total_mrr',
  'Active Hires':   'number_of_active_deals',
  'Pod':            'pod',
  'Company Domain': 'domain',
};

// ─── Active company list ──────────────────────────────────────────────────────
const ACTIVE_LIST_ID = '5410';

// ─── Test mode ────────────────────────────────────────────────────────────────
// Add company IDs here to test on specific companies only.
// Leave empty ([]) to run against all companies in the active list.
const TEST_COMPANY_IDS = [];

// ─── Performance ──────────────────────────────────────────────────────────────
const CONCURRENCY       = 5;
const BATCH_DELAY_MS    = 1000;
const ASANA_BATCH_SIZE  = 5;
const ASANA_BATCH_DELAY = 500;
const MAX_RETRIES       = 3;
const PROGRESS_FILE     = path.join(__dirname, 'progress.json');

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

      const isRateLimit =
        err.response?.status === 429 ||
        err.response?.data?.message?.includes('ten_secondly_rolling') ||
        err.response?.data?.message?.includes('secondly');

      const wait = isRateLimit ? 10000 : attempt * 1000;
      console.warn(`    [${label}] attempt ${attempt} failed — retrying in ${wait / 1000}s…${isRateLimit ? ' (rate limit)' : ''}`);
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

// ═══════════════════════════════════════════════════════════════════════════════
// HUBSPOT API CALLS
// ═══════════════════════════════════════════════════════════════════════════════

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

async function fetchCompanyDetails(companies) {
  const details = [];
  const ids = companies.map((c) => c.id);

  for (let i = 0; i < ids.length; i += 100) {
    const chunk = ids.slice(i, i + 100);
    const res = await withRetry(
      () => axios.post(
        `${BASE}/crm/v3/objects/companies/batch/read`,
        {
          inputs: chunk.map((id) => ({ id })),
          properties: [
            'name',
            'hs_object_id',
            'total_mrr',
            'number_of_active_deals',
            'pod',
            'domain',
            'hubspot_owner_id',
          ],
        },
        { headers: HEADERS }
      ),
      'fetchCompanyDetails'
    );
    details.push(...res.data.results);
  }

  return details;
}

async function fetchHubSpotOwners() {
  const owners = {};
  let after;

  while (true) {
    const params = { limit: 100 };
    if (after) params.after = after;

    const res = await withRetry(
      () => axios.get(`${BASE}/crm/v3/owners`, { headers: HEADERS, params }),
      'fetchHubSpotOwners'
    );

    for (const owner of res.data.results || []) {
      if (owner.id && owner.email) owners[owner.id] = owner.email.toLowerCase();
    }

    if (res.data.paging?.next?.after) {
      after = res.data.paging.next.after;
    } else {
      break;
    }
  }

  return owners;
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

      const qualifying = batchRes.data.results.filter(isQualifyingDeal);
      const rejected   = batchRes.data.results.filter((d) => !isQualifyingDeal(d));

      // Debug: show exactly what the API returns for deals that don't qualify
      rejected.forEach((d) => {
        console.log(`    [DEBUG] deal ${d.id} rejected — pipeline: "${d.properties.pipeline}" stage: "${d.properties.dealstage}"`);
      });

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

// ═══════════════════════════════════════════════════════════════════════════════
// ASANA API CALLS
// ═══════════════════════════════════════════════════════════════════════════════

async function fetchAsanaWorkspaceGid() {
  const res = await withRetry(
    () => axios.get(
      `${ASANA_BASE}/projects/${ASANA_PROJECT_ID}`,
      { headers: ASANA_HEADERS, params: { opt_fields: 'workspace.gid' } }
    ),
    'fetchAsanaWorkspace'
  );
  return res.data.data.workspace.gid;
}

async function fetchAsanaUsers(workspaceGid) {
  const users = {};
  let offset;

  while (true) {
    const params = { limit: 100, opt_fields: 'gid,email' };
    if (offset) params.offset = offset;

    const res = await withRetry(
      () => axios.get(
        `${ASANA_BASE}/workspaces/${workspaceGid}/users`,
        { headers: ASANA_HEADERS, params }
      ),
      'fetchAsanaUsers'
    );

    for (const user of res.data.data || []) {
      if (user.email) users[user.email.toLowerCase()] = user.gid;
    }

    if (res.data.next_page?.offset) {
      offset = res.data.next_page.offset;
    } else {
      break;
    }
  }

  return users;
}

async function fetchAsanaCustomFields() {
  const res = await withRetry(
    () => axios.get(
      `${ASANA_BASE}/projects/${ASANA_PROJECT_ID}`,
      {
        headers: ASANA_HEADERS,
        params: {
          opt_fields: [
            'custom_field_settings.custom_field.gid',
            'custom_field_settings.custom_field.name',
            'custom_field_settings.custom_field.type',
            'custom_field_settings.custom_field.enum_options.gid',
            'custom_field_settings.custom_field.enum_options.name',
          ].join(','),
        },
      }
    ),
    'fetchAsanaCustomFields'
  );

  const fieldMap = {};
  for (const setting of res.data.data.custom_field_settings || []) {
    const f = setting.custom_field;
    fieldMap[f.name] = f;
  }
  return fieldMap;
}

async function fetchAllAsanaTasks() {
  const taskMap = {};
  let offset;

  while (true) {
    const params = { project: ASANA_PROJECT_ID, limit: 100, opt_fields: 'gid,name' };
    if (offset) params.offset = offset;

    const res = await withRetry(
      () => axios.get(`${ASANA_BASE}/tasks`, { headers: ASANA_HEADERS, params }),
      'fetchAllAsanaTasks'
    );

    for (const task of res.data.data || []) {
      taskMap[task.name] = task.gid;
    }

    if (res.data.next_page?.offset) {
      offset = res.data.next_page.offset;
    } else {
      break;
    }
  }

  return taskMap;
}

function buildAsanaCustomFields(company, asanaFields) {
  const result = {};

  for (const [fieldName, hubspotProp] of Object.entries(ASANA_FIELD_MAP)) {
    const field = asanaFields[fieldName];
    if (!field) {
      console.warn(`    Asana field "${fieldName}" not found in project — skipping`);
      continue;
    }

    const raw = company.properties[hubspotProp];

    if (field.type === 'number') {
      const num = parseFloat(raw);
      result[field.gid] = isNaN(num) ? 0 : num;
    } else if (field.type === 'enum') {
      const match = (field.enum_options || []).find(
        (o) => o.name.toLowerCase() === String(raw || '').toLowerCase()
      );
      if (match) result[field.gid] = match.gid;
    } else {
      result[field.gid] = raw || null;
    }
  }

  // HubSpot URL is computed from company ID
  const urlField = asanaFields['Hubspot URL'];
  if (urlField) {
    result[urlField.gid] = `https://app.hubspot.com/contacts/${HS_PORTAL_ID}/company/${company.id}`;
  }

  return result;
}

async function syncCompanyToAsana(company, asanaFields, taskMap, hubspotOwners, asanaUsers) {
  const companyId   = company.id;
  const companyName = company.properties?.name || companyId;
  const taskName    = `${companyName} - ${companyId}`;
  const ownerId     = company.properties?.hubspot_owner_id;
  const ownerEmail  = ownerId ? hubspotOwners[ownerId] : null;
  const assigneeGid = ownerEmail ? asanaUsers[ownerEmail] : null;

  const customFields = buildAsanaCustomFields(company, asanaFields);
  const taskData     = { custom_fields: customFields };
  if (assigneeGid) taskData.assignee = assigneeGid;

  const existingEntry = Object.entries(taskMap).find(([name]) =>
    name.endsWith(`- ${companyId}`)
  );

  if (existingEntry) {
    const [, taskGid] = existingEntry;
    await withRetry(
      () => axios.put(
        `${ASANA_BASE}/tasks/${taskGid}`,
        { data: taskData },
        { headers: ASANA_HEADERS }
      ),
      `updateAsanaTask(${companyId})`
    );
    console.log(`  [${companyName}] Asana updated${assigneeGid ? ` → ${ownerEmail}` : ' (no assignee match)'}`);
    return 'updated';
  } else {
    await withRetry(
      () => axios.post(
        `${ASANA_BASE}/tasks`,
        { data: { name: taskName, projects: [ASANA_PROJECT_ID], ...taskData } },
        { headers: ASANA_HEADERS }
      ),
      `createAsanaTask(${companyId})`
    );
    console.log(`  [${companyName}] Asana created${assigneeGid ? ` → ${ownerEmail}` : ' (no assignee match)'}`);
    return 'created';
  }
}

async function syncToAsana(companies) {
  if (!ASANA_KEY) {
    console.warn('\nASANA_API_KEY not set — skipping Asana sync');
    return;
  }

  console.log(`\n[${new Date().toISOString()}] Starting Asana sync`);

  let companyDetails, asanaFields, taskMap, hubspotOwners, asanaUsers;

  try {
    console.log('Fetching updated company details from HubSpot…');
    companyDetails = await fetchCompanyDetails(companies);

    console.log('Fetching HubSpot owners…');
    hubspotOwners = await fetchHubSpotOwners();
    console.log(`${Object.keys(hubspotOwners).length} owners found`);

    console.log('Fetching Asana workspace…');
    const workspaceGid = await fetchAsanaWorkspaceGid();

    console.log('Fetching Asana users…');
    asanaUsers = await fetchAsanaUsers(workspaceGid);
    console.log(`${Object.keys(asanaUsers).length} Asana users found`);

    console.log('Fetching Asana custom field definitions…');
    asanaFields = await fetchAsanaCustomFields();
    console.log(`Custom fields found: ${Object.keys(asanaFields).join(', ')}`);

    console.log('Fetching existing Asana tasks…');
    taskMap = await fetchAllAsanaTasks();
    console.log(`${Object.keys(taskMap).length} existing tasks found`);
  } catch (err) {
    console.error('Asana setup failed:', err.response?.data || err.message);
    return;
  }

  let created = 0;
  let updated = 0;
  let errors  = 0;

  const totalBatches = Math.ceil(companyDetails.length / ASANA_BATCH_SIZE);

  for (let i = 0; i < companyDetails.length; i += ASANA_BATCH_SIZE) {
    const batch    = companyDetails.slice(i, i + ASANA_BATCH_SIZE);
    const batchNum = Math.floor(i / ASANA_BATCH_SIZE) + 1;
    const rangeEnd = Math.min(i + ASANA_BATCH_SIZE, companyDetails.length);

    console.log(`\nAsana batch ${batchNum}/${totalBatches}  (companies ${i + 1}–${rangeEnd})`);

    for (const company of batch) {
      try {
        const result = await syncCompanyToAsana(company, asanaFields, taskMap, hubspotOwners, asanaUsers);
        if (result === 'created') created++;
        else updated++;
      } catch (err) {
        console.error(
          `  [${company.properties?.name || company.id}] Asana ERROR:`,
          err.response?.data?.errors?.[0]?.message || err.message
        );
        errors++;
      }
    }

    if (i + ASANA_BATCH_SIZE < companyDetails.length) {
      await sleep(ASANA_BATCH_DELAY);
    }
  }

  console.log(
    `\n[${new Date().toISOString()}] Asana sync done.  Created: ${created}  Updated: ${updated}  Errors: ${errors}`
  );
}

// ═══════════════════════════════════════════════════════════════════════════════
// MRR SYNC
// ═══════════════════════════════════════════════════════════════════════════════

async function processCompany(company, processed) {
  const companyId   = company.id;
  const companyName = company.properties?.name || companyId;

  try {
    const deals = await fetchQualifyingDeals(companyId);

    const total = Math.round(
      deals.reduce((sum, deal) => {
        const raw = deal.properties[DEAL_PROPERTY];
        const val = raw !== null && raw !== undefined && raw !== '' ? parseFloat(raw) : 0;
        return sum + (isNaN(val) ? 0 : val);
      }, 0) * 100
    ) / 100;

    // Always write even if 0 so no company ever has a blank value
    await updateCompany(companyId, total, deals.length);

    if (deals.length === 0) {
      console.log(`  [${companyName}] no qualifying deals — wrote 0`);
    } else {
      console.log(`  [${companyName}] ${deals.length} deal(s) → ${MRR_PROPERTY} = ${total}, ${ACTIVE_DEALS_PROPERTY} = ${deals.length}`);
    }

    processed.add(companyId);
    saveProgress(processed);
    return { status: deals.length === 0 ? 'skipped' : 'updated' };
  } catch (err) {
    console.error(
      `  [${companyName}] ERROR after ${MAX_RETRIES} retries:`,
      err.response?.data?.message || err.message
    );
    return { status: 'error' };
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

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
    `\n[${new Date().toISOString()}] MRR sync done.  Updated: ${updated}  Skipped: ${skipped}  Errors: ${errors}`
  );

  if (errors === 0 && fs.existsSync(PROGRESS_FILE)) {
    fs.unlinkSync(PROGRESS_FILE);
    console.log('Progress file cleared.');
  }

  await syncToAsana(pool);

  if (errors > 0) process.exit(1);
}

main();
