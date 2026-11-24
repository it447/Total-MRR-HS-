require('dotenv').config();
const axios        = require('axios');
const fs           = require('fs');
const path         = require('path');
const { google }   = require('googleapis');

// ── Environment ───────────────────────────────────────────────────────────────

const HUBSPOT_API_KEY = process.env.HUBSPOT_API_KEY;

if (!HUBSPOT_API_KEY) { console.error('ERROR: HUBSPOT_API_KEY is not set'); process.exit(1); }

// ── HubSpot constants ─────────────────────────────────────────────────────────

const HS_BASE    = 'https://api.hubapi.com';
const HS_HEADERS = { Authorization: `Bearer ${HUBSPOT_API_KEY}`, 'Content-Type': 'application/json' };

const CLIENT_SUCCESS_PIPELINE = '826172857';
const CLIENT_SUCCESS_EXCLUDED = ['1223751309', '1271488125'];
const MOF_PIPELINE            = '12344141';
const MOF_INCLUDED_STAGES     = ['100553797', '66160700', '72205400', '72362554'];
const MOF_COUNT_STAGES        = ['100553797', '206636190', '266814820', '158677007', '49137444', '41905479', '50302346', '41905480', '72187285', '266767789', '266842935', '72198959', '36059460'];

const DEAL_PROPERTY          = 'margin__price___salary_';
const MRR_PROPERTY           = 'total_mrr';
const ACTIVE_DEALS_PROPERTY  = 'number_of_active_deals';
const MOF_DEALS_PROPERTY     = 'number_of_deals_in_mof';

const HS_PORTAL_ID   = '22650739';

const DRIVE_PARENT_FOLDER_ID = '1Zvl6h5QhlbcjBn3RoEamjFWqXDnjQ23Z';

// ── Run config ────────────────────────────────────────────────────────────────

// Leave empty to run all companies in the active list.
const TEST_COMPANY_IDS = [];

const CONCURRENCY       = 5;
const BATCH_DELAY_MS    = 1000;
const MAX_RETRIES       = 3;
const PROGRESS_FILE     = path.join(__dirname, 'progress.json');

// ── Utilities ─────────────────────────────────────────────────────────────────

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      const data = JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
      return new Set(data.processed || []);
    }
  } catch { console.warn('Could not load progress file — starting fresh'); }
  return new Set();
}

function saveProgress(processed) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({ processed: [...processed] }, null, 2));
}

async function withRetry(fn, label) {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (attempt === MAX_RETRIES) throw err;
      const isRateLimit = err.response?.status === 429;
      const wait = isRateLimit ? 10000 : attempt * 1000;
      console.warn(`  [${label}] attempt ${attempt} failed (${isRateLimit ? 'rate limit' : (err.response?.status || err.message)}) — retrying in ${wait}ms…`);
      await sleep(wait);
    }
  }
}

// ── Deal filtering ────────────────────────────────────────────────────────────

function isQualifyingDeal(d) {
  const pipeline = d.properties.pipeline;
  const stage    = d.properties.dealstage;
  if (pipeline === CLIENT_SUCCESS_PIPELINE) return !CLIENT_SUCCESS_EXCLUDED.includes(stage);
  if (pipeline === MOF_PIPELINE)            return MOF_INCLUDED_STAGES.includes(stage);
  return false;
}

function isMofCountDeal(d) {
  return d.properties.pipeline === MOF_PIPELINE && MOF_COUNT_STAGES.includes(d.properties.dealstage);
}

// ── HubSpot API ───────────────────────────────────────────────────────────────

async function fetchActiveCompanyIds() {
  const companyIds = new Set();

  // All stage IDs that qualify a company for syncing across both pipelines
  const allMofStages = [...new Set([...MOF_INCLUDED_STAGES, ...MOF_COUNT_STAGES])];

  const searchBodies = [
    {
      filterGroups: [{
        filters: [
          { propertyName: 'pipeline', operator: 'EQ', value: CLIENT_SUCCESS_PIPELINE },
          { propertyName: 'dealstage', operator: 'NOT_IN', values: CLIENT_SUCCESS_EXCLUDED },
        ],
      }],
      properties: ['hs_object_id'],
      limit: 100,
    },
    {
      filterGroups: [{
        filters: [
          { propertyName: 'pipeline', operator: 'EQ', value: MOF_PIPELINE },
          { propertyName: 'dealstage', operator: 'IN', values: allMofStages },
        ],
      }],
      properties: ['hs_object_id'],
      limit: 100,
    },
  ];

  for (const baseBody of searchBodies) {
    let after;
    while (true) {
      const body = { ...baseBody, ...(after ? { after } : {}) };
      const res = await withRetry(
        () => axios.post(`${HS_BASE}/crm/v3/objects/deals/search`, body, { headers: HS_HEADERS }),
        'searchDeals'
      );

      const dealIds = (res.data.results || []).map(d => d.id);

      if (dealIds.length > 0) {
        const assocRes = await withRetry(
          () => axios.post(
            `${HS_BASE}/crm/v4/associations/deals/companies/batch/read`,
            { inputs: dealIds.map(id => ({ id })) },
            { headers: HS_HEADERS }
          ),
          'fetchDealCompanyAssoc'
        );
        for (const result of assocRes.data.results || []) {
          for (const assoc of result.to || []) {
            companyIds.add(String(assoc.toObjectId));
          }
        }
      }

      if (res.data.paging?.next?.after) {
        after = res.data.paging.next.after;
      } else {
        break;
      }
    }
  }

  return [...companyIds];
}

async function fetchCompanyDetails(ids) {
  const companies = [];
  const CHUNK = 100;

  for (let i = 0; i < ids.length; i += CHUNK) {
    const chunk = ids.slice(i, i + CHUNK);
    const res = await withRetry(
      () => axios.post(
        `${HS_BASE}/crm/v3/objects/companies/batch/read`,
        {
          inputs: chunk.map(id => ({ id })),
          properties: ['name', 'hs_object_id', MRR_PROPERTY, ACTIVE_DEALS_PROPERTY, MOF_DEALS_PROPERTY, 'pod', 'domain', 'hubspot_owner_id'],
        },
        { headers: HS_HEADERS }
      ),
      'fetchCompanyDetails'
    );
    companies.push(...res.data.results);
  }

  return companies;
}

async function fetchQualifyingDeals(companyId, stageNameMap) {
  // Use v4 associations API — returns primary AND non-primary company-deal links
  const dealIds = new Set();
  let after;

  while (true) {
    const params = { limit: 500 };
    if (after) params.after = after;

    const res = await withRetry(
      () => axios.get(
        `${HS_BASE}/crm/v4/objects/companies/${companyId}/associations/deals`,
        { headers: HS_HEADERS, params }
      ),
      `associations(${companyId})`
    );

    for (const r of res.data.results || []) {
      dealIds.add(String(r.toObjectId));
    }

    if (res.data.paging?.next?.after) {
      after = res.data.paging.next.after;
    } else {
      break;
    }
  }

  if (dealIds.size === 0) return { qualifying: [], mofCount: 0, stageNames: [] };

  // Batch-read deal properties in chunks of 100
  const qualifying = [];
  let mofCount     = 0;
  const stagesSeen = new Set();
  const ids        = [...dealIds];

  for (let i = 0; i < ids.length; i += 100) {
    const chunk    = ids.slice(i, i + 100);
    const batchRes = await withRetry(
      () => axios.post(
        `${HS_BASE}/crm/v3/objects/deals/batch/read`,
        {
          inputs: chunk.map(id => ({ id })),
          properties: ['dealstage', 'pipeline', DEAL_PROPERTY],
        },
        { headers: HS_HEADERS }
      ),
      `batchRead(${companyId})`
    );

    const allDeals = batchRes.data.results;
    allDeals.forEach(d => {
      const passes = isQualifyingDeal(d);
      if (TEST_COMPANY_IDS.length > 0) {
        console.log(`    [deal ${d.id}] pipeline:"${d.properties.pipeline}" stage:"${d.properties.dealstage}" value:${d.properties[DEAL_PROPERTY]} → ${passes ? 'QUALIFIES' : 'rejected'}`);
      }
      if (isMofCountDeal(d) || passes) {
        const stageName = stageNameMap?.get(d.properties.dealstage) || d.properties.dealstage;
        stagesSeen.add(stageName);
      }
      if (isMofCountDeal(d)) mofCount++;
    });
    qualifying.push(...allDeals.filter(isQualifyingDeal));
  }

  return { qualifying, mofCount, stageNames: [...stagesSeen] };
}

async function fetchDealStageNames() {
  const stageMap = new Map();

  for (const pipelineId of [CLIENT_SUCCESS_PIPELINE, MOF_PIPELINE]) {
    const res = await withRetry(
      () => axios.get(
        `${HS_BASE}/crm/v3/pipelines/deals/${pipelineId}/stages`,
        { headers: HS_HEADERS }
      ),
      `fetchStages(${pipelineId})`
    );
    for (const stage of res.data.results || []) {
      stageMap.set(stage.id, stage.label);
    }
  }

  return stageMap;
}

async function updateCompany(companyId, mrr, activeDeals, mofDeals) {
  await withRetry(
    () => axios.patch(
      `${HS_BASE}/crm/v3/objects/companies/${companyId}`,
      { properties: { [MRR_PROPERTY]: mrr, [ACTIVE_DEALS_PROPERTY]: activeDeals, [MOF_DEALS_PROPERTY]: mofDeals } },
      { headers: HS_HEADERS }
    ),
    `updateCompany(${companyId})`
  );
}

// ── Google Drive ──────────────────────────────────────────────────────────────

async function getDriveClient() {
  const credentials = JSON.parse(process.env.GOOGLE_DRIVE_SERVICE_ACCOUNT);
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/drive'],
  });
  return google.drive({ version: 'v3', auth });
}

async function getOrCreateDriveFolder(drive, folderName) {
  const safeName = folderName.replace(/'/g, "\\'");
  const res = await withRetry(
    () => drive.files.list({
      q: `name='${safeName}' and '${DRIVE_PARENT_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`,
      fields: 'files(id,webViewLink)',
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
    }),
    `findFolder(${folderName})`
  );

  if (res.data.files.length > 0) {
    return res.data.files[0].webViewLink;
  }

  const folder = await withRetry(
    () => drive.files.create({
      requestBody: {
        name: folderName,
        mimeType: 'application/vnd.google-apps.folder',
        parents: [DRIVE_PARENT_FOLDER_ID],
      },
      fields: 'id,webViewLink',
      supportsAllDrives: true,
    }),
    `createFolder(${folderName})`
  );

  return folder.data.webViewLink;
}

// ── MRR sync ──────────────────────────────────────────────────────────────────

async function runMRRSync(companies) {
  const processed = loadProgress();
  if (processed.size > 0) console.log(`Resuming — ${processed.size} companies already processed`);

  const remaining = companies.filter(c => !processed.has(c.id));
  console.log(`${companies.length} companies in scope, ${remaining.length} remaining to process`);

  console.log('Fetching deal stage names from HubSpot…');
  const stageNameMap = await fetchDealStageNames();
  console.log(`${stageNameMap.size} deal stages loaded`);

  let updated = 0, skipped = 0, errors = 0;
  const totalBatches = Math.ceil(remaining.length / CONCURRENCY);

  for (let i = 0; i < remaining.length; i += CONCURRENCY) {
    const batch    = remaining.slice(i, i + CONCURRENCY);
    const batchNum = Math.floor(i / CONCURRENCY) + 1;
    const rangeEnd = Math.min(i + CONCURRENCY, remaining.length);
    console.log(`\nBatch ${batchNum}/${totalBatches}  (companies ${i + 1}–${rangeEnd})`);

    const results = await Promise.allSettled(
      batch.map(async company => {
        const { qualifying, mofCount, stageNames } = await fetchQualifyingDeals(company.id, stageNameMap);
        const total = qualifying.reduce((sum, d) => {
          const raw = d.properties[DEAL_PROPERTY];
          const v   = (raw !== null && raw !== undefined && raw !== '') ? parseFloat(raw) : 0;
          return sum + (isNaN(v) ? 0 : v);
        }, 0);

        // Always write even if 0 so no company ever has a blank value
        await updateCompany(company.id, total, qualifying.length, mofCount);

        const name = company.properties?.name || company.id;
        if (qualifying.length === 0) {
          console.log(`  [${name} | id:${company.id}] no qualifying deals — wrote 0  MOF count = ${mofCount}`);
        } else {
          console.log(`  [${name} | id:${company.id}] ${qualifying.length} deal(s) → ${MRR_PROPERTY} = ${total}  MOF count = ${mofCount}  stages: ${stageNames.join(', ')}`);
        }
        return { hasDeals: qualifying.length > 0, stageNames };
      })
    );

    for (let j = 0; j < results.length; j++) {
      const res     = results[j];
      const company = batch[j];
      if (res.status === 'fulfilled') {
        res.value.hasDeals ? updated++ : skipped++;
        company._stageNames = res.value.stageNames;
        processed.add(company.id);
        saveProgress(processed);
      } else {
        const name = company.properties?.name || company.id;
        console.error(`  [${name}] ERROR: ${res.reason?.response?.data?.message || res.reason?.message}`);
        errors++;
      }
    }

    if (i + CONCURRENCY < remaining.length) await sleep(BATCH_DELAY_MS);
  }

  console.log(`\n[${new Date().toISOString()}] MRR sync done.  Updated: ${updated}  Skipped: ${skipped}  Errors: ${errors}`);

  if (errors === 0 && fs.existsSync(PROGRESS_FILE)) {
    fs.unlinkSync(PROGRESS_FILE);
    console.log('Progress file cleared.');
  }

  return errors;
}

// ── Google Drive sync ─────────────────────────────────────────────────────────

// Ensures every active company has a "{Company Name} - {ID}" folder in Drive.
async function runDriveSync(companies) {
  if (!process.env.GOOGLE_DRIVE_SERVICE_ACCOUNT) {
    console.log('GOOGLE_DRIVE_SERVICE_ACCOUNT not set — skipping Drive folder sync');
    return 0;
  }

  console.log(`\n[${new Date().toISOString()}] Starting Drive folder sync`);

  let drive;
  try {
    drive = await getDriveClient();
  } catch (err) {
    console.warn('Could not initialise Drive client — skipping folder sync:', err.message);
    return 0;
  }

  let errors = 0;
  for (const company of companies) {
    const companyName = company.properties?.name || company.id;
    const folderName  = `${companyName} - ${company.id}`;
    try {
      await getOrCreateDriveFolder(drive, folderName);
    } catch (err) {
      console.warn(`  [${companyName}] Drive folder warning: ${err.message}`);
      errors++;
    }
  }

  console.log(`[${new Date().toISOString()}] Drive folder sync done.  Warnings: ${errors}`);
  return 0; // Drive issues are non-fatal
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`[${new Date().toISOString()}] Starting HubSpot MRR sync`);
  console.log(`Client Success pipeline: ${CLIENT_SUCCESS_PIPELINE}  |  Excluded stages: ${CLIENT_SUCCESS_EXCLUDED.join(', ')}`);
  console.log(`MOF pipeline: ${MOF_PIPELINE}  |  Included stages: ${MOF_INCLUDED_STAGES.join(', ')}`);
  console.log(`Concurrency: ${CONCURRENCY} companies at a time`);

  if (TEST_COMPANY_IDS.length > 0) {
    console.log(`TEST MODE — restricting to ${TEST_COMPANY_IDS.length} companies: ${TEST_COMPANY_IDS.join(', ')}`);
  }

  let companyIds;
  try {
    companyIds = await fetchActiveCompanyIds();
  } catch (err) {
    console.error('Failed to fetch active companies:', err.response?.data || err.message);
    process.exit(1);
  }

  if (TEST_COMPANY_IDS.length > 0) {
    companyIds = companyIds.filter(id => TEST_COMPANY_IDS.includes(id));
  }

  console.log(`${companyIds.length} companies in active list`);

  let companies;
  try {
    companies = await fetchCompanyDetails(companyIds);
  } catch (err) {
    console.error('Failed to fetch company details:', err.response?.data || err.message);
    process.exit(1);
  }

  const mrrErrors = await runMRRSync(companies);
  await runDriveSync(companies);

  if (mrrErrors > 0) process.exit(1);
}

main();
