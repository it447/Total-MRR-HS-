require('dotenv').config();
const axios        = require('axios');
const fs           = require('fs');
const path         = require('path');
const { google }   = require('googleapis');

// ── Environment ───────────────────────────────────────────────────────────────

const HUBSPOT_API_KEY = process.env.HUBSPOT_API_KEY;
const ASANA_API_KEY   = process.env.ASANA_API_KEY;

if (!HUBSPOT_API_KEY) { console.error('ERROR: HUBSPOT_API_KEY is not set'); process.exit(1); }

// ── HubSpot constants ─────────────────────────────────────────────────────────

const HS_BASE    = 'https://api.hubapi.com';
const HS_HEADERS = { Authorization: `Bearer ${HUBSPOT_API_KEY}`, 'Content-Type': 'application/json' };

const CLIENT_SUCCESS_PIPELINE = '826172857';
const CLIENT_SUCCESS_EXCLUDED = ['1223751309', '1271488125'];
const MOF_PIPELINE            = '12344141';
const MOF_INCLUDED_STAGES     = ['66160700', '72205400', '72362554'];
const MOF_COUNT_STAGES        = ['100553797', '206636190', '266814820', '158677007', '49137444', '41905479', '50302346', '41905480', '72187285', '266767789', '266842935', '72198959', '36059460'];

const DEAL_PROPERTY          = 'margin__price___salary_';
const MRR_PROPERTY           = 'total_mrr';
const ACTIVE_DEALS_PROPERTY  = 'number_of_active_deals';
const MOF_DEALS_PROPERTY     = 'number_of_deals_in_mof';

const ACTIVE_LIST_ID = '5410';
const HS_PORTAL_ID   = '22650739';

const DRIVE_PARENT_FOLDER_ID = '1Zvl6h5QhlbcjBn3RoEamjFWqXDnjQ23Z';

// ── Asana constants ───────────────────────────────────────────────────────────

const AS_BASE      = 'https://app.asana.com/api/1.0';
const AS_HEADERS   = { Authorization: `Bearer ${ASANA_API_KEY}`, 'Content-Type': 'application/json' };
const ASANA_PROJECT_ID   = process.env.ASANA_PROJECT_ID || '1214241148472876';
const TICKETS_PROJECT_ID = '1214392758833108';

// ── Run config ────────────────────────────────────────────────────────────────

// Leave empty to run all companies in the active list.
const TEST_COMPANY_IDS = [];

const CONCURRENCY       = 5;
const BATCH_DELAY_MS    = 1000;
const ASANA_BATCH_SIZE  = 5;
const ASANA_BATCH_DELAY = 500;
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
  const ids = [];
  let after;

  while (true) {
    const params = { limit: 100 };
    if (after) params.after = after;

    const res = await withRetry(
      () => axios.get(`${HS_BASE}/crm/v3/lists/${ACTIVE_LIST_ID}/memberships`, { headers: HS_HEADERS, params }),
      'fetchActiveCompanyIds'
    );

    for (const m of res.data.results || []) {
      ids.push(m.recordId ?? m.id);
    }

    if (res.data.paging?.next?.after) {
      after = res.data.paging.next.after;
    } else {
      break;
    }
  }

  return [...new Set(ids)];
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

async function fetchHubSpotOwners() {
  const owners = new Map();
  let after;

  while (true) {
    const params = { limit: 100 };
    if (after) params.after = after;

    const res = await withRetry(
      () => axios.get(`${HS_BASE}/crm/v3/owners`, { headers: HS_HEADERS, params }),
      'fetchOwners'
    );

    for (const o of res.data.results || []) {
      if (o.email) owners.set(String(o.id), o.email.toLowerCase());
    }

    if (res.data.paging?.next?.after) {
      after = res.data.paging.next.after;
    } else {
      break;
    }
  }

  return owners;
}

// ── Asana API ─────────────────────────────────────────────────────────────────

async function attachDriveFolderToTask(taskGid, folderUrl, folderName) {
  const existing = await withRetry(
    () => axios.get(`${AS_BASE}/attachments`, { headers: AS_HEADERS, params: { parent: taskGid, opt_fields: 'gid,name,external.url' } }),
    `fetchAttachments(${taskGid})`
  );
  const already = (existing.data.data || []).some(a => a.name === folderName || (a.external?.url || '').includes('drive.google.com'));
  if (already) return;

  await withRetry(
    () => axios.post(`${AS_BASE}/attachments`, {
      data: { resource_subtype: 'external', parent: taskGid, name: folderName, url: folderUrl }
    }, { headers: AS_HEADERS }),
    `attachDrive(${taskGid})`
  );
}

async function fetchAsanaWorkspaceGid() {
  const res = await withRetry(
    () => axios.get(`${AS_BASE}/workspaces`, { headers: AS_HEADERS }),
    'fetchWorkspace'
  );
  return res.data.data[0]?.gid;
}

async function fetchAsanaUsers(workspaceGid) {
  const users = new Map();
  let offset;

  while (true) {
    const params = { opt_fields: 'gid,email', limit: 100 };
    if (offset) params.offset = offset;

    const res = await withRetry(
      () => axios.get(`${AS_BASE}/workspaces/${workspaceGid}/users`, { headers: AS_HEADERS, params }),
      'fetchAsanaUsers'
    );

    for (const u of res.data.data || []) {
      if (u.email) users.set(u.email.toLowerCase(), u.gid);
    }

    if (res.data.next_page?.offset) {
      offset = res.data.next_page.offset;
    } else {
      break;
    }
  }

  return users;
}

async function fetchAsanaCustomFields(projectGid) {
  const res = await withRetry(
    () => axios.get(
      `${AS_BASE}/projects/${projectGid}/custom_field_settings`,
      {
        headers: AS_HEADERS,
        params: { opt_fields: 'custom_field.gid,custom_field.name,custom_field.resource_subtype,custom_field.enum_options' },
      }
    ),
    'fetchCustomFields'
  );

  const fields = {};
  for (const s of res.data.data || []) {
    const cf = s.custom_field;
    fields[cf.name] = { gid: cf.gid, type: cf.resource_subtype, enumOptions: cf.enum_options || [] };
  }
  return fields;
}

async function fetchAllAsanaTasks(projectGid) {
  const tasks = new Map();
  let offset;

  while (true) {
    const params = { project: projectGid, opt_fields: 'gid,name', limit: 100 };
    if (offset) params.offset = offset;

    const res = await withRetry(
      () => axios.get(`${AS_BASE}/tasks`, { headers: AS_HEADERS, params }),
      'fetchAsanaTasks'
    );

    for (const t of res.data.data || []) {
      tasks.set(t.name, t.gid);
    }

    if (res.data.next_page?.offset) {
      offset = res.data.next_page.offset;
    } else {
      break;
    }
  }

  return tasks;
}

function buildAsanaCustomFields(company, customFieldDefs) {
  const fields = {};
  const props  = company.properties;

  for (const [fieldName, def] of Object.entries(customFieldDefs)) {
    const { gid, type, enumOptions } = def;

    if (fieldName === 'MRR') {
      const v = parseFloat(props[MRR_PROPERTY]);
      fields[gid] = isNaN(v) ? 0 : v;
    } else if (fieldName === 'Active Hires') {
      const v = parseInt(props[ACTIVE_DEALS_PROPERTY], 10);
      fields[gid] = isNaN(v) ? 0 : v;
    } else if (fieldName === 'Active Searches') {
      const v = parseInt(props[MOF_DEALS_PROPERTY], 10);
      fields[gid] = isNaN(v) ? 0 : v;
    } else if (fieldName.toLowerCase() === 'pod') {
      const val = props['pod'];
      if (val) {
        if (type === 'enum') {
          const opt = enumOptions.find(o => o.name.toLowerCase() === val.toLowerCase());
          if (opt) fields[gid] = opt.gid;
        } else {
          fields[gid] = val;
        }
      }
    } else if (fieldName === 'Deal Stages') {
      fields[gid] = (company._stageNames || []).join(', ');
    } else if (fieldName.toLowerCase() === 'deal status') {
      const activeHires    = parseInt(props[ACTIVE_DEALS_PROPERTY], 10) || 0;
      const activeSearches = parseInt(props[MOF_DEALS_PROPERTY], 10)    || 0;
      const statusName = activeHires >= 1 ? 'Active Client'
                       : activeSearches >= 1 ? 'Active Search'
                       : 'Lost';
      const opt = enumOptions.find(o => o.name === statusName);
      if (opt) fields[gid] = opt.gid;
    } else if (fieldName === 'Open Tickets') {
      fields[gid] = company._openTicketCount || 0;
    } else if (fieldName === 'Company Domain') {
      if (props['domain']) fields[gid] = props['domain'];
    } else if (fieldName === 'Hubspot URL') {
      fields[gid] = `https://app.hubspot.com/contacts/${HS_PORTAL_ID}/company/${company.id}`;
    }
  }

  return fields;
}

async function fetchOpenTicketCounts() {
  const counts = new Map();
  let offset;

  while (true) {
    const params = {
      project: TICKETS_PROJECT_ID,
      opt_fields: 'gid,custom_fields.name,custom_fields.display_value,custom_fields.enum_value',
      limit: 100,
    };
    if (offset) params.offset = offset;

    const res = await withRetry(
      () => axios.get(`${AS_BASE}/tasks`, { headers: AS_HEADERS, params }),
      'fetchTicketTasks'
    );

    for (const task of res.data.data || []) {
      const cfs             = task.custom_fields || [];
      const clientNameField = cfs.find(cf => cf.name === 'Client Name');
      const statusField     = cfs.find(cf => cf.name === 'Status');

      const clientNameValue = clientNameField?.display_value || '';
      const statusValue     = statusField?.enum_value?.name  || '';

      if (statusValue === 'Resolved') continue;

      // Extract HubSpot company ID from "Company Name - {ID}"
      const match = clientNameValue.match(/-\s*(\d+)\s*$/);
      if (!match) continue;

      const companyId = match[1];
      counts.set(companyId, (counts.get(companyId) || 0) + 1);
    }

    if (res.data.next_page?.offset) {
      offset = res.data.next_page.offset;
    } else {
      break;
    }
  }

  return counts;
}

async function syncCompanyToAsana(company, existingTasks, customFieldDefs, ownerEmailMap, asanaUserMap, projectGid, drive) {
  const companyId   = company.id;
  const companyName = company.properties?.name || companyId;
  const taskName    = `${companyName} - ${companyId}`;

  const customFields = buildAsanaCustomFields(company, customFieldDefs);

  const ownerIdStr  = String(company.properties?.hubspot_owner_id || '');
  const ownerEmail  = ownerEmailMap.get(ownerIdStr);
  const assigneeGid = ownerEmail ? asanaUserMap.get(ownerEmail) : undefined;

  const isInactive = (parseInt(company.properties?.[ACTIVE_DEALS_PROPERTY], 10) || 0) === 0 &&
                     (parseInt(company.properties?.[MOF_DEALS_PROPERTY], 10) || 0) === 0;

  const taskBody = {
    name: taskName,
    custom_fields: customFields,
    completed: isInactive,
    ...(assigneeGid ? { assignee: assigneeGid } : {}),
  };

  let existingGid;
  for (const [name, gid] of existingTasks) {
    if (name.endsWith(`- ${companyId}`)) { existingGid = gid; break; }
  }

  let finalGid = existingGid;
  if (existingGid) {
    await withRetry(
      () => axios.put(`${AS_BASE}/tasks/${existingGid}`, { data: taskBody }, { headers: AS_HEADERS }),
      `updateTask(${companyId})`
    );
  } else {
    const created = await withRetry(
      () => axios.post(`${AS_BASE}/tasks`, { data: { ...taskBody, projects: [projectGid] } }, { headers: AS_HEADERS }),
      `createTask(${companyId})`
    );
    finalGid = created.data.data.gid;
  }

  const action = existingGid ? 'updated' : 'created';

  if (drive && finalGid) {
    const folderName = `${companyName} - ${companyId}`;
    try {
      const folderUrl = await getOrCreateDriveFolder(drive, folderName);
      await attachDriveFolderToTask(finalGid, folderUrl, folderName);
    } catch (err) {
      console.warn(`  [${companyName}] Drive folder warning: ${err.message}`);
    }
  }

  console.log(`  [${companyName}] Asana ${action} — ${isInactive ? 'COMPLETED (0/0)' : 'active'}${assigneeGid ? ` → ${ownerEmail}` : ' (no assignee match)'}`);
  return action;
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

// ── Asana sync ────────────────────────────────────────────────────────────────

async function runAsanaSync(companies) {
  if (!ASANA_API_KEY) {
    console.log('ASANA_API_KEY not set — skipping Asana sync');
    return 0;
  }

  console.log(`\n[${new Date().toISOString()}] Starting Asana sync`);

  let drive = null;
  if (process.env.GOOGLE_DRIVE_SERVICE_ACCOUNT) {
    try {
      drive = await getDriveClient();
      console.log('Google Drive client initialised');
    } catch (err) {
      console.warn('Could not initialise Drive client — skipping folder creation:', err.message);
    }
  } else {
    console.log('GOOGLE_DRIVE_SERVICE_ACCOUNT not set — skipping Drive folder creation');
  }

  console.log('Fetching open ticket counts from Asana tickets board…');
  const openTicketCounts = await fetchOpenTicketCounts();
  console.log(`Open ticket data loaded for ${openTicketCounts.size} companies`);

  console.log('Fetching updated company details from HubSpot…');
  const freshCompanies = await fetchCompanyDetails(companies.map(c => c.id));
  const stageNamesById = Object.fromEntries(companies.map(c => [c.id, c._stageNames || []]));
  freshCompanies.forEach(c => {
    c._stageNames      = stageNamesById[c.id] || [];
    c._openTicketCount = openTicketCounts.get(String(c.id)) || 0;
  });

  console.log('Fetching HubSpot owners…');
  const ownerEmailMap = await fetchHubSpotOwners();
  console.log(`${ownerEmailMap.size} owners found`);

  console.log('Fetching Asana workspace…');
  const workspaceGid = await fetchAsanaWorkspaceGid();

  console.log('Fetching Asana users…');
  const asanaUserMap = await fetchAsanaUsers(workspaceGid);
  console.log(`${asanaUserMap.size} Asana users found`);

  console.log('Fetching Asana custom field definitions…');
  const customFieldDefs = await fetchAsanaCustomFields(ASANA_PROJECT_ID);
  console.log(`Custom fields found: ${Object.keys(customFieldDefs).join(', ')}`);

  console.log('Fetching existing Asana tasks…');
  const existingTasks = await fetchAllAsanaTasks(ASANA_PROJECT_ID);
  console.log(`${existingTasks.size} existing tasks found`);

  let created = 0, asanaUpdated = 0, errors = 0;
  const totalBatches = Math.ceil(freshCompanies.length / ASANA_BATCH_SIZE);

  for (let i = 0; i < freshCompanies.length; i += ASANA_BATCH_SIZE) {
    const batch    = freshCompanies.slice(i, i + ASANA_BATCH_SIZE);
    const batchNum = Math.floor(i / ASANA_BATCH_SIZE) + 1;
    const rangeEnd = Math.min(i + ASANA_BATCH_SIZE, freshCompanies.length);
    console.log(`\nAsana batch ${batchNum}/${totalBatches}  (companies ${i + 1}–${rangeEnd})`);

    for (const company of batch) {
      try {
        const action = await syncCompanyToAsana(
          company, existingTasks, customFieldDefs, ownerEmailMap, asanaUserMap, ASANA_PROJECT_ID, drive
        );
        action === 'created' ? created++ : asanaUpdated++;
      } catch (err) {
        const name = company.properties?.name || company.id;
        console.error(`  [${name}] Asana ERROR: ${err.response?.data?.errors?.[0]?.message || err.message}`);
        errors++;
      }
    }

    if (i + ASANA_BATCH_SIZE < freshCompanies.length) await sleep(ASANA_BATCH_DELAY);
  }

  console.log(`\n[${new Date().toISOString()}] Asana sync done.  Created: ${created}  Updated: ${asanaUpdated}  Errors: ${errors}`);
  return errors;
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

  const mrrErrors   = await runMRRSync(companies);
  const asanaErrors = await runAsanaSync(companies);

  if (mrrErrors > 0 || asanaErrors > 0) process.exit(1);
}

main();
