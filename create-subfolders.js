require('dotenv').config();
const { google } = require('googleapis');

const DRIVE_PARENT_FOLDER_ID = '1Zvl6h5QhlbcjBn3RoEamjFWqXDnjQ23Z';
const SUBFOLDERS = ['Agreements', 'Client Ops'];

async function getClients() {
  const credentials = JSON.parse(process.env.GOOGLE_DRIVE_SERVICE_ACCOUNT);
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: [
      'https://www.googleapis.com/auth/drive',
      'https://www.googleapis.com/auth/documents',
    ],
  });
  return {
    drive: google.drive({ version: 'v3', auth }),
    docs:  google.docs({ version: 'v1', auth }),
  };
}

async function getAllCompanyFolders(drive) {
  const folders = [];
  let pageToken;

  while (true) {
    const res = await drive.files.list({
      q: `'${DRIVE_PARENT_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`,
      fields: 'nextPageToken, files(id, name)',
      pageToken,
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
    });

    folders.push(...(res.data.files || []));
    if (res.data.nextPageToken) {
      pageToken = res.data.nextPageToken;
    } else {
      break;
    }
  }

  return folders;
}

async function getSubfolderId(drive, parentId, name) {
  const res = await drive.files.list({
    q: `name='${name}' and '${parentId}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`,
    fields: 'files(id)',
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });
  return res.data.files.length > 0 ? res.data.files[0].id : null;
}

async function ensureSubfolder(drive, parentId, subfolderName) {
  const res = await drive.files.list({
    q: `name='${subfolderName}' and '${parentId}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`,
    fields: 'files(id)',
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });

  if (res.data.files.length > 0) return { status: 'exists', id: res.data.files[0].id };

  const created = await drive.files.create({
    requestBody: {
      name: subfolderName,
      mimeType: 'application/vnd.google-apps.folder',
      parents: [parentId],
    },
    fields: 'id',
    supportsAllDrives: true,
  });

  return { status: 'created', id: created.data.id };
}

function buildKocTemplate(companyName) {
  return `☎️ Kick-Off Call – ${companyName} - [ROLE NAME]

Goal:
Align on the role requirements, candidate profile, and hiring expectations to ensure we launch the search effectively.
Attendees:
Client:
Role:
Client Owner:
AE:

Call Structure (15–20 minutes)
Introduction
Client Owner: I will be your Account Manager and I will be leading search & delivery as well as your main POC post-hire (onboarding and support)

Flexible Structure for the Call

1. Confirm Working Hours & Budget
Q: What are the expected working hours / time zone?
Q: The budget discussed was [X or X–X] — does that still align?
Q: Are you open to flexibility for a strong candidate?

2. Scope the Role (JD Deep Dive)
(This is the most important section—go beyond the written JD)
Q: Can you walk me through what this person will actually do in a typical day/week?
Q: What are the top 3–5 priorities for this role?
Q: What tasks or responsibilities are non-negotiable?
Q: What could be flexible or learned on the job?
Q: What tools, systems, or workflows will they be working with daily?
Q: What level of ownership do you expect (execution vs strategy vs both)?
Q: What type of background tends to work best for this role? (industry, company type, etc.)
Q: Are there any profiles you've tried before that didn't work? Why?
👉 AM note: Clarify vague JDs here. If something sounds broad, narrow it down.

3. Candidate Profile & Decision Criteria
Q: What are the must-have requirements?
Q: What are the nice-to-haves?
Q: What are the biggest green flags?
Q: What are the red flags or dealbreakers?
Q: When comparing candidates, what will make you choose one over another?
Q: What would make you say: "this is the one"?

4. Success Metrics
Q: What does success look like in the first 30 / 60 / 90 days?
Q: What KPIs or outcomes define success?
Q: What would make this hire a big win for you?

5. Team Structure & Context
Q: Who will this person report to?
Q: Who will they work closely with?
Q: How is the team structured today?
Q: Will this person interact with other teams? If so, which ones and with whom?

6. Hiring Process & Urgency
Q: How urgent is this hire (1–10)?
Q: What happens if this role isn't filled in the next 30 days?
Q: Who is involved in the interview process?
Q: Are you available to review and interview candidates within a week?
Q: In past hires, where has the process slowed down?
Q: Anything we should proactively avoid?

7. Candidate Selling Points
Q: Why would a strong candidate choose your company?
Q: What makes this role exciting or unique?
Q: What growth opportunities can we highlight?

8. Batch 0 Alignment (If applicable)
AM: "We've prepared a few sample candidates to align on profile."
Q: What do you like / dislike about these profiles?
Q: What would you want to see differently?

9. Time Off Policy
Q: Do you have an existing PTO policy?
If not, introduce standard policy:
25 days per year (vacation, holidays, sick leave)
Accrued monthly
Up to 3 unaccrued days
Additional days unpaid
Resets annually

10. Payment Method
Q: Preferred payment method? (Credit card or ACH)
Note:
Credit card: +3% fee
ACH: no fee
First payment via card (fee waived during ACH setup)

11. Growth & Future Hiring (Soft Discovery)
(Keep this light and conversational—this is where upsell insight comes from)
Q: How is your team expected to grow in the next 6–12 months?
Q: Are there any additional roles you're considering soon?
Q: Where do you feel the biggest gaps are today outside of this role?

12. Next Steps & Alignment
Q: Can we schedule the Candidate Review Session (CRS) for 5–7 business days after contract + deposit?
AM (close):
"Based on everything we discussed, we'll target candidates with [X profile]. Does that feel aligned?"
`;
}

async function ensureKocDoc(drive, docs, parentId, docName, companyName) {
  const res = await drive.files.list({
    q: `name='${docName}' and '${parentId}' in parents and mimeType='application/vnd.google-apps.document' and trashed=false`,
    fields: 'files(id)',
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });

  if (res.data.files.length > 0) return 'exists';

  const created = await drive.files.create({
    requestBody: {
      name: docName,
      mimeType: 'application/vnd.google-apps.document',
      parents: [parentId],
    },
    fields: 'id',
    supportsAllDrives: true,
  });

  const docId = created.data.id;

  await docs.documents.batchUpdate({
    documentId: docId,
    requestBody: {
      requests: [{
        insertText: {
          location: { index: 1 },
          text: buildKocTemplate(companyName),
        },
      }],
    },
  });

  return 'created';
}

function extractCompanyName(folderName) {
  const match = folderName.match(/^(.+?)\s+-\s+\d+$/);
  return match ? match[1].trim() : folderName;
}

async function main() {
  console.log(`[${new Date().toISOString()}] Starting subfolder creation`);

  if (!process.env.GOOGLE_DRIVE_SERVICE_ACCOUNT) {
    console.error('ERROR: GOOGLE_DRIVE_SERVICE_ACCOUNT not set');
    process.exit(1);
  }

  const { drive, docs } = await getClients();

  console.log('Fetching company folders…');
  const folders = await getAllCompanyFolders(drive);
  console.log(`${folders.length} company folders found`);

  let created = 0, skipped = 0, errors = 0;

  for (const folder of folders) {
    for (const sub of SUBFOLDERS) {
      try {
        const { status: subStatus } = await ensureSubfolder(drive, folder.id, sub);
        if (subStatus === 'created') {
          console.log(`  [${folder.name}] created "${sub}"`);
          created++;
        } else {
          console.log(`  [${folder.name}] "${sub}" already exists — skipped`);
          skipped++;
        }

        if (sub === 'Client Ops') {
          const clientOpsFolderId = await getSubfolderId(drive, folder.id, 'Client Ops');
          if (!clientOpsFolderId) continue;

          const companyName  = extractCompanyName(folder.name);
          const saFolderName = `${companyName} <> SA - External`;

          const { status: saStatus, id: saFolderId } = await ensureSubfolder(drive, clientOpsFolderId, saFolderName);
          if (saStatus === 'created') {
            console.log(`  [${folder.name}] created "${saFolderName}"`);
            created++;
          } else {
            console.log(`  [${folder.name}] "${saFolderName}" already exists — skipped`);
            skipped++;
          }

          const trsName = `TRS - ${companyName} - Scale Army`;
          try {
            const { status: trsStatus } = await ensureSubfolder(drive, saFolderId, trsName);
            if (trsStatus === 'created') { console.log(`  [${folder.name}] created "${trsName}"`); created++; }
            else { skipped++; }
          } catch (err) {
            console.error(`  [${folder.name}] ERROR creating "${trsName}": ${err.message}`);
            errors++;
          }

          const jdsName = `JDs - ${companyName} - Scale Army`;
          try {
            const { status: jdsStatus } = await ensureSubfolder(drive, saFolderId, jdsName);
            if (jdsStatus === 'created') { console.log(`  [${folder.name}] created "${jdsName}"`); created++; }
            else { skipped++; }
          } catch (err) {
            console.error(`  [${folder.name}] ERROR creating "${jdsName}": ${err.message}`);
            errors++;
          }

          const kocDocName = `${companyName} - KOC - [Position]`;
          try {
            const kocStatus = await ensureKocDoc(drive, docs, saFolderId, kocDocName, companyName);
            if (kocStatus === 'created') { console.log(`  [${folder.name}] created doc "${kocDocName}"`); created++; }
            else { skipped++; }
          } catch (err) {
            console.error(`  [${folder.name}] ERROR creating doc "${kocDocName}": ${err.message}`);
            errors++;
          }
        }
      } catch (err) {
        console.error(`  [${folder.name}] ERROR creating "${sub}": ${err.message}`);
        errors++;
      }
    }
  }

  console.log(`\n[${new Date().toISOString()}] Done.  Created: ${created}  Skipped: ${skipped}  Errors: ${errors}`);
  if (errors > 0) process.exit(1);
}

main();
