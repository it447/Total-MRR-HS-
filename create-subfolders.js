require('dotenv').config();
const { google } = require('googleapis');

const DRIVE_PARENT_FOLDER_ID = '1Zvl6h5QhlbcjBn3RoEamjFWqXDnjQ23Z';
const SUBFOLDERS = ['Agreements', 'Client Ops'];

async function getDriveClient() {
  const credentials = JSON.parse(process.env.GOOGLE_DRIVE_SERVICE_ACCOUNT);
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/drive'],
  });
  return google.drive({ version: 'v3', auth });
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

async function getSubfolderId(drive, parentId, subfolderName) {
  const res = await drive.files.list({
    q: `name='${subfolderName}' and '${parentId}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`,
    fields: 'files(id)',
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });
  return res.data.files.length > 0 ? res.data.files[0].id : null;
}

async function ensureSubfolder(drive, parentId, subfolderName) {
  const res = await drive.files.list({
    q: `name='${subfolderName}' and '${parentId}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`,
    fields: 'files(id,name)',
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });

  if (res.data.files.length > 0) return 'exists';

  await drive.files.create({
    requestBody: {
      name: subfolderName,
      mimeType: 'application/vnd.google-apps.folder',
      parents: [parentId],
    },
    fields: 'id',
    supportsAllDrives: true,
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

  const drive = await getDriveClient();

  console.log('Fetching company folders…');
  const folders = await getAllCompanyFolders(drive);
  console.log(`${folders.length} company folders found`);

  let created = 0, skipped = 0, errors = 0;

  for (const folder of folders) {
    for (const sub of SUBFOLDERS) {
      try {
        const result = await ensureSubfolder(drive, folder.id, sub);
        if (result === 'created') {
          console.log(`  [${folder.name}] created "${sub}"`);
          created++;
        } else {
          console.log(`  [${folder.name}] "${sub}" already exists — skipped`);
          skipped++;
        }

        if (sub === 'Client Ops') {
          const clientOpsFolderId = await getSubfolderId(drive, folder.id, 'Client Ops');
          if (clientOpsFolderId) {
            const companyName = extractCompanyName(folder.name);
            const saFolderName = `${companyName} <> SA - External`;
            try {
              const saResult = await ensureSubfolder(drive, clientOpsFolderId, saFolderName);
              if (saResult === 'created') {
                console.log(`  [${folder.name}] created "${saFolderName}" inside Client Ops`);
                created++;
              } else {
                console.log(`  [${folder.name}] "${saFolderName}" already exists inside Client Ops — skipped`);
                skipped++;
              }
            } catch (err) {
              console.error(`  [${folder.name}] ERROR creating "${saFolderName}": ${err.message}`);
              errors++;
            }
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
