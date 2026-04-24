# HubSpot Total MRR Sync

Automatically sums `margin__price___salary_` across all **Closed Won** deals
for each HubSpot company and writes the result to the `total_mrr` company property.

---

## How it works

1. Fetches every company in your HubSpot account (handles pagination).
2. For each company, fetches all associated deals where `dealstage = closedwon`.
3. Sums the `margin__price___salary_` field across those deals (blank/null = 0).
4. Writes the total back to the `total_mrr` property on the company record.
5. Logs every step to stdout so you can monitor it in GitHub Actions.

---

## Local setup

### HubSpot Private App scopes required
- `crm.objects.companies.read`
- `crm.objects.companies.write`
- `crm.objects.deals.read`

### Steps

```bash
# 1. Clone the repo
git clone https://github.com/<your-org>/<your-repo>.git
cd <your-repo>

# 2. Install dependencies
npm install

# 3. Create your local .env file
cp .env.example .env
# Then open .env and paste in your Private App token

# 4. Run
npm start

### GitHub secrets required

Go to **Settings → Secrets and variables → Actions → New repository secret** and add all three:

| Secret name | Value |
|---|---|
| `HUBSPOT_API_KEY` | Your HubSpot Private App token |
| `ASANA_API_KEY` | Your Asana Personal Access Token (found at app.asana.com → Profile → My Settings → Apps → Personal access tokens) |
| `ASANA_PROJECT_ID` | `1214241148472876` |
