# tft-orchestrator

Public GitHub Actions runner for the TFT Intel data pipeline.

This repo contains only the data refresh script — no app code.
The Next.js frontend lives in a separate **private** repository.

## How it works

1. GitHub Actions runs `scripts/refresh.py` on a schedule (every 6 hours).
2. The script fetches challenger data from the Riot API, computes insights,
   writes everything to a shared PostgreSQL database, and generates a static
   JSON snapshot (`public/data/snapshot_na1_challenger.json`).
3. The workflow pushes the snapshot into the private app repo via a PAT.
4. Vercel detects the push and redeploys the app automatically.

## Required Secrets (Settings → Secrets and variables → Actions)

| Secret | Description |
|---|---|
| `RIOT_API_KEY` | Riot Games API key |
| `DATABASE_URL` | PostgreSQL connection string |
| `APP_REPO_PAT` | Fine-grained PAT with **Contents: Read & Write** on the private app repo |

## Required Variables (same page, Variables tab)

| Variable | Example |
|---|---|
| `APP_REPO` | `your-username/tft` |
| `TFT_ACTIVE_SET` | `17` |
| `TFT_PATCH_START_TIMESTAMP` | `1774310400` |

## Creating the PAT

1. GitHub → Settings → Developer settings → Personal access tokens → Fine-grained tokens
2. **Resource owner**: your account
3. **Repository access**: Only select repositories → choose the private app repo
4. **Permissions**: Repository permissions → Contents → **Read and write**
5. Copy the token → add as `APP_REPO_PAT` secret in THIS repo

## Manual runs

Go to Actions → "Refresh TFT Data" → "Run workflow".

You can also trigger a historical backfill by entering a set number in the
`backfill_set` input (e.g. `16` to backfill Set 16 stats).
