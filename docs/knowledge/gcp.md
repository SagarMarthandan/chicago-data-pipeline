# Google Cloud Platform (GCP)

Reference for GCP setup, auth model, and Terraform integration. Built during Phase 4.1 (warehouse choice + GCP project setup) before writing Terraform config in Phase 4.2.

## GCP Auth Model — Two Layers of Identity

The #1 stumbling block for Terraform-on-GCP. There are **two separate identities** involved, and confusing them causes most auth errors.

### Layer 1: Personal Google account (human identity)
- Your `@gmail.com` account.
- Used **once**, via browser, to: log in, create a project, link billing, enable APIs, create a service account, grant it roles, download its key.
- You do **not** use this for Terraform directly. A `credentials.json` for a personal account has full owner power — you don't want that file lying around.

### Layer 2: Service account (machine identity)
- Looks like `name@project-id.iam.gserviceaccount.com`.
- Terraform authenticates as this, not as you.
- You download a `credentials.json` key file for it, point Terraform at that file, and Terraform acts with the service account's scoped permissions.

### Why the split?
**Principle of least privilege.** Your personal account can delete the project, change billing, create more service accounts. A service account gets only the roles you grant (e.g., BigQuery admin, Storage admin). If its key leaks, the blast radius is limited. You never want a `credentials.json` with owner-level power on disk.

### The flow
```
personal Gmail (browser) → create project → link billing
                         → enable APIs (BigQuery, Storage, Resource Manager)
                         → create service account + grant scoped roles
                         → download credentials.json key
Terraform → reads credentials.json → auths as service account → creates resources
```

## Setup Process (step-by-step)

Run these in **PowerShell on Windows** (browser auth needs Windows, not WSL). Move the key to WSL afterward.

### Step 1 — Log in with personal Gmail (opens browser)
```powershell
gcloud auth login
```
Opens browser → pick Gmail → authorize. Sets your human identity for the CLI. Does NOT affect Terraform yet.

Verify:
```powershell
gcloud auth list
```
Your Gmail should be ACTIVE. If an old account is active, switch:
```powershell
gcloud config set account YOUR_GMAIL@gmail.com
```

### Step 2 — Create a GCP project
```powershell
gcloud projects create chicago-divvy-pipeline --name="Chicago Divvy Pipeline"
gcloud config set project chicago-divvy-pipeline
```
**What a project is:** GCP's resource container. Everything (BigQuery datasets, GCS buckets, service accounts) lives inside a project. Billing, IAM, and API enablement are project-scoped. One project = one isolated workspace.

### Step 3 — Link a billing account (required even for free tier)
```powershell
gcloud components install beta   # if beta components not installed
gcloud beta billing accounts list
gcloud beta billing projects link chicago-divvy-pipeline --billing-account=ACCOUNT_ID
```
BigQuery's free tier won't activate without a billing account linked. If `accounts list` is empty, create one at https://console.cloud.google.com/billing (requires a credit card — you won't be charged if you stay in free tier, but the card must be on file).

### Step 4 — Enable APIs Terraform needs
```powershell
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
```
GCP APIs are off by default. Terraform's Google provider calls these APIs to create resources. Enabling takes ~30s each. If you see `PERMISSION_DENIED: ... API has not been used in project ... or it is disabled`, that's this step being skipped.

### Step 5 — Create a service account
```powershell
gcloud iam service-accounts create terraform-runner --display-name="Terraform Runner"
```
Creates `terraform-runner@chicago-divvy-pipeline.iam.gserviceaccount.com`.

### Step 6 — Grant scoped roles (least privilege)
```powershell
gcloud projects add-iam-policy-binding chicago-divvy-pipeline --member="serviceAccount:terraform-runner@chicago-divvy-pipeline.iam.gserviceaccount.com" --role="roles/bigquery.dataOwner"
gcloud projects add-iam-policy-binding chicago-divvy-pipeline --member="serviceAccount:terraform-runner@chicago-divvy-pipeline.iam.gserviceaccount.com" --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding chicago-divvy-pipeline --member="serviceAccount:terraform-runner@chicago-divvy-pipeline.iam.gserviceaccount.com" --role="roles/storage.admin"
gcloud projects add-iam-policy-binding chicago-divvy-pipeline --member="serviceAccount:terraform-runner@chicago-divvy-pipeline.iam.gserviceaccount.com" --role="roles/iam.serviceAccountTokenCreator"
```
| Role | What it allows |
|---|---|
| `bigquery.dataOwner` | Create/delete datasets + tables |
| `bigquery.jobUser` | Run queries / load jobs |
| `storage.admin` | Create/delete GCS buckets + objects |
| `iam.serviceAccountTokenCreator` | Needed for some Terraform operations that impersonate |

**Do NOT grant `roles/owner`** to the service account — that's the over-privileged mistake. These four roles cover exactly what Terraform needs for this project.

### Step 7 — Download the credentials.json key
```powershell
gcloud iam service-accounts keys create C:\Users\sagar\chicago-divvy-pipeline-credentials.json --iam-account=terraform-runner@chicago-divvy-pipeline.iam.gserviceaccount.com
```
**Use an explicit Windows path, NOT `~`.** gcloud is a Python tool; it treats `~/...` as a literal filename and fails with `No such file or directory: '~/...'`.

### Step 8 — Move key to WSL + lock permissions
```bash
cp /mnt/c/Users/sagar/chicago-divvy-pipeline-credentials.json ~/chicago-divvy-pipeline-credentials.json
chmod 600 ~/chicago-divvy-pipeline-credentials.json
ls -l ~/chicago-divvy-pipeline-credentials.json
```
`chmod 600` = readable/writable by you only (`-rw-------`). A world-readable key file is a common security audit failure.

### Step 9 — Gitignore the key
`.gitignore` must exclude credential files. These patterns are in the repo `.gitignore`:
```
*-credentials.json
credentials.json
google-credentials.json
```
Verify: `git check-ignore -v ~/chicago-data-pipeline/terraform/credentials.json` (once the terraform dir exists).

## WSL vs Windows/PowerShell — Command Differences

This project runs gcloud on **Windows PowerShell** (browser auth) and Terraform on **WSL** (where the repo + code live). The key bridges the two.

| Concept | WSL (bash) | Windows (PowerShell) |
|---|---|---|
| Home dir | `~` = `/home/sagar/` | `~` = `C:\Users\sagar\` (but gcloud doesn't expand it — use explicit path) |
| Line continuation | `\` (backslash) | `` ` `` (backtick) |
| Path separator | `/` | `\` |
| Access other FS | Windows files at `/mnt/c/Users/sagar/` | WSL files at `\\wsl$\<distro>\home\sagar\` |
| File permissions | `chmod 600` works | No chmod concept (NTFS ACLs instead) |

### Pitfalls hit during setup
1. **`~` not expanded by gcloud** — `gcloud iam service-accounts keys create ~/file.json` fails with `No such file or directory: '~/file.json'`. gcloud (Python) treats `~` as literal. Fix: use explicit path (`C:\Users\sagar\file.json` on Windows, `/home/sagar/file.json` on WSL). Also affects `gcloud auth activate-service-account --key-file=~/...` — use `/home/sagar/...`.
2. **`\` line continuation fails in PowerShell** — `gcloud iam service-accounts create terraform-runner \` → `unrecognized arguments: \`. PowerShell uses backtick `` ` ``, not backslash. Fix: put command on one line, or use backtick.
3. **`gcloud beta` not installed by default** — `gcloud beta billing accounts list` → `You do not currently have this command group installed`. Fix: `gcloud components install beta`.
4. **Browser auth doesn't work in WSL** — `gcloud auth login` tries to open a browser; WSL has no browser. Fix: run gcloud auth in PowerShell on Windows, then move artifacts (credentials.json) to WSL.
5. **WSL gcloud has separate config + auth state from Windows gcloud** — `gcloud config set project` in PowerShell does NOT carry over to WSL. WSL maintains its own config at `~/.config/gcloud/`. After moving the key to WSL, you must also (a) `gcloud config set project chicago-divvy-pipeline` in WSL, and (b) auth WSL gcloud. Since WSL can't do browser auth, use the service account key:
   ```bash
   gcloud auth activate-service-account terraform-runner@chicago-divvy-pipeline.iam.gserviceaccount.com \
     --key-file=/home/sagar/chicago-divvy-pipeline-credentials.json
   gcloud config set account terraform-runner@chicago-divvy-pipeline.iam.gserviceaccount.com
   ```
   This is how CI/CD and automation auth too — non-interactive, key-based.
6. **`gcloud services list` fails with AUTH_PERMISSION_DENIED for the SA** — expected, not a bug. The scoped roles (bigquery.dataOwner, storage.admin, etc.) deliberately do NOT include `serviceusage.services.list` (that's an admin role). Least privilege working as designed. Verify APIs from your personal Gmail in PowerShell instead. The SA CAN run `bq ls` + `gsutil ls` — those permissions are in its roles.

## Pitfalls, Risks, Cautions

### Risks (security)
- **Leaked service account key = full access to whatever roles it has.** Treat `credentials.json` like a password. `chmod 600`, gitignore, never commit, never paste in chat.
- **`roles/owner` on a service account is a security smell.** Grant scoped roles instead. Owner can delete the project, change billing, create more keys.
- **Key files in `~` survive WSL reinstall?** No — `~` is inside the WSL distro. But if you distro-export, the key goes with it. Store keys deliberately; rotate if exposed.

### Cautions (operational)
- **Billing account must be linked even for free tier.** BigQuery free tier (1 TB queries/mo + 10 GB storage/mo) won't activate without a billing account on file. You won't be charged if you stay in limits, but the card must be there.
- **APIs are off by default.** `PERMISSION_DENIED: ... API has not been used in project ... or it is disabled` means you skipped `gcloud services enable`. Enable BigQuery + Storage + Resource Manager at minimum.
- **`Application Default Credentials` quota project warning** — after `gcloud config set project`, you may see `WARNING: Your active project does not match the quota project in your local Application Default Credentials file`. Harmless for Terraform (it uses the key file, not ADC). Can ignore.
- **`terraform destroy` is satisfying and terrifying.** It deletes everything Terraform created. State file tracks what to destroy. Always `terraform plan -destroy` first to see what will be removed.

### Pitfalls (learning)
- **Don't confuse `gcloud auth login` (human) with `gcloud auth application-default login` (ADC for libraries).** Terraform with a key file uses neither — it reads `credentials.json` directly via the Google provider. ADC is for libraries that auto-discover creds (e.g., Python google-cloud-bigquery). We use the key file explicitly.
- **Project ID is globally unique, immutable.** Once created, you can't rename it. Pick carefully. (Display name is mutable; ID is not.)
- **Service account email is tied to project ID.** `terraform-runner@chicago-divvy-pipeline.iam.gserviceaccount.com` — if you delete the project, the service account is gone.

## Our Setup (what was actually created — 2026-07-21)

| Resource | Value |
|---|---|
| GCP project ID | `chicago-divvy-pipeline` (numeric: `480666653891`) |
| Project name | Chicago Divvy Pipeline |
| Billing account | `01A22E-2FC963-7B008D` (My Billing Account) — linked |
| APIs enabled | `bigquery.googleapis.com`, `storage.googleapis.com`, `cloudresourcemanager.googleapis.com` |
| Service account | `terraform-runner@chicago-divvy-pipeline.iam.gserviceaccount.com` |
| Roles granted | `bigquery.dataOwner`, `bigquery.jobUser`, `storage.admin`, `iam.serviceAccountTokenCreator` |
| Key file (WSL) | `~/chicago-divvy-pipeline-credentials.json` (chmod 600, gitignored) |
| Key file (Windows source) | `C:\Users\sagar\chicago-divvy-pipeline-credentials.json` |
| Owner account | `sagar.marthandan.india@gmail.com` |
| Region | `US` (for BigQuery datasets + GCS bucket — set in Terraform) |

## Useful Commands (ongoing reference)

```bash
# Auth state
gcloud auth list                              # which accounts are credentialed
gcloud config list                            # current project + account
gcloud config set project PROJECT_ID          # switch default project
gcloud config set account EMAIL               # switch active account

# Projects
gcloud projects create PROJECT_ID --name="NAME"
gcloud projects list
gcloud projects describe PROJECT_ID

# Billing
gcloud beta billing accounts list
gcloud beta billing projects link PROJECT_ID --billing-account=ACCOUNT_ID
gcloud beta billing projects describe PROJECT_ID

# APIs
gcloud services list --available              # all APIs you could enable
gcloud services list --enabled                # what's currently on
gcloud services enable API_NAME
gcloud services disable API_NAME

# Service accounts
gcloud iam service-accounts create NAME --display-name="..."
gcloud iam service-accounts list
gcloud iam service-accounts describe SA_EMAIL
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:SA_EMAIL" --role="ROLE"
gcloud projects get-iam-policy PROJECT_ID     # see all bindings
gcloud iam service-accounts keys create PATH.json --iam-account=SA_EMAIL
gcloud iam service-accounts keys list --iam-account=SA_EMAIL

# BigQuery (Terraform manages datasets — these are for verification only)
bq ls                                         # list datasets in current project
bq ls --project_id=PROJECT_ID                 # explicit project
bq show PROJECT_ID:dataset                    # dataset details
bq query --use_legacy_sql=false 'SELECT ...'  # run a query (standard SQL)

# GCS
gsutil ls                                     # list buckets (deprecated — see note below)
gcloud storage ls                             # newer equivalent (Google is phasing out gsutil)
gsutil ls gs://BUCKET_NAME/                   # list objects in a bucket
gsutil mb gs://BUCKET_NAME                    # create bucket (don't — use Terraform)
```

### gsutil deprecation note
`gsutil` commands print: *"Google recommends using Gcloud storage CLI instead of gsutil."* Google is phasing out `gsutil` in favor of `gcloud storage` (e.g., `gcloud storage ls` instead of `gsutil ls`). Both work for now; `gsutil` is fine for this project. Migration guide: https://docs.cloud.google.com/storage/docs/gsutil-transition-to-gcloud

## Terraform

Terraform provisions the cloud resources (BigQuery datasets + GCS bucket) inside the GCP project. It auths to GCP via the service account key created in Steps 5–7 above — NOT via `gcloud auth login`.

**Full Terraform reference:** [`docs/knowledge/terraform.md`](terraform.md) — concepts, workflow, file structure, key decisions, errors hit (including the WSL gcloud account-switch issue), verification, useful commands.

What Terraform created (verified 2026-07-21): `google_bigquery_dataset.raw`, `google_bigquery_dataset.mart`, `google_storage_bucket.data_lake`.

## What's Next (Phase 4.3 — Architecture change)

GCP containers exist. Next: make the pipeline use them.
- Spark writes to GCS (Parquet) instead of Postgres
- Airbyte replaces `download_crime.py` (Socrata source → BigQuery destination)
- DBT `profiles.yml` switches Postgres → BigQuery adapter
- Streaming path decision: keep on Postgres (small data) or move to BigQuery via `foreachBatch` → GCS → BigQuery load job

---
