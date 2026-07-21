# Terraform

Reference for Terraform concepts, workflow, our setup, and errors hit during Phase 4.2. Terraform provisions cloud infrastructure (BigQuery datasets + GCS bucket) as code — see `docs/knowledge/gcp.md` for the GCP auth model that Terraform builds on.

## What Terraform Does (concept)

Terraform is **infrastructure as code**. You write declarative config files describing what cloud resources you want (a BigQuery dataset, a GCS bucket). Terraform figures out the diff between your config and reality, and makes reality match your config.

### The three-command workflow
```bash
terraform init      # download provider plugins, initialize state
terraform plan      # show the diff (what will be created/changed/destroyed) — no changes made
terraform apply     # execute the plan (prompts "yes")
terraform destroy   # delete everything Terraform created (DESTRUCTIVE)
terraform fmt       # format .tf files (run before commit)
terraform validate  # syntax + internal consistency check
```

Always run `plan` before `apply` to review what will happen. Always run `plan -destroy` before `destroy` to see what will be removed.

### State — the key concept
Terraform tracks what it created in a `terraform.tfstate` file. That file *is* your infrastructure's source of truth from Terraform's perspective:
- If you lose it, Terraform forgets what it manages and can't destroy cleanly.
- If you commit it with secrets, that's a leak (state files can contain sensitive values).
- For a learning project with one operator, **local state** is fine.
- For team/production, use a **remote backend** (e.g., GCS) so state is shared + locked (prevents two people running `apply` at once).

Our state file: `terraform/terraform.tfstate` (gitignored).

### Providers
A provider is a plugin that knows how to talk to a specific cloud (Google, AWS, Azure). We use `hashicorp/google` (v7.40.0, pinned `~> 7.40`). The provider config includes how to authenticate — in our case, via a service account key file path. See `docs/knowledge/gcp.md` for the auth model.

## File Structure

```
terraform/
├── providers.tf          # Google provider config (auths via service account key)
├── variables.tf          # Input variables (the "function signature")
├── main.tf               # Resources: 2 BigQuery datasets + 1 GCS bucket
├── terraform.tfvars      # Actual values (GITIGNORED — environment-specific)
└── terraform.tfvars.example  # Template to copy from (committable)
```

**Why split into 4 files?** Standard Terraform layout:
- `providers.tf` separates "which cloud + how to auth" from "what to create" — swapping providers means touching one file.
- `variables.tf` declares the *shape* of inputs (types, descriptions) — like a function signature.
- `terraform.tfvars` provides the *values* — like arguments at call time. Gitignored because it's environment-specific (your project ID, your key path).
- `main.tf` is the resource declarations — the actual infrastructure.

## Resources We Manage (Phase 4.2)

| Resource | Type | Purpose |
|---|---|---|
| `google_bigquery_dataset.raw` | BigQuery dataset | Raw landing zone — crime + Divvy trips land here |
| `google_bigquery_dataset.mart` | BigQuery dataset | DBT-built analytics marts (fact_station_day, crime_ridership_correlation) |
| `google_storage_bucket.data_lake` | GCS bucket | Parquet files — Spark writes here instead of Postgres |

We do NOT create BigQuery tables in Terraform. DBT (Phase 4.3+) creates tables inside these datasets. **Terraform manages the containers; DBT manages the contents.**

## Key Decisions Baked Into the Config

- **`delete_contents_on_destroy = true`** on BigQuery datasets + `force_destroy = true` on bucket → `terraform destroy` wipes data too. Safe for learning (re-run pipelines from scratch); **NEVER in production** — it would delete your warehouse on a typo.
- **90-day lifecycle rule on bucket** → auto-deletes Parquet objects older than 90 days. Parquet is a staging format; once loaded into BigQuery it's redundant. Adjust for production.
- **`uniform_bucket_level_access = true`** → use IAM, not legacy ACLs. Recommended by Google.
- **Provider version pinned `~> 7.40`** → patch updates only (7.40.1, 7.40.2), no minor-version surprises (7.41). Stable, non-experimental versions per user preference.
- **Local state first** (`terraform.tfstate` in `terraform/`, gitignored). Plan says migrate to GCS backend later for team use.

## How Terraform Authenticates to GCP

Terraform does NOT use `gcloud auth login`. It reads the service account key file directly via the Google provider config:

```hcl
provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = var.credentials_path   # path to ~/chicago-divvy-pipeline-credentials.json
}
```

The key file is the same one created in Phase 4.1 (`docs/knowledge/gcp.md` Step 7). Terraform authenticates as the `terraform-runner` service account with scoped roles (bigquery.dataOwner, storage.admin, etc.) — least privilege. See `docs/knowledge/gcp.md` for the full auth model.

## Errors Hit During Phase 4.2

### 1. WSL gcloud had separate config + auth state from Windows
**Symptom:** In WSL, `gcloud services list` failed with `AUTH_PERMISSION_DENIED` authenticated as `terraform-runner@dtc-de-course-497317.iam.gserviceaccount.com` (the OLD DataTalksClub course project), not the new project.

**Root cause:** WSL and Windows gcloud maintain **separate state**. `gcloud config set project` and `gcloud auth login` in PowerShell did NOT carry over to WSL. WSL has its own config at `~/.config/gcloud/` and was still authed as the old course service account.

**Fix:** Activate the service account in WSL using the key file (non-interactive — same way CI/CD auths):
```bash
gcloud auth activate-service-account terraform-runner@chicago-divvy-pipeline.iam.gserviceaccount.com \
  --key-file=/home/sagar/chicago-divvy-pipeline-credentials.json
gcloud config set account terraform-runner@chicago-divvy-pipeline.iam.gserviceaccount.com
gcloud config set project chicago-divvy-pipeline
```

**Lesson:** `gcloud auth activate-service-account` is the non-interactive auth path — use it in WSL (no browser) and in CI/CD. `gcloud auth login` (browser) is for humans only. See `docs/knowledge/gcp.md` pitfall #5.

### 2. `~` not expanded by gcloud (recurring)
**Symptom:** `gcloud auth activate-service-account --key-file=~/chicago-divvy-pipeline-credentials.json` → `No such file or directory: '~/chicago-divvy-pipeline-credentials.json'`

**Root cause:** gcloud is a Python tool, not a shell. It treats `~` as a literal path, not as `/home/sagar/`. (Same pitfall as Phase 4.1 Step 7.)

**Fix:** Use explicit absolute path: `/home/sagar/chicago-divvy-pipeline-credentials.json`.

**Lesson:** Any gcloud command taking a file path needs the full path, not `~`. See `docs/knowledge/gcp.md` pitfall #1.

### 3. `gcloud services list` AUTH_PERMISSION_DENIED (expected, not a bug)
**Symptom:** After authing the SA in WSL, `gcloud services list --enabled` still failed with `AUTH_PERMISSION_DENIED`.

**Root cause:** The SA's scoped roles (bigquery.dataOwner, bigquery.jobUser, storage.admin, iam.serviceAccountTokenCreator) deliberately do NOT include `serviceusage.services.list` (that's an admin role). **Least privilege working as designed.**

**Fix:** Not a bug. Verified APIs from personal Gmail in PowerShell (already done in Phase 4.1). Used `bq ls` + `gsutil ls` for resource verification instead — those permissions ARE in the SA's roles.

**Lesson:** Least privilege means some admin commands fail by design. If `gcloud services list` worked for the SA, you'd have over-granted. Verify resources with commands that match the SA's actual permissions (`bq ls`, `gsutil ls`), not admin commands.

## Verification (Phase 4.2 — 2026-07-21)

| Check | Command | Result |
|---|---|---|
| Terraform init | `terraform init` | Google provider v7.40.0 installed ✅ |
| Terraform plan | `terraform plan` | 3 to add, 0 change, 0 destroy ✅ |
| Terraform apply | `terraform apply` | Resources created ✅ |
| BigQuery datasets | `bq ls` | Shows `raw` + `mart` ✅ |
| GCS bucket | `gsutil ls` | Shows `gs://chicago-divvy-pipeline-data-lake/` ✅ |

## Useful Commands

```bash
# Workflow
cd ~/chicago-data-pipeline/terraform
terraform init
terraform plan
terraform apply
terraform destroy          # DESTRUCTIVE — wipes all resources (+ data due to delete_contents_on_destroy)
terraform fmt              # format .tf files
terraform validate         # syntax check
terraform output           # print outputs (if any defined)

# State inspection
terraform state list       # what resources is Terraform managing?
terraform state show google_bigquery_dataset.raw   # details of one resource
terraform show             # full state (from tfstate file)

# Plan variants
terraform plan -destroy    # preview what destroy will do (ALWAYS run first)
terraform plan -out=tfplan # save plan to a file
terraform apply tfplan     # apply a saved plan (no prompt)

# Import existing resources (if something was made outside Terraform)
terraform import google_bigquery_dataset.raw projects/PROJECT_ID/datasets/raw

# Lock file — commit this
# terraform/.terraform.lock.hcl records exact provider versions. Commit it so
# everyone running terraform init gets the same provider.
```

## What's Next (Phase 4.3 — Architecture change)

GCP containers exist. Next: make the pipeline use them.
- Spark writes to GCS (Parquet) instead of Postgres
- Airbyte replaces `download_crime.py` (Socrata source → BigQuery destination)
- DBT `profiles.yml` switches Postgres → BigQuery adapter
- Streaming path decision: keep on Postgres (small data) or move to BigQuery via `foreachBatch` → GCS → BigQuery load job

---
