# Input variables — the "function signature" of this Terraform module.
# Values are supplied in terraform.tfvars (gitignored — environment-specific).

variable "project_id" {
  description = "GCP project ID that owns the BigQuery datasets + GCS bucket"
  type        = string
}

variable "region" {
  description = "GCP region for BigQuery datasets + GCS bucket. US = multi-region (cheapest, best for learning)"
  type        = string
  default     = "US"
}

variable "credentials_path" {
  description = "Absolute path to the GCP service account credentials.json key file"
  type        = string
  sensitive   = true # marks the value as secret in plan output (path itself isn't secret, but good hygiene)
}

variable "bucket_name" {
  description = "Globally-unique name for the GCS data-lake bucket. Convention: {project_id}-data-lake"
  type        = string
}
