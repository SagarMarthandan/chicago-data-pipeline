# Terraform + Google Cloud provider configuration
#
# This file declares WHICH cloud we target and HOW Terraform authenticates.
# It is separate from main.tf (what to create) so swapping providers later
# means touching only this file.

terraform {
  required_version = ">= 1.15.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.40"
    }
  }
}

# The Google provider authenticates using the service account key file
# (NOT gcloud auth login). This is the machine identity created in Phase 4.1:
#   terraform-runner@chicago-divvy-pipeline.iam.gserviceaccount.com
# The key path is passed via var.credentials_path (see variables.tf + terraform.tfvars).
provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = var.credentials_path
}
