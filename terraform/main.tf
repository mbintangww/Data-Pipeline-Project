provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "gcs_bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true  
  uniform_bucket_level_access = true
}


resource "google_bigquery_dataset" "stg_dataset" {
  dataset_id                  = var.stg_dataset_id
  location                    = "asia-southeast1"
  delete_contents_on_destroy = true  

}

resource "google_bigquery_dataset" "prod_dataset" {
  dataset_id                  = var.prod_dataset_id
  location                    = "asia-southeast1"
  delete_contents_on_destroy = true  
}

