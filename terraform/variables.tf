variable "project_id" {
  description = "Google Cloud project ID"
  type        = string
}

variable "region" {
  description = "Bucket location"
  type        = string
  default     = "ASIA"
}

variable "bucket_name" {
  description = "Name of the GCS bucket"
  type        = string
}

variable "stg_dataset_id" {
  description = "ID dataset for stg BigQuery"
  type        = string
}

variable "prod_dataset_id" {
  description = "ID dataset for prod BigQuery"
  type        = string
}