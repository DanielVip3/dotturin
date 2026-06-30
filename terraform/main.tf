terraform {
  required_providers {
    minio = {
      source  = "aminueza/minio"
      version = ">= 1.0.0"
    }
  }
}

variable "minio_root_user" {
  description = "Username for MinIO"
  type        = string
  sensitive   = true
}

variable "minio_root_password" {
  description = "Password for MinIO"
  type        = string
  sensitive   = true
}

# Setup MinIO provider
provider "minio" {
  minio_server   = "localhost:9000"
  minio_user     = var.minio_root_user
  minio_password = var.minio_root_password
  minio_ssl      = false
}

# Bucket for raw data
resource "minio_s3_bucket" "dotturin-raw" {
  bucket = "dotturin-raw"
  acl    = "private"
}

# Bucket for processed data
resource "minio_s3_bucket" "dotturin-processed" {
  bucket = "dotturin-processed"
  acl    = "private"
}