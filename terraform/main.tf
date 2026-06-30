terraform {
  required_providers {
    minio = {
      source  = "aminueza/minio"
      version = ">= 1.0.0"
    }
  }
}

# Setup Minio provider
provider "minio" {
  minio_server   = "localhost:9000"
  minio_user     = "rootuser"
  minio_password = "rootpassword123"
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