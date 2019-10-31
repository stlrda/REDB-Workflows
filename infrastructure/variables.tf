variable "vpc_name" {
  default = "default-vpc-name"
}

variable "region" {
  default = "us-east-1"
}

variable "vpc_availability_zones" {
  default = [
    "us-east-1a",
    "us-east-1b",
    "us-east-1c"
  ]
}

variable "db_username" {}

variable "db_password" {}

variable "cluster_name" {
  default = "airflow_cluster"
}

variable "cluster_stage" {
  default = "dev"
}

variable "fernet_key" {}

variable "s3_log_bucket" {
  default = "airflow_logs"
}

variable "key_pair" {
}