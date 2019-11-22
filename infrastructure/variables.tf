#PREFIX CONVENTIONS
#no prefix - used in multiple places
#airflow - applies to airflow infrastructure
#redb - applies to redb infrastructure

#Universal
variable "region" {
  default = "us-east-1"
}


#Airflow
variable "airflow_vpc_name" {}
variable "airflow_vpc_availability_zones" {
  default = [
    "us-east-1a",
    "us-east-1b",
    "us-east-1c"
  ] }
variable "airflow_db_username" {}
variable "airflow_db_password" {}
variable "airflow_cluster_name" {
  default = "airflow_cluster" }
variable "airflow_cluster_stage" {
  default = "dev" }
variable "airflow_fernet_key" {}
variable "airflow_s3_log_bucket" {
  default = "airflow_logs" }
variable "airflow_key_pair" {}
variable "airflow_private_keypath" {}
variable "airflow_public_keypath" {}
variable "airflow_admin_email" {}
variable "airflow_admin_lastname" {}
variable "airflow_admin_name" {}
variable "airflow_admin_password" {}
variable "airflow_admin_username" {}
variable "airflow_tags" {
  default = [
  "airflow",
  "STLRDA"
  ]
}
#REDB
variable "redb_db_allowed_cidr_blocks" {}
variable "redb_db_backup_retention_period" {
  #in days
  default = "7" }
variable "redb_db_name" {}
variable "redb_deletion_protection" {
  default = "true" }
variable "redb_db_engine" {default = "aurora-postgresql"
}
variable "redb_db_master_username" {
}
variable "redb_db_master_password" {}
variable "redb_db_backup_window" {
  default = "02:00-03:00"
}
variable "redb_db_maintenance_window" {
  default = "sun:03:00-sun:04:00"
}
variable "redb_db_scale_connections" {
  #average number of connections to trigger autoscale
  default = "700"
}
variable "redb_db_replica_scale_enabled" {
  default = "true"
}
variable "redb_db_replica_scale_in_cooldown" {
  #seconds after a scale up before can scale again
  default = "300"
}
variable "redb_db_replica_scale_out_cooldown" {
  #seconds after a scale down before can scale again
  default = "300"
}
variable "redb_db_replica_scale_max" {
  default = "3"
}
variable "redb_db_replica_scale_min" {
  default = "1"
}
variable "redb_tags" {
  default = [
  "redb",
  "STLRDA"]
}
