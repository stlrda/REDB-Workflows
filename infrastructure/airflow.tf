module "airflow" {
  key_name=var.key_pair
  source="PowerDataHub/airflow/aws"
  version="0.9.2"
  cluster_name=var.cluster_name
  cluster_stage= var.cluster_stage
  db_password=var.db_password
  fernet_key=var.fernet_key
  db_dbname="airflow"
  s3_bucket_name=var.s3_log_bucket
}