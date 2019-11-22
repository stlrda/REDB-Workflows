module "airflow" {
  key_name=var.airflow_key_pair
  private_key_path=var.airflow_private_keypath
  public_key_path=var.airflow_public_keypath
  source="PowerDataHub/airflow/aws"
  version="0.9.2"
  cluster_name=var.airflow_cluster_name
  cluster_stage= var.airflow_cluster_stage
  db_password=var.airflow_db_password
  fernet_key=var.airflow_fernet_key
  db_dbname="airflow"
  s3_bucket_name=var.airflow_s3_log_bucket
  load_example_dags = "true"
  rbac = "true"
  admin_email = var.airflow_admin_email
  admin_lastname = var.airflow_admin_lastname
  admin_name = var.airflow_admin_name
  admin_password = var.airflow_admin_password
  admin_username = var.airflow_admin_username
}
