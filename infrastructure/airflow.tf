module "airflow" {
  key_name=var.key_pair
  private_key_path=var.private_keypath
  public_key_path=var.public_keypath
  source="PowerDataHub/airflow/aws"
  version="0.9.2"
  cluster_name=var.cluster_name
  cluster_stage= var.cluster_stage
  db_password=var.db_password
  fernet_key=var.fernet_key
  db_dbname="airflow"
  s3_bucket_name=var.s3_log_bucket
  load_example_dags = "true"
  rbac = "true"
  admin_email = var.admin_email
  admin_lastname = var.admin_lastname
  admin_name = var.admin_name
  admin_password = var.admin_password
  admin_username = var.admin_username
}