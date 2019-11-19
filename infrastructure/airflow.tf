module "airflow" {
  key_name=var.key_pair
  private_key_path="/Users/jonathanleek/.ssh/jfl_airflow.pem"
  public_key_path="/Users/jonathanleek/.ssh/jfl_airflow.pub"
  source="PowerDataHub/airflow/aws"
  version="0.9.2"
  cluster_name=var.cluster_name
  cluster_stage= var.cluster_stage
  db_password=var.db_password
  fernet_key=var.fernet_key
  db_dbname="airflow"
  s3_bucket_name=var.s3_log_bucket
  load_example_dags = "true"
}