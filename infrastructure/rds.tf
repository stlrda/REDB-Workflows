module "db" {
  source = "terraform-aws-modules/rds-aurora/aws"
  version = "~> 2.0"

  name = var.redb_db_name
  apply_immediately = "true"
  backup_retention_period = var.redb_db_backup_retention_period
  database_name = var.redb_db_name
  deletion_protection = var.redb_deletion_protection
  engine = var.redb_db_engine
  instance_type = var.redb_db_instance_type
  username = var.redb_db_master_username
  password = var.redb_db_master_password
  preferred_backup_window = var.redb_db_backup_window
  preferred_maintenance_window = var.redb_db_maintenance_window
  replica_scale_connections = var.redb_db_scale_connections
  replica_scale_enabled = var.redb_db_replica_scale_enabled
  replica_scale_in_cooldown = var.redb_db_replica_scale_in_cooldown
  replica_scale_out_cooldown = var.redb_db_replica_scale_out_cooldown
  replica_scale_max = var.redb_db_replica_scale_max
  replica_scale_min = var.redb_db_replica_scale_min
  tags = var.redb_tags
  vpc_id = module.vpc.default_vpc_id
}
