
module "db" {
  source  = "terraform-aws-modules/rds-aurora/aws"
  version = "~> 2.0"

  name = "redb"
  engine = "aurora-postgresql"

  replica_scale_enable = "true"
  replica_scale_min = "1"
  replica_scale_max = "4"
  replica_scale_cpu = "70"

  apply_immeadiate = "true"
  backup_retention_period =

}
