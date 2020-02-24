/*
# vpc

A terraform module to create a vpc
*/

module "vpc" {
  source = "terraform-aws-modules/vpc/aws"

  name = "${var.airflow_vpc_name}"

  cidr = "10.10.0.0/16"

  azs = var.airflow_vpc_availability_zones
  private_subnets = [
    "10.10.1.0/24",
    "10.10.2.0/24",
    "10.10.3.0/24"]
  public_subnets = [
    "10.10.11.0/24",
    "10.10.12.0/24",
    "10.10.13.0/24",]
  database_subnets = [
    "10.10.21.0/24",
    "10.10.22.0/24",
    "10.10.23.0/24"]

  enable_dns_hostnames = true
  enable_dns_support = true
  map_public_ip_on_launch = true

  enable_nat_gateway = true
  single_nat_gateway = true

  create_database_subnet_group = true
  create_database_subnet_route_table = true
  create_database_internet_gateway_route = true


  tags = {
    Project = "redb"
    Organization = "STLRDA"
  }

}