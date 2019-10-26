region = "us-east-2"
vpc_name = "stl-rda-ckan"
//at least three vpc_availability_zones
vpc_availability_zones = [
  "us-east-2a",
  "us-east-2b",
  "us-east-2c"
]
hosted_zone = ""
rds_username = ""
rds_password = ""
admin-cidr-blocks = [
  "134.124.130.148/32",
  "162.207.120.61/32",
  "131.207.245.53/32",
  "134.124.27.64/32"
]
ckan_admin = ""
ckan_admin_password = ""