//This file defines AWS as the cloud provider we will be using.
//CHANGING THIS IS NOT SUFFICIENT TO SWITCH TO A DIFFERENT PROVIDER

provider "aws" {
  region = var.region
}
