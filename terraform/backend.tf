terraform {
  backend "s3" {
    bucket         = "emr-serverless-terraform-backend" 
    key            = "eu-north-1/state-files/terraform.tfstate"
    region         = "eu-north-1"
    encrypt        = true
  }
}