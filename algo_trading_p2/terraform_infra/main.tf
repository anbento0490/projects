
# 1. Terraform Config 
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }
  required_version = ">= 1.2.0"
}
################################
# 2. AWS Provider Config
provider "aws" {
  region  = "eu-central-1"
  profile = "default"
}
################################
# 3. AMI Data Source
data "aws_ami" "latest_amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}
################################
# 4. EC2 Instance Resource Definition
resource "aws_instance" "ec2_instance_prod" {
  ami                         = data.aws_ami.latest_amazon_linux.id
  instance_type               = "t2.medium"
  key_name                    = "ec2_key_pair_prod"
  # subnet_id                   = "xxxxx"
  # vpc_security_group_ids      = "xxxxx"

  root_block_device {
    delete_on_termination = true
    volume_size           = 8
    volume_type           = "gp2"
  }
  user_data = <<-EOF
    #!/bin/bash
    amazon-linux-extras install docker -y
    service docker start
    usermod -a -G docker ec2-user
  EOF
}# main.tf 
# 1. Terraform Config 
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }
  required_version = ">= 1.2.0"
}
################################
# 2. AWS Provider Config
provider "aws" {
  region  = "eu-central-1"
  profile = "default"
}
################################
# 3. AMI Data Source
data "aws_ami" "latest_amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}
################################
# 4. EC2 Instance Resource Definition
resource "aws_instance" "ec2_instance_prod" {
  ami                         = data.aws_ami.latest_amazon_linux.id
  instance_type               = "t2.medium"
  key_name                    = "ec2_key_pair_prod"
  # subnet_id                   = "xxxxx"
  # vpc_security_group_ids      = "xxxxx"

  root_block_device {
    delete_on_termination = true
    volume_size           = 8
    volume_type           = "gp2"
  }
  user_data = <<-EOF
    #!/bin/bash
    amazon-linux-extras install docker -y
    service docker start
    usermod -a -G docker ec2-user
  EOF
}