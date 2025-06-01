provider "aws" {
  region  = "eu-central-1"
  profile = "personal_main"
}

resource "aws_instance" "ec2_instance_prod" {
  ami                         = "ami-05792a34356f4d8fb"
  instance_type               = "t2.medium"
  key_name                    = "ec2_key_pair_prod"
  subnet_id                   = "subnet-021d485e8eb620c6d"
  vpc_security_group_ids      = ["sg-0ec66ea9269d57981"] 
  associate_public_ip_address = true 

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