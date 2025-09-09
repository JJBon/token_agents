resource "aws_db_subnet_group" "this" {
  name       = "${var.name}-subnets"
  subnet_ids = var.subnet_ids
}

resource "aws_security_group" "this" {
  name   = "${var.name}-sg"
  vpc_id = var.vpc_id
  dynamic "ingress" {
    for_each = var.allowed_cidrs
    content {
      from_port   = 5432
      to_port     = 5432
      protocol    = "tcp"
      cidr_blocks = [ingress.value]
    }
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_rds_cluster" "this" {
  cluster_identifier          = var.name
  engine                      = "aurora-postgresql"
  engine_version              = var.engine_version
  database_name               = var.db_name
  master_username             = var.master_username
  db_subnet_group_name        = aws_db_subnet_group.this.name
  vpc_security_group_ids      = [aws_security_group.this.id]
  enable_http_endpoint       = var.enable_data_api
  skip_final_snapshot = true
  manage_master_user_password = true # creates Secrets Manager secret
  serverlessv2_scaling_configuration {
    min_capacity = var.min_acu
    max_capacity = var.max_acu
  }
}

resource "aws_rds_cluster_instance" "writer" {
  identifier         = "${var.name}-writer"
  cluster_identifier = aws_rds_cluster.this.id
  engine             = aws_rds_cluster.this.engine
  engine_version     = aws_rds_cluster.this.engine_version
  instance_class     = "db.serverless"
}


output "cluster_arn" { value = aws_rds_cluster.this.arn }
output "secret_arn" { value = aws_rds_cluster.this.master_user_secret[0].secret_arn }
output "db_name" { value = var.db_name }
output "security_group_id" { value = aws_security_group.this.id }