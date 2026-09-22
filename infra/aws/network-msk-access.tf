resource "aws_security_group" "msk" {
  name_prefix = "${local.resource_name}-msk-"
  description = "Amazon MSK broker access for CEX services"
  vpc_id      = aws_vpc.cex.id

  egress {
    description = "Allow broker outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "cex_clients" {
  name_prefix = "${local.resource_name}-clients-"
  description = "CEX application services such as the ledger service and outbox relay"
  vpc_id      = aws_vpc.cex.id

  egress {
    description = "Allow CEX services to reach dependencies and AWS APIs"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "msk_iam_clients" {

  security_group_id            = aws_security_group.msk.id
  referenced_security_group_id = aws_security_group.cex_clients.id
  description                  = "Kafka IAM/TLS from CEX application services"
  ip_protocol                  = "tcp"
  from_port                    = 9098
  to_port                      = 9098
}

resource "aws_vpc_security_group_ingress_rule" "msk_broker_internal" {
  security_group_id            = aws_security_group.msk.id
  referenced_security_group_id = aws_security_group.msk.id
  description                  = "Broker-to-broker traffic"
  ip_protocol                  = "-1"
}
