data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  availability_zones = slice(data.aws_availability_zones.available.names, 0, var.availability_zone_count)
  nat_gateway_count  = var.nat_gateway_per_az ? var.availability_zone_count : 1
}

resource "aws_vpc" "cex" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${local.resource_name}-vpc"
  }
}

resource "aws_internet_gateway" "cex" {
  vpc_id = aws_vpc.cex.id

  tags = {
    Name = "${local.resource_name}-igw"
  }
}

resource "aws_subnet" "public" {
  count = var.availability_zone_count

  vpc_id                  = aws_vpc.cex.id
  availability_zone       = local.availability_zones[count.index]
  cidr_block              = cidrsubnet(var.vpc_cidr, 4, count.index)
  map_public_ip_on_launch = false

  tags = {
    Name = "${local.resource_name}-public-${local.availability_zones[count.index]}"
    Tier = "public"
  }
}

resource "aws_subnet" "private" {
  count = var.availability_zone_count

  vpc_id            = aws_vpc.cex.id
  availability_zone = local.availability_zones[count.index]
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, count.index + var.availability_zone_count)

  tags = {
    Name = "${local.resource_name}-private-${local.availability_zones[count.index]}"
    Tier = "private"
  }
}

resource "aws_eip" "nat" {
  count = local.nat_gateway_count

  domain = "vpc"

  tags = {
    Name = "${local.resource_name}-nat-${count.index + 1}"
  }

  depends_on = [aws_internet_gateway.cex]
}

resource "aws_nat_gateway" "cex" {
  count = local.nat_gateway_count

  allocation_id = aws_eip.nat[count.index].id
  subnet_id     = aws_subnet.public[count.index].id

  tags = {
    Name = "${local.resource_name}-nat-${local.availability_zones[count.index]}"
  }

  depends_on = [aws_internet_gateway.cex]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.cex.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.cex.id
  }

  tags = {
    Name = "${local.resource_name}-public"
  }
}

resource "aws_route_table_association" "public" {
  count = var.availability_zone_count

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  count = var.availability_zone_count

  vpc_id = aws_vpc.cex.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.cex[var.nat_gateway_per_az ? count.index : 0].id
  }

  tags = {
    Name = "${local.resource_name}-private-${local.availability_zones[count.index]}"
  }
}

resource "aws_route_table_association" "private" {
  count = var.availability_zone_count

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[count.index].id
}
