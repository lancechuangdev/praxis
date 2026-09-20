resource "aws_appautoscaling_target" "order" {
  count = var.order_image_digest == null ? 0 : 1

  service_namespace  = "ecs"
  scalable_dimension = "ecs:service:DesiredCount"
  resource_id        = "service/${aws_ecs_cluster.this.name}/${aws_ecs_service.order[0].name}"
  min_capacity       = 1
  max_capacity       = 3
}

resource "aws_appautoscaling_policy" "order_cpu" {
  count = var.order_image_digest == null ? 0 : 1

  name               = "${local.resource_name}-order-cpu"
  policy_type        = "TargetTrackingScaling"
  service_namespace  = aws_appautoscaling_target.order[0].service_namespace
  scalable_dimension = aws_appautoscaling_target.order[0].scalable_dimension
  resource_id        = aws_appautoscaling_target.order[0].resource_id

  target_tracking_scaling_policy_configuration {
    target_value       = 60
    scale_out_cooldown = 60
    scale_in_cooldown  = 120

    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
  }
}
