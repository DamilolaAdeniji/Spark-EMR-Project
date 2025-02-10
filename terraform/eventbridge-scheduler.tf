# Create EventBridge execution role and attach the IAM policy to the EventBridge execution role
resource "aws_iam_role" "eventbridge_execution_role" {
  name               = "eventbridge-execution-role"
  assume_role_policy = data.aws_iam_policy_document.eventbridge_trust_policy.json
}


# Attach the Step Function invocation policy to the created EventBridge execution role
resource "aws_iam_role_policy" "eventbridge_invoke_stfn_policy_attachment" {
  name   = "eventbridge-invoke-stfn-policy-attachment"
  role   = aws_iam_role.eventbridge_execution_role.name
  policy = data.aws_iam_policy_document.eventbridge_invoke_stfn_policy.json
}


# EventBridge Rule for scheduling the Step Function
resource "aws_cloudwatch_event_rule" "schedule_emr_step_function" {
  name                = "ScheduleEMRServerlessStepFunction"
  description         = "This EventBridge rule schedules the execution of Step Functions to manage EMR serverless."  
  schedule_expression = "cron(41 17 1 10 ? 2024)" ## replace with rate

}


# Add the Step Functions as the target for the EventBridge Rule
resource "aws_cloudwatch_event_target" "step_function_target" {
  rule      = aws_cloudwatch_event_rule.schedule_emr_step_function.name
  target_id = "StepFunctionTarget"
  arn       = aws_sfn_state_machine.emr_sfn_state_machine.arn  
  role_arn  = aws_iam_role.eventbridge_execution_role.arn  # IAM Role to allow EventBridge to invoke Step Functions
}