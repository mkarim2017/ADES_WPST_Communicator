provider "aws" {
  shared_credentials_file = var.shared_credentials_file
  region                  = var.region
  profile                 = var.profile
}

locals {
    request_queue_name = "${var.ades_basename}-wpst-request"
    response_queue_name = "${var.ades_basename}-wpst-response"
    jobs_queue_name = "${var.ades_basename}-wpst-jobs"
    metrics_queue_name = "${var.ades_basename}-metrics"
}


resource "aws_sqs_queue" "maap-hec-request-queue" {
  name = local.request_queue_name
}

resource "aws_sqs_queue" "maap-hec-response-queue" {
  name = local.response_queue_name
}

resource "aws_sqs_queue" "maap-hec-job-queue" {
  name = local.jobs_queue_name
}

resource "aws_sqs_queue" "maap-hec-metrics-queue" {
  name = local.metrics_queue_name
}

data "aws_iam_policy_document" "maap-hec-queue" {
  policy_id = "SQSDefaultPolicy"
  statement {
    actions = [
      "SQS:SendMessage"
    ]
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = ["*"]
    }
    sid = "Sid1571258347580"
  }
}

resource "aws_sqs_queue_policy" "maap-hec-request-queue" {
  depends_on = [aws_sqs_queue.maap-hec-request-queue, data.aws_iam_policy_document.maap-hec-queue]

  queue_url = aws_sqs_queue.maap-hec-request-queue.url
  policy = data.aws_iam_policy_document.maap-hec-queue.json
}

resource "aws_sqs_queue_policy" "maap-hec-response-queue" {
  depends_on = [aws_sqs_queue.maap-hec-response-queue, data.aws_iam_policy_document.maap-hec-queue]

  queue_url = aws_sqs_queue.maap-hec-response-queue.url
  policy = data.aws_iam_policy_document.maap-hec-queue.json
}

resource "aws_sqs_queue_policy" "maap-hec-job-queue" {
  depends_on = [aws_sqs_queue.maap-hec-job-queue, data.aws_iam_policy_document.maap-hec-queue]

  queue_url = aws_sqs_queue.maap-hec-job-queue.url
  policy = data.aws_iam_policy_document.maap-hec-queue.json
}

resource "aws_sqs_queue_policy" "maap-hec-metrics-queue" {
  depends_on = [aws_sqs_queue.maap-hec-metrics-queue, data.aws_iam_policy_document.maap-hec-queue]

  queue_url = aws_sqs_queue.maap-hec-metrics-queue.url
  policy = data.aws_iam_policy_document.maap-hec-queue.json
}


