resource "aws_secretsmanager_secret" "github_token" {
  name        = local.github_secret_prefix
  description = "GitHub API token used by the GitPulse Ingestion Lambda"
  kms_key_id  = aws_kms_key.data_lake.key_id

  tags = {
    Name = "GitPulse GitHub Token"
  }
}

# Value is set OUTSIDE Terraform (never committed)
# `lifecycle.ignore_changes` stops terraform from ever reverting/overwriting
# the token you set manually via CLI/console after the secret is created
resource "aws_secretsmanager_secret_version" "github_token" {
  secret_id     = aws_secretsmanager_secret.github_token.id
  secret_string = jsonencode({ token = "REPLACE_ME_VIA_CLI" })

  lifecycle {
    ignore_changes = [secret_string]
  }
}
