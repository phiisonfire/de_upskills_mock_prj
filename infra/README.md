# AWS foundation (AWS CDK)

This CDK stack defines the deployable AWS foundation for `movielens_t2013_daily_v2`:

- One private, versioned, SSE-S3 encrypted S3 lake bucket with public access blocked and TLS required.
- Run-scoped S3 key layout, represented by prefixes under `runs/<run_id>/`.
- A Glue Data Catalog database and scoped Glue job role.
- An Athena workgroup with encrypted results, CloudWatch metrics, enforced settings, and a scan limit.
- An Athena query role and project-specific permissions for an MWAA DAG role.
- A monthly AWS Budget. Email alerts are configured when an address is supplied.

The stack does not create or upload the dataset, Glue ETL jobs, an MWAA environment, or a CloudTrail trail. The orchestration role contains permissions for a DAG to start/poll Glue and read/write run control records; add MWAA's baseline environment permissions before associating it with an MWAA environment. The Glue role has CloudWatch permissions through `AWSGlueServiceRole`; configure an account-level CloudTrail trail if the course requires an audit trail for S3 object access. S3 prefixes appear as keys when objects are uploaded; they are not standalone directories.

The stack also defines a Glue 5.0 Bronze ingestion job. It processes one file per run, keeps source columns as strings, checks that the file checksum and size match the manifest, and adds source lineage including a 1-based data row number (excluding the CSV header). An Iceberg merge key makes retries idempotent. A file-level control table records status and row-count reconciliation. The job script itself must be uploaded to S3 after the stack is deployed.

## Prerequisites

- An AWS account and a configured local AWS identity (AWS CLI profile or IAM Identity Center).
- AWS CLI version 2 (the SSO profile setup below requires CLI v2).
- Node.js and the AWS CDK v2 command line (`npm install -g aws-cdk`).
- Python 3.10 or newer.

Do not put AWS access keys or session tokens in `.env` or commit them. Use AWS IAM Identity Center (recommended) or a named AWS CLI profile; the actual credentials stay in the local AWS CLI files outside the repository. `.env` should contain only non-secret selectors such as `AWS_PROFILE` and `AWS_REGION`.

If AWS CLI v1 is installed, AWS CLI v2 can be installed alongside it for your user using AWS's Linux installer:

```bash
curl -fsSL https://awscli.amazonaws.com/v2/install.sh | bash
~/.local/bin/aws --version
```

From the repository root, configure an SSO profile once, then create your local `.env` from the template:

```bash
~/.local/bin/aws configure sso --profile movielens-dev
~/.local/bin/aws sso login --profile movielens-dev
cp .env.example .env
```

Edit `.env` if you used a different profile or region. Load those settings in the current shell before running CDK:

```bash
set -a
source .env
set +a
```

Use `~/.local/bin/aws` for SSO setup if the system `aws` command still resolves to v1. Then continue with the install, bootstrap, and deploy commands below. The existing `.gitignore` excludes `.env`; keep it that way. CDK reads credentials from the AWS CLI credential chain and uses `AWS_PROFILE` to select the profile; it does not load `.env` by itself.

## Configure

Edit `context` in `cdk.json` before deployment:

- `region`: region for the stack and data lake.
- `bucket_name`: leave empty to let CloudFormation generate a globally unique bucket name, or set an unused globally unique name.
- `monthly_budget_usd`: monthly account cost threshold in USD.
- `budget_alert_email`: email address to receive budget alerts. Leave empty to deploy the budget without email notifications.
- `athena_scan_limit_mb`: maximum bytes Athena may scan in one query.

The budget covers account-level cost rather than only these resources. Budget notifications are advisory and do not stop resources or spending.

## Bootstrap and deploy

From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r infra/requirements.txt
cd infra
cdk bootstrap aws://<ACCOUNT_ID>/<REGION>
cdk synth
cdk diff
cdk deploy
```

The `cdk diff` output is the review point before deployment. The bucket has a retain policy and is not automatically emptied or deleted by stack removal.

After deployment, confirm the stack outputs and verify the bucket, Glue database, and Athena workgroup in the AWS console. Budget email subscriptions require recipient confirmation. To let a project user assume `AthenaQueryRole`, grant that IAM Identity Center user/group permission to assume the output role ARN.

## Upload this run

From the repository root, after reading the `BucketName` stack output:

```bash
~/.local/bin/aws s3 cp data/simulated/movielens_t2013_daily_v2/manifest.json \
  s3://<BUCKET_NAME>/runs/movielens_t2013_daily_v2/landing/movielens/manifest.json
~/.local/bin/aws s3 sync data/simulated/movielens_t2013_daily_v2/landing/movielens/ \
  s3://<BUCKET_NAME>/runs/movielens_t2013_daily_v2/landing/movielens/
```

This copies the provenance snapshots too. The ingestion code must use the manifest's `arrival_files` and bootstrap snapshots, while treating `source_snapshot/rating.csv` and `source_snapshot/tag.csv` as provenance only. Verify uploaded sizes and SHA-256 checksums against `manifest.json` before processing.

## Run one Bronze file

Upload the Glue script to the location configured on the job:

```bash
~/.local/bin/aws s3 cp infra/jobs/bronze_ingest.py \
  s3://<BUCKET_NAME>/runs/movielens_t2013_daily_v2/jobs/bronze_ingest.py
```

Start with the small `rating/batch_0001` file. Replace `<BUCKET_NAME>` with the `BucketName` stack output:

```bash
~/.local/bin/aws glue start-job-run \
  --job-name cineinsight-movielens_t2013_daily_v2-bronze \
  --arguments '{"--source_table":"rating","--batch_id":"batch_0001","--input_uri":"s3://<BUCKET_NAME>/runs/movielens_t2013_daily_v2/landing/movielens/arrivals/rating/batch_id=batch_0001/00000000.csv"}'
```

Use the same command with the matching `source_table`, `batch_id`, and S3 URI for each later file. The job accepts manifest-listed event arrivals, movie CDC files, and bootstrap snapshots; it rejects the full rating and tag provenance snapshots. The rating/tag `duplicate_delivery` files are separate deliveries and are retained as such in Bronze for downstream deduplication. Check the Glue run logs and the `bronze_ingestion_control` Iceberg table for `SUCCEEDED` and `RECONCILED`, and compare `actual_row_count` with the manifest. Retry a failed run with the same arguments; the merge key prevents a repeated file from adding the same source rows twice.

## Run Silver DQ and incremental merges

The stack defines a Glue 5.0 Silver job that reads exactly one `source_table`, `batch_id`, and `landing_uri` from Bronze. It creates Iceberg Silver tables in the Glue Catalog, stores row-level rejects in `quarantine/`, and stores DQ results and per-file status in `control/`. The job retains source timestamps as `timestamp_ntz` plus their raw strings; it does not assign UTC semantics. Re-running a successfully completed file is a no-op. A blocking DQ failure is quarantined and recorded before the job exits unsuccessfully. Row-level blank user tags are quarantined as a warning and do not stop the batch.

Upload the Silver script after deploying the stack:

```bash
~/.local/bin/aws s3 cp infra/jobs/silver_merge.py \
  s3://<BUCKET_NAME>/runs/movielens_t2013_daily_v2/jobs/silver_merge.py
```

Silver event references require the content and taxonomy bootstraps first. Run Bronze and then Silver in this order:

1. `movie` snapshot (`batch_0000_history`)
2. `genome_tags` snapshot (`batch_0000_history`)
3. `link` snapshot (`batch_0000_history`)
4. `genome_scores` snapshot (`batch_0000_history`)
5. Rating and tag history slices and each arrival file, using each manifest-listed `batch_id` and exact Landing URI
6. `movie_cdc` batches `batch_0001` through `batch_0004` in order

For each Bronze file, first run the Bronze command above. Then run Silver with the exact same source table, batch ID, and input URI as `landing_uri`. For example, once `movie` has been loaded to Bronze:

```bash
~/.local/bin/aws glue start-job-run \
  --job-name cineinsight-movielens_t2013_daily_v2-silver \
  --arguments '{"--source_table":"movie","--batch_id":"batch_0000_history","--landing_uri":"s3://<BUCKET_NAME>/runs/movielens_t2013_daily_v2/landing/movielens/source_snapshot/movie.csv"}'
```

Use the `arrival_files` and bootstrap entries in `data/simulated/movielens_t2013_daily_v2/manifest.json` to enumerate inputs. Treat `source_snapshot/rating.csv` and `source_snapshot/tag.csv` as provenance only: process their split history slices and arrival files instead. Preserve `batch_0002` late arrivals as separate inputs; Silver merges use stable source event keys and hashes, so an exact repeated delivery is ignored while a conflicting payload is quarantined. The Silver tables `silver_batch_control`, `silver_dq_result`, and `silver_quarantine` record outcomes and row-level reasons. A failed Glue run with blocking DQ must be resolved before treating its batch as complete.

## Clean up

To remove IAM roles, the Glue database, Athena workgroup, and budget while retaining the data bucket:

```bash
cdk destroy
```

The bucket uses `RETAIN`; inspect and explicitly remove its contents and bucket in the S3 console only when the data is no longer needed. Versioned object data must be fully deleted, including prior versions, before S3 will allow bucket deletion.
