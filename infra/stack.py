"""AWS foundation resources for the MovieLens mock project."""

from __future__ import annotations

import re

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_athena as athena,
    aws_budgets as budgets,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct


class MovieLensFoundationStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs: object) -> None:
        super().__init__(scope, construct_id, **kwargs)

        run_id = str(self.node.try_get_context("run_id") or "movielens_t2013_daily_v2")
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", run_id):
            raise ValueError("run_id may contain only letters, numbers, underscores, and hyphens")
        bucket_name = self.node.try_get_context("bucket_name")
        budget_limit = float(self.node.try_get_context("monthly_budget_usd") or 25)
        alert_email = self.node.try_get_context("budget_alert_email")
        scan_limit_mb = int(self.node.try_get_context("athena_scan_limit_mb") or 1024)
        prefix = f"runs/{run_id}"

        bucket_props: dict[str, object] = {
            "block_public_access": s3.BlockPublicAccess.BLOCK_ALL,
            "encryption": s3.BucketEncryption.S3_MANAGED,
            "enforce_ssl": True,
            "object_ownership": s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            "versioned": True,
            "removal_policy": RemovalPolicy.RETAIN,
            "auto_delete_objects": False,
            "lifecycle_rules": [
                s3.LifecycleRule(
                    id="ExpireNoncurrentVersions",
                    noncurrent_version_expiration=Duration.days(90),
                )
            ],
        }
        if bucket_name:
            bucket_props["bucket_name"] = str(bucket_name)
        self.data_bucket = s3.Bucket(self, "DataLakeBucket", **bucket_props)  # type: ignore[arg-type]

        database_name = f"movielens_{re.sub('[^a-z0-9_]', '_', run_id.lower())}"
        database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(name=database_name),
        )

        # The Glue runtime role can read landing data and maintain only this run's
        # lake prefixes. AWSGlueServiceRole supplies Glue's runtime/CloudWatch/ENI
        # permissions; the inline S3 and catalog permissions remain prefix-scoped.
        glue_role = iam.Role(
            self,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description=f"Glue processing role for {run_id}",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ],
        )
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket", "s3:ListBucketMultipartUploads"],
                resources=[self.data_bucket.bucket_arn],
                conditions={"StringLike": {"s3:prefix": [f"{prefix}/*"]}},
            )
        )
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetBucketLocation"],
                resources=[self.data_bucket.bucket_arn],
            )
        )
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:GetObjectVersion"],
                resources=[
                    self.data_bucket.arn_for_objects(f"{prefix}/landing/*"),
                    self.data_bucket.arn_for_objects(f"{prefix}/jobs/*"),
                ],
            )
        )
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                    "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
                ],
                resources=[
                    self.data_bucket.arn_for_objects(f"{prefix}/bronze/*"),
                    self.data_bucket.arn_for_objects(f"{prefix}/silver/*"),
                    self.data_bucket.arn_for_objects(f"{prefix}/gold/*"),
                    self.data_bucket.arn_for_objects(f"{prefix}/quarantine/*"),
                    self.data_bucket.arn_for_objects(f"{prefix}/control/*"),
                ],
            )
        )
        catalog_arns = [
            f"arn:{self.partition}:glue:{self.region}:{self.account}:catalog",
            f"arn:{self.partition}:glue:{self.region}:{self.account}:database/{database_name}",
            f"arn:{self.partition}:glue:{self.region}:{self.account}:table/{database_name}/*",
        ]
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetDatabase", "glue:GetDatabases", "glue:CreateTable",
                    "glue:GetTable", "glue:GetTables", "glue:UpdateTable",
                    "glue:DeleteTable", "glue:BatchCreatePartition",
                    "glue:BatchGetPartition", "glue:CreatePartition",
                    "glue:UpdatePartition", "glue:DeletePartition",
                ],
                resources=catalog_arns,
            )
        )
        glue_role.node.add_dependency(database)

        bronze_job = glue.CfnJob(
            self,
            "BronzeIngestionJob",
            name=f"cineinsight-{run_id}-bronze",
            description=f"Ingest one manifest-listed MovieLens file into Bronze for {run_id}",
            role=glue_role.role_arn,
            glue_version="5.0",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{self.data_bucket.bucket_name}/{prefix}/jobs/bronze_ingest.py",
            ),
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            worker_type="G.1X",
            number_of_workers=2,
            timeout=120,
            max_retries=1,
            default_arguments={
                "--run_id": run_id,
                "--database_name": database_name,
                "--warehouse_uri": f"s3://{self.data_bucket.bucket_name}/{prefix}/bronze/",
                "--datalake-formats": "iceberg",
                "--enable-glue-datacatalog": "true",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--job-bookmark-option": "job-bookmark-disable",
                "--TempDir": f"s3://{self.data_bucket.bucket_name}/{prefix}/control/glue-temp/",
                "--conf": (
                    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                    "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog "
                    f"--conf spark.sql.catalog.glue_catalog.warehouse=s3://{self.data_bucket.bucket_name}/{prefix}/bronze/ "
                    "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
                    "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
                ),
            },
        )
        bronze_job.node.add_dependency(database)

        results_location = f"s3://{self.data_bucket.bucket_name}/{prefix}/athena-results/"
        workgroup = athena.CfnWorkGroup(
            self,
            "AthenaWorkGroup",
            name=f"cineinsight-{re.sub('[^a-z0-9-]', '-', run_id.lower())[:50]}",
            description=f"Athena workgroup for {run_id}",
            state="ENABLED",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics_enabled=True,
                bytes_scanned_cutoff_per_query=scan_limit_mb * 1024 * 1024,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=results_location,
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3"
                    ),
                ),
            ),
        )

        # This role is assumable by principals in this account. Grant it to
        # project users through IAM Identity Center or an account-specific policy.
        athena_role = iam.Role(
            self,
            "AthenaQueryRole",
            assumed_by=iam.AccountPrincipal(self.account),
            description=f"Read MovieLens curated data and query it in {run_id}",
        )
        athena_role.add_to_policy(
            iam.PolicyStatement(
                actions=["athena:StartQueryExecution", "athena:StopQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:GetWorkGroup", "athena:ListQueryExecutions"],
                resources=[f"arn:{self.partition}:athena:{self.region}:{self.account}:workgroup/{workgroup.ref}"],
            )
        )
        athena_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket", "s3:ListBucketMultipartUploads"],
                resources=[self.data_bucket.bucket_arn],
                conditions={"StringLike": {"s3:prefix": [f"{prefix}/bronze/*", f"{prefix}/silver/*", f"{prefix}/gold/*", f"{prefix}/athena-results/*"]}},
            )
        )
        athena_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetBucketLocation"],
                resources=[self.data_bucket.bucket_arn],
            )
        )
        athena_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts",
                ],
                resources=[
                    self.data_bucket.arn_for_objects(f"{prefix}/bronze/*"),
                    self.data_bucket.arn_for_objects(f"{prefix}/silver/*"),
                    self.data_bucket.arn_for_objects(f"{prefix}/gold/*"),
                    self.data_bucket.arn_for_objects(f"{prefix}/athena-results/*"),
                ],
            )
        )
        athena_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartition", "glue:GetPartitions"],
                resources=catalog_arns,
            )
        )

        # These are the project-specific permissions for MWAA DAG tasks. Add
        # MWAA's baseline environment permissions before using this as an
        # environment execution role.
        mwaa_role = iam.Role(
            self,
            "OrchestrationRole",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("airflow.amazonaws.com"),
                iam.ServicePrincipal("airflow-env.amazonaws.com"),
            ),
            description=f"Project-specific orchestration permissions for {run_id}",
        )
        mwaa_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"],
                resources=[f"arn:{self.partition}:glue:{self.region}:{self.account}:job/cineinsight-{run_id}-*"],
            )
        )
        mwaa_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket"],
                resources=[self.data_bucket.bucket_arn],
                conditions={"StringLike": {"s3:prefix": [f"{prefix}/control/*", f"{prefix}/landing/*"]}},
            )
        )
        mwaa_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject"],
                resources=[
                    self.data_bucket.arn_for_objects(f"{prefix}/control/*"),
                    self.data_bucket.arn_for_objects(f"{prefix}/landing/*"),
                ],
            )
        )
        mwaa_role.add_to_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:GetLogEvents", "logs:DescribeLogGroups", "logs:DescribeLogStreams"],
                resources=[f"arn:{self.partition}:logs:{self.region}:{self.account}:log-group:airflow-cineinsight-{run_id}-*:*"] ,
            )
        )

        notifications: list[budgets.CfnBudget.NotificationWithSubscribersProperty] = []
        if alert_email:
            notifications = [
                budgets.CfnBudget.NotificationWithSubscribersProperty(
                    notification=budgets.CfnBudget.NotificationProperty(
                        notification_type="ACTUAL",
                        comparison_operator="GREATER_THAN",
                        threshold=threshold,
                        threshold_type="PERCENTAGE",
                    ),
                    subscribers=[budgets.CfnBudget.SubscriberProperty(
                        address=str(alert_email), subscription_type="EMAIL"
                    )],
                )
                for threshold in (50, 80, 100)
            ]
        budgets.CfnBudget(
            self,
            "MonthlyBudget",
            budget=budgets.CfnBudget.BudgetDataProperty(
                budget_name=f"cineinsight-{run_id}-monthly",
                budget_type="COST",
                time_unit="MONTHLY",
                budget_limit=budgets.CfnBudget.SpendProperty(amount=budget_limit, unit="USD"),
            ),
            notifications_with_subscribers=notifications,
        )

        CfnOutput(self, "BucketName", value=self.data_bucket.bucket_name)
        CfnOutput(self, "RunPrefix", value=f"s3://{self.data_bucket.bucket_name}/{prefix}/")
        CfnOutput(self, "GlueDatabaseName", value=database_name)
        CfnOutput(self, "AthenaWorkGroupName", value=workgroup.name)
        CfnOutput(self, "GlueJobRoleArn", value=glue_role.role_arn)
        CfnOutput(self, "BronzeJobName", value=bronze_job.name or bronze_job.ref)
        CfnOutput(
            self,
            "BronzeScriptUri",
            value=f"s3://{self.data_bucket.bucket_name}/{prefix}/jobs/bronze_ingest.py",
        )
        CfnOutput(self, "AthenaQueryRoleArn", value=athena_role.role_arn)
        CfnOutput(self, "OrchestrationRoleArn", value=mwaa_role.role_arn)
