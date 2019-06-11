account:
    aws_account_id: 251584899486
    aws_lambda_role: lightweight_exec_role_1
    aws_region: eu-central-1


# lambda:
#     memory : 1536
#     timeout : 300
#     function_name : pywren1

s3:
    bucket: p-lightweight-175
    pywren_prefix: lightweight.jobs
    bucket_output: p-lightweight-10

# runtime:
#     s3_bucket: RUNTIME_BUCKET
#     s3_key: pywren.runtimes/default_3.6.meta.json

scheduler:
    map_item_limit: 10000

standalone:
    ec2_instance_type: m4.large
    sqs_queue_name: pywren-queue
    visibility: 10
    ec2_ssh_key : PYWREN_DEFAULT_KEY
    target_ami : TARGET_AMI
    instance_name: pywren-standalone
    instance_profile_name: pywren-standalone
    max_idle_time: 60
    idle_terminate_granularity: 3600
