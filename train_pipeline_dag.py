#!/usr/bin/env python
import datetime as dt
import os
import sys
from datetime import timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import \
    KubernetesPodOperator

SRC_HOME = os.path.dirname(os.path.realpath(__file__))
if SRC_HOME not in sys.path:
    sys.path.append(SRC_HOME)

from config import config as conf
from config import set_env_minIO
from kubernetes.client import models as k8s

with DAG(
    dag_id=f"{conf['proj_name']}-train-pipeline",
    default_args={
        "retries": 1,
        "retry_delay": dt.timedelta(minutes=2)
    },
    schedule_interval=conf["train_schedule_interval"],
    dagrun_timeout=timedelta(minutes=60),
    description=f"{conf['proj_name']} - train pipeline",
    start_date=dt.datetime.strptime(conf['train_start_date'], conf['date_format'])
) as dag:

    git_volume_name = "git-sync-volume"

    git_sync_volume = k8s.V1Volume(
        name=git_volume_name,
        empty_dir=k8s.V1EmptyDirVolumeSource()
    )
    
    volume_mounts = [
        k8s.V1VolumeMount(mount_path=conf['git_mount_root_path'], name=git_volume_name, sub_path=None, read_only=False),
    ]


    init_environments = [k8s.V1EnvVar(name="GIT_SYNC_REPO", value=conf['git_repo']),
                         k8s.V1EnvVar(name="GIT_SYNC_ROOT", value=conf['git_mount_root_path']),
                         k8s.V1EnvVar(name="GIT_SYNC_DEST", value=conf['proj_name']),
                         k8s.V1EnvVar(name="GIT_SYNC_BRANCH", value=conf['git_branch']),
                         k8s.V1EnvVar(name="GIT_SYNC_ONE_TIME", value="true")]

                        
    env_vars = [k8s.V1EnvVar(name="MLFLOW_TRACKING_URI", value=conf["mlflow_tracking_uri"]),
                k8s.V1EnvVar(name="MLFLOW_REGISTRY_URI", value=conf["mlflow_registry_uri"]),
                k8s.V1EnvVar(name="MINIO_AWS_BUCKET_NAME", value=conf["minio_aws_bucket_name"]),
                k8s.V1EnvVar(name="MINIO_AWS_ACCESS_KEY_ID", value=conf["minio_aws_access_key_id"]),
                k8s.V1EnvVar(name="MINIO_AWS_SECRET_ACCESS_KEY", value=conf["minio_aws_secret_access_key"]),
                k8s.V1EnvVar(name="MINIO_MLFLOW_S3_ENDPOINT_URL", value=conf["minio_mlflow_s3_endpoint_url"])]


    init_container = k8s.V1Container(
        name="init-container",
        image=f"{conf['git_sync_image_repository']}:{conf['git_sync_image_tag']}",
        image_pull_policy="IfNotPresent",
        volume_mounts=volume_mounts,
        env=init_environments
    )

    train_pipeline_container = KubernetesPodOperator(
        namespace="airflow-bitnami",
        image=f"{conf['train_pipeline_image_repository']}:{conf['train_pipeline_image_tag']}",
        init_containers=[init_container],
        arguments=["python", os.path.join(conf['git_mount_root_path'], conf['proj_name'], "src", "train.py")],
        labels={"foo": "bar"},
        name=f"{conf['proj_name']}-train-pipeline",
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id=f"{conf['proj_name']}-train-pipeline",
        volumes=[git_sync_volume],
        volume_mounts=volume_mounts,
        env_vars=env_vars,
        dag=dag
    )

    train_pipeline_container