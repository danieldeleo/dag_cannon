import os
from google.cloud import storage
import multiprocessing

def write_bad_dags(i):
    file_name = f"dags/bad{i}.py"
    content = f"""
import datetime

import airflow
from airflow import models
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

with models.DAG(
    dag_id="bad{i}",
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
    max_active_tasks=100,
    default_args={{
        "retries": 10,
        "retry_delay": datetime.timedelta(seconds=10),
    }},
) as dag:
    for x in range(10):
        task_id = f"bad{i}_{{x}}"
        tpt = KubernetesPodOperator(
            task_id=task_id,
            name="sleepy",
            cmds=["bash"],
            arguments=[
                "-c",
                rf\"\"\"
                set -e && \
                echo "Try number: $AIRFLOW_RETRY_NUMBER" && \
                echo "Sleeping for 5 minutes" && \
                sleep 5m
                \"\"\",
            ],
            env_vars={{"AIRFLOW_RETRY_NUMBER": "{{{{ task_instance.try_number }}}}"}},
            namespace="composer-user-workloads",
            image="teradata/tpt:latest",
            config_file="/home/airflow/composer_kube_config",
            kubernetes_conn_id="kubernetes_default",
            container_resources=k8s.V1ResourceRequirements(
                requests={{
                    "cpu": "100m",
                    "memory": "64Mi",
                }},
                limits={{
                    "cpu": "100m",
                    "memory": "64Mi",
                }},
            ),
            # Increase pod startup timeout to 10 minutes
            startup_timeout_seconds=600,
        )
"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(COMPOSER_BUCKET_NAME)
        blob = bucket.blob(file_name)
        blob.upload_from_string(content)

        return(f"Successfully wrote gs://{COMPOSER_BUCKET_NAME}/{file_name}")

    except Exception as e:
        print(f"Error writing to GCS: {e}")

def write_good_dags(i):
    file_name = f"dags/good{i}.py"
    content = f"""
import datetime

import airflow
from airflow import models
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

with models.DAG(
    dag_id="good{i}",
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
    max_active_tasks=100,
    default_args={{
        "retries": 10,
        "retry_delay": datetime.timedelta(seconds=10),
    }},
) as dag:
    for x in range(10):
        task_id = f"good{i}_{{x}}"
        tpt = KubernetesPodOperator(
            task_id=task_id,
            name="sleepy",
            cmds=["bash"],
            arguments=[
                "-c",
                rf\"\"\"
                set -e && \
                echo "Try number: $AIRFLOW_RETRY_NUMBER" && \
                echo "Sleeping for 5 minutes" && \
                sleep 5m
                \"\"\",
            ],
            env_vars={{"AIRFLOW_RETRY_NUMBER": "{{{{ task_instance.try_number }}}}"}},
            namespace="composer-user-workloads",
            image="teradata/tpt:latest",
            config_file="/home/airflow/composer_kube_config",
            kubernetes_conn_id="kubernetes_default",
            container_resources=k8s.V1ResourceRequirements(
                requests={{
                    "cpu": "100m",
                    "memory": "64Mi",
                }},
                limits={{
                    "cpu": "100m",
                    "memory": "64Mi",
                }},
            ),
            # Increase pod startup timeout to 10 minutes
            startup_timeout_seconds=600,
        )
"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(COMPOSER_BUCKET_NAME)
        blob = bucket.blob(file_name)
        blob.upload_from_string(content)

        return(f"Successfully wrote gs://{COMPOSER_BUCKET_NAME}/{file_name}")

    except Exception as e:
        print(f"Error writing to GCS: {e}")

COMPOSER_BUCKET_NAME = os.environ.get("COMPOSER_BUCKET_NAME")
print(f"COMPOSER_BUCKET_NAME: {COMPOSER_BUCKET_NAME}")

if __name__ == '__main__':
    with multiprocessing.Pool(processes=10) as pool:
        pool.map(write_good_dags, range(3000))
        # pool.map(write_bad_dags, range(4000))
    print("Done blasting DAGs into Composer!")