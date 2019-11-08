import datetime as dt
from airflow import DAG, models
from airflow.contrib.operators import dataproc_operator as dpo
from airflow.utils import trigger_rule

MAIN_JAR = 'file:///usr/lib/spark/examples/jars/spark-examples.jar'
MAIN_CLASS = 'org.apache.spark.examples.SparkPi'
CLUSTER_NAME = 'quickspark-cluster-{{ ds_nodash }}'

yesterday = dt.datetime.combine(
    dt.datetime.today() - dt.timedelta(1),
    dt.datetime.min.time())

default_dag_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(seconds=30),
    'project_id': models.Variable.get('gcp_project')
}

with DAG('dataproc_spark_submit', schedule_interval='0 17 * * *',
    default_args=default_dag_args) as dag:

    create_dataproc_cluster = dpo.DataprocClusterCreateOperator(
        project_id = default_dag_args['project_id'],
        task_id = 'create_dataproc_cluster',
        cluster_name = CLUSTER_NAME,
        num_workers = 2,
        zone = models.Variable.get('gce_zone')
    )

    run_spark_job = dpo.DataProcSparkOperator(
        task_id = 'run_spark_job',
        #main_jar = MAIN_JAR,
        main_class = MAIN_CLASS,
        cluster_name = CLUSTER_NAME
    )

    delete_dataproc_cluster = dpo.DataprocClusterDeleteOperator(
        project_id = default_dag_args['project_id'],
        task_id = 'delete_dataproc_cluster',
        cluster_name = CLUSTER_NAME,
        trigger_rule = trigger_rule.TriggerRule.ALL_DONE
    )

    create_dataproc_cluster >> run_spark_job >> delete_dataproc_cluster