import datetime as dt
import os
from airflow import DAG, models
from airflow.contrib.operators import dataproc_operator as dpo
from airflow.utils import trigger_rule
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

"""
    Airflow DAG does following job :
    1. Notify slack channel about the job.
    2. Create a Dataproc cluster using pramameters.
    3. Run bash command, which will trigger spark job on dataproc cluster created. 
    4. Delete data proc cluster.

    The DAG will notify success or failure on each step, on configured slack channel. 
"""


#Slack connection created in aifrlow's admin
SLACK_CONN_ID = "slack"

#Parsing arguments.
gcp_creds = models.Variable.get('GOOGLE_APPLICATION_CREDENTIALS')
goal = models.Variable.get('goal')
requester = models.Variable.get('requester')
cluster_name = models.Variable.get("cluster_name")
python_file = models.Variable.get("python_file")
projectID = models.Variable.get("project_id")
num_workers = models.Variable.get("num_workers")
region = models.Variable.get("region")
subnet = models.Variable.get("subnetwork_uri")
bash_command = models.Variable.get("bash_command")
cluster_properties =  {'dataproc:dataproc.logging.stackdriver.job.driver.enable':'true',
'dataproc:dataproc.logging.stackdriver.enable':'true',
'dataproc:jobs.file-backed-output.enable':'true',
'dataproc:dataproc.logging.stackdriver.job.yarn.container.enable':'true',
'dataproc:dataproc.logging.stackdriver.enable':'true'}


#Setting environment variable with service account.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_creds

yesterday = dt.datetime.combine(
    dt.datetime.today() - dt.timedelta(1),
    dt.datetime.min.time())


def notify_success():
    return True
def alert_job_requester(context):
    """
    The alert will be send to slack, about the begining of the task
    """
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :large_blue_circle: Task requested!
            *Requester*: {requester}
            *Goal*: {goal} 
            *Task*: {task}  
            *Dag*: {dag1} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        requester=requester,
        goal=goal,
        task=context.get("task_instance").task_id,
        dag1=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )

    request_job = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )

    return request_job.execute(context=context)

def task_success_slack_alert(context):
    """
    Callback task that can be used in DAG to alert of successful task completion

    Args:
        context (dict): Context variable passed in from Airflow

    Returns:
        None: Calls the SlackWebhookOperator execute method internally

    """
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :large_blue_circle: Task Succeeded! 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )

    success_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )

    return success_alert.execute(context=context)


def task_fail_slack_alert(context):
    """
    Callback task that can be used in DAG to alert of failure task completion

    Args:
        context (dict): Context variable passed in from Airflow

    Returns:
        None: Calls the SlackWebhookOperator execute method internally

    """
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )

    failed_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )

    return failed_alert.execute(context=context)

default_dag_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': dt.timedelta(seconds=5),
    'project_id': projectID,
    'on_failure_callback': task_fail_slack_alert
}

with DAG('dataproc_spark_submit', schedule_interval='@once',
    default_args=default_dag_args) as dag:

    #Request for job
    request_job = PythonOperator(
        task_id='Request_for_dataproc_job',
        python_callable=notify_success,
        dag=da  g,
        on_success_callback=alert_job_requester
    )

    #Creating dataproc cluster.
    create_dataproc_cluster = dpo.DataprocClusterCreateOperator(
        project_id = projectID,
        task_id = 'create_dataproc_cluster',
        cluster_name = cluster_name,
        num_workers = num_workers,
        region = region,
        #zone = 'us-central1-a',
        #network_uri = 'default',
        subnetwork_uri = subnet,
        properties = cluster_properties,
        on_success_callback=task_success_slack_alert,
        trigger_rule = trigger_rule.TriggerRule.ALL_SUCCESS
    )

    #Run spark job on the above cluster.
    run_spark_job = BashOperator(
        task_id = 'run_spark_job',
        bash_command = bash_command,
        dag=dag,
        on_success_callback=task_success_slack_alert,
        trigger_rule = trigger_rule.TriggerRule.ALL_SUCCESS
    )

    #Delete the cluster.
    delete_dataproc_cluster = dpo.DataprocClusterDeleteOperator(
        project_id = projectID,
        task_id = 'delete_dataproc_cluster',
        cluster_name = cluster_name,
        region = region,
        #zone = 'us-central1-a',
        #network_uri = 'default',
        subnetwork_uri = subnet,
        on_success_callback=task_success_slack_alert,
        trigger_rule = trigger_rule.TriggerRule.ALL_DONE
    )


    request_job >> create_dataproc_cluster >> run_spark_job >> delete_dataproc_cluster