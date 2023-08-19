import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator


# this is just an example of how to use SPARK_STEPS, you need to define your own steps
SPARK_STEPS = [
    {
        'Name': 'wcd_data_engineer',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                's3://midterm-yw/script/sales.py',
                "{{ ds }}",
                # "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}"
            ]
        }
    }
]


DEFAULT_ARGS = {
    'owner': 'wcd_data_engineer',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['airflow_data_eng@wcd.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

def retrieve_s3_files(**kwargs):
    data = kwargs['dag_run'].conf['data']
    kwargs['ti'].xcom_push(key = 'data', value = data)


dag = DAG(
    'midterm_dag',
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(hours=2),
    schedule_interval = None
)

parse_request = PythonOperator(task_id = 'parse_request',
                                provide_context = True, 
                                python_callable = retrieve_s3_files,
                                dag = dag
                                ) 

JOB_FLOW_OVERRIDES = {
    'Name': 'mid-cluster',
    'ReleaseLabel': 'emr-6.10.0',
    'Applications': [{'Name': 'Spark'}],
    'Instances': {
        'InstanceGroups': [
            {'InstanceRole': 'MASTER', 'InstanceType': 'm5.xlarge', 'InstanceCount': 1},
            {'InstanceRole': 'CORE', 'InstanceType': 'm5.xlarge', 'InstanceCount': 1},
            {'InstanceRole': 'TASK', 'InstanceType': 'm5.xlarge', 'InstanceCount': 1},
        ],
        'Ec2KeyName': 'demo-key',
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False
    },
    'VisibleToAllUsers': True,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'ScaleDownBehavior': 'TERMINATE_AT_TASK_COMPLETION',
    'LogUri': 's3://emr-logs-midterm-yw/'
}

create_emr_cluster = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    aws_conn_id='aws_default',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    dag=dag,
)


step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id="aws_default",
    dag=dag
)

parse_request >> create_emr_cluster >> step_adder >> step_checker