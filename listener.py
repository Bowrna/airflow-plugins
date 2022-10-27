from airflow.listeners import hookimpl
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from airflow.models.taskinstance import TaskInstance
    from airflow.utils.state import TaskInstanceState



@hookimpl
def on_task_instance_running(
    previous_state: "TaskInstanceState", task_instance: "TaskInstance", session):
    print("Task instance is running:", task_instance.__dict__, " Previous state:", previous_state)
    #  ti_name, ti_status, dag_run, dagrun_status, start_time, end_time
    task_instance_state: TaskInstanceState = task_instance.state
    task_instance_name: str = task_instance.task_id
#Task Instance
#      {
#   "_sa_instance_state": <sqlalchemy.orm.state.InstanceState object at 0x10757fa30>,
#   "dag_id": "example_bash_operator",
#   "hostname": "sathishs-macbook-air.local",
#   "operator": "BashOperator",
#   "next_method": None,
#   "run_id": "manual__2022-10-22T03:06:15.904616+00:00",
#   "unixname": "sathishkannan",
#   "queued_dttm": datetime.datetime(2022, 10, 22, 3, 6, 16, 422842, tzinfo=Timezone("UTC")),
#   "next_kwargs": None,
#   "map_index": -1,
#   "job_id": 173,
#   "queued_by_job_id": 141,
#   "start_date": datetime.datetime(2022, 10, 22, 3, 6, 17, 139340, tzinfo=Timezone("UTC")),
#   "pool": "default_pool",
#   "pid": None,
#   "end_date": None,
#   "pool_slots": 1,
#   "executor_config": {},
#   "_try_number": 1,
#   "duration": None,
#   "queue": "default",
#   "external_executor_id": None,
#   "task_id": "runme_0",
#   "state": <TaskInstanceState.RUNNING: "running">,
#   "priority_weight": 3,
#   "trigger_id": None,
#   "max_tries": 0,
#   "trigger_timeout": None,
#   "dag_run": <DagRun example_bash_operator @ 2022-10-22 03:06:15.904616+00:00: manual__2022-10-22T03:06:15.904616+00:00, state:running, queued_at: 2022-10-22 03:06:15.912586+00:00. externally triggered: True>,
#   "rendered_task_instance_fields": None,
#   "_log": "<Logger airflow.task (INFO)>",
#   "test_mode": False,
#   "task": "<Task(BashOperator): runme_0>"
# }
    dagrun = task_instance.dag_run
    dagrun_status = dagrun.state
    task = task_instance.task
    print('Task dict:', task.__dict__)
# Task   
#  {
#   "_BaseOperator__init_kwargs": {
#     "task_id": "runme_0"
#   },
#   "_BaseOperator__from_mapped": False,
#   "task_id": "runme_0",
#   "owner": "airflow",
#   "email": None,
#   "email_on_retry": True,
#   "email_on_failure": True,
#   "execution_timeout": None,
#   "on_execute_callback": None,
#   "on_failure_callback": None,
#   "on_success_callback": None,
#   "on_retry_callback": None,
#   "_pre_execute_hook": None,
#   "_post_execute_hook": None,
#   "executor_config": {},
#   "run_as_user": None,
#   "retries": 0,
#   "queue": "default",
#   "pool": "default_pool",
#   "pool_slots": 1,
#   "sla": None,
#   "trigger_rule": "<TriggerRule.ALL_SUCCESS: all_success>",
#   "depends_on_past": False,
#   "ignore_first_depends_on_past": True,
#   "wait_for_downstream": False,
#   "retry_delay": datetime.timedelta(seconds=300),
#   "retry_exponential_backoff": False,
#   "max_retry_delay": None,
#   "params": {
#     "example_key": "example_value"
#   },
#   "priority_weight": 1,
#   "weight_rule": "<WeightRule.DOWNSTREAM: downstream>",
#   "resources": None,
#   "max_active_tis_per_dag": None,
#   "do_xcom_push": True,
#   "doc_md": None,
#   "doc_json": None,
#   "doc_yaml": None,
#   "doc_rst": None,
#   "doc": None,
#   "upstream_task_ids": set(),
#   "downstream_task_ids": {"run_after_loop"},
#   "_log": "<Logger airflow.task.operators (INFO)>",
#   "inlets": [],
#   "outlets": [],
#   "_BaseOperator__instantiated": True,
#   "_task_type": "BashOperator",
#   "ui_color": "#f0ede4",
#   "ui_fgcolor": "#000",
#   "template_ext": [
#     ".sh",
#     ".bash"
#   ],
#   "template_fields": [
#     "bash_command",
#     "env"
#   ],
#   "operator_extra_links": "()",
#   "template_fields_renderers": {
#     "bash_command": "bash",
#     "env": "json"
#   },
#   "_task_module": "airflow.operators.bash",
#   "_is_empty": False,
#   "bash_command": "echo '{{ task_instance_key_str }}' && sleep 1",
#   "_operator_name": "BashOperator",
#   "env": None,
#   "task_group": "<weakproxy at 0x105b387c0 to SerializedTaskGroup at 0x105bc8430>",
#   "_dag": "<DAG: example_bash_operator>",
#   "start_date": "DateTime(2021, 1, 1, 0, 0, 0, tzinfo=Timezone(UTC))",
#   "end_date": None
# }
    dag = task_instance.task.dag
    dag_name = dag.dag_id
    print('Dag dict:', dag.__dict__)
# DAG    
# {
#   "owner_links": "{}",
#   "user_defined_macros": "None",
#   "user_defined_filters": "None",
#   "default_args": {},
#   "params": {
#     "example_key": "example_value"
#   },
#   "_dag_id": "example_bash_operator",
#   "_max_active_tasks": 16,
#   "_pickle_id": "None",
#   "_description": "None",
#   "fileloc": "/Users/sathishkannan/.pyenv/versions/3.8.10/envs/airflow-env/lib/python3.8/site-packages/airflow/example_dags/example_bash_operator.py",
#   "task_dict": {
#     "run_this_last": "<Task(EmptyOperator): run_this_last>",
#     "run_after_loop": "<Task(BashOperator): run_after_loop>",
#     "runme_0": "<Task(BashOperator): runme_0>",
#     "runme_1": "<Task(BashOperator): runme_1>",
#     "runme_2": "<Task(BashOperator): runme_2>",
#     "also_run_this": "<Task(BashOperator): also_run_this>",
#     "this_will_skip": "<Task(BashOperator): this_will_skip>"
#   },
#   "timezone": "Timezone(UTC)",
#   "start_date": "DateTime(2021, 1, 1, 0, 0, 0, tzinfo=Timezone(UTC))",
#   "end_date": "None",
#   "dataset_triggers": [],
#   "schedule_interval": "0 0 * * *",
#   "timetable": "<airflow.timetables.interval.CronDataIntervalTimetable object at 0x104ef1df0>",
#   "template_searchpath": "None",
#   "template_undefined": "<class jinja2.runtime.StrictUndefined>",
#   "last_loaded": "datetime.datetime(2022, 10, 22, 6, 46, 14, 687166, tzinfo=Timezone(UTC))",
#   "safe_dag_id": "example_bash_operator",
#   "max_active_runs": 16,
#   "dagrun_timeout": "datetime.timedelta(seconds=3600)",
#   "sla_miss_callback": "None",
#   "_default_view": "grid",
#   "orientation": "LR",
#   "catchup": "False",
#   "partial": "False",
#   "on_success_callback": "None",
#   "on_failure_callback": "None",
#   "edge_info": {},
#   "has_on_success_callback": "False",
#   "has_on_failure_callback": "False",
#   "_access_control": "None",
#   "is_paused_upon_creation": "None",
#   "auto_register": "True",
#   "jinja_environment_kwargs": "None",
#   "render_template_as_native_obj": "False",
#   "doc_md": "None",
#   "tags": [
#     "example",
#     "example2"
#   ],
#   "_task_group": "<airflow.serialization.serialized_objects.SerializedTaskGroup object at 0x105054490>",
#   "dag_dependencies": []
# }
    print('Dag Run dict:', dagrun.__dict__)
    print('airflow_dag_id:', task.dag_id)
    print('task_id:', task.task_id)
    print('airflow_run_id:', dagrun.run_id)
    print('Start time:', task_instance.start_date)
    print('End date:', task_instance.end_date)
    print('Duration:', task_instance.duration)
    """Called when task state changes to RUNNING. Previous_state can be State.NONE."""
    pass

@hookimpl
def on_task_instance_success(
    previous_state: "TaskInstanceState", task_instance: "TaskInstance", session):
    print("Task instance in success state:", task_instance.__dict__)
#     {
#   "_sa_instance_state": "<sqlalchemy.orm.state.InstanceState object at 0x108d02820>",
#   "dag_id": "example_bash_operator",
#   "hostname": "sathishs-macbook-air.local",
#   "operator": "BashOperator",
#   "next_method": "None",
#   "run_id": "manual__2022-10-22T06:46:13.743972+00:00",
#   "unixname": "sathishkannan",
#   "queued_dttm": "datetime.datetime(2022, 10, 22, 6, 46, 14, 369724, tzinfo=Timezone(UTC))",
#   "next_kwargs": "None",
#   "map_index": -1,
#   "job_id": 185,
#   "queued_by_job_id": 141,
#   "start_date": "datetime.datetime(2022, 10, 22, 6, 46, 15, 44414, tzinfo=Timezone(UTC))",
#   "pool": "default_pool",
#   "pid": 48142,
#   "end_date": "datetime.datetime(2022, 10, 22, 6, 46, 16, 425082, tzinfo=Timezone(UTC))",
#   "pool_slots": 1,
#   "executor_config": {},
#   "_try_number": 1,
#   "duration": 1.380668,
#   "queue": "default",
#   "external_executor_id": "None",
#   "task_id": "runme_0",
#   "state": "<TaskInstanceState.SUCCESS: success>",
#   "priority_weight": 3,
#   "trigger_id": "None",
#   "max_tries": 0,
#   "trigger_timeout": "None",
#   "dag_run": "<DagRun example_bash_operator @ 2022-10-22 06:46:13.743972+00:00: manual__2022-10-22T06:46:13.743972+00:00, state:running, queued_at: 2022-10-22 06:46:13.748840+00:00. externally triggered: True>",
#   "rendered_task_instance_fields": "None",
#   "_log": "<Logger airflow.task (INFO)>",
#   "test_mode": "False"
# }
    # task_identifier = task_instance.dag_id+' '+task_instance.task_id+' '+task_instance.run_id
    dagrun = task_instance.dag_run
    print('Dag run dict:', dagrun.__dict__)
#     {
#   "_sa_instance_state": "<sqlalchemy.orm.state.InstanceState object at 0x107b39f10>",
#   "run_id": "manual__2022-10-22T06:52:58.124175+00:00",
#   "last_scheduling_decision": "datetime.datetime(2022, 10, 22, 6, 52, 59, 552551, tzinfo=Timezone(UTC))",
#   "_state": "running",
#   "creating_job_id": "None",
#   "dag_hash": "0d896ac8d3d318d2090b6adc56ac0f76",
#   "id": 30,
#   "external_trigger": "True",
#   "log_template_id": 2,
#   "dag_id": "example_bash_operator",
#   "run_type": "manual",
#   "queued_at": "datetime.datetime(2022, 10, 22, 6, 52, 58, 143098, tzinfo=Timezone(UTC))",
#   "conf": "{}",
#   "execution_date": "datetime.datetime(2022, 10, 22, 6, 52, 58, 124175, tzinfo=Timezone(UTC))",
#   "data_interval_start": "datetime.datetime(2022, 10, 21, 0, 0, tzinfo=Timezone(UTC))",
#   "start_date": "datetime.datetime(2022, 10, 22, 6, 52, 59, 549431, tzinfo=Timezone(UTC))",
#   "data_interval_end": "datetime.datetime(2022, 10, 22, 0, 0, tzinfo=Timezone(UTC))",
#   "end_date": "None"
# }
    dag_id = task_instance.dag_id
    hostname = task_instance.hostname
    operator = task_instance.operator
    time_queued = task_instance.queued_dttm
    next_method = task_instance.next_method
    next_kwargs = task_instance.next_kwargs
    state = task_instance.state
    queued_at = task_instance.dag_run.queued_at
    # task = task_instance.task
    # dag = task_instance.task.dag
    # print('airflow_dag_id:', task.dag_id)
    # print('task_id:', task.task_id)
    print('airflow_run_id:', dagrun.run_id)
    print('Start time:', task_instance.start_date)
    print('End date:', task_instance.end_date)
    print('Duration:', task_instance.duration)

    """Called when task state changes to SUCCESS. Previous_state can be State.NONE."""
    pass

@hookimpl
def on_task_instance_failed(
    previous_state: "TaskInstanceState", task_instance: "TaskInstance", session):
    print("Task instance in failed state:", task_instance.__dict__)
    # task_identifier = task_instance.dag_id+' '+task_instance.task_id+' '+task_instance.run_id
    dagrun = task_instance.dag_run
    task = task_instance.task
    dag = task_instance.task.dag
    # print('Task identifier:', task_identifier)
    print('airflow_dag_id:', task.dag_id)
    print('task_id:', task.task_id)
    print('airflow_run_id:', dagrun.run_id)
    print('Start time:', task_instance.start_date)
    print('End date:', task_instance.end_date)
    print('Duration:', task_instance.duration)
    """Called when task state changes to FAIL. Previous_state can be State.NONE.""" 
    pass