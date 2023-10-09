from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

# execution date 사용
date = "{{execution_date.strftime('%Y-%m-%d')}}"

# default argument 설정
default_args = {
    'owner': 'spotify:1.0.0',
    'depends_on_past': True,
    'start_date': datetime(2022,1,20)
}

# dag 설정
dag = DAG('load_fixtures_data',
		  default_args=default_args,
		  tags=['Extract','fixtures'],
		  max_active_runs=1,
		  schedule_interval="@once")

# start
start = EmptyOperator(
	task_id = 'start',
	dag=dag
)

# curl 전송 bash 오퍼레이터
curl_demo = BashOperator(
	task_id='curl.demo',
	bash_command=f"""
	curl -X 'GET' \
    'http://222.108.81.83:8000/json/albums?cnt=1' \
    -H 'accept: application/json'
	""",
	dag=dag
)

# curl 전송 bash 오퍼레이터
curl_demo_2 = BashOperator(
	task_id='curl.demo.2',
	bash_command=f"""
	curl '222.108.81.83:8000/json/albums?cnt=1'
	""",
	dag=dag
)

# LINE NOTI 전송 기능
send_noti = BashOperator(
    task_id='send.noti',
    bash_command='''
    curl -X POST -H 'Authorization: Bearer fxANtArqOzDWxjissz34JryOGhwONGhC1uMN8qc59Z3'
                 -F '<MESSAGE>' 
                 https://notify-api.line.me/api/notify
    ''',
    dag=dag
)

# finish
finish = EmptyOperator(
	task_id = 'finish',
	dag = dag,
    trigger_rule='all_done'
)

# 
start >> curl_demo >> curl_demo_2 >> send_noti >> finish