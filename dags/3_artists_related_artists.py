from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable

host_fastapi = Variable.get("host_fastapi")
port_fastapi = Variable.get("port_fastapi")

# execution date 사용
# 당일 데이터를 호출해야 하므로 + 1일
# datetime 모듈을 사용하는 것과의 실질적인 차이는 X
date = "{{ (execution_date + macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"

# default argument 설정
default_args = {
    'owner': 'spotify:1.0.0',
    'depends_on_past': True,
    'start_date': datetime(2023,10,13)
}

# dag 설정
dag = DAG('artists.related.artists',
		  default_args=default_args,
		  tags=['API', 'MySQL'],
		  max_active_runs=1,
		  schedule_interval="@once")

# start
start = EmptyOperator(
	task_id = 'start',
	dag=dag
)

# curl 전송 bash 오퍼레이터
curl = BashOperator(
	task_id='curl',
	bash_command=f"""
	curl '{host_fastapi}:{port_fastapi}/mysql/artists/related_artists?cnt=3&insert_date={date}'
	""",
	dag=dag
)

# success
succeed = EmptyOperator(
	task_id = 'succeed',
	dag=dag,
    trigger_rule='all_success'
)

# failed
failed = EmptyOperator(
	task_id = 'failed',
	dag=dag,
    trigger_rule='one_failed'
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


# task order
start >> curl >> [succeed, failed]
failed >> send_noti
[succeed, send_noti] >> finish