from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


default_args = {
    'depends_on_past': False,
    'email': ['pamith.r@hsenidmobile.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}


@dag('my_demo_pipeline', default_args=default_args, 
     schedule=timedelta(seconds=20), start_date=datetime(2023, 12, 4, 14, 45), 
     catchup=False, tags=['demo'])
def taskflow():

    @task(task_id='step1')
    def perform_step1():
        
        n = 1
        print(f'Step 1 - {n}')

        return n

    @task(task_id='step2')
    def perform_step2(n):

        n += 1
        print(f'Step 2 - {n}')

        return n

    @task(task_id='step3')
    def perform_step3(n):

        n += 1
        print(f'Step 3 - {n/0}')

        return n

    @task(task_id='step4')
    def perform_step4(n):

        n += 1
        print(f'Step 4 - {n}')

        return n

    init = EmptyOperator(task_id='init')
    end = EmptyOperator(task_id='end')

    t1 = perform_step1()
    t2 = perform_step2(t1)
    t3 = perform_step3(t2)
    t4 = perform_step4(t3)

    init >> t1 
    t4 >> end


taskflow()