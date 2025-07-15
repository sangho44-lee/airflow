from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_with_postgres",
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["example"]
) as dag:

    def insrt_postgres(ip, port, dbname, user, passwd, **kwargs):
        import psycopg2
        from contextlib import closing

        with closing(psycopg2.connect(
            host=ip,
            dbname=dbname,
            user=user,
            password=passwd,
            port=int(port))
        ) as conn:
            with closing(conn.cursor()) as cursor:
                ti = kwargs["ti"]
                dag_id = ti.dag_id
                task_id = ti.task_id
                run_id = ti.run_id
                msg = 'insrt 수행'
                sql = 'insert into py_opr_drct_insrt values(%s, %s, %s, %s);'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()

    PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_args=['172.28.0.3', '5434', 'shlee', 'shlee', 'shlee']
    )
