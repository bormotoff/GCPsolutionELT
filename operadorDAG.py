from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

def coleta_dados():
    print("Coletando dados...")

sql_query = """
WITH active_subscriptions AS (
    SELECT
        s.id_assinatura,
        s.Unidade,
        s.data_criacao_assinatura,
        s.data_vencimento_assinatura,
        SUM(i.total_paid) AS total_receita_paga
    FROM
        `seu_project_id.seu_dataset_id.subscriptions` s
    JOIN
        `seu_project_id.seu_dataset_id.invoices` i
    ON
        s.id_assinatura = i.id_assinatura
    WHERE
        s.Assinatura_ativa = 'true'
    GROUP BY
        s.id_assinatura,
        s.Unidade,
        s.data_criacao_assinatura,
        s.data_vencimento_assinatura
)
SELECT * FROM active_subscriptions;
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='pipeline_coleta_e_processamento',
    default_args=default_args,
    description='DAG que coleta dados, carrega no BigQuery e executa consultas',
    schedule_interval='@hourly',
    start_date=datetime(2024, 10, 1),
    catchup=False,
) as dag:

    coleta_dados_task = PythonOperator(
        task_id='coleta_dados',
        python_callable=coleta_dados,
    )

    carregar_para_bigquery_task = GCSToBigQueryOperator(
        task_id='carregar_dados_gcs_para_bigquery',
        bucket='company_hero_subscriptions',
        source_objects=['invoices/Invoices.csv', 'subscriptions/Subscriptions.csv'],
        destination_project_dataset_table='seu_project_id.seu_dataset_id.invoices',
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
    )

    executar_consulta_sql_task = BigQueryInsertJobOperator(
        task_id='executar_consulta_sql',
        configuration={
            "query": {
                "query": sql_query,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "ID",
                    "datasetId": "Subscriptions",
                    "tableId": "active_subscriptions",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )
    coleta_dados_task >> carregar_para_bigquery_task >> executar_consulta_sql_task
