import os
from google.cloud import bigquery
from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"
client = bigquery.Client()
project_id = "ID"
dataset_id = "Subscriptions"

invoices_table_id = f"{project_id}.{dataset_id}.invoices"
subscriptions_table_id = f"{project_id}.{dataset_id}.subscriptions"

invoices_gcs_uri = "gs://company_hero_subscriptions/invoices/Invoices.csv"
subscriptions_gcs_uri = "gs://company_hero_subscriptions/subscriptions/Subscriptions.csv"

def load_csv_to_bigquery(table_id, gcs_uri, schema):
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
    )
    
    load_job = client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config
    )
    
    print(f"Iniciando o job {load_job.job_id} para carregar {gcs_uri} em {table_id}...")
    load_job.result()
    print(f"Dados carregados em {table_id}.")

    destination_table = client.get_table(table_id)
    print(f"Total de linhas na tabela {table_id}: {destination_table.num_rows}")


invoices_schema = [
    bigquery.SchemaField("id_assinatura", "STRING"),
    bigquery.SchemaField("id_fatura", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("invoice_date", "DATE"),
    bigquery.SchemaField("total_paid", "FLOAT"),
]

subscriptions_schema = [
    bigquery.SchemaField("id_assinatura", "STRING"),
    bigquery.SchemaField("plano", "STRING"),
    bigquery.SchemaField("Unidade", "STRING"),
    bigquery.SchemaField("cupom", "STRING"),
    bigquery.SchemaField("data_criacao_assinatura", "DATE"),
    bigquery.SchemaField("data_vencimento_assinatura", "DATE"),
    bigquery.SchemaField("Assinatura_ativa", "STRING"),
]

load_csv_to_bigquery(invoices_table_id, invoices_gcs_uri, invoices_schema)
load_csv_to_bigquery(subscriptions_table_id, subscriptions_gcs_uri, subscriptions_schema)