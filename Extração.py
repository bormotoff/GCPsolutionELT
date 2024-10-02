import pandas as pd
from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

invoices_path = 'Desafio Tecnico Engenheiro de dados - Invoices.csv'
subscriptions_path = 'Desafio Tecnico Engenheiro de dados - Subscriptions.csv'

def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    if not os.path.exists(source_file_path):
        print(f"Erro: O arquivo {source_file_path} não existe.")
        return
    
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        if not bucket.exists():
            print(f"Erro: O bucket {bucket_name} não existe.")
            return
        
        blob = bucket.blob(destination_blob_name)
        if blob.exists():
            print(f"O arquivo {destination_blob_name} já existe no bucket {bucket_name}.")
            return
        
        blob.upload_from_filename(source_file_path)
        print(f"Arquivo {source_file_path} foi carregado para {destination_blob_name} no bucket {bucket_name}.")
    
    except Exception as e:
        print(f"Erro ao carregar {source_file_path} para {destination_blob_name}: {e}")

def read_csv_file(file_path):
    if not os.path.exists(file_path):
        print(f"Erro: O arquivo {file_path} não existe.")
        return None
    
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            print(f"Erro: O arquivo {file_path} está vazio.")
            return None
        return df
    except pd.errors.EmptyDataError:
        print(f"Erro: O arquivo {file_path} está vazio.")
        return None
    except pd.errors.ParserError:
        print(f"Erro ao tentar ler o arquivo {file_path}. O formato pode estar incorreto.")
        return None
    except Exception as e:
        print(f"Ocorreu um erro ao ler o arquivo {file_path}: {e}")
        return None

try:
    invoices_df = read_csv_file(invoices_path)
    subscriptions_df = read_csv_file(subscriptions_path)

    if invoices_df is not None and subscriptions_df is not None:
        print("Invoices Data:")
        print(invoices_df.head())

        print("\nSubscriptions Data:")
        print(subscriptions_df.head())

        bucket_name = "company_hero_subscriptions"
        upload_to_gcs(bucket_name, invoices_path, "invoices/Invoices.csv")
        upload_to_gcs(bucket_name, subscriptions_path, "subscriptions/Subscriptions.csv")
    else:
        print("Erro: Um ou ambos os DataFrames não foram carregados corretamente.")
    
except Exception as e:
    print(f"Ocorreu um erro inesperado: {e}")
