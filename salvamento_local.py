import psycopg2
import pandas as pd
import time
import os
import csv
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime

def conexao_bando_de_dados():
    conexao = psycopg2.connect(
        host='localhost',
        database='postgres',
        user='postgres',
        password='1234'
    )
    return conexao

def listar_tabelas(conexao):
    nome_tabelas = []
    cursor = conexao.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tabelas = cursor.fetchall()
    for i in tabelas:
        nome_tabelas.append(i[0])
    return nome_tabelas


def salvamento_local_tabelas_postgre():
    conexao = conexao_bando_de_dados()
    tabelas = listar_tabelas(conexao)
    cursor = conexao.cursor()
    data = data_hoje()
    for tabela in tabelas:
        cursor.execute(f"SELECT * FROM {tabela}")
        dados = cursor.fetchall()
        colunas = [desc[0] for desc in cursor.description]
        caminho_saida = f"data/postgres/{tabela}/{data}"
        os.makedirs(caminho_saida, exist_ok=True)
        with open(f"{caminho_saida}/dados.csv", 'w', newline='', encoding='utf-8') as arquivo:
            escritor_csv = csv.writer(arquivo)
            escritor_csv.writerow(colunas)
            escritor_csv.writerows(dados)

def leitura_csv(caminho_csv):
    return pd.read_csv(caminho_csv)

def data_hoje():
    dia_atual = time.localtime()
    ano = dia_atual.tm_year
    mes = dia_atual.tm_mon
    dia = dia_atual.tm_mday
    data = f"{ano}-{mes}-{dia}"
    return data

def salvamento_local_csv():
    caminho_csv = "dados/order_details.csv"
    df = leitura_csv(caminho_csv).to_csv(lineterminator="\n", index=False)
    data = data_hoje() 
    caminho_saida = f"data/csv/{data}"
    os.makedirs(caminho_saida, exist_ok=True)
    with open(f"{caminho_saida}/dados.csv", "w") as arquivo:
        arquivo.write(df)
        
    
def main():
    with DAG("carregamento_local", start_date=datetime(2024, 6, 10), schedule_interval="* * * * *", catchup=False) as dag:
        salvar_csv_task = PythonOperator(task_id="salvamento_local_csv",python_callable=salvamento_local_csv,)   
        

if __name__ == "__main__":
    main()
    
    