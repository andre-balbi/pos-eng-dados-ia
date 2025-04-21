"""
Data Lakehouse ETL Process - Bronze, Silver e Gold

Este script implementa um pipeline ETL de três camadas para processamento de dados de produtos,
utilizando DuckDB como mecanismo de processamento e armazenamento.

Autor: André Balbi
Data: Abril 2025
"""

import os
from datetime import datetime
import pandas as pd
import duckdb
import logging
import time

# Configuração de logging para acompanhamento do processo
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

# Constantes globais
LANDING_DIR = '../landing'
DB_FILE = 'dados_duckdb.db'
EXT = '.csv'

# Inicia a medição do tempo
start_time = time.time()
logger.info("Iniciando medição do tempo de execução do pipeline")

# ------------------------------------------------------------
# Camada Bronze - Ingestão de dados brutos
# ------------------------------------------------------------
logger.info("Iniciando processo ETL - Camada Bronze")

# Inicializa conexão com DuckDB
con = duckdb.connect(database=DB_FILE, read_only=False)
logger.info(f"Conexão estabelecida com o banco {DB_FILE}")

# Define nome da tabela para camada bronze
TABLE_NAME_BRONZE = 'bronze_produtos'

# Remove tabela existente, se houver (idempotência)
con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME_BRONZE}")

# Cria tabela bronze_produtos com esquema apropriado
con.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME_BRONZE} (
        NATBR VARCHAR,    -- ID do produto
        MAKTX VARCHAR,    -- Nome do produto
        WERKS VARCHAR,    -- ID da categoria
        MAINS VARCHAR,    -- Fornecedor
        LABST VARCHAR,    -- Preço
        ingest_time TIMESTAMP,  -- Timestamp de ingestão
        file_name VARCHAR        -- Arquivo de origem
    )
""")

# Lista arquivos CSV na pasta de landing
try:
    csv_files = [f for f in os.listdir(LANDING_DIR) if f.endswith(EXT)]
    logger.info(f"Encontrados {len(csv_files)} arquivos para processamento")
except Exception as e:
    logger.error(f"Erro ao listar arquivos CSV: {e}")
    raise

# Itera sobre os arquivos CSV e insere os dados na camada bronze
for file in csv_files:
    try:
        file_path = os.path.join(LANDING_DIR, file)
        ingest_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Lê o CSV e adiciona metadados
        df = pd.read_csv(file_path)
        df['ingest_time'] = ingest_time
        df['file_name'] = file
        
        # Insere dados no DuckDB
        con.execute(f"INSERT INTO {TABLE_NAME_BRONZE} SELECT * FROM df")
        logger.info(f"Arquivo {file} processado com sucesso: {len(df)} registros")
    except Exception as e:
        logger.error(f"Erro ao processar arquivo {file}: {e}")

# Exibe resultado para conferência
bronze_result = con.execute(f"SELECT COUNT(*) as total_records FROM {TABLE_NAME_BRONZE}").fetchone()
logger.info(f"Total de registros na camada Bronze: {bronze_result[0]}")


# ------------------------------------------------------------
# Camada Silver - Transformação e limpeza de dados
# ------------------------------------------------------------
logger.info("Iniciando processo ETL - Camada Silver")

# Define nome da tabela para camada silver
TABLE_NAME_SILVER = 'silver_produtos'

# Obtém os registros mais recentes de cada produto (deduplicação)
# usando window function para ranquear por timestamp de ingestão
try:
    silver_df = con.execute("""
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY NATBR ORDER BY ingest_time DESC) AS rn
            FROM bronze_produtos
            WHERE ingest_time >= '2025-01-01 00:00:00'  -- Filtra por data recente
        )
        SELECT *
        FROM ranked
        WHERE rn = 1  -- Seleciona apenas o registro mais recente
    """).fetchdf()
    
    logger.info(f"Dados extraídos para camada Silver: {len(silver_df)} registros únicos")
except Exception as e:
    logger.error(f"Erro ao processar dados para camada Silver: {e}")
    raise

# Limpeza e transformação dos dados
# - Remove colunas desnecessárias
# - Renomeia colunas conforme padrão de negócio
# - Converte tipos de dados
silver_df.drop(columns=['ingest_time', 'file_name', 'rn'], inplace=True)

# Renomeia colunas para nomes mais significativos
silver_df.rename(columns={
    'NATBR': 'id',  
    'MAKTX': 'prod_name',  
    'WERKS': 'id_category', 
    'MAINS': 'supplier',  
    'LABST': 'price'
}, inplace=True)

# Converte tipos de dados para os formatos corretos
try:
    silver_df = silver_df.astype({
        'id': 'int64',
        'prod_name': 'string',
        'id_category': 'string',
        'supplier': 'int64',
        'price': 'float32'
    })
except Exception as e:
    logger.error(f"Erro na conversão de tipos de dados: {e}")
    raise

# Adiciona timestamp de processamento
silver_df['ingest_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Cria tabela silver_produtos
con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME_SILVER}")

con.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME_SILVER} (
        id bigint,
        prod_name string,
        id_category string,
        supplier bigint,
        price float,
        ingest_time timestamp
    )
""")

# Insere dados transformados na tabela silver
con.execute(f"INSERT INTO {TABLE_NAME_SILVER} SELECT * FROM silver_df")
logger.info("Dados inseridos na camada Silver com sucesso")


# ------------------------------------------------------------
# Camada Gold - Modelagem dimensional
# ------------------------------------------------------------
logger.info("Iniciando processo ETL - Camada Gold")

# Define nomes das tabelas para camada gold
TABLE_NAME_FAT = 'gold_produtos_fat'    # Tabela fato
TABLE_NAME_DIM = 'gold_produtos_dim'    # Tabela dimensão

# Cria tabela fato com id, nome e preço
try:
    df_fato = con.execute("""
        SELECT DISTINCT id, prod_name, price
        FROM silver_produtos    
    """).fetchdf()
    
    # Cria tabela fato no banco de dados
    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME_FAT}")
    
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME_FAT} (
            id bigint,           -- Chave primária
            prod_name string,    -- Nome do produto  
            price float          -- Preço do produto
        )
    """)
    
    # Insere dados na tabela fato
    con.execute(f"INSERT INTO {TABLE_NAME_FAT} SELECT * FROM df_fato")
    logger.info(f"Tabela fato criada com {len(df_fato)} registros")
except Exception as e:
    logger.error(f"Erro ao criar tabela fato: {e}")
    raise

# Cria tabela dimensão com id, id_categoria e fornecedor
try:
    df_dim = con.execute("""
        SELECT DISTINCT id, id_category, supplier
        FROM silver_produtos    
    """).fetchdf()
    
    # Cria tabela dimensão no banco de dados
    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME_DIM}")
    
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME_DIM} (
            id bigint,           -- Chave primária
            id_category string,  -- Categoria do produto
            supplier bigint      -- ID do fornecedor
        )
    """)
    
    # Insere dados na tabela dimensão
    con.execute(f"INSERT INTO {TABLE_NAME_DIM} SELECT * FROM df_dim")
    logger.info(f"Tabela dimensão criada com {len(df_dim)} registros")
except Exception as e:
    logger.error(f"Erro ao criar tabela dimensão: {e}")
    raise

# Exibe estatísticas finais
logger.info("Pipeline ETL concluído com sucesso")
logger.info("Estatísticas finais:")
bronze_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME_BRONZE}").fetchone()[0]
silver_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME_SILVER}").fetchone()[0]
fact_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME_FAT}").fetchone()[0]
dim_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME_DIM}").fetchone()[0]

logger.info(f"- Registros na camada Bronze: {bronze_count}")
logger.info(f"- Registros na camada Silver: {silver_count}")
logger.info(f"- Registros na tabela Fato: {fact_count}")
logger.info(f"- Registros na tabela Dimensão: {dim_count}")

# ------------------------------------------------------------
# Exibição dos dados para validação
# ------------------------------------------------------------
print("\n" + "="*80)
print("ESTRUTURA DO DATA LAKEHOUSE - LISTAGEM DE TABELAS")
print("="*80)

# Lista todas as tabelas no banco
all_tables = con.execute("SHOW TABLES").fetchdf()
print("\nTabelas no banco de dados:")
print(all_tables)

# Função para exibir amostra de tabela
def exibir_amostra(nome_tabela, descricao):
    print("\n" + "-"*80)
    print(f"{descricao} ({nome_tabela})")
    print("-"*80)
    dados = con.execute(f"SELECT * FROM {nome_tabela} LIMIT 5").fetchdf()
    print(dados)
    print(f"Total de registros: {con.execute(f'SELECT COUNT(*) FROM {nome_tabela}').fetchone()[0]}")

# Exibe amostras de cada camada
print("\n\n" + "="*80)
print("AMOSTRAS DE DADOS DAS CAMADAS")
print("="*80)

# Camada Bronze
exibir_amostra(TABLE_NAME_BRONZE, "CAMADA BRONZE - Dados brutos com metadados")

# Camada Silver
exibir_amostra(TABLE_NAME_SILVER, "CAMADA SILVER - Dados limpos e transformados")

# Camada Gold - Tabela Fato
exibir_amostra(TABLE_NAME_FAT, "CAMADA GOLD - Tabela Fato (Produtos)")

# Camada Gold - Tabela Dimensão
exibir_amostra(TABLE_NAME_DIM, "CAMADA GOLD - Tabela Dimensão (Categorias e Fornecedores)")

# Fecha a conexão com o banco de dados
con.close()
logger.info("Conexão com o banco encerrada")

# Calcula e exibe o tempo total de execução
end_time = time.time()
execution_time = end_time - start_time

logger.info(f"Tempo total de execução do pipeline: {execution_time:.2f} segundos")