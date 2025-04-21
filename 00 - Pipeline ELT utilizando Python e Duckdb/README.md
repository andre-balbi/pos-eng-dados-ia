# 🌊 Pipeline ELT utilizando Python e Duckdb

![Data Lakehouse Architecture](https://img.shields.io/badge/Architecture-Data%20Lakehouse-blue)
![DuckDB](https://img.shields.io/badge/Database-DuckDB-orange)
![Python](https://img.shields.io/badge/Language-Python%203-green)

Um pipeline ETL completo implementando uma arquitetura moderna de **Data Lakehouse** com camadas Bronze, Silver e Gold, utilizando Python e DuckDB como mecanismo de processamento e armazenamento.

## 📋 Descrição do Projeto

Este projeto implementa um pipeline de dados em três camadas (Bronze, Silver e Gold) seguindo a arquitetura moderna de Data Lakehouse popularizada pelo Databricks e amplamente adotada na indústria.

As camadas são estruturadas para proporcionar uma experiência de gerenciamento de dados que combina a flexibilidade de um Data Lake com a confiabilidade e performance de um Data Warehouse.

### 🏗️ Arquitetura

```
[CSV Files (landing)] → [BRONZE] → [SILVER] → [GOLD (Fact/Dimension Tables)]
```

- **Camada Bronze**: Ingestão de dados brutos
- **Camada Silver**: Processamento, limpeza e transformação
- **Camada Gold**: Modelagem dimensional (tabelas fato e dimensão)

## 🔧 Funcionalidades

- **Ingestão de dados** de múltiplos arquivos CSV
- **Deduplicação inteligente** mantendo apenas os registros mais recentes
- **Transformação e limpeza** de dados com tipagem apropriada
- **Modelagem dimensional** com tabelas fato e dimensão
- **Logging** completo do processo
- **Visualização** dos dados em cada camada

## 🚀 Tecnologias Utilizadas

- **Python**: Linguagem principal
- **Pandas**: Manipulação e análise de dados
- **DuckDB**: Banco de dados analítico em memória
- **Logging**: Sistema de logs para acompanhamento do processo

## Exemplo de Execução

Abaixo está um exemplo da saída do pipeline quando executado com dois arquivos CSV contendo dados de produtos:

```
2025-04-21 19:24:15,537 - INFO - Iniciando medição do tempo de execução do pipeline
2025-04-21 19:24:15,538 - INFO - Iniciando processo ETL - Camada Bronze
2025-04-21 19:24:15,561 - INFO - Conexão estabelecida com o banco dados_duckdb.db
2025-04-21 19:24:16,322 - INFO - Encontrados 2 arquivos para processamento
2025-04-21 19:24:16,372 - INFO - Arquivo z0019_1.csv processado com sucesso: 10 registros
2025-04-21 19:24:16,407 - INFO - Arquivo z0019_2.csv processado com sucesso: 50 registros
2025-04-21 19:24:16,408 - INFO - Total de registros na camada Bronze: 60
2025-04-21 19:24:16,409 - INFO - Iniciando processo ETL - Camada Silver
2025-04-21 19:24:16,416 - INFO - Dados extraídos para camada Silver: 10 registros únicos
2025-04-21 19:24:16,492 - INFO - Dados inseridos na camada Silver com sucesso
2025-04-21 19:24:16,493 - INFO - Iniciando processo ETL - Camada Gold
2025-04-21 19:24:16,577 - INFO - Tabela fato criada com 10 registros
2025-04-21 19:24:16,673 - INFO - Tabela dimensão criada com 10 registros
2025-04-21 19:24:16,674 - INFO - Pipeline ETL concluído com sucesso
2025-04-21 19:24:16,674 - INFO - Estatísticas finais:
2025-04-21 19:24:16,677 - INFO - - Registros na camada Bronze: 60
2025-04-21 19:24:16,677 - INFO - - Registros na camada Silver: 10
2025-04-21 19:24:16,678 - INFO - - Registros na tabela Fato: 10
2025-04-21 19:24:16,678 - INFO - - Registros na tabela Dimensão: 10
2025-04-21 19:24:16,829 - INFO - Conexão com o banco encerrada
2025-04-21 19:24:16,829 - INFO - Tempo total de execução do pipeline: 1.29 segundos
```

### Tabelas Resultantes

```
Tabelas no banco de dados:
                name
0    bronze_produtos
1  gold_produtos_dim
2  gold_produtos_fat
3    silver_produtos
```

### Amostras de Dados

#### Camada Bronze
```
   NATBR           MAKTX WERKS MAINS LABST         ingest_time    file_name
0  10001        PARAFUSO  BT10   100   100 2025-04-21 18:41:13  z0019_1.csv
1  10002  CHAVE DE FENDA  BT11   110   105 2025-04-21 18:41:13  z0019_1.csv
2  10003         MARTELO  BT12   120   115 2025-04-21 18:41:13  z0019_1.csv
3  10004         ALICATE  BT13   130   125 2025-04-21 18:41:13  z0019_1.csv
4  10005         SERROTE  BT14   140   135 2025-04-21 18:41:13  z0019_1.csv
Total de registros: 60
```

#### Camada Silver
```
      id       prod_name id_category  supplier  price         ingest_time
0  10009       LIXADEIRA        BT18       180  175.0 2025-04-21 18:41:14
1  10007  NÍVEL DE BOLHA        BT16       160  155.0 2025-04-21 18:41:14
2  10001        PARAFUSO        BT10       100  100.0 2025-04-21 18:41:14
3  10004         ALICATE        BT13       130  125.0 2025-04-21 18:41:14
4  10003         MARTELO        BT12       120  115.0 2025-04-21 18:41:14
Total de registros: 10
```

#### Camada Gold - Tabela Fato
```
      id       prod_name  price
0  10006   CHAVE INGLESA  145.0
1  10008       FURADEIRA  165.0
2  10005         SERROTE  135.0
3  10004         ALICATE  125.0
4  10007  NÍVEL DE BOLHA  155.0
Total de registros: 10
```

#### Camada Gold - Tabela Dimensão
```
      id id_category  supplier
0  10007        BT16       160
1  10006        BT15       150
2  10005        BT14       140
3  10008        BT17       170
4  10009        BT18       180
Total de registros: 10
```

## 📈 Resultados e Observações

- O pipeline demonstrou eficácia na ingestão e processamento de dados brutos
- A deduplicação foi bem-sucedida, reduzindo 60 registros brutos para 10 registros únicos na camada Silver
- A modelagem dimensional separa corretamente os dados de produtos e suas categorias/fornecedores
- O desempenho mostra-se adequado com processamento completo em apenas ~1.3 segundos

---