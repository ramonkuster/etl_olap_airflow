ETL Orquestrado com Apache Airflow e Docker: Governança e Qualidade de Dados

Este repositório contém o código e a arquitetura para um pipeline robusto de Extract, Transform, Load (ETL), focado na orquestração de Data Warehouse (DW) utilizando Apache Airflow, conteinerizado com Docker, e com uma etapa rigorosa de Data Quality (DQ) para garantir a integridade dos dados analíticos.

🚀 Tecnologias

Categoria

Tecnologia

Uso

Orquestração

Apache Airflow

Agendamento e monitoramento dos fluxos de ETL (DAGs).

Conteinerização

Docker/Docker Compose

Isolamento e reprodutibilidade do ambiente (Airflow, PostgreSQL).

Banco de Dados

PostgreSQL

Servidor do Data Warehouse (DW) e simulação do sistema de origem (OLTP/Stage).

Modelagem

Star Schema

Modelo dimensional otimizado para consultas analíticas.

📐 Arquitetura do Projeto

A solução é composta por uma arquitetura multi-container que simula um ambiente de produção de dados completo:

Airflow Containers: Inclui o Scheduler, Worker e Webserver, gerenciando a execução das DAGs.

PostgreSQL DW: O banco de dados de destino onde o Star Schema é construído e os dados são carregados.

PostgreSQL Stage: Simula o banco de dados de origem (OLTP), de onde o Airflow extrai os dados brutos.

O Modelo Dimensional

O DW é modelado em Star Schema, garantindo performance analítica:

Tabela Fato (FactOrder): Contém as métricas de negócio (ex: SalesAmount) e as chaves substitutas (SKs).

Tabelas Dimensão (DimCustomer, DimProduct, DimDate): Fornecem o contexto para as análises.

✅ Data Quality (DQ) como Gate de Governança

O principal diferencial deste pipeline é a implementação de um gate de Data Quality (DQ) após o carregamento da Tabela Fato.

A tarefa de DQ executa comandos SQL para verificar:

Integridade Referencial: Contagem de chaves substitutas (SK) nulas na FactOrder.

Validade de Domínio: Verificação de valores ilógicos em métricas críticas (ex: SalesAmount <= 0).

Mecanismo de Falha: Se a contagem de erros for maior que zero, o pipeline é abortado imediatamente. Isso impede que dados inconsistentes cheguem à camada de Business Intelligence (BI), assegurando a confiabilidade dos relatórios.

⚙️ Visão Geral das DAGs

As DAGs (Directed Acyclic Graphs) são responsáveis por cada etapa do ETL.

DAG

Função

Descrição

dag_init_dw.py

Inicialização

Executa o DDL (Data Definition Language) para criar o Schema e todas as tabelas do Star Schema no PostgreSQL.

dag_load_dim_date.py

Carga de Dimensão Única

Gera e popula a tabela DimDate com atributos temporais para um horizonte de tempo definido.

etl_data_warehouse_v2

Pipeline de Produção

A DAG principal. Executa o carregamento de dimensões em paralelo, carrega a FactOrder e, por último, executa a Data Quality Check (DQ).

🛠 Como Rodar o Projeto

Para replicar o ambiente localmente, você precisará ter o Docker e o Docker Compose instalados.

Clone o repositório:

git clone (https://github.com/ramonkuster/etl_olap_airflow.git)
cd airflow-project


Inicie os Containers:
O docker-compose.yml irá subir o Airflow (Webserver, Scheduler, Worker), o PostgreSQL do DW e o PostgreSQL de Stage.

docker-compose up -d


Acesse o Airflow:
Aguarde alguns minutos para que todos os serviços inicializem.

Interface Web: Acesse http://localhost:8080 (usuário e senha padrão: airflow/airflow).

Execute as DAGs:

Execute primeiro a DAG de inicialização: dag_init_dw.py.

Em seguida, execute as DAGs de carga, começando pela principal, etl_data_warehouse_v2, para observar o fluxo paralelo e a checagem de DQ.
