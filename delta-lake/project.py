from pyspark.sql import SparkSession
from delta import *
import os

# Inicializa a sessão Spark com suporte ao Delta Lake
spark = (
    SparkSession
    .builder
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# ALTERAR ESSE CAMINHO PARA O DIRETÓRIO LOCAL DA SUA MÁQUINA
file_base = ' '

fullpath = os.path.join(file_base, 'RestaurantePedidosDelta', 'delta-lake', 'spark-warehouse', 'pedido_delta')

# CRIA A TABELA E A PASTA ONDE OS ARQUIVOS PARQUET SÃO SALVOS E EXIBE A TABELA SEM OS DADOS
spark.sql(
    f"""
    CREATE OR REPLACE TABLE pedido_delta (
        id INT, 
        produto STRING,
        quantidade INT,
        preco FLOAT,
        cliente STRING,
        data_pedido STRING
    ) 
    USING delta 
    LOCATION '{fullpath}'
    """
)
spark.sql("SELECT * FROM pedido_delta").show()

# CARREGA O ARQUIVO DE HISTÓRICO DE ALTERAÇÕES E EXIBE
from delta.tables import DeltaTable
pedido = DeltaTable.forPath(spark, "./spark-warehouse/pedido_delta")
pedido.history().show()

# INSERE DADOS DENTRO DA TABELA CRIADA ANTERIORMENTE E EXIBE
spark.sql("""
    INSERT INTO pedido_delta VALUES 
    (1, 'Pizza Margherita', 2, 40.50, 'João Silva', '2023-04-01'),
    (2, 'Hamburguer', 1, 25.00, 'Maria Oliveira', '2023-04-02'),
    (3, 'Salada Caesar', 1, 18.75, 'Carlos Lima', '2023-04-03')
""")
spark.sql("SELECT * FROM pedido_delta").show()
pedido.history().show(truncate=False)

# ADICIONA NOVAS COLUNAS NA TABELA E EXIBE
spark.sql("""
    ALTER TABLE pedido_delta ADD COLUMNS (
        endereco_entrega STRING,
        status_pedido STRING
    )
""")
spark.sql("SELECT * FROM pedido_delta").show()

# ATUALIZA OS DADOS INSERIDOS ANTERIORMENTE E EXIBE
spark.sql("""
    UPDATE pedido_delta 
    SET endereco_entrega = 'Rua das Flores, 123', status_pedido = 'Entregue' 
    WHERE id = 1
""")
spark.sql("""
    UPDATE pedido_delta 
    SET endereco_entrega = 'Av. Paulista, 1000', status_pedido = 'Em Preparo' 
    WHERE id = 2
""")
spark.sql("""
    UPDATE pedido_delta 
    SET endereco_entrega = 'Rua do Sol, 456', status_pedido = 'Entregue' 
    WHERE id = 3
""")
spark.sql("SELECT * FROM pedido_delta").show()

# EXIBE O HISTÓRICO DE ALTERAÇÕES DA TABELA pedido_delta
spark.sql("DESCRIBE HISTORY pedido_delta").show(truncate=False)
