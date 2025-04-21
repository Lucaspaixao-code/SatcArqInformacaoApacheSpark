from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from delta import *
import os

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# A cada interação com a tabela 'carro_delta' serão criados arquivos dentro do diretório delta-lake/spark-warehouse/carro_delta.
# Os arquivos consistem de dois tipos: '.parquet' e '.parquet.crc'.
# Os arquivos '.parquet' é onde são salvos as informações em formato colunar, onde vão ser lidos de maneira eficiente pelo ambiente analítico.
# Os arquivos '.parquet.crc' são arquivos de checksum que servem para validar a integridade dos arquivos '.parquet'. 


# ALTERAR ESSE CAMINHO DE FORMA QUE APONTE PARA ONDE O DIRETÓRIO DO TRABALHO ESTÁ LOCALIZADO NA SUA MÁQUINA
file_base = 'file:////home/ed/SatcArqInformacaoApacheSpark/'


fullpath = os.path.join(file_base, 'delta-lake', 'spark-warehouse', 'carro_delta')

# CRIA A TABELA E A PASTA ONDE OS ARQUIVOS PARQUET SÃO SALVOS E EXIBE A TABELA SEM OS DADOS
spark.sql(
  f"""
  CREATE OR REPLACE TABLE carro_delta (id INT, placa STRING) 
  USING delta 
  LOCATION '{fullpath}'
  """
)
spark.sql("select * from carro_delta").show()

# CARREGA O ARQUIVO DE HISTÓRICO DE ALTERAÇÕES E EXIBE
from delta.tables import DeltaTable
carro = DeltaTable.forPath(spark, "./delta-lake/spark-warehouse/carro_delta") # ESTE ENDEREÇO DEVE ESTAR COMPATÍVEL COM O ENDEREÇO fullpath
carro.history().show()

# INSERE DADOS DENTRO DA TABELA CRIADA ANTERIORMENTE E EXIBE
spark.sql("INSERT INTO carro_delta VALUES (1, 'XYZ1J34'), (2, 'RLC5B93'), (3, 'ABV1V23')")
spark.sql("select * from carro_delta").show()
carro.history().show(truncate=False)

# CRIA NOVAS COLUNAS NA TABELA E EXIBE
spark.sql("""alter table carro_delta add column marca STRING, modelo STRING, ano INT""")
spark.sql("""select * from carro_delta""").show()

# REALIZA UPDATE DOS DADOS INSERIDOS ANTERIORMENTE E EXIBE
spark.sql("""update carro_delta set marca = 'Renault', modelo = 'Sandero', ano = 2021 where id = 1""")
spark.sql("""select * from carro_delta""").show()

# REALIZA UPDATE DOS DADOS INSERIDOS ANTERIORMENTE E EXIBE
spark.sql('update carro_delta set marca="GM", modelo="tracker", ano=2020 where id = 2')
spark.sql('update carro_delta set marca="Ford", modelo="EcoSport", ano=2022 where id = 3')
spark.sql('select * from carro_delta').show()

# REALIZA EXIBIÇÃO DO HISTÓRICO DE ALTERAÇÕES DA TABELA carro_delta
spark.sql('describe HISTORY carro_delta').show(truncate=False)