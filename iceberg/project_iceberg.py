from pyspark.sql import SparkSession
import phonenumbers

# CRIA UMA SEÇÃO SPARK COM O NOME "IcebergLocalDevelopment"
spark = SparkSession.builder \
  .appName("IcebergLocalDevelopment") \
  .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1') \
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
  .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
  .config("spark.sql.catalog.local.type", "hadoop") \
  .config("spark.sql.catalog.local.warehouse", "spark-warehouse/iceberg") \
  .getOrCreate()

# COMANDO PARA APAGAR A TABELA JÁ EXISTENTE E COMEÇAR DO ZERO
spark.sql("DROP TABLE IF EXISTS local.medico_iceberg")

# CRIA OU RECRIA UMA TABELA CHAMADA medico_iceberg COM 2 COLUNAS: id e nome
spark.sql(
  """
  CREATE TABLE local.medico_iceberg (id INT, nome STRING) USING iceberg
  """
)

# MOSTRA A TABELA (ainda vazia)
spark.sql("select * from local.medico_iceberg").show()

# INSERE OS NOVOS REGISTROS NA TABELA
spark.sql("INSERT INTO local.medico_iceberg VALUES (1, 'William'), (2, 'Iuri'), (3, 'Lucas')")

# MOSTRA A TABELA ATUALIZADA COM OS NOVOS REGISTROS
spark.sql("select * from local.medico_iceberg").show()

# ADICIONA 3 NOVAS COLUNAS: registro, especialidade e telefone
spark.sql(
    """
    alter table local.medico_iceberg add column registro INT, especialidade STRING, telefone STRING
    """
)

# MOSTRA A TABELA COMPLETA COM AS NOVAS COLUNAS E CAMPOS AINDA NULOS
spark.sql(
    """
    select * from local.medico_iceberg
    """
).show()

# ATUALIZA AS NOVAS COLUNAS DO CARRO COM id = 1
spark.sql(
    """
    update local.medico_iceberg set registro = 20205178, especialidade = 'Cardiologista', telefone = '48 99632-1234' where id = 1
    """
)

# MOSTRA A TABELA APÓS A ATUALIZAÇÃO DO CARRO COM id = 1
spark.sql(
    """
    select * from local.medico_iceberg
    """
).show()

# ATUALIZA AS NOVAS COLUNAS DOS CARROS COM id = 2 e 3
spark.sql('update local.medico_iceberg set registro=20221982, especialidade="Ortopedista", telefone="51 98840-9876" where id = 2')
spark.sql('update local.medico_iceberg set registro=20230943, especialidade="Clinico Geral", telefone="41 99154-5555" where id = 3')

# MOSTRA A TABELA APÓS A ATUALIZAÇÃO DOS CARROS COM id = 2 e 3
spark.sql('select * from local.medico_iceberg').show()

# DELETA O CARRO COM id = 3
spark.sql('delete from local.medico_iceberg where id = 3')

# MOSTRA A TABELA ATUALIZADA APÓS A EXCLUSÃO DO id = 3
spark.sql('select * from local.medico_iceberg').show()

# MOSTRA TODO O HISTÓRICO DO QUE FOI REALIZADO
spark.sql("SELECT * FROM local.medico_iceberg.history").show(truncate=False)
