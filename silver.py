from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum, when
from pyspark.sql.types import IntegerType, FloatType, StringType, DateType

def listar_nulos(df):
    null_counts = df.select([
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df.columns
    ])
    null_counts.show()

# Caminhos
bronze_path_brasileirao = "Files/bronze/brasileirao"
bronze_path_times = "Files/bronze/times"
bronze_path_logo = "Files/bronze/logos"
bronze_path_resultados = "Files/bronze/resultados"

# Ler os dados salvos para verificar
df_brasileirao = spark.read.format("delta").load(bronze_path_brasileirao)
df_times = spark.read.format("delta").load(bronze_path_times)
df_logo = spark.read.format("delta").load(bronze_path_logo)
df_resultados = spark.read.format("delta").load(bronze_path_resultados)

# Exibir o DataFrame
display(df_brasileirao)
display(df_times)
display(df_logo)
display(df_resultados)
df_resultados.printSchema()
df_logo.printSchema()
df_brasileirao.printSchema()
df_times.printSchema()

listar_nulos(df_resultados)
listar_nulos(df_times)
listar_nulos(df_brasileirao)
listar_nulos(df_logo)
df_resultados = df_resultados.withColumn("data", F.to_date(df_resultados["data"], "dd-mm-yyyy"))
df_resultados = df_resultados.withColumn("gols_casa", df_resultados["gols_casa"].cast(IntegerType()))
df_resultados = df_resultados.withColumn("gols_visitante", df_resultados["gols_visitante"].cast(IntegerType()))

df_times = df_times.withColumn("ano_de_fundacao", F.to_date(df_times["ano_de_fundacao"], "yyyy"))

df_brasileirao = df_brasileirao.withColumn("pontos", df_brasileirao["pontos"].cast(IntegerType()))
df_brasileirao = df_brasileirao.withColumn("partidas", df_brasileirao["partidas"].cast(IntegerType()))
df_brasileirao = df_brasileirao.withColumn("colocacao", df_brasileirao["colocacao"].cast(IntegerType()))
df_brasileirao = df_brasileirao.withColumn("vitorias", df_brasileirao["vitorias"].cast(IntegerType()))
df_brasileirao = df_brasileirao.withColumn("empates", df_brasileirao["empates"].cast(IntegerType()))
df_brasileirao = df_brasileirao.withColumn("derrotas", df_brasileirao["derrotas"].cast(IntegerType()))
df_brasileirao = df_brasileirao.withColumn("gols", df_brasileirao["gols"].cast(IntegerType()))
df_brasileirao = df_brasileirao.withColumn("gols_sofridos", df_brasileirao["gols_sofridos"].cast(IntegerType()))
df_brasileirao = df_brasileirao.withColumn("saldo_de_gols", df_brasileirao["saldo_de_gols"].cast(IntegerType()))

df_brasileirao = df_brasileirao.withColumn("ano", F.to_date(df_brasileirao["ano"], "yyyy"))


df_resultados.printSchema()
display(df_resultados)
df_brasileirao.printSchema()
display(df_brasileirao)
df_times.printSchema()
display(df_times)

silver_path_brasileirao = "Files/silver/brasileirao"
silver_path_times = "Files/silver/times"
silver_path_logo = "Files/silver/logos"
silver_path_resultados = "Files/silver/resultados"

df_times.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_path_times)

df_resultados.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_path_resultados)

df_logo.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_path_logo)

df_brasileirao.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_path_brasileirao)

df_times_silver = spark.read.format("delta").load(silver_path_times)
display(df_times_silver)

df_logo_silver = spark.read.format("delta").load(silver_path_logo)
display(df_logo_silver)

df_brasileirao_silver = spark.read.format("delta").load(silver_path_brasileirao)
display(df_brasileirao_silver)

df_resultados_silver = spark.read.format("delta").load(silver_path_resultados)
display(df_resultados_silver)

df_times.unpersist()
df_times_silver.unpersist()
df_brasileirao.unpersist()
df_brasileirao_silver.unpersist()
df_logo.unpersist()
df_logo_silver.unpersist()
df_resultados.unpersist()
df_resultados_silver.unpersist()
