from pyspark.sql import SparkSession

# Inicialização do SparkSession
spark = SparkSession.builder.appName("Criar Tabelas Delta").getOrCreate()

# Caminhos
dim_times_path = "Files/gold/dim_times"
dim_localizacao_regiao_path = "Files/gold/dim_localizacao_regiao"
dim_localizacao_estado_path = "Files/gold/dim_localizacao_estado"
dim_tempo_path = "Files/gold/dim_tempo"
dim_logo_path = "Files/gold/dim_logos"
df_fato_path = "Files/gold/df_fato"
df_times_path = "Files/gold/df_times"

# Times
df_times = spark.read.format("delta").load(dim_times_path).dropDuplicates()
df_times.write.format("delta").mode("overwrite").saveAsTable("dim_times")

# Dados Times
df_dados_times = spark.read.format("delta").load(df_times_path).dropDuplicates()
df_dados_times.write.format("delta").mode("overwrite").saveAsTable("df_times")


# Localização
df_estado = spark.read.format("delta").load(dim_localizacao_estado_path).dropDuplicates()
df_estado.write.format("delta").mode("overwrite").saveAsTable("dim_estado")

df_regiao = spark.read.format("delta").load(dim_localizacao_regiao_path).dropDuplicates()
df_regiao.write.format("delta").mode("overwrite").saveAsTable("dim_regiao")

# Tempo
df_tempo = spark.read.format("delta").load(dim_tempo_path).dropDuplicates()
df_tempo.write.format("delta").mode("overwrite").saveAsTable("dim_tempo")

# Logo
df_logo = spark.read.format("delta").load(dim_logo_path).dropDuplicates()
df_logo.write.format("delta").mode("overwrite").saveAsTable("dim_logo")

# Fato
df_fato = spark.read.format("delta").load(df_fato_path).dropDuplicates()
df_fato.write.format("delta").mode("overwrite").saveAsTable("df_fato")


# valida as tabelas
tabelas = spark.sql("SHOW TABLES")

tabelas.show()
