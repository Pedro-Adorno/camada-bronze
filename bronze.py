times = "Files/teams.csv"
brasileirao = "Files/brasileirao.csv"
resultados = "Files/results.csv"
logo = "Files/logos.csv"

df_resultados = spark.read.option("delimiter", ";").format("csv").option("header","true").load(resultados)
df_brasileirao = spark.read.format("csv").option("header","true").load(brasileirao)
df_times = spark.read.format("csv").option("header","true").load(times)
df_logo = spark.read.format("csv").option("header", "true").load(logo)

display(df_resultados)
display(df_logo)
display(df_brasileirao)
display(df_times)


bronze_path_times = "Files/bronze/times"
bronze_path_brasileirao = "Files/bronze/brasileirao"
bronze_path_logo = "Files/bronze/logos"
bronze_path_resultados = "Files/bronze/resultados"

df_resultados = df_resultados.withColumnRenamed("date", "data") \
        .withColumnRenamed("home_team", "time_casa") \
        .withColumnRenamed("away_team", "time_visitante") \
        .withColumnRenamed("home_score", "gols_casa") \
        .withColumnRenamed("away_score", "gols_visitante")



df_times = df_times.withColumnRenamed("team", "time") \
        .withColumnRenamed("acronym", "sigla") \
        .withColumnRenamed("full_name", "nome_completo") \
        .withColumnRenamed("founded", "ano_de_fundacao") \
        .withColumnRenamed("stadium", "estadio") \
        .withColumnRenamed("city", "cidade") \
        .withColumnRenamed("state", "estado") \
        .withColumnRenamed("region", "regiao")

df_brasileirao = df_brasileirao.withColumnRenamed("season", "ano") \
        .withColumnRenamed("place", "colocacao") \
        .withColumnRenamed("team", "time") \
        .withColumnRenamed("points", "pontos") \
        .withColumnRenamed("played", "partidas") \
        .withColumnRenamed("won", "vitorias") \
        .withColumnRenamed("draw", "empates") \
        .withColumnRenamed("loss", "derrotas") \
        .withColumnRenamed("goals", "gols") \
        .withColumnRenamed("goals_taken", "gols_sofridos") \
        .withColumnRenamed("goals_diff", "saldo_de_gols") \

df_times.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(bronze_path_times)

df_resultados.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(bronze_path_resultados)

df_logo.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(bronze_path_logo)

df_brasileirao.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(bronze_path_brasileirao)

df_resultados_bronze = spark.read.format("delta").load(bronze_path_resultados)
df_logo_bronze = spark.read.format("delta").load(bronze_path_logo)
df_times_bronze = spark.read.format("delta").load(bronze_path_times)
df_brasileirao_bronze = spark.read.format("delta").load(bronze_path_brasileirao)
display(df_resultados_bronze)
display(df_times_bronze)
display(df_brasileirao_bronze)
display(df_logo_bronze)

df_resultados.unpersist()
df_resultados_bronze.unpersist()
df_times.unpersist()
df_times_bronze.unpersist()
df_brasileirao.unpersist()
df_brasileirao_bronze.unpersist()
df_logo.unpersist()
df_logo_bronze.unpersist()
