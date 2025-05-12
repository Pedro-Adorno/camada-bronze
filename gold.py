from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window
from pyspark.sql import functions as F

silver_path_brasileirao = "Files/silver/brasileirao"
silver_path_times = "Files/silver/times"
silver_path_logo = "Files/silver/logos"
silver_path_resultados = "Files/silver/resultados"

# Ler os dados salvos para verificar
df_times = spark.read.format("delta").load(silver_path_times)
df_brasileirao = spark.read.format("delta").load(silver_path_brasileirao)
df_logo = spark.read.format("delta").load(silver_path_logo)
df_resultados = spark.read.format("delta").load(silver_path_resultados)

# Exibir o DataFrame
display(df_times)
display(df_brasileirao)
display(df_logo)


# Seleciona a coluna e remove duplicados
dim_times = df_times.select('time').dropDuplicates()

# Cria uma janela ordenando pela coluna 'time' (ordem alfabética ou numérica)
window_spec = Window.orderBy('time')

# Cria a chave substituta (surrogate key) com base na ordem alfabética ou numérica de 'time'
dim_times = dim_times.withColumn("sk_times", row_number().over(window_spec))

# Conta total de linhas
total_linhas = dim_times.count()
print(f'Total de {total_linhas} Linhas')

display(dim_times)

display(df_resultados)

df_resultados.printSchema()
df_logo.printSchema()
df_times.printSchema()
df_brasileirao.printSchema()


# Seleciona a coluna e remove duplicados
dim_logo = df_logo.select('link').dropDuplicates()
dim_link = df_logo.select('time', 'link').drop_duplicates()

# Cria uma janela ordenando pela coluna 'time' (ordem alfabética ou numérica)
window_spec = Window.orderBy('link')

# Cria a chave substituta (surrogate key) com base na ordem alfabética ou numérica de 'time'
dim_logo = dim_logo.withColumn("sk_logos", row_number().over(window_spec))

dim_logo = dim_link.join(
    dim_logo,
    on=['link'],
    how='inner'
)

# Conta total de linhas
total_linhas = dim_logo.count()
print(f'Total de {total_linhas} Linhas')

display(dim_logo)

# Seleciona colunas e remove duplicados
dim_localizacao_regiao = df_times.select('regiao').dropDuplicates()
dim_times_regiao = df_times.select('time', 'regiao').drop_duplicates()
dim_localizacao_estado = df_times.select('estado').dropDuplicates()
dim_times_estado = df_times.select('time', 'estado').drop_duplicates()

# Cria janelas ordenadas alfabeticamente
window_regiao = Window.orderBy('regiao')
window_estado = Window.orderBy('estado')

# Cria chave substituta incremental com base na ordenação alfabética
dim_localizacao_regiao = dim_localizacao_regiao.withColumn("sk_regiao", row_number().over(window_regiao))
dim_localizacao_estado = dim_localizacao_estado.withColumn("sk_estado", row_number().over(window_estado))

# Faz o join para obter a dimensão final com a chave
dim_localizacao_regiao = dim_times_regiao.join(
    dim_localizacao_regiao,
    on=['regiao'],
    how='inner'
)

dim_localizacao_estado = dim_times_estado.join(
    dim_localizacao_estado,
    on=['estado'],
    how='inner'
)

# Conta total de linhas
total_linhas_regiao = dim_localizacao_regiao.count()
print(f'Total de {total_linhas_regiao} Linhas')

total_linhas_estado = dim_localizacao_estado.count()
print(f'Total de {total_linhas_estado} Linhas')

display(dim_localizacao_regiao)
display(dim_localizacao_estado)
# Seleciona a coluna de data e remove duplicados
dim_tempo = df_brasileirao.select('ano').dropDuplicates()

# Seleciona a coluna de ano e remove duplicados
dim_tempo = df_brasileirao.select('ano').dropDuplicates()

# Cria surrogate key incremental com ordenação pelo ano
window_spec = Window.orderBy("ano")
dim_tempo = dim_tempo.withColumn("sk_ano", row_number().over(window_spec))

# Conta total de linhas
total_linhas = dim_tempo.count()
print(f'Total de {total_linhas} Linhas')

# Exibe o DataFrame em ordem crescente de ano
display(dim_tempo.orderBy("ano"))

# Join com dimensão produto
df_fato = df_brasileirao
df_times = df_times
df_resultados = df_resultados

# Join com dimensão produto
df_fato = df_fato.join(
    dim_times,
    on=['time'],
    how='inner'
)

# Join com dimensão localização
df_fato = df_fato.join(
    dim_localizacao_regiao,
        on=['time'],
    how='inner'
)

df_fato = df_fato.join(
    dim_localizacao_estado,
    on=['time'],
    how='inner'
)

df_fato = df_fato.join(
    dim_logo,
    on=['time'],
    how='inner'
)


# Join com dimensão tempo
df_fato = df_fato.join(
    dim_tempo,
    on='ano',
    how='inner'
)

# Seleciona apenas as colunas necessárias
df_fato = df_fato.select(
    'sk_regiao',
    'sk_ano',
    'sk_estado',
    'sk_times',
    'sk_logos',
    'time',
    'ano',
    'colocacao',
    'pontos',
    'vitorias',
    'derrotas',
    'empates',
    'gols',
    'gols_sofridos',
    'saldo_de_gols',
    'partidas'
)


# Conta total de linhas
total_linhas = df_fato.count()
print(f'Total de {total_linhas} Linhas')

display(df_fato)


# Caminhos
dim_times_path = "Files/gold/dim_times"
dim_localizacao_regiao_path = "Files/gold/dim_localizacao_regiao"
dim_localizacao_estado_path = "Files/gold/dim_localizacao_estado"
dim_tempo_path = "Files/gold/dim_tempo"
dim_logo_path = "Files/gold/dim_logos"
df_fato_path = "Files/gold/df_fato"
df_times_path = "Files/gold/df_times"
df_resultados_path = "Files/gold/df_resultados"


# Salvar em Delta Parquet

dim_times.write.format("delta").mode("overwrite").save(dim_times_path)
dim_localizacao_regiao.write.format("delta").mode("overwrite").save(dim_localizacao_regiao_path)
dim_localizacao_estado.write.format("delta").mode("overwrite").save(dim_localizacao_estado_path)
dim_tempo.write.format("delta").mode("overwrite").save(dim_tempo_path)
dim_logo.write.format("delta").mode("overwrite").save(dim_logo_path)
df_fato.write.format("delta").mode("overwrite").save(df_fato_path)
df_times.write.format("delta").mode("overwrite").save(df_times_path)
df_times.write.format("delta").mode("overwrite").save(df_resultados_path)
