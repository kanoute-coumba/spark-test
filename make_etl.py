# coding: utf-8
# Importation des bibliothèques nécessaires
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count, window
from pyspark.sql.window import Window

# Initialisons la session Spark
spark = SparkSession.builder.appName("Analyse des retards de vol").getOrCreate()

# Chargeons le jeu de données dans un DataFrame Spark
df = spark.read.csv("datasets/flights.csv", header=True, inferSchema=True)

# On affiche les 10 premières lignes et imprime le schéma pour comprendre la structure du jeu de données
df.show(10)
df.printSchema()

# Nettoyage des données avec l'API DataFrame
# On ajoute une nouvelle colonne indiquant si un vol a été retardé de plus de 15 minutes
df = df.withColumn("retard_plus_15", when(col("DEPARTURE_DELAY") > 15, 1).otherwise(0))
# on gere les valeurs manquantes de manière appropriée dans les colonnes critiques pour l'analyse
df_cleaned = df.dropna()
df_filled = df.fillna(value=0)
'''
# Remplir les valeurs manquantes avec les statistiques agrégées
# D'abord on calcule les statistiques agrégées pour chaque colonne
df_stats = df.select([avg(c).alias(c) for c in df.columns])
# On récupére la première ligne du DataFrame de statistiques agrégées
row_stats = df_stats.first()
# Conversion de la ligne en un dictionnaire
stats_dict = row_stats.asDict()
# On emplie les valeurs manquantes avec les statistiques agrégées
df_imputed = df.na.fill(stats_dict)
# le résultat
df_imputed.show()
'''
# Agrégation et regroupement
# Calculer la moyenne du retard par compagnie et par aéroport de départ
df_grouped = df.groupBy("AIRLINE", "ORIGIN_AIRPORT").agg(avg("DEPARTURE_DELAY").alias("mean_delay"))
# On affiche le résultat
df_grouped.show()
# Sous forme de tableau - beaucoup plus lisible
df_grouped.orderBy("AIRLINE", "ORIGIN_AIRPORT").show()

'''
# Tri et classement
# Liste les 10 premiers aéroports les plus retardés
df_sorted = df.orderBy("retard_depart", ascending=False)
df_sorted.show(10)

# Opérations avancées avec les fonctions de fenêtre
# Classer les aéroports par le nombre de vols de départ
window_spec = Window.partitionBy("aeroport_depart").orderBy("retard_depart")
df_ranked = df.select("aeroport_depart", "retard_depart", window.rank().over(window_spec).alias("rang"))
df_ranked.show(10)

# Opérations sur les RDD
# Convertir le DataFrame en RDD
rdd = df.rdd

# Effectuer une opération map-reduce pour compter le nombre de vols par compagnie
vols_par_compagnie = rdd.map(lambda row: (row.compagnie, 1)).reduceByKey(lambda a, b: a + b)
vols_par_compagnie_df = spark.createDataFrame(vols_par_compagnie, ["compagnie", "vols"])
vols_par_compagnie_df.show()

# Partitionnement
# Partitionner les données en fonction d'une clé appropriée, par exemple l'aéroport d'arrivée
df_partitioned = df.repartition(10, "aeroport_arrivee")

# Analyse et rapport
# Fournir des insights sur les données via les opérations effectuées, mettant en évidence les résultats intéressants
print("Moyenne du retard par compagnie:")
df_grouped_by_compagnie.show()

print("Moyenne du retard par aéroport de départ:")
df_grouped_by_depart.show()

print("Nombre de vols par compagnie:")
vols_par_compagnie_df.show()

print("Top 10 des aéroports les plus retardés:")
df_ranked.filter(col("rang") <= 10).show()
'''