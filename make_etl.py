# coding: utf-8
# Importation des bibliothèques nécessaires
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count, window
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, row_number

# Pour le github Actions : variable d'environnement pour définir le chemin du dataset
dataset_path = os.environ.get("DATASET_PATH", "/datasets/flights.csv")
# Initialisons la session Spark
spark = SparkSession.builder.appName("Analyse des retards de vol").getOrCreate()

# Chemin complet du fichier CSV
csv_path = os.path.join(os.getcwd(), dataset_path)

###################################################################
print("Chemin complet du fichier CSV :", csv_path)
print("Répertoire de travail actuel :", os.getcwd())
###################################################################

# Chargeons le fichier CSV dans un DataFrame Spark
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Chargeons le jeu de données dans un DataFrame Spark
# df = spark.read.csv("datasets/flights.csv", header=True, inferSchema=True)

# On affiche les 10 premières lignes et imprime le schéma pour comprendre la structure du jeu de données
df.show(10)
df.printSchema()

# Nettoyage des données avec l'API DataFrame
# On ajoute une nouvelle colonne indiquant si un vol a été retardé de plus de 15 minutes
df = df.withColumn("retard_plus_15", when(col("DEPARTURE_DELAY") > 15, 1).otherwise(0))
# on gere les valeurs manquantes de manière appropriée dans les colonnes critiques pour l'analyse
df_cleaned = df.dropna()
df_filled = df.fillna(value=0)

# Agrégation et regroupement
# Calculer la moyenne du retard par compagnie et par aéroport de départ
df_grouped = df.groupBy("AIRLINE", "ORIGIN_AIRPORT").agg(avg("DEPARTURE_DELAY").alias("mean_delay"))
# On affiche le résultat
df_grouped.show()
# Triée par compagnie et par aeroport de depart  
df_grouped.orderBy("AIRLINE", "ORIGIN_AIRPORT").show(df_grouped.count(), truncate=False)

# Tri et classement
# Liste les 10 premiers aéroports les plus retardés
# on calcule d'abord la moyenne du retard par aéroport de départ
df_grouped = df.groupBy("ORIGIN_AIRPORT").agg(avg("DEPARTURE_DELAY").alias("mean_delay"))
# On trie les aéroports par retard moyen décroissant
df_sorted = df_grouped.orderBy(df_grouped["mean_delay"].desc())
# Et on affiche les 10 premiers aéroports les plus retardés
df_sorted.limit(10).show()

# Opérations avancées avec les fonctions de fenêtre
# Classer les aéroports par le nombre de vols de départ
# On définit une fenêtre pour trier les aéroports par nombre de vols décroissant
window_spec = Window.orderBy(desc(col("FLIGHT_NUMBER")))
# On calcule le nombre de vols par aéroport de départ
df_count = df.groupBy("ORIGIN_AIRPORT").agg(count("FLIGHT_NUMBER").alias("count"))
# On joute un numéro de rang pour chaque aéroport de départ
df_ranked = df_count.withColumn("rank", row_number().over(window_spec))
# Puis on affiche les 10 premiers aéroports par nombre de vols de départ
df_ranked.filter(df_ranked["rank"] <= 10).orderBy(df_ranked["rank"]).show()

# Opérations sur les RDD
# Convertir le DataFrame en RDD
rdd = df.rdd

# Effectuer une opération map-reduce pour compter le nombre de vols par compagnie
# On calcule le nombre de vols par compagnie aérienne
df_grouped = df.groupBy("AIRLINE").agg(count("FLIGHT_NUMBER").alias("count"))
# On affiche les résultats
df_grouped.show()

# Partitionnement
# Partitionner les données en fonction d'une clé appropriée, par exemple l'aéroport d'arrivée
# On définit une fenêtre pour partitionner les données par aéroport d'arrivée
window_spec = Window.partitionBy("DESTINATION_AIRPORT").orderBy(desc("count"))
# On regroupe les données par aéroport d'arrivée et on compte le nombre de vols
df_grouped = df.groupBy("DESTINATION_AIRPORT").agg(count("FLIGHT_NUMBER").alias("count"))
# On ajoute un numéro de rang pour chaque ligne de la fenêtre de partitionnement
df_ranked = df_grouped.withColumn("rank", row_number().over(window_spec))
# Enfin on affiche les résultats
df_ranked.orderBy(["DESTINATION_AIRPORT", "rank"], ascending=[True, False]).show()

# Analyse et rapport
# Fournir des insights sur les données via les opérations effectuées, mettant en évidence les résultats intéressants
# Retards les plus fréquents
df_delays = df.groupBy("DEPARTURE_DELAY").count().orderBy(desc("count"))
df_delays.show(10)
# Retards moyens par compagnie aérienne
df_airline_delays = df.groupBy("AIRLINE").agg(avg("DEPARTURE_DELAY").alias("mean_delay")).orderBy(desc("mean_delay"))
df_airline_delays.show(10)
# Retards moyens par aéroport de départ
df_origin_airport_delays = df.groupBy("ORIGIN_AIRPORT").agg(avg("DEPARTURE_DELAY").alias("mean_delay")).orderBy(desc("mean_delay"))
df_origin_airport_delays.show(10)
# Retards moyens par aéroport d'arrivée
df_destination_airport_delays = df.groupBy("DESTINATION_AIRPORT").agg(avg("DEPARTURE_DELAY").alias("mean_delay")).orderBy(desc("mean_delay"))
df_destination_airport_delays.show(10)
# Retards moyens par jour de la semaine
df_day_of_week_delays = df.groupBy("DAY_OF_WEEK").agg(avg("DEPARTURE_DELAY").alias("mean_delay")).orderBy(desc("mean_delay"))
df_day_of_week_delays.show(10)