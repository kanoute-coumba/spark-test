# coding: utf-8
# Importation des bibliothèques nécessaires

import os
import zipfile
import pandas as pd
from pyspark.sql import SparkSession
import kaggle
from pyspark.sql.functions import when, avg, col
from pyspark.sql.window import Window

# Répertoire contenant la clé API kaggle.json
os.environ['KAGGLE_CONFIG_DIR'] = '~/.kaggle/kaggle.json'

# Configurons la session Spark
spark = SparkSession.builder.appName("FlightDelaysETL").getOrCreate()
spark.conf.set("fs.defaultFS", "hdfs://192.168.1.100:9000")
os.environ['HADOOP_CONF_DIR'] = '$HADOOP_HOME/etc/hadoop'
os.environ['YARN_CONF_DIR'] = '$HADOOP_HOME/etc/hadoop'

# L'URL du dataset Kaggle
kaggle_dataset_url = "usdot/flight-delays"

# Chemin local d'extraction
local_extract_path = '/home/hadoop/spark-test/dataset'

# Téléchargement du dataset en tant que Pandas DataFrame
kaggle.api.dataset_download_files(dataset=kaggle_dataset_url, unzip=True, quiet=False, path=local_extract_path)

# Liste des fichiers extraits
extracted_files = os.listdir(local_extract_path)

# Initialisation des DataFrames Spark
airlines_df = None
airports_df = None
flights_df = None

# Lire chaque fichier CSV et charger dans le DataFrame Spark correspondant
for file_name in extracted_files:
    csv_file_path = os.path.join(local_extract_path, file_name)

    if "airlines" in file_name:
        airlines_df = spark.read.csv("file://" + csv_file_path, header=True, inferSchema=True)
    elif "airports" in file_name:
        airports_df = spark.read.csv("file://" + csv_file_path, header=True, inferSchema=True)
    elif "flights" in file_name:
        flights_df = spark.read.csv("file://" + csv_file_path, header=True, inferSchema=True)

# Chemin dans HDFS où vous souhaitez sauvegarder les DataFrames
hdfs_path = 'hdfs://192.168.1.100:9000/'

# Copie des fichiers vers HDFS
for file_name in extracted_files:
    local_file_path = os.path.join(local_extract_path, file_name)
    hdfs_file_path = hdfs_path + file_name
    os.system(f'hdfs dfs -copyFromLocal {local_file_path} {hdfs_file_path}')

# Sauvegarde des DataFrames dans HDFS
airlines_df.write.csv(hdfs_path + 'airlines')
airports_df.write.csv(hdfs_path + 'airports')
flights_df.write.csv(hdfs_path + 'flights')

# Chargement des DataFrames depuis HDFS
airlines_df = spark.read.csv(hdfs_path + 'airlines', header=True, inferSchema=True)
airports_df = spark.read.csv(hdfs_path + 'airports', header=True, inferSchema=True)
flights_df = spark.read.csv(hdfs_path + 'flights', header=True, inferSchema=True)

# Affichons le schéma et quelques lignes du DataFrame des vols
flights_df.printSchema()
flights_df.show(10)

# Nettoyage des données avec l'API DataFrame
donnees_vols = flights_df.withColumn("retardé", when(col("ARRIVAL_DELAY") > 15, 1).otherwise(0))
donnees_vols = donnees_vols.fillna(0, subset=["ARRIVAL_DELAY"])  # pour gérer les valeurs manquantes

# Agrégation et regroupement des données
retard_moyen_par_compagnie = donnees_vols.groupBy("AIRLINE").agg(avg("ARRIVAL_DELAY").alias("retard_moyen"))
retard_moyen_par_aeroport = donnees_vols.groupBy("ORIGIN_AIRPORT").agg(avg("ARRIVAL_DELAY").alias("retard_moyen"))

# Tri et ordonnancement
top_10_vols_plus_retardes = donnees_vols.orderBy("ARRIVAL_DELAY", ascending=False).limit(10)

# Manipulation avancée des données avec les fonctions de fenêtre
fenetre_speciale = Window.partitionBy("ORIGIN_AIRPORT").orderBy(col("DEPARTURE_DELAY").desc())
aeroports_classes = donnees_vols.withColumn("rang", dense_rank().over(fenetre_speciale))

# Opérations RDD
donnees_vols_rdd = donnees_vols.rdd
vols_par_compagnie = donnees_vols_rdd.map(lambda x: (x["AIRLINE"], 1)).reduceByKey(lambda a, b: a + b)

# Partitionnement
donnees_vols_partitionnees = donnees_vols.repartition("DESTINATION_AIRPORT")

# Analyse et rapports
# Insights basés sur les opérations effectuées

# Analyse : Retard moyen par compagnie aérienne
retard_moyen_par_compagnie.show()

# Analyse : Retard moyen par aéroport d'origine
retard_moyen_par_aeroport.show()

# Analyse : Top 10 des vols les plus retardés
top_10_vols_plus_retardes.show()

# Analyse : Classement des aéroports par nombre de vols au départ
aeroports_par_nombre_departs = donnees_vols.groupBy("ORIGIN_AIRPORT").count().orderBy("count", ascending=False)
aeroports_par_nombre_departs.show()

# Analyse : Nombre total de vols par compagnie aérienne
vols_par_compagnie.collect()

# Analyse : Partitionnement par aéroport de destination
donnees_vols_partitionnees.show()

# Arrêt de la session Spark
spark.stop()

