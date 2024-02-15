from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, dense_rank
from pyspark.sql.window import Window
from pyspark import SparkFiles

# Initialisation la session Spark
spark = SparkSession.builder.appName("AnalyseRetardsVols").getOrCreate()

# Ajout du fichier token Kaggle à l'environnement Spark
chemin_token_kaggle = "/home/hadoop/.kaggle/kaggle.json"
spark.sparkContext.addFile("file://" + chemin_token_kaggle)

# Chargement et exploration des données depuis Kaggle
kaggle_dataset_url = "kaggle datasets download -d usdot/flight-delays -p /content"
spark.sparkContext.addFile(kaggle_dataset_url)
donnees_vols = spark.read.csv("file://" + SparkFiles.get("flight-delays.zip"), header=True, inferSchema=True)

# Affichage les 10 premières lignes et imprimer le schéma pour comprendre la structure des données
donnees_vols.show(10)
donnees_vols.printSchema()

# Nettoyage des données avec l'API DataFrame
donnees_vols = donnees_vols.withColumn("retardé", when(col("ARRIVAL_DELAY") > 15, 1).otherwise(0))
donnees_vols = donnees_vols.fillna(0, subset=["ARRIVAL_DELAY"])  # pour gerer les valeurs manquantes

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

# ...

# Arrêter la session Spark
spark.stop()

