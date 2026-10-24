import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Inicializar Spark Session con soporte para Kafka
spark = SparkSession.builder \
    .appName("CryptoStreamingPipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir el esquema del JSON recibido desde Kafka
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("bitcoin_usd", DoubleType(), True),
    StructField("ethereum_usd", DoubleType(), True),
    StructField("cardano_usd", DoubleType(), True)
])

# Leer el flujo de datos desde Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_prices") \
    .load()

# Extraer y parsear el valor JSON
df_json = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Imprimir los datos en la consola en tiempo real
query = df_json.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
