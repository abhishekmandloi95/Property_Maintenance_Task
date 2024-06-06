import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

logging.basicConfig(filename='data_pipeline.log', level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

spark = SparkSession.builder \
    .appName("PropertyMaintenanceDataPipeline") \
    .config("spark.jars", "postgresql-42.7.3.jar") \
    .getOrCreate()

# Load the dataset
file_path = './synthetic_property_data.csv'
df = spark.read.csv(file_path, header=True, inferSchema=True)
logging.info('Dataset loaded successfully')

# Database connection
db_url = "jdbc:postgresql://localhost:5432/property_db"
db_properties = {
    "user": "myuser",
    "password": "mypassword",
    "driver": "org.postgresql.Driver"
}

# Creating dimension tables
try:
    property_dim = df.select("property_id", "region_name", "construction_year", "occupants").distinct()
    property_dim.write.jdbc(url=db_url, table="property_dimension", mode="overwrite", properties=db_properties)
    logging.info('property_dimension table created successfully')
except Exception as e:
    logging.error(f'Error creating property_dimension table: {e}')

try:
    date_dim = df.select("repair_year").distinct()
    date_dim = date_dim.withColumn("repair_date_id", monotonically_increasing_id())
    date_dim.write.jdbc(url=db_url, table="date_dimension", mode="overwrite", properties=db_properties)
    logging.info('date_dimension table created successfully')
except Exception as e:
    logging.error(f'Error creating date_dimension table: {e}')

# Creating fact table
try:
    repairs_fact = df.join(date_dim, "repair_year").select("property_id", "repair_date_id", "repair_count", "total_repair_cost")
    repairs_fact.write.jdbc(url=db_url, table="repairs_fact", mode="overwrite", properties=db_properties)
    logging.info('repairs_fact table created successfully')
except Exception as e:
    logging.error(f'Error creating repairs_fact table: {e}')

logging.info("Data loaded successfully")