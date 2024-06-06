import unittest
from pyspark.sql import SparkSession
import logging

logging.basicConfig(filename='test_results.log', level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

class TestDataPipeline(unittest.TestCase):
    def setUp(self):
        
        self.spark = SparkSession.builder \
            .appName("PropertyMaintenanceDataPipelineTest") \
            .config("spark.jars", "postgresql-42.7.3.jar") \
            .getOrCreate()

        # Database connection
        self.db_url = "jdbc:postgresql://localhost:5432/property_db"
        self.db_properties = {
            "user": "myuser",
            "password": "mypassword",
            "driver": "org.postgresql.Driver"
        }

    def check_nulls(self, df, column_name):
        total_count = df.count()
        null_count = df.filter(df[column_name].isNull()).count()
        return null_count, total_count

    # Test if data is loaded correctly
    def test_data_loading(self):
        
        property_dim = self.spark.read.jdbc(url=self.db_url, table="property_dimension", properties=self.db_properties)
        self.assertGreater(property_dim.count(), 0, "property_dimension table is empty")
        
        date_dim = self.spark.read.jdbc(url=self.db_url, table="date_dimension", properties=self.db_properties)
        self.assertGreater(date_dim.count(), 0, "date_dimension table is empty")
        
        repairs_fact = self.spark.read.jdbc(url=self.db_url, table="repairs_fact", properties=self.db_properties)
        self.assertGreater(repairs_fact.count(), 0, "repairs_fact table is empty")
    
    # Check non-null columns
    def test_non_null_columns(self):
         
        property_dim = self.spark.read.jdbc(url=self.db_url, table="property_dimension", properties=self.db_properties)
        null_count, total_count = self.check_nulls(property_dim, "property_id")
        self.assertEqual(null_count, 0, f"property_id contains {null_count} NULL values out of {total_count}")
        null_count, total_count = self.check_nulls(property_dim, "region_name")
        self.assertEqual(null_count, 0, f"region_name contains {null_count} NULL values out of {total_count}")

        date_dim = self.spark.read.jdbc(url=self.db_url, table="date_dimension", properties=self.db_properties)
        null_count, total_count = self.check_nulls(date_dim, "repair_year")
        self.assertEqual(null_count, 0, f"repair_year contains {null_count} NULL values out of {total_count}")

        repairs_fact = self.spark.read.jdbc(url=self.db_url, table="repairs_fact", properties=self.db_properties)
        null_count, total_count = self.check_nulls(repairs_fact, "property_id")
        self.assertEqual(null_count, 0, f"property_id in repairs_fact contains {null_count} NULL values out of {total_count}")
        null_count, total_count = self.check_nulls(repairs_fact, "repair_date_id")
        self.assertEqual(null_count, 0, f"repair_date_id in repairs_fact contains {null_count} NULL values out of {total_count}")

    # Test unique primary keys
    def test_unique_primary_keys(self):
    
        property_dim = self.spark.read.jdbc(url=self.db_url, table="property_dimension", properties=self.db_properties)
        self.assertEqual(property_dim.count(), property_dim.select("property_id").distinct().count(), "property_id is not unique in property_dimension")

        date_dim = self.spark.read.jdbc(url=self.db_url, table="date_dimension", properties=self.db_properties)
        self.assertEqual(date_dim.count(), date_dim.select("repair_date_id").distinct().count(), "repair_date_id is not unique in date_dimension")

    # Check foreign key integrity 
    def test_foreign_key_integrity(self):
        
        repairs_fact = self.spark.read.jdbc(url=self.db_url, table="repairs_fact", properties=self.db_properties)
        property_dim = self.spark.read.jdbc(url=self.db_url, table="property_dimension", properties=self.db_properties)
        date_dim = self.spark.read.jdbc(url=self.db_url, table="date_dimension", properties=self.db_properties)
        
        property_join_count = repairs_fact.join(property_dim, "property_id").count()
        self.assertEqual(property_join_count, repairs_fact.count(), f"Foreign key integrity issue with property_id: {repairs_fact.count() - property_join_count} mismatches out of {repairs_fact.count()}")

        date_join_count = repairs_fact.join(date_dim, "repair_date_id").count()
        self.assertEqual(date_join_count, repairs_fact.count(), f"Foreign key integrity issue with repair_date_id: {repairs_fact.count() - date_join_count} mismatches out of {repairs_fact.count()}")

    # Test data types 
    def test_data_types(self):
    
        property_dim = self.spark.read.jdbc(url=self.db_url, table="property_dimension", properties=self.db_properties)
        self.assertEqual(property_dim.schema["property_id"].dataType.typeName(), "string", "property_id is not of type string")
        self.assertEqual(property_dim.schema["region_name"].dataType.typeName(), "string", "region_name is not of type string")
        self.assertEqual(property_dim.schema["construction_year"].dataType.typeName(), "integer", "construction_year is not of type integer")
        self.assertEqual(property_dim.schema["occupants"].dataType.typeName(), "integer", "occupants is not of type integer")

    
        date_dim = self.spark.read.jdbc(url=self.db_url, table="date_dimension", properties=self.db_properties)
        self.assertEqual(date_dim.schema["repair_year"].dataType.typeName(), "integer", "repair_year is not of type integer")

        repairs_fact = self.spark.read.jdbc(url=self.db_url, table="repairs_fact", properties=self.db_properties)
        self.assertEqual(repairs_fact.schema["property_id"].dataType.typeName(), "string", "property_id in repairs_fact is not of type string")
        self.assertEqual(repairs_fact.schema["repair_date_id"].dataType.typeName(), "long", "repair_date_id in repairs_fact is not of type long")
        self.assertEqual(repairs_fact.schema["repair_count"].dataType.typeName(), "integer", "repair_count is not of type integer")
        self.assertEqual(repairs_fact.schema["total_repair_cost"].dataType.typeName(), "double", "total_repair_cost is not of type double")

    # Test reasonable value ranges
    def test_reasonable_value_ranges(self):
    
        property_dim = self.spark.read.jdbc(url=self.db_url, table="property_dimension", properties=self.db_properties)
        self.assertTrue(property_dim.filter(property_dim.construction_year < 1800).count() == 0, "construction_year contains unreasonable values")
        self.assertTrue(property_dim.filter(property_dim.occupants < 0).count() == 0, "occupants contains negative values")

        repairs_fact = self.spark.read.jdbc(url=self.db_url, table="repairs_fact", properties=self.db_properties)
        self.assertTrue(repairs_fact.filter(repairs_fact.repair_count < 0).count() == 0, "repair_count contains negative values")
        self.assertTrue(repairs_fact.filter(repairs_fact.total_repair_cost < 0).count() == 0, "total_repair_cost contains negative values")

if __name__ == '__main__':
    
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDataPipeline)

    # Run the test suite and write the results to a log file
    with open('test_results.log', 'w') as log_file:
        runner = unittest.TextTestRunner(stream=log_file, verbosity=2)
        result = runner.run(suite)