from pyspark.sql import SparkSession
import yaml
from yaml_input import yaml_input
from py4j.protocol import Py4JJavaError


def extract(spark):

    # Load the YAML configuration file
    with open('db_config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    if 'mysql' in config:
        if 'password' in config['mysql']:
            pass
        else:
            yaml_input()
            with open('db_config.yaml', 'r') as file:
                config = yaml.safe_load(file)

    arr = []
    try:
        # Define MySQL connection properties
        mysql_config = config['mysql']
        jdbc_url = f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
        connection_properties = {
            "user": mysql_config['username'],
            "password": mysql_config['password'],
            "driver": "com.mysql.cj.jdbc.Driver"
        }

        # Reading data from MySQL table into a PySpark DataFrame
        customer_data = spark.read.jdbc(url=jdbc_url, table="customer_data",
                                        properties=connection_properties)

        order_data = spark.read.jdbc(url=jdbc_url, table="Order_data",
                                     properties=connection_properties)

        discount_data = spark.read.jdbc(url=jdbc_url, table="Discount_Data",
                                        properties=connection_properties)

    except Py4JJavaError as e:
        # Catching the Java error from the underlying MySQL driver
        if "Access denied for user" in str(e):
            print("Authentication failed: wrong username or password.")
        else:
            print(f"An error occurred: {e}")
    except Exception as e:
        # Catch any other unexpected errors
        print(f"An unexpected error occurred: {e}")

    return customer_data, order_data, discount_data
