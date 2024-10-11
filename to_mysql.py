from pyspark.sql import SparkSession
import yaml
from yaml_input import yaml_input
from py4j.protocol import Py4JJavaError


def to_mysql(spark, df):
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

        # Write DataFrame to MySQL table
        df.write.jdbc(url=jdbc_url, table="Total_Cost_Per_Customer",
                      mode="overwrite", properties=connection_properties)

    except Py4JJavaError as e:
        # Catching the Java error from the underlying MySQL driver
        if "Access denied for user" in str(e):
            print("Authentication failed: wrong username or password.")
        else:
            print(f"An error occurred: {e}")
    except Exception as e:
        # Catch any other unexpected errors
        print(f"An unexpected error occurred: {e}")
