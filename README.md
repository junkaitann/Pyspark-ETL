The following code is a simple example of using Pyspark to read and write into MysqlDB. Attached are the Customers, Order and Discount Tables Mock CSV and the code will calculate the total amount each customer spent after discount

Set up db_config.yaml with the following details:

mysql:
  database: 
  host: 
  port: 
  username: 

Code will prompt user to enter password. The password will be save in this file and will be remove using yaml_remove function
