from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'create_and_load_dim',
    default_args=default_args,
    description='ETL for food delivery data into Redshift',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:


 create_schema = PostgresOperator(
    task_id = "create_schema",
    postgres_conn_id='redshift_connection_id',
    sql="CREATE SCHEMA IF NOT EXISTS food_delivery_datamart;",
)
 
 drop_dimCustomers = PostgresOperator(
  task_id = "drop_dimCustomers",
  postgres_conn_id='redshift_connection_id',
  sql="DROP TABLE IF EXISTS food_delivery_datamart.dimCustomers;",
 )

 drop_dimRestaurants = PostgresOperator(
        task_id='drop_dimRestaurants',
        postgres_conn_id='redshift_connection_id',
        sql="DROP TABLE IF EXISTS food_delivery_datamart.dimRestaurants;",
    )

 drop_dimDeliveryRiders = PostgresOperator(
        task_id='drop_dimDeliveryRiders',
        postgres_conn_id='redshift_connection_id',
        sql="DROP TABLE IF EXISTS food_delivery_datamart.dimDeliveryRiders;",
    )

 drop_factOrders = PostgresOperator(
        task_id='drop_factOrders',
        postgres_conn_id='redshift_connection_id',
        sql="DROP TABLE IF EXISTS food_delivery_datamart.factOrders;",
    )
 
 create_dimCustomers = PostgresOperator(
  task_id = "create_dimCustomer",
  postgres_conn_id='redshift_connection_id',
  sql="""
    CREATE TABLE food_delivery_datamart.dimCustomer
    (RestaurantID INT PRIMARY KEY,
    RestaurantName VARCHAR(255),
    CuisineType VARCHAR(100),
    RestaurantAddress VARCHAR(500),
    RestaurantRating DECIMAL(3,1));
""")
 
 create_dimRestaurants = PostgresOperator(
  task_id ="create_dimRestaurants",
  postgres_conn_id = 'redshift_connection_id',
  sql= """
 CREATE TABLE food_delivery_datamart.dimRestaurants (
                RestaurantID INT PRIMARY KEY,
                RestaurantName VARCHAR(255),
                CuisineType VARCHAR(100),
                RestaurantAddress VARCHAR(500),
                RestaurantRating DECIMAL(3,1)
            );
"""
 )
 

 create_dimDeliveryRiders = PostgresOperator(
  task_id ="create_dimDeliveryRiders",
  postgres_conn_id = 'redshift_connection_id',
  sql="""
  CREATE TABLE food_delivery_datamart.dimDeliveryRiders (
                RiderID INT PRIMARY KEY,
                RiderName VARCHAR(255),
                RiderPhone VARCHAR(50),
                RiderVehicleType VARCHAR(50),
                VehicleID VARCHAR(50),
                RiderRating DECIMAL(3,1)
            );
"""
 )

 create_factOrders = PostgresOperator(
    task_id='create_factOrders',
    postgres_conn_id='redshift_connection_id',
    sql= """
 CREATE TABLE food_delivery_datamart.factOrders (
                OrderID INT PRIMARY KEY,
                CustomerID INT REFERENCES food_delivery_datamart.dimCustomers(CustomerID),
                RestaurantID INT REFERENCES food_delivery_datamart.dimRestaurants(RestaurantID),
                RiderID INT REFERENCES food_delivery_datamart.dimDeliveryRiders(RiderID),
                OrderDate TIMESTAMP WITHOUT TIME ZONE,
                DeliveryTime INT,
                OrderValue DECIMAL(8,2),
                DeliveryFee DECIMAL(8,2),
                TipAmount DECIMAL(8,2),
                OrderStatus VARCHAR(50)
            );
"""
    )
 

 load_dimCustomers = S3ToRedshiftOperator(
    task_id='load_dimCustomers',
    schema='food_delivery_datamart',
    table='dimCustomers',
    s3_bucket='food-delivery-data-analyis-new',
    s3_key='dims/dimCustomers.csv',
    redshift_conn_id='redshift_connection_id',
    aws_conn_id='aws_default',
    copy_options=['CSV', 'IGNOREHEADER 1', 'QUOTE as \'"\''],
    )
 
 load_dimRestaurants = S3ToRedshiftOperator(
   task_id='load_dimCustomers',
   schema='food_delivery_datamart',
   table='dimCustomers',
   s3_bucket='food-delivery-data-analyis-new',
   s3_key='dims/dimDeliveryRiders.csv',
   redshift_conn_id='redshift_connection_id',
   aws_conn_id='aws_default',
   copy_options=['CSV', 'IGNOREHEADER 1', 'QUOTE as \'"\''],
  
 )

 load_dimDeliveryRiders = S3ToRedshiftOperator(
   task_id='load_dimCustomers',
   schema='food_delivery_datamart',
   table='dimCustomers',
   s3_bucket='food-delivery-data-analyis-new',
   s3_key='dims/dimRestaurants.csv',
   redshift_conn_id='redshift_connection_id',
   aws_conn_id='aws_default',
   copy_options=['CSV', 'IGNOREHEADER 1', 'QUOTE as \'"\''],
 )

 trigger_spark_streaming_dag = TriggerDagRunOperator(
  task_id='trigger_spark_streaming',
  trigger_dag_id="submit_pyspark_streaming_job_to_emr",

 )


# First, create the schema
create_schema >> [drop_dimCustomers, drop_dimRestaurants, drop_dimDeliveryRiders, drop_factOrders]

# Once the existing tables are dropped, proceed to create new tables
drop_dimCustomers >> create_dimCustomers
drop_dimRestaurants >> create_dimRestaurants
drop_dimDeliveryRiders >> create_dimDeliveryRiders
drop_factOrders >> create_factOrders


[create_dimCustomers, create_dimRestaurants, create_dimDeliveryRiders] >> create_factOrders

# After each table is created, load the corresponding data
create_dimCustomers >> load_dimCustomers
create_dimRestaurants >> load_dimRestaurants
create_dimDeliveryRiders >> load_dimDeliveryRiders

[load_dimCustomers, load_dimRestaurants, load_dimDeliveryRiders] >> trigger_spark_streaming_dag

