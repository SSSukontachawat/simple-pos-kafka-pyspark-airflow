from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
import os, psycopg2

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SimplePOS") \
    .config("spark.jars", "/opt/jars/postgresql-42.5.6.jar") \
    .config("spark.driver.extraClassPath", "/opt/jars/postgresql-42.5.6.jar") \
    .config("spark.executor.extraClassPath", "/opt/jars/postgresql-42.5.6.jar") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Database connection properties
DB_URL = "jdbc:postgresql://postgres:5432/airflow"
DB_PROPERTIES = {
    "user": "user",  # Replace with actual username
    "password": "password",  # Replace with actual password
    "host": "postgres",  # Replace with actual host (or keep 'postgres' if using Docker service name)
    "port": 5432,  # Replace with actual port
    "dbname": "airflow",  # Replace with actual database name
    "driver": "org.postgresql.Driver"
}

SALES_DATA_DIR = "/opt/airflow/sales_data"
PRODUCTS_DATA_DIR = "/opt/airflow/products_data"
CUSTOMERS_DATA_DIR = "/opt/airflow/customers_data"

# PostgreSQL connection via psycopg2
def psycopg2_connect():
    conn = psycopg2.connect(
        host=DB_PROPERTIES["host"],
        dbname=DB_PROPERTIES["dbname"],
        user=DB_PROPERTIES["user"],
        password=DB_PROPERTIES["password"],
        port=DB_PROPERTIES["port"]
    )
    return conn

# Load existing data from PostgreSQL
def load_postgresql(table_name):
    table_data = spark.read \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", f"public.{table_name}") \
        .option("user", DB_PROPERTIES["user"]) \
        .option("password", DB_PROPERTIES["password"]) \
        .option("driver", DB_PROPERTIES["driver"]) \
        .load()
    return table_data

# Extract file for daily transactions
def latest_transactions_file():
    today_date = datetime.now()
    today_date_str = today_date.strftime("%Y%m%d")
    latest_file = [f for f in os.listdir(SALES_DATA_DIR) if f.startswith("sales_") and today_date_str in f]
    if not latest_file:
        print(f"No transactions for today: {today_date_str}")
        return None
    filename = os.path.join(SALES_DATA_DIR,latest_file[0])
    return filename

# Extract file for modified file
def modified_file(PATH):
    today_date = datetime.now()
    files = os.listdir(PATH)
    modified_files = []
    for file in files:
        file_list = os.path.join(PATH, file)
        file_mod_time = os.path.getmtime(file_list)
        if file_mod_time >= (today_date - timedelta(hours=1)).timestamp():
            modified_files.append(file_list)
    print(f"Files modified in the last 1 hour: {sorted(modified_files)}")
    if not modified_files:
        print(f"No files were modified in the last 1 hour.")
        return None
    return sorted(modified_files)

# Daily transaction update
def daily_transaction_update():
    latest_file = latest_transactions_file()
    if not latest_file:
        print(f"No transactions for today")
        return
    print(f"Processing latest transactions: {latest_file}")
    
    # Read latest transactions
    df = spark.read.option("header", True).csv(latest_file)
    if df.rdd.isEmpty():
        print(f"No data in file {latest_file}")
        return
    
    # Clean data (remove nulls and duplicates)
    df = df.dropna().dropDuplicates()

    df = df.withColumn("sale_id", F.col("sale_id").cast("int"))
    df = df.withColumn("sale_date", F.to_timestamp("sale_date", "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("customer_id", F.col("customer_id").cast("int"))
    df = df.withColumn("product_id", F.col("product_id").cast("int"))
    df = df.withColumn("quantity", F.col("quantity").cast("int"))
    df = df.withColumn("price", F.col("price").cast("float"))
    df = df.withColumn("total_price", F.col("total_price").cast("float"))


    # Append data to PostgreSQL
    try:
        df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", f"public.sales") \
        .option("user", DB_PROPERTIES["user"]) \
        .option("password", DB_PROPERTIES["password"]) \
        .option("driver", DB_PROPERTIES["driver"]) \
        .mode("append") \
        .save()

        print(f"Successfully loaded sales.")
    except Exception as e:
        print(f"Error processing sales: {str(e)}")

# Transactions modification
def transactions_update():
    transactions_modified = modified_file(SALES_DATA_DIR)
    
    # Check last modified 1 hour
    if not transactions_modified:
        print("No transactions update")
        return
    
    # Load existing data from PostgreSQL
    transactions_data = load_postgresql("sales")

    # Initialize a variable for combined data
    df_combined = None

    # Process all CSV files
    for transactions in transactions_modified:
        print(f"Processing file: {transactions}")
        
        # Load new data from CSV
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(transactions)
        
        # Combine all the data into a single DataFrame
        if df_combined is None:
            df_combined = df
        else:
            df_combined = df_combined.union(df)

    # Find records to DELETE (in DB but no longer in the new data)
    deletes = transactions_data.join(df_combined, on="sale_id", how="left_anti").orderBy("sale_id")

    try:
        # PostgreSQL connection
        conn = psycopg2_connect()
        cur = conn.cursor()

        # DELETE operation
        if not deletes.rdd.isEmpty():
            delete_ids = [row.sale_id for row in deletes.collect()]
            delete_query = f"DELETE FROM public.sales WHERE sale_id IN ({', '.join(map(str, delete_ids))})"
            cur.execute(delete_query)
            conn.commit()
            print("Transactions delete")

        # UPSERT (INSERT + UPDATE)
        if not df_combined.rdd.isEmpty():
            upsert_records = df_combined.collect()
            upsert_values = [
                (row.sale_id, row.sale_date, row.customer_id, row.product_id, row.quantity, row.price, row.total_price, row.payment_method)
                for row in upsert_records
            ]

            upsert_query = """
            INSERT INTO public.sales (sale_id, sale_date, customer_id, product_id, quantity, price, total_price, payment_method)
            VALUES %s
            ON CONFLICT (sale_id) DO UPDATE SET
                sale_date = EXCLUDED.sale_date,
                customer_id = EXCLUDED.customer_id,
                product_id = EXCLUDED.product_id,
                quantity = EXCLUDED.quantity,
                price = EXCLUDED.price,
                total_price = EXCLUDED.total_price,
                payment_method = EXCLUDED.payment_method;
            """

            execute_values(cur, upsert_query, upsert_values)
            conn.commit()
            print("Transactions UPSERT (INSERT + UPDATE) done.")

        cur.close()
        conn.close()

        print(f"Update {transactions} Successfully")
    
    except Exception as e:
        print(f"Error processing transactions: {str(e)}")

# Product modification
def products_update():
    products_modified = modified_file(PRODUCTS_DATA_DIR)
    
    # Check last modified 1 hour
    if not products_modified:
        print("No products update")
        return
    
    # Load existing data from PostgreSQL
    products_data = load_postgresql("products")

    # Initialize a variable for combined data
    df_combined = None

    # Process all CSV files
    for products in products_modified:
        print(f"Processing file: {products}")
        
        # Load new data from CSV
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(products)
        
        # Combine all the data into a single DataFrame
        if df_combined is None:
            df_combined = df
        else:
            df_combined = df_combined.union(df)

    # Find records to DELETE (in DB but no longer in the new data)
    deletes = products_data.join(df_combined, on="product_id", how="left_anti").orderBy("product_id")

    try:
        # PostgreSQL connection
        conn = psycopg2_connect()
        cur = conn.cursor()

        # DELETE operation
        if not deletes.rdd.isEmpty():
            delete_ids = [row.product_id for row in deletes.collect()]
            delete_query = f"DELETE FROM public.products WHERE product_id IN ({', '.join(map(str, delete_ids))})"
            cur.execute(delete_query)
            conn.commit()
            print("Products delete")

        # UPSERT (INSERT + UPDATE)
        if not df_combined.rdd.isEmpty():
            upsert_records = df_combined.collect()
            upsert_values = [
                (row.product_id, row.product_name, row.product_description, row.product_category, row.product_price, row.stock_level)
                for row in upsert_records
            ]

            upsert_query = """
            INSERT INTO public.products (product_id, product_name, product_description, product_category, product_price, stock_level)
            VALUES %s
            ON CONFLICT (product_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                product_description = EXCLUDED.product_description,
                product_category = EXCLUDED.product_category,
                product_price = EXCLUDED.product_price,
                stock_level = EXCLUDED.stock_level;
            """

            execute_values(cur, upsert_query, upsert_values)
            conn.commit()
            print("Products UPSERT (INSERT + UPDATE) done.")

        cur.close()
        conn.close()

        print(f"Update {products} Successfully")
    
    except Exception as e:
        print(f"Error processing products: {str(e)}")
        
# Customer modification
def customers_update():
    customers_modified = modified_file(CUSTOMERS_DATA_DIR)
    # Check last modified 1 hour
    if not customers_modified:
        print("No customers update")
        return
    
    # Load existing data from postgresql
    customers_data = load_postgresql("customers")

    # Initialize a variable for combined data
    df_combined = None

    for customers in customers_modified:
        print(f"Processing file: {customers}")

        # Load new data from CSV
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(customers)
        
        # Update membership level
        df = member_level(df)
        # Update purchase frequency
        df = frequency(df)

        # Clean data (remove nulls and duplicates)
        df = df.dropna().dropDuplicates()

       # Combine all the data into a single DataFrame
        if df_combined is None:
            df_combined = df
        else:
            df_combined = df_combined.union(df)

    # Find records to DELETE (in DB but no longer in the new data)
    deletes = customers_data.join(df_combined, on="customer_id", how="left_anti").orderBy("customer_id")

    try:
        # PostgreSQL connection
        conn = psycopg2_connect()
        cur = conn.cursor()

        # DELETE operation
        if not deletes.rdd.isEmpty():
            delete_ids = [row.customer_id for row in deletes.collect()]
            delete_query = f"DELETE FROM public.customers WHERE customer_id IN ({', '.join(map(str, delete_ids))})"
            cur.execute(delete_query)
            conn.commit()
            print("Customers delete")

        # UPSERT (INSERT + UPDATE)
        if not df_combined.rdd.isEmpty():
            upsert_records = df_combined.collect()
            upsert_values = [
                (row.customer_id, row.customer_name, row.customer_location, row.membership_level, row.purchase_frequency)
                for row in upsert_records
            ]

            upsert_query = """
            INSERT INTO public.customers (customer_id, customer_name, customer_location, membership_level, purchase_frequency)
            VALUES %s
            ON CONFLICT (customer_id) DO UPDATE SET
                customer_name = EXCLUDED.customer_name,
                customer_location = EXCLUDED.customer_location,
                membership_level = EXCLUDED.membership_level,
                purchase_frequency = EXCLUDED.purchase_frequency;
            """

            execute_values(cur, upsert_query, upsert_values)
            conn.commit()
            print("Customers UPSERT (INSERT + UPDATE) done.")

        cur.close()
        conn.close()

        print(f"Update {customers} Successfully")
    
    except Exception as e:
        print(f"Error processing products: {str(e)}")

# Update Membership level  
def member_level(df):
    transactions = load_postgresql("sales")
    
    # Check if transactions is empty
    if transactions.count() == 0:
        print("No data in sales table, setting default membership level for all customers.")
        # Set default membership level (e.g., "Bronze") for all customers
        df = df.withColumn("membership_level", F.lit("Bronze"))
        return df
    
    transactions_sum = transactions.groupby("customer_id") \
        .agg(F.sum("total_price").alias("sum_purchase"))

    level = transactions_sum.withColumn(
        "membership_level",
        F.when(F.col("sum_purchase") < 100, "Bronze") \
        .when((F.col("sum_purchase") >= 100) & (F.col("sum_purchase") < 500), "Silver") \
        .when((F.col("sum_purchase") >= 500) & (F.col("sum_purchase") < 2000), "Gold") \
        .otherwise("Platinum")
    )

    # Join with the original dataframe to add the membership level
    df_level = df.join(level.select("customer_id", "membership_level"), on="customer_id", how="left")
    
    # If no membership level is found, set it to "Bronze"
    df_level = df_level.withColumn("membership_level", F.coalesce(F.col("membership_level"), F.lit("Bronze")))
    
    return df_level

# Update purchase frequency
def frequency(df):
    transactions = load_postgresql("sales")
    
    # Check if transactions is empty
    if transactions.count() == 0:
        print("No data in sales table, setting default purchase frequency for all customers.")
        # Set default purchase_frequency (e.g., 0) for all customers
        df = df.withColumn("purchase_frequency", F.lit(0))
        return df
    
    purchase_sum = transactions.groupby("customer_id") \
        .agg(F.count("sale_id").alias("purchase_frequency"))

    # Join with the original dataframe to add the purchase frequency
    df_frequency = df.join(purchase_sum.select("customer_id", "purchase_frequency"), on="customer_id", how="left")
    
    # If no purchase frequency is found, set it to 0
    df_frequency = df_frequency.withColumn("purchase_frequency", F.coalesce(F.col("purchase_frequency"), F.lit(0)))
    
    return df_frequency


# All update implement
def all_update_data():
    transactions_update()
    products_update()
    customers_update()

# Import Airflow components
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define default arguments for the tasks
default_args = {
    "owner": "airflow",
    "start_date": None,
    #"retries": 1,
    #'retry_delay': timedelta(seconds=15),
    "catchup": False,
}


# DAG for hourly task
dag_hourly = DAG(
    "hourly_etl",
    default_args=default_args,
    description="DAG for hourly ETL tasks",
    schedule_interval="@hourly",  # Task runs hourly
)

# DAG for daily task
dag_daily = DAG(
    "daily_etl",
    default_args=default_args,
    description="DAG for daily ETL tasks",
    schedule_interval="0 22 * * *",  # Task runs daily at 10:00 PM
)

# Task for hourly ETL in the hourly DAG
hourly_etl_task = PythonOperator(
    task_id="hourly_update",
    python_callable=all_update_data,
    dag=dag_hourly,
)

# Task for daily ETL in the daily DAG
daily_etl_task = PythonOperator(
    task_id="daily_transactions",
    python_callable=daily_transaction_update,
    dag=dag_daily,
)
