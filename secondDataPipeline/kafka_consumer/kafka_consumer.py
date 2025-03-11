import json
import os
from kafka import KafkaConsumer
from datetime import datetime
import threading
import pandas as pd

# Kafka Configuration
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:29092")
group_id = os.getenv("GROUP_ID", "my-consumer-group")

# Kafka Consumer Configuration
def create_consumer(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=60000,  # Timeout after 1 minute if no message
        api_version=(2, 7, 0)  # Deserialize the message
    )
    return consumer

# Create consumers for different topics
transactions_sale = create_consumer("transactions_sale")
transactions_edit = create_consumer("transactions_edit")
transactions_remove = create_consumer("transactions_remove")

products_add = create_consumer("products_add")
products_edit = create_consumer("products_edit")
products_remove = create_consumer("products_remove")

customers_add = create_consumer("customers_add")
customers_edit = create_consumer("customers_edit")
customers_remove = create_consumer("customers_remove")

# Constant
SALES_CSV_DIR = "/opt/airflow/sales_data"
PRODUCTS_CSV_DIR = "/opt/airflow/products_data"
CUSTOMERS_CSV_DIR = "/opt/airflow/customers_data"

PRODUCT_CATEGORY = {
        "1" : "Daily",
        "2" : "Meat",
        "3" : "Seafood",
        "4" : "Vegetable & Fruit",
        "5" : "Snack",
        "6" : "Beverage",
        "7" : "Alcohol"
    }
# Ensure directory exists
os.makedirs(SALES_CSV_DIR, exist_ok=True)
os.makedirs(PRODUCTS_CSV_DIR,exist_ok=True)
os.makedirs(CUSTOMERS_CSV_DIR,exist_ok=True)

# Sale ID Tracking File
tracking_file = "/data/sale_id_tracker.txt"

def get_last_sale_id():
    """Retrieve the last used sale_id from the tracking file."""
    if os.path.exists(tracking_file):
        with open(tracking_file, "r") as f:
            return int(f.read().strip())
    return 0  # Start from 1 if no tracking file exists

def update_last_sale_id(new_id):
    """Update the last used sale_id."""
    with open(tracking_file, "w") as f:
        f.write(str(new_id))

def sales_transaction():
    while True:
        try:
            # Load the last recorded sale_id
            last_sale_id = get_last_sale_id()
            
            # Process Incoming Messages
            for message in transactions_sale:
                data = message.value
                last_sale_id += 1  # Increment sale_id

                # Determine filename (sales_YYYYMMDD.csv)
                today = datetime.now().strftime("%Y%m%d")
                filename = os.path.join(SALES_CSV_DIR,f"sales_{today}.csv")
                
                # Check if file exists (to write headers if it's new)
                file_exists = os.path.exists(filename)

                # Write transaction to CSV
                if not file_exists:
                    # Write header only if file is new
                    df = pd.DataFrame(columns=["sale_id", "sale_date", "customer_id", "product_id", "quantity", "price", "total_price", "payment_method"])
                else:
                    df = pd.read_csv(filename)

                # Create a new DataFrame with the data to append
                new_data = pd.DataFrame([data])
                new_data.insert(0,"sale_id",last_sale_id)

                # Append the new data to the existing DataFrame
                df = pd.concat([df, new_data], ignore_index=True)

                # Sorted before save
                df_sorted = df.sort_values(by="sale_id", ascending=True)

                # Write the updated DataFrame to the CSV file
                df_sorted.to_csv(filename, index=False)
                    
                # Update sale_id tracker
                update_last_sale_id(last_sale_id)
                # Update stock level
                product_id = str(data["product_id"])
                category_path = os.path.join(PRODUCTS_CSV_DIR,f"{PRODUCT_CATEGORY[product_id[0]]}.csv")
                stock = pd.read_csv(category_path)
                product = stock.loc[stock["product_id"] == data["product_id"]]
                remaining = product["stock_level"].values - data["quantity"]
                stock.loc[stock["product_id"] == data["product_id"],["stock_level"]] = remaining
                stock.to_csv(category_path, index=False)
                # Print each transaction to console
                print(f"✅ New Sale Recorded: {last_sale_id} - {data}")
        except Exception as e:
            print(f"Error: {e}")
            # Handle any exceptions, possibly retry or log them.

def edit_transaction():
    while True:
        try:
            # Process Incoming Messages
            for message in transactions_edit:
                data = message.value

                # Determine filename (sales_YYYYMMDD.csv)
                filename = os.path.join(SALES_CSV_DIR,data["csv_path"])
                
                # Check if file exists (to write headers if it's new)
                file_exists = os.path.exists(filename)

                # Write transaction to CSV
                if not file_exists:
                    # Write header only if file is new
                    df = pd.DataFrame(columns=["sale_id", "sale_date", "customer_id", "product_id", "quantity", "price", "total_price", "payment_method"])
                else:
                    df = pd.read_csv(filename)
                if data["sale_id"] in df["sale_id"].values:
                    print("correct")
                else:
                    print("fuck")

                for i in df["sale_id"].values:
                    if i == data["sale_id"]:
                        print(i)
                
                # Edit row matching sale id
                df.loc[df["sale_id"] == data["sale_id"], ["customer_id", "product_id", "quantity", "price", "total_price", "payment_method"]] = [
                            data["customer_id"], data["product_id"], data["quantity"], data["price"],data["total_price"], data["payment_method"]
                            ]
                # Save to CSV
                df.to_csv(filename, index=False)

                # Print each transaction to console
                print(f"Sale Edited: {data}")
        except Exception as e:
            print(f"Error: {e}")
            # Handle any exceptions, possibly retry or log them.
            
def remove_transaction():
    while True:
        try:
            # Process Incoming Messages
            for message in transactions_remove:
                data = message.value

                # Determine filename (sales_YYYYMMDD.csv)
                filename = os.path.join(SALES_CSV_DIR,data["csv_path"])
                
                # Check if file exists (to write headers if it's new)
                file_exists = os.path.exists(filename)

                # Write transaction to CSV
                if not file_exists:
                    # Write header only if file is new
                    df = pd.DataFrame(columns=["sale_id", "sale_date", "customer_id", "product_id", "quantity", "price", "total_price", "payment_method"])
                else:
                    df = pd.read_csv(filename)
                
                # Delete Selected row
                df = df.loc[df["sale_id"] != data["sale_id"]]

                # Save to CSV
                df.to_csv(filename, index=False)
                
                # Print each transaction to console
                print(f"Sale Removed: {data}")
        except Exception as e:
            print(f"Error: {e}")
            # Handle any exceptions, possibly retry or log them.
    
def add_product():
    while True:
        try:
            # Poll messages from all the topics in a loop
            for message in products_add:
                data = message.value
                # Add product
                filename = os.path.join(PRODUCTS_CSV_DIR, f'{data["product_category"]}.csv')
                # Check if file exists (to write headers if it's new)
                file_exists = os.path.exists(filename)

                # Write transaction to CSV
                if not file_exists:
                    # Write header only if file is new
                    df = pd.DataFrame(columns=["product_id", "product_name", "product_description", "product_category", "product_price", "stock_level"])
                else:
                    df = pd.read_csv(filename)
                # Create a new DataFrame with the data to append
                new_data = pd.DataFrame([data])

                # Append the new data to the existing DataFrame
                df = pd.concat([df, new_data], ignore_index=True)

                # Sorted before save
                df_sorted = df.sort_values(by="product_id", ascending=True)

                # Write the updated DataFrame to the CSV file
                df_sorted.to_csv(filename, index=False)

                # Print each product to console
                print(f"✅ {len(df)}New Product Recorded: {data}")
        except Exception as e:
            print(f"Error: {e}")
            # Handle any exceptions, possibly retry or log them.

def edit_product():
    while True:
        try:
            for message in products_edit:
                data = message.value
                # Edit product
                filename = os.path.join(PRODUCTS_CSV_DIR, f'{data["product_category"]}.csv')

                # Check if file exists (to write headers if it's new)
                file_exists = os.path.exists(filename)

                # Write transaction to CSV
                if not file_exists:
                    # Write header only if file is new
                    df = pd.DataFrame(columns=["product_id", "product_name", "product_description", "product_category", "product_price", "stock_level"])
                else:
                    df = pd.read_csv(filename)
                
                # Edit row matching product id
                df.loc[df["product_id"] == data["product_id"], ["product_name", "product_description", "product_category", "product_price", "stock_level"]] = [
                    data["product_name"], data["product_description"], data["product_category"], data["product_price"], data["stock_level"]
                    ]
                # Save to CSV
                df.to_csv(filename, index=False)
                print(f"📝 Product Edited: {data}")
        except Exception as e:
            print(f"Error: {e}")
            # Handle any exceptions, possibly retry or log them.

def remove_product():
    while True:
        try:
            for message in products_remove:
                data = message.value
                # Delete product
                filename = os.path.join(PRODUCTS_CSV_DIR, f'{data["product_category"]}.csv')

                # Check if file exists (to write headers if it's new)
                file_exists = os.path.exists(filename)

                # Write transaction to CSV
                if not file_exists:
                    # Write header only if file is new
                    df = pd.DataFrame(columns=["product_id", "product_name", "product_description", "product_category", "product_price", "stock_level"])
                else:
                    df = pd.read_csv(filename)
                
                # Delete Selected row
                df = df.loc[df["product_id"] != data["product_id"]]

                # Save to CSV
                df.to_csv(filename, index=False)
                print(f"📝 Product Removed: {data}")
        except Exception as e:
            print(f"Error: {e}")
            # Handle any exceptions, possibly retry or log them.

def add_customer():
    while True:
        try:
            # Poll messages from all the topics in a loop
            for message in customers_add:
                data = message.value
                # Add customer
                filename = os.path.join(CUSTOMERS_CSV_DIR, 'Customers.csv')
                # Check if file exists (to write headers if it's new)
                file_exists = os.path.exists(filename)

                # Write transaction to CSV
                if not file_exists:
                    # Write header only if file is new
                    df = pd.DataFrame(columns=["customer_id", "customer_name", "customer_location"])
                else:
                    df = pd.read_csv(filename)
                # Create a new DataFrame with the data to append
                new_data = pd.DataFrame([data])

                # Append the new data to the existing DataFrame
                df = pd.concat([df, new_data], ignore_index=True)

                # Sorted before save
                df_sorted = df.sort_values(by="customer_id", ascending=True)

                # Write the updated DataFrame to the CSV file
                df_sorted.to_csv(filename, index=False)

                # Print each product to console
                print(f"✅ {len(df)}New Customer Recorded: {data}")
        except Exception as e:
            print(f"Error: {e}")
            # Handle any exceptions, possibly retry or log them.

def edit_customer():
    while True:
        try:
            for message in customers_edit:
                data = message.value
                # Edit customer
                filename = os.path.join(CUSTOMERS_CSV_DIR, 'Customers.csv')

                # Check if file exists (to write headers if it's new)
                file_exists = os.path.exists(filename)

                # Write transaction to CSV
                if not file_exists:
                    # Write header only if file is new
                    df = pd.DataFrame(columns=["customer_id", "customer_name", "customer_location"])
                else:
                    df = pd.read_csv(filename)
                
                # Edit row matching product id
                df.loc[df["customer_id"] == data["customer_id"], ["customer_name", "customer_location"]] = [data["customer_name"], data["customer_location"]]
                # Save to CSV
                df.to_csv(filename, index=False)
                print(f"📝 Customer Edited: {data}")
        except Exception as e:
            print(f"Error: {e}")
            # Handle any exceptions, possibly retry or log them.

def remove_customer():
    while True:
        try:
            for message in customers_remove:
                data = message.value
                # Remove customer
                filename = os.path.join(CUSTOMERS_CSV_DIR, 'Customers.csv')

                # Check if file exists (to write headers if it's new)
                file_exists = os.path.exists(filename)

                # Write transaction to CSV
                if not file_exists:
                    # Write header only if file is new
                    df = pd.DataFrame(columns=["customer_id", "customer_name", "customer_location"])
                else:
                    df = pd.read_csv(filename)
                
                # Delete Selected row
                df = df.loc[df["customer_id"] != data["customer_id"]]

                # Save to CSV
                df.to_csv(filename, index=False)
                print(f"📝 Customer Removed: {data}")
        except Exception as e:
            print(f"Error: {e}")
            # Handle any exceptions, possibly retry or log them.

if __name__ == '__main__':
    # Use threads to run consumers concurrently
    transactions_sale_thread = threading.Thread(target=sales_transaction, daemon=True)
    transactions_edit_thread = threading.Thread(target=edit_transaction, daemon=True)
    transactions_remove_thread = threading.Thread(target=remove_transaction, daemon=True)
    products_add_thread = threading.Thread(target=add_product, daemon=True)
    products_edit_thread = threading.Thread(target=edit_product, daemon=True)
    products_remove_thread = threading.Thread(target=remove_product, daemon=True)
    customers_add_thread = threading.Thread(target=add_customer, daemon=True)
    customers_edit_thread = threading.Thread(target=edit_customer, daemon=True)
    customers_remove_thread = threading.Thread(target=remove_customer, daemon=True)

    transactions_sale_thread.start()
    transactions_edit_thread.start()
    transactions_remove_thread.start()
    products_add_thread.start()
    products_edit_thread.start()
    products_remove_thread.start()
    customers_add_thread.start()
    customers_edit_thread.start()
    customers_remove_thread.start()

    transactions_sale_thread.join()
    transactions_edit_thread.join()
    transactions_remove_thread.join()
    products_add_thread.join()
    products_edit_thread.join()
    products_remove_thread.join()
    customers_add_thread.join()
    customers_edit_thread.join()
    customers_remove_thread.join()