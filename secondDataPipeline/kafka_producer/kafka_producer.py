import streamlit as st
import logging
from datetime import datetime
from kafka import KafkaProducer
import json
import os
import pandas as pd
import time

# Kafka Producer Setup
KAFKA_TOPIC_TRANSACTIONS_SALE = "transactions_sale"
KAFKA_TOPIC_TRANSACTIONS_EDIT = "transactions_edit"
KAFKA_TOPIC_TRANSACTIONS_REMOVE = "transactions_remove"
KAFKA_TOPIC_PRODUCTS_ADD = 'products_add'
KAFKA_TOPIC_PRODUCTS_EDIT = 'products_edit'
KAFKA_TOPIC_PRODUCTS_REMOVE = 'products_remove'
KAFKA_TOPIC_CUSTOMERS_ADD = 'customers_add'
KAFKA_TOPIC_CUSTOMERS_EDIT = 'customers_edit'
KAFKA_TOPIC_CUSTOMERS_REMOVE = 'customers_remove'

KAFKA_SERVER = "kafka:29092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Get latest file to show latest transaction
SALES_CSV_DIR = "/opt/airflow/sales_data"
PRODUCTS_CSV_DIR = "/opt/airflow/products_data"
CUSTOMERS_CSV_DIR = "/opt/airflow/customers_data"

def get_today_filename():
    today = datetime.today().date().strftime('%Y%m%d')
    return os.path.join(SALES_CSV_DIR, f'sales_{today}.csv')

def get_filename(directory: str,name: str):
    filename = os.path.join(directory,f"{name}.csv")
    return filename

def is_data(key_check, column_check, directory, datapath):
    filename = get_filename(directory, datapath)
    if not os.path.exists(filename):
        return False  # If file does not exist, no duplicates are possible
    df = pd.read_csv(filename)
    if key_check in df[column_check].astype(str).values:
        return True # Data Exists
    else: 
        return False # No Data Exists

def is_sales_data(sale_id):
    csv_files = [entry.name for entry in os.scandir(SALES_CSV_DIR) if entry.is_file() and entry.name.startswith("sales_") and entry.name.endswith(".csv")]
    for csv_file in csv_files:
        filename = os.path.join(SALES_CSV_DIR, csv_file)
        df = pd.read_csv(filename)
        if sale_id in df["sale_id"].astype(str).values:
            return True
    return False

def find_csv(sale_id):
    csv_files = [entry.name for entry in os.scandir(SALES_CSV_DIR) if entry.is_file() and entry.name.startswith("sales_") and entry.name.endswith(".csv")]
    for csv_file in csv_files:
        filename = os.path.join(SALES_CSV_DIR, csv_file)
        df = pd.read_csv(filename)
        if sale_id in df["sale_id"].astype(str).values:
            return csv_file
    return None

def out_of_stock(product_id,quantity):
    category_check = {
        "1" : "Daily",
        "2" : "Meat",
        "3" : "Seafood",
        "4" : "Vegetable & Fruit",
        "5" : "Snack",
        "6" : "Beverage",
        "7" : "Alcohol"
    }
    category_path = os.path.join(PRODUCTS_CSV_DIR,f"{category_check[product_id[0]]}.csv")
    df = pd.read_csv(category_path)
    product = df.loc[df["product_id"].astype(str) == product_id]
    if int(quantity) > product["stock_level"].values:
        return True
    else:
        return False

# Streamlit UI
st.title("POS System")

# Navigation Menu
menu = st.radio("Menu", ["🏠 Home", "💲 Transactions", "📦 Product", "👤 Membership"])

if menu == "🏠 Home":
    st.write("Welcome to the POS System! Choose an option from the menu.")

elif menu == "💲 Transactions":
    sub_menu = st.radio("Select Action", ["Sales", "Update Transactions", "View Transactions"])
    if sub_menu == "Sales":
        # Store form data in session state if not already stored
        if 'customer_id' not in st.session_state:
            st.session_state.customer_id = ''
        if 'product_id' not in st.session_state:
            st.session_state.product_id = ''
        if 'quantity' not in st.session_state:
            st.session_state.quantity = 1
        if 'price' not in st.session_state:
            st.session_state.price = 0.00
        if 'payment_method' not in st.session_state:
            st.session_state.payment_method = ''

        # Input Fields for Transaction
        customer_id = st.text_input("Customer ID", value=st.session_state.customer_id)
        product_id = st.text_input("Product ID", value=st.session_state.product_id)
        quantity = st.number_input("Quantity", min_value=1, step=1, value=st.session_state.quantity)
        price = st.number_input("Price", min_value=0.00, step=0.01, value=st.session_state.price)
        payment_method = st.selectbox("Payment Method", ['',"Cash", "Credit Card", "Debit Card", "PayPal"], 
                                      index=['',"Cash", "Credit Card", "Debit Card", "PayPal"].index(st.session_state.payment_method))

        if st.button("Submit Transaction"):
            if not (customer_id and product_id and quantity and price and payment_method):
                st.error("All fields are required!")
            elif out_of_stock(product_id,quantity):
                st.error(f"No enough product {product_id} in stock")
            else:
                sale_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                total_price = quantity * price
                transaction = {
                    "sale_date": sale_date,
                    "customer_id": int(customer_id),
                    "product_id": int(product_id),
                    "quantity": quantity,
                    "price": price,
                    "total_price": total_price,
                    "payment_method": payment_method
                }
                # Send to Kafka
                try:
                    producer.send(KAFKA_TOPIC_TRANSACTIONS_SALE, transaction)
                    producer.flush()
                    st.success("Transaction recorded successfully!")
                    logging.info(f"Transaction sent: {transaction}")
                    time.sleep(2)
                except Exception as e:
                    st.error("Failed to send data to Kafka")
                    logging.error(f"Kafka send failed: {str(e)}")      
                st.rerun() 
        # Display latest transactions
        if os.path.exists(get_today_filename()):
            df = pd.read_csv(get_today_filename())
            st.subheader('Latest Transactions')
            st.dataframe(df.tail(5).sort_values(by='sale_id', ascending=False))
        else:
            st.subheader('No Transactions')
    elif sub_menu == "Update Transactions":
        sub_sub_menu = st.radio("Select Action", ["Edit", "Remove"])
        if sub_sub_menu == "Edit":
            # Store form data in session state if not already stored
            if 'sale_id' not in st.session_state:
                st.session_state.sale_id = ''
            if 'customer_id' not in st.session_state:
                st.session_state.customer_id = ''
            if 'product_id' not in st.session_state:
                st.session_state.product_id = ''
            if 'quantity' not in st.session_state:
                st.session_state.quantity = 1
            if 'price' not in st.session_state:
                st.session_state.price = 0.00
            if 'payment_method' not in st.session_state:
                st.session_state.payment_method = ''

            # Input Fields for Transaction
            sale_id = st.text_input("Sale ID", value=st.session_state.sale_id)
            customer_id = st.text_input("Customer ID", value=st.session_state.customer_id)
            product_id = st.text_input("Product ID", value=st.session_state.product_id)
            quantity = st.number_input("Quantity", min_value=1, step=1, value=st.session_state.quantity)
            price = st.number_input("Price", min_value=0.00, step=0.01, value=st.session_state.price)
            payment_method = st.selectbox("Payment Method", ['',"Cash", "Credit Card", "Debit Card", "PayPal"], 
                                        index=['',"Cash", "Credit Card", "Debit Card", "PayPal"].index(st.session_state.payment_method))

            if st.button("Edit Transaction"):
                if not (customer_id and product_id and quantity and price and payment_method):
                    st.error("All fields are required!")
                elif not is_sales_data(sale_id):
                    st.error("No Sale ID in history")
                else:
                    total_price = quantity * price
                    transaction = {
                        "sale_id": int(sale_id),
                        "customer_id": int(customer_id),
                        "product_id": int(product_id),
                        "quantity": quantity,
                        "price": price,
                        "total_price": total_price,
                        "payment_method": payment_method,
                        "csv_path": find_csv(sale_id)
                    }
                    # Send to Kafka
                    try:
                        producer.send(KAFKA_TOPIC_TRANSACTIONS_EDIT, transaction)
                        producer.flush()
                        st.success("Transaction edited successfully!")
                        logging.info(f"Transaction edited: {transaction}")
                        time.sleep(2)
                    except Exception as e:
                        st.error("Failed to send data to Kafka")
                        logging.error(f"Kafka send failed: {str(e)}")      
                    st.rerun() 
        elif sub_sub_menu == "Remove":
            # Store form data in session state if not already stored
            if 'sale_id' not in st.session_state:
                st.session_state.sale_id = ''
            if 'customer_id' not in st.session_state:
                st.session_state.customer_id = ''
            if 'product_id' not in st.session_state:
                st.session_state.product_id = ''
            if 'quantity' not in st.session_state:
                st.session_state.quantity = 1
            if 'price' not in st.session_state:
                st.session_state.price = 0.00
            if 'payment_method' not in st.session_state:
                st.session_state.payment_method = ''

            # Input Fields for Transaction
            sale_id = st.text_input("Sale ID", value=st.session_state.sale_id)
            customer_id = st.text_input("Customer ID", value=st.session_state.customer_id)
            product_id = st.text_input("Product ID", value=st.session_state.product_id)
            quantity = st.number_input("Quantity", min_value=1, step=1, value=st.session_state.quantity)
            price = st.number_input("Price", min_value=0.00, step=0.01, value=st.session_state.price)
            payment_method = st.selectbox("Payment Method", ['',"Cash", "Credit Card", "Debit Card", "PayPal"], 
                                        index=['',"Cash", "Credit Card", "Debit Card", "PayPal"].index(st.session_state.payment_method))

            if st.button("Remove Transaction"):
                if not (customer_id and product_id and quantity and price and payment_method):
                    st.error("All fields are required!")
                elif not is_sales_data(sale_id):
                    st.error("No Sale ID in history")
                else:
                    total_price = quantity * price
                    transaction = {
                        "sale_id": int(sale_id),
                        "customer_id": int(customer_id),
                        "product_id": int(product_id),
                        "quantity": quantity,
                        "price": price,
                        "total_price": total_price,
                        "payment_method": payment_method,
                        "csv_path": find_csv(sale_id)
                    }
                    # Send to Kafka
                    try:
                        producer.send(KAFKA_TOPIC_TRANSACTIONS_REMOVE, transaction)
                        producer.flush()
                        st.success("Transaction removed successfully!")
                        logging.info(f"Transaction removed: {transaction}")
                        time.sleep(2)
                    except Exception as e:
                        st.error("Failed to send data to Kafka")
                        logging.error(f"Kafka send failed: {str(e)}")      
                    st.rerun() 
    elif sub_menu == "View Transactions":
            # Input Fields for Transaction
            start_date = st.date_input("Start date")
            end_date = st.date_input("End date")

            if st.button("View Transactions"):
                if not (start_date and end_date):
                    st.error("All fields are required!")
                else:
                    start_date = str(start_date).replace("-","")
                    start_date = f"sales_{start_date}.csv"
                    end_date = str(end_date).replace("-","")
                    end_date = f"sales_{end_date}.csv"
                    csv_files = [entry.name for entry in os.scandir(SALES_CSV_DIR) if entry.is_file() and entry.name.startswith("sales_") and entry.name.endswith(".csv")]
                    csv_files.sort()
                    view_list = []
                    for csv_file in csv_files:
                        if csv_file >= start_date and csv_file <= end_date:
                            filename = os.path.join(SALES_CSV_DIR, csv_file)
                            df = pd.read_csv(filename)
                            view_list.append(df)
                    # Combine all DataFrames in view_list into one DataFrame
                    combined_df = pd.concat(view_list, ignore_index=True)
                    st.dataframe(combined_df)
                                         
elif menu == "📦 Product":
    sub_menu = st.radio("Select Action", ["Update Product", "View Product List"])
    if sub_menu == "Update Product":
        sub_sub_menu = st.radio("Update Type", ["Add", "Edit", "Remove"])
        if sub_sub_menu == "Add":
            st.subheader("Add New Product")
            # Store form data in session state if not already stored
            if 'product_id' not in st.session_state:
                st.session_state.product_id = ''
            if 'product_name' not in st.session_state:
                st.session_state.product_name = ''
            if 'product_description' not in st.session_state:
                st.session_state.product_name = ''
            if 'product_category' not in st.session_state:
                st.session_state.product_category = ''
            if 'product_price' not in st.session_state:
                st.session_state.product_price = 0.00
            if 'stock_level' not in st.session_state:
                st.session_state.stock_level = 0

            # Set prefix product id for each category
            category = {
                "": "Select Category",
                "Daily": "1",
                "Meat" : "2",
                "Seafood" : "3",
                "Vegetable & Fruit" : "4",
                "Snack" : "5",
                "Beverage" : "6",
                "Alcohol" : "7"
            }

            # Input Fields for Product
            product_category = st.selectbox("Category", ['', "Daily", "Meat", "Seafood", "Vegetable & Fruit", "Snack", "Beverage", "Alcohol"], 
                                            index=['', "Daily", "Meat", "Seafood", "Vegetable & Fruit", "Snack", "Beverage", "Alcohol"].index(st.session_state.product_category))
            prefix = category[product_category]
            product_id = st.text_input("Product ID(Don't delete the prefix)", value=f"{prefix}")
            product_name = st.text_input("Product Name")
            product_description = st.text_input("Product Description")
            product_price = st.number_input("Product Price", min_value=0.00, step=0.01, value=st.session_state.product_price)
            stock_level = st.number_input("Stock Level", min_value=0, step=1, value=st.session_state.stock_level)
            if st.button("Submit Product"):
                if not (product_id and product_name and product_category and product_price and stock_level):
                    print("All fields are required!")
                elif is_data(product_id, "product_id", PRODUCTS_CSV_DIR, product_category):
                    st.error(f"Product ID {product_id} already exists in {product_category}.csv!")
                else:
                    product = {
                        "product_id": int(product_id),
                        "product_name": product_name,
                        "product_description": product_description,
                        "product_category": product_category,
                        "product_price": product_price,
                        "stock_level": stock_level
                    }
                    # Send to Kafka
                    try:
                        producer.send(KAFKA_TOPIC_PRODUCTS_ADD, product)
                        producer.flush()
                        st.success(f"Product {product_name} added successfully!")
                        logging.info(f"Product updated: {product}")
                    except Exception as e:
                        st.error("Failed to send data to Kafka")
                        logging.error(f"Kafka send failed: {str(e)}")
        elif sub_sub_menu == "Edit":
            st.subheader("Edit Product")
            # Store form data in session state if not already stored
            if 'product_id' not in st.session_state:
                st.session_state.product_id = ''
            if 'product_name' not in st.session_state:
                st.session_state.product_name = ''
            if 'product_description' not in st.session_state:
                st.session_state.product_name = ''
            if 'product_category' not in st.session_state:
                st.session_state.product_category = ''
            if 'product_price' not in st.session_state:
                st.session_state.product_price = 0.00
            if 'stock_level' not in st.session_state:
                st.session_state.stock_level = 0

            # Input Fields for Product
            product_id = st.text_input("Product ID")
            product_name = st.text_input("Product Name")
            product_description = st.text_input("Product Description")
            product_category = st.selectbox("Category", ['', "Daily", "Meat", "Seafood", "Vegetable & Fruit", "Snack", "Beverage", "Alcohol"], 
                                            index=['', "Daily", "Meat", "Seafood", "Vegetable & Fruit", "Snack", "Beverage", "Alcohol"].index(st.session_state.product_category))
            product_price = st.number_input("Product Price", min_value=0.00, step=0.01, value=st.session_state.product_price)
            stock_level = st.number_input("Stock Level", min_value=0, step=1, value=st.session_state.stock_level)
            if st.button("Edit"):
                if not (product_id and product_name and product_category and product_price and stock_level):
                    print("All fields are required!")
                elif not is_data(product_id, "product_id", PRODUCTS_CSV_DIR, product_category):
                    st.error(f"Product ID {product_id} is not exsist in {product_category}.csv!")
                else:
                    product = {
                        "product_id": int(product_id),
                        "product_name": product_name,
                        "product_description": product_description,
                        "product_category": product_category,
                        "product_price": product_price,
                        "stock_level": stock_level
                    }
                    # Send to Kafka
                    try:
                        producer.send(KAFKA_TOPIC_PRODUCTS_EDIT, product)
                        producer.flush()
                        st.success(f"Product {product_name} edited successfully!")
                        logging.info(f"Product updated: {product}")
                    except Exception as e:
                        st.error("Failed to send data to Kafka")
                        logging.error(f"Kafka send failed: {str(e)}")
        elif sub_sub_menu == "Remove":
            st.subheader("Remove Product")
            # Store form data in session state if not already stored
            if 'product_id' not in st.session_state:
                st.session_state.product_id = ''
            if 'product_name' not in st.session_state:
                st.session_state.product_name = ''
            if 'product_description' not in st.session_state:
                st.session_state.product_name = ''
            if 'product_category' not in st.session_state:
                st.session_state.product_category = ''
            if 'product_price' not in st.session_state:
                st.session_state.product_price = 0.00
            if 'stock_level' not in st.session_state:
                st.session_state.stock_level = 0

            # Input Fields for Product
            product_id = st.text_input("Product ID")
            product_name = st.text_input("Product Name")
            product_description = st.text_input("Product Description")
            product_category = st.selectbox("Category", ['', "Daily", "Meat", "Seafood", "Vegetable & Fruit", "Snack", "Beverage", "Alcohol"], 
                                            index=['', "Daily", "Meat", "Seafood", "Vegetable & Fruit", "Snack", "Beverage", "Alcohol"].index(st.session_state.product_category))
            product_price = st.number_input("Product Price", min_value=0.00, step=0.01, value=st.session_state.product_price)
            stock_level = st.number_input("Stock Level", min_value=0, step=1, value=st.session_state.stock_level)
            if st.button("Remove"):
                if not (product_id and product_name and product_category and product_price and stock_level):
                    print("All fields are required!")
                elif not is_data(product_id, "product_id", PRODUCTS_CSV_DIR, product_category):
                    st.error(f"Product ID {product_id} is not exsist in {product_category}.csv!")
                else:
                    product = {
                        "product_id": int(product_id),
                        "product_name": product_name,
                        "product_description": product_description,
                        "product_category": product_category,
                        "product_price": product_price,
                        "stock_level": stock_level
                    }
                    # Send to Kafka
                    try:
                        producer.send(KAFKA_TOPIC_PRODUCTS_REMOVE, product)
                        producer.flush()
                        st.success(f"Product {product_name} removed successfully!")
                        logging.info(f"Product removed: {product}")
                    except Exception as e:
                        st.error("Failed to send data to Kafka")
                        logging.error(f"Kafka send failed: {str(e)}")
    elif sub_menu == "View Product List":
        # Input Fields for Transaction
            category = st.selectbox("Category", ["All", "Daily", "Meat", "Seafood", "Vegetable & Fruit", "Snack", "Beverage", "Alcohol"])

            if st.button("View Transactions"):
                if not (category):
                    st.error("All fields are required!")
                elif category == "All":
                    category_files = [entry.name for entry in os.scandir(PRODUCTS_CSV_DIR) if entry.is_file() and entry.name.endswith(".csv")]
                    view_list = []
                    for category_file in category_files:
                        filename = os.path.join(PRODUCTS_CSV_DIR, category_file)
                        df = pd.read_csv(filename)
                        view_list.append(df)
                    # Combine all DataFrames in view_list into one DataFrame
                    combined_df = pd.concat(view_list, ignore_index=True)
                    st.dataframe(combined_df)
                else:
                    filename = get_filename(PRODUCTS_CSV_DIR, category)
                    df = pd.read_csv(filename)
                    st.dataframe(df)

elif menu == "👤 Membership":
    sub_menu = st.radio("Select Action", ["Update Customer", "View Customer List"])
    if sub_menu == "Update Customer":
        sub_sub_menu = st.radio("Update Type", ["Add", "Edit", "Remove"])
        if sub_sub_menu == "Add":
            customer_id = st.text_input("Customer ID")
            customer_name = st.text_input("Customer Name")
            customer_location = st.text_input("Customer Location")
            if st.button("Register"):
                # Save member information in your database or file
                if not (customer_id and customer_name and customer_location):
                    print("All fields are required")
                elif is_data(customer_id, "customer_id", CUSTOMERS_CSV_DIR, "Customers"):
                    st.error(f"Customer ID {customer_id} already exists in Customers.csv!")
                else:
                    customer = {
                        "customer_id": int(customer_id),
                        "customer_name": customer_name,
                        "customer_location": customer_location
                    }
                    # Send to Kafka
                    try:
                        producer.send(KAFKA_TOPIC_CUSTOMERS_ADD, customer)
                        producer.flush()
                        st.success(f"Member {customer_name} added successfully!")
                        logging.info(f"Customer added: {customer}")
                    except Exception as e:
                        st.error("Failed to send data to Kafka")
                        logging.error(f"Kafka send failed: {str(e)}")
        elif sub_sub_menu == "Edit":
            customer_id = st.text_input("Customer ID")
            customer_name = st.text_input("Customer Name")
            customer_location = st.text_input("Customer Location")
            if st.button("Edit"):
                # Save member information in your database or file
                if not (customer_id and customer_name and customer_location):
                    print("All fields are required")
                elif not is_data(customer_id, "customer_id", CUSTOMERS_CSV_DIR, "Customers"):
                    st.error(f"Customer ID {customer_id} does not exist in the Customers.csv!")
                else:
                    customer = {
                        "customer_id": int(customer_id),
                        "customer_name": customer_name,
                        "customer_location": customer_location
                    }
                    # Send to Kafka
                    try:
                        producer.send(KAFKA_TOPIC_CUSTOMERS_EDIT, customer)
                        producer.flush()
                        st.success(f"Member {customer_name} edited successfully!")
                        logging.info(f"Customer edited: {customer}")
                    except Exception as e:
                        st.error("Failed to send data to Kafka")
                        logging.error(f"Kafka send failed: {str(e)}")
        elif sub_sub_menu == "Remove":
            customer_id = st.text_input("Customer ID")
            customer_name = st.text_input("Customer Name")
            customer_location = st.text_input("Customer Location")
            if st.button("Remove"):
                # Save member information in your database or file
                if not (customer_id and customer_name and customer_location):
                    print("All fields are required")
                elif not is_data(customer_id, "customer_id", CUSTOMERS_CSV_DIR, "Customers"):
                    st.error(f"Customer ID {customer_id} does not exist in the Customers.csv!")
                else:
                    customer = {
                        "customer_id": int(customer_id),
                        "customer_name": customer_name,
                        "customer_location": customer_location
                    }
                    # Send to Kafka
                    try:
                        producer.send(KAFKA_TOPIC_CUSTOMERS_REMOVE, customer)
                        producer.flush()
                        st.success(f"Member {customer_name} removed successfully!")
                        logging.info(f"Customer removed: {customer}")
                    except Exception as e:
                        st.error("Failed to send data to Kafka")
                        logging.error(f"Kafka send failed: {str(e)}")
    elif sub_menu == "View Customer List":
        filename = get_filename(CUSTOMERS_CSV_DIR, "Customers")
        df = pd.read_csv(filename)
        st.dataframe(df)
