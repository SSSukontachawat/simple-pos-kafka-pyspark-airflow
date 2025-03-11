# Simple POS - Kafka, PySpark, Airflow

## 🚀 Overview
This project is a **simple POS (Point-of-Sale) system** using **Kafka, PySpark, Airflow, and PostgreSQL**, running in Docker.

## 🏗️ Architecture
1. **Kafka Producer (Streamlit App)**
   - Allows users to **add, edit, and delete transactions, products, and members**.
   - Publishes data to Kafka topics.

2. **Kafka Consumer**
   - Reads Kafka messages and stores them in **CSV files**.

3. **PySpark & Airflow**
   - **Processes CSV data** and loads it into **PostgreSQL**.
   - **Hourly:** Updates recent data.
   - **Daily:** Uploads all transactions at 10 PM.

4. **Docker Compose**
   - Manages all services in containers.

## 🛠️ How to Run
### **1️⃣ Clone the Repository**
```bash
git clone https://github.com/SSSukontachawat/simple-pos-kafka-pyspark-airflow.git
cd simple-pos-kafka-pyspark-airflow
```
### **2️⃣ Run Docker
```
docker-compose up -d --build
```
### **3️⃣ Access Services
Streamlit App (Producer UI): http://localhost:8501
Airflow UI: http://localhost:8080
```
docker-compose down
```

## 🏆 Key Features
✅ Real-time data streaming with Kafka
✅ Data processing with PySpark
✅ Task scheduling with Airflow
✅ CI/CD automation with GitHub Actions

## 📜 License
MIT License.
