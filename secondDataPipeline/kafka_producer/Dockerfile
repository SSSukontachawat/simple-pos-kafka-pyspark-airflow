# Dockerfile for Kafka Producer Service
FROM python:3.9.21-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements_producer.txt .

# Install dependencies
RUN pip install -r requirements_producer.txt

# Copy all your source code into the container
COPY . .

# Expose the Streamlit port
EXPOSE 8501

# Command to run the producer (Streamlit app)
CMD ["streamlit", "run", "kafka_producer.py"]