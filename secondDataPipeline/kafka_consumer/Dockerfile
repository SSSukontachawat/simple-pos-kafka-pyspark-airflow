# Dockerfile for Kafka Consumer Service
FROM python:3.9.21-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements_consumer.txt .

# Install dependencies
RUN pip install -r requirements_consumer.txt

# Copy all your source code into the container
COPY . .

# Expose the required port
EXPOSE 5000

# Command to run the Kafka consumer
CMD ["python", "kafka_consumer.py"]

# Command to keep the container running for debugging
# CMD ["tail", "-f", "/dev/null"]
