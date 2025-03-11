# Use the official Apache Airflow image as the base
FROM apache/airflow:2.10.5-python3.12

# Set the working directory inside the container
WORKDIR /opt/airflow

# Copy the requirements file into the container
COPY requirements_DAG.txt .

# Install Python dependencies first
RUN pip install --no-cache-dir -r requirements_DAG.txt
# Install Celery and Redis dependencies
RUN pip install "apache-airflow[celery]"
RUN pip install apache-airflow-providers-celery
# Switch to root user for system-level package installation
USER root

# Update apt and install OpenJDK 17 (headless) and necessary certificates
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    ca-certificates-java && \
    apt-get clean;

RUN apt-get update && apt-get install -y procps

# Set JAVA_HOME environment variable for OpenJDK 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Disable unnecessary services (e.g., dbus, polkit) during the installation
RUN echo "exit 0" > /usr/sbin/policy-rc.d

# Copy the DAG files into the container
COPY dags/ /opt/airflow/dags/

# Expose the web UI port (optional, only if you need to access the UI)
EXPOSE 8080
