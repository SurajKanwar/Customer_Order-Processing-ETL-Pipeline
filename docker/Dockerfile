# # Use the official Apache Airflow image
# FROM apache/airflow:2.7.0

# # Use root user to install dependencies
# # USER root

# # Install additional dependencies (e.g., pandas, sqlalchemy, psycopg2, pymysql)
# RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary pymysql

# # Switch back to airflow user
# USER airflow

# # Set environment variables
# #ENV AIRFLOW_HOME=/usr/local/airflow

# # Copy the DAGs from the local Windows machine to the container
# # Make sure this path points to the correct location where the dags are stored
# COPY ./dags /C:/Users/Dusty/Downloads/Internship/Git_Repo/Customer-Order-Processing-ETL-Pipeline/docker/dags:/opt/airflow/dags

# # Set the entry point to start Airflow when the container is run
# ENTRYPOINT ["airflow"]
# CMD ["webserver", "-p", "8080"]



# Use the official Apache Airflow image
FROM apache/airflow:2.6.0

# Install dependencies
RUN pip install pymysql psycopg2-binary pandas
# Set the working directory
WORKDIR /opt/airflow

# Set the user back to airflow
USER airflow
# Copy your DAGs and other necessary files into the container
COPY ./dags C:/Users/Dusty/Downloads/Internship/Git_Repo/Customer-Order-Processing-ETL-Pipeline/docker/dags:/opt/airflow/dags
#COPY ./plugins /C:/Users/Dusty/Downloads/Internship/Git_Repo/Customer-Order-Processing-ETL-Pipeline/docker/plugins:/opt/airflow/plugins


ENTRYPOINT ["airflow"]
CMD ["airflow", "webserver", "-p", "8080"]