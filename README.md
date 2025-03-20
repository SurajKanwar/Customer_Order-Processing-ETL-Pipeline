# Data Engineering Training for Interns

## Project: Customer Order Processing ETL Pipeline

This repository contains a structured **1-week Data Engineering Training Plan** and the capstone project: **Customer Order Processing ETL Pipeline**. The training will cover essential tools, libraries, and technologies required to build a robust data pipeline.

---

## Repository Structure:

```
root/
|-- README.md
|-- data/               # Contains sample datasets
|-- notebooks/          # Jupyter notebooks for training
|-- scripts/            # Python scripts for ETL tasks
|-- airflow_dags/       # Airflow DAGs for automation
|-- docker/             # Dockerfiles for containerization
|-- big_data/           # PySpark scripts
```

---

## **1-Week Training Plan**

### **Day 1: Introduction to Data Engineering**
- Overview of Data Engineering concepts and ecosystem.
- Tools: SQL, Python, Airflow, Spark, Docker.

#### **Tasks:**
1. Research the role of a Data Engineer and data pipeline concepts.
2. Document findings in a Markdown file.

## Documentation
- [Data Engineering Concepts](docs/Data_Engineering_Concepts.md)

---

### **Day 2: SQL Fundamentals**
- Learn CRUD operations, joins, aggregations, window functions.
- Tools: SQLite.

#### **Tasks:**
1. Import a sample SQLite database from `data/sample_orders.db`.
2. Write and execute SQL queries.

**Reference Notebook:** `notebooks/sql_basics.ipynb`

---

### **Day 3: Python for Data Engineering**
- Data manipulation using Pandas.
- File handling (CSV, JSON).
- API data fetching.

#### **Tasks:**
1. Load `data/sample_orders.csv` and perform data cleaning.
2. Fetch data from an API and save it locally.

**Reference Notebook:** `notebooks/python_pandas_api.ipynb`

---

### **Day 4: ETL Pipeline Basics**
- Automate ETL tasks using Python.
- Tools: Pandas, SQLite.

#### **Tasks:**
1. Build an ETL pipeline:
   - Extract: Read data from CSV.
   - Transform: Clean and standardize.
   - Load: Insert into SQLite database.
2. Document the steps in a notebook.

**Reference Script:** `scripts/etl_pipeline.py`

---

### **Day 5: Workflow Orchestration with Airflow**
- DAG creation and scheduling.

#### **Tasks:**
1. Set up Apache Airflow using Docker.
2. Create a DAG to automate the ETL pipeline from Day 4.

**Reference DAG:** `airflow_dags/customer_order_dag.py`

---

### **Day 6: Big Data with PySpark**
- Distributed processing using PySpark.
- Tools: PySpark, Jupyter Notebook.

#### **Tasks:**
1. Process large datasets using PySpark.
2. Perform aggregations and transformations.

**Reference Notebook:** `notebooks/pyspark_basics.ipynb`

---

### **Day 7: Docker for Data Engineering**
- Basics of Docker.
- Containerizing Python applications.

#### **Tasks:**
1. Create a `Dockerfile` to containerize the ETL pipeline.
2. Build and run the Docker container.

**Reference Directory:** `docker/`

---

## Capstone Project: Customer Order Processing ETL Pipeline

**Objective:** Create an end-to-end ETL pipeline that processes customer order data, stores it in a database, and automates the workflow using Airflow.

#### **Steps:**
1. **Extract:** Read order data from a CSV file.
2. **Transform:** Clean data (e.g., handle missing values, format standardization).
3. **Load:** Store the cleaned data in a SQLite database.
4. **Automate:** Schedule the ETL pipeline with Airflow.
5. **Scale:** Use PySpark to process large datasets.
6. **Containerize:** Package the pipeline using Docker.

---

## Instructions:
1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/data-engineering-training.git
   cd data-engineering-training
   ```
2. Follow the training plan in order.
3. Refer to the `README.md` files in each folder for task-specific details.

---

## References:

### Courses:
1. [Data Engineering on Google Cloud (Coursera)](https://www.coursera.org/professional-certificates/google-cloud-data-engineering)
2. [Introduction to SQL (Datacamp)](https://www.datacamp.com/courses/intro-to-sql-for-data-science)
3. [Big Data with PySpark (Coursera)](https://www.coursera.org/learn/big-data-analysis-with-spark)

### Tools Documentation:
- [Pandas Documentation](https://pandas.pydata.org/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Docker Documentation](https://docs.docker.com/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)

---
