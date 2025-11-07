from __future__ import annotations
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from psycopg2 import Error as DatabaseError
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import shutil
from faker import Faker
import pandas as pd

OUTPUT_DIR = "/opt/airflow/data"
TARGET_TABLE = 'employees'

default_args = {
    "owner": "IDS706",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

dag = DAG(
    dag_id="pipeline",
    default_args=default_args,
    description="Employee data pipeline with PySpark transformations and analysis",
    schedule="@daily",
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["assignment", "pyspark", "employees"]
)


# 1. Fetch persons data
@task(dag=dag)
def fetch_persons(quantity=100):
    """Generate persons dataset (Dataset 1)"""
    fake = Faker()
    data = []
    
    for _ in range(quantity):
        data.append({
            "firstname": fake.first_name(),
            "lastname": fake.last_name(),
            "email": fake.free_email(),
            "phone": fake.phone_number(),
            "address": fake.street_address(),
            "city": fake.city(),
            "country": fake.country(),
            "age": fake.random_int(min=22, max=65),
            "salary": fake.random_int(min=40000, max=150000)
        })

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, "persons.csv")

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    print(f"Created {len(data)} persons")
    return filepath


# 2. Fetch companies data
@task(dag=dag)
def fetch_companies(quantity=100):
    """Generate companies dataset (Dataset 2)"""
    fake = Faker()
    data = []
    
    for _ in range(quantity):
        data.append({
            "name": fake.company(),
            "email": f"info@{fake.domain_name()}",
            "phone": fake.phone_number(),
            "country": fake.country(),
            "website": fake.url(),
            "industry": fake.bs().split()[0].capitalize(),
            "catch_phrase": fake.catch_phrase(),
            "employees_count": fake.random_int(min=10, max=5000),
            "founded_year": fake.random_int(min=1950, max=2023),
            "revenue": fake.random_int(min=100000, max=10000000)
        })

    filepath = os.path.join(OUTPUT_DIR, "companies.csv")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    print(f"Created {len(data)} companies")
    return filepath


# 3. Transform persons with PySpark
@task(dag=dag)
def transform_persons_pyspark(persons_path):
    """Apply PySpark transformations to persons dataset"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, concat_ws, when, split

    
    spark = SparkSession.builder \
        .appName("PersonsTransform") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        df = spark.read.csv(persons_path, header=True, inferSchema=True)
        
        print(f"Loaded {df.count()} persons into Spark")
        
        # Transformation 1: Create full name
        df = df.withColumn("fullname", concat_ws(" ", col("firstname"), col("lastname")))
        
        # Transformation 2: Extract email domain
        df = df.withColumn("email_domain", split(col("email"), "@").getItem(1))
        
        # Transformation 3: Age category
        df = df.withColumn("age_category",
            when(col("age") < 35, "Young")
            .when(col("age") < 50, "Mid-Career")
            .otherwise("Senior")
        )
        
        # Transformation 4: Salary bracket
        df = df.withColumn("salary_bracket",
            when(col("salary") < 70000, "Entry")
            .when(col("salary") < 110000, "Mid")
            .otherwise("Senior")
        )
        
        df.show(5)
        
        # Save
        output_path = os.path.join(OUTPUT_DIR, "persons_transformed.csv")
        temp_path = output_path + "_temp"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)
        
        # Extract CSV from Spark output
        csv_file = [f for f in os.listdir(temp_path) if f.endswith('.csv')][0]
        shutil.move(os.path.join(temp_path, csv_file), output_path)
        shutil.rmtree(temp_path)
        
        print(f"Transformed persons saved to {output_path}")
        return output_path
        
    finally:
        spark.stop()
        print("PySpark session stopped")


# 4. Transform companies with PySpark
@task(dag=dag)
def transform_companies_pyspark(companies_path):
    """Apply PySpark transformations to companies dataset"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when, lit, upper
    
    print("Starting PySpark transformation for companies...")
    
    spark = SparkSession.builder \
        .appName("CompaniesTransform") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        df = spark.read.csv(companies_path, header=True, inferSchema=True)
        
        print(f"Loaded {df.count()} companies into Spark")
        
        # Transformation 1: Company size category
        df = df.withColumn("company_size",
            when(col("employees_count") < 100, "Small")
            .when(col("employees_count") < 1000, "Medium")
            .otherwise("Large")
        )
        
        # Transformation 2: Company age
        current_year = datetime.now().year
        df = df.withColumn("company_age", lit(current_year) - col("founded_year"))
        
        # Transformation 3: Revenue category
        df = df.withColumn("revenue_category",
            when(col("revenue") < 1000000, "Low")
            .when(col("revenue") < 5000000, "Mid")
            .otherwise("High")
        )
        
        # Transformation 4: Industry standardization
        df = df.withColumn("industry_clean", upper(col("industry")))
        
        print("PySpark transformations complete!")
        df.show(5)
        
        # Save
        output_path = os.path.join(OUTPUT_DIR, "companies_transformed.csv")
        temp_path = output_path + "_temp"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)
        
        # Extract CSV from Spark output
        csv_file = [f for f in os.listdir(temp_path) if f.endswith('.csv')][0]
        shutil.move(os.path.join(temp_path, csv_file), output_path)
        shutil.rmtree(temp_path)
        
        print(f"Transformed companies saved to {output_path}")
        return output_path
        
    finally:
        spark.stop()
        print("PySpark session stopped")


# 5. Merge datasets
@task(dag=dag)
def merge_csvs(persons_path, companies_path):
    """Merge persons and companies datasets"""
    persons_df = pd.read_csv(persons_path)
    companies_df = pd.read_csv(companies_path)
    
    min_len = min(len(persons_df), len(companies_df))
    
    merged_data = []
    for i in range(min_len):
        merged_data.append({
            "employee_id": i + 1,
            "firstname": persons_df.iloc[i]["firstname"],
            "lastname": persons_df.iloc[i]["lastname"],
            "fullname": persons_df.iloc[i]["fullname"],
            "email": persons_df.iloc[i]["email"],
            "email_domain": persons_df.iloc[i]["email_domain"],
            "age": persons_df.iloc[i]["age"],
            "age_category": persons_df.iloc[i]["age_category"],
            "salary": persons_df.iloc[i]["salary"],
            "salary_bracket": persons_df.iloc[i]["salary_bracket"],
            "city": persons_df.iloc[i]["city"],
            "country": persons_df.iloc[i]["country"],
            "company_name": companies_df.iloc[i]["name"],
            "company_email": companies_df.iloc[i]["email"],
            "industry": companies_df.iloc[i]["industry_clean"],
            "company_size": companies_df.iloc[i]["company_size"],
            "company_age": companies_df.iloc[i]["company_age"],
            "revenue_category": companies_df.iloc[i]["revenue_category"]
        })
    
    merged_path = os.path.join(OUTPUT_DIR, "merged_data.csv")
    
    with open(merged_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=merged_data[0].keys())
        writer.writeheader()
        writer.writerows(merged_data)

    print(f"Merged {len(merged_data)} rows")
    return merged_path


# 6. Load to PostgreSQL
@task(dag=dag)
def load_to_database(file_path):
    """Load merged data into PostgreSQL"""
    df = pd.read_csv(file_path)

    hook = PostgresHook(postgres_conn_id="Postgres")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("CREATE SCHEMA IF NOT EXISTS week8_demo;")
    cursor.execute(f"DROP TABLE IF EXISTS week8_demo.{TARGET_TABLE};")

    create_table = """
        CREATE TABLE week8_demo.employees (
            employee_id INTEGER,
            firstname TEXT,
            lastname TEXT,
            fullname TEXT,
            email TEXT,
            email_domain TEXT,
            age INTEGER,
            age_category TEXT,
            salary INTEGER,
            salary_bracket TEXT,
            city TEXT,
            country TEXT,
            company_name TEXT,
            company_email TEXT,
            industry TEXT,
            company_size TEXT,
            company_age INTEGER,
            revenue_category TEXT
        );
    """
    cursor.execute(create_table)

    for index, row in df.iterrows():
        insert = f"INSERT INTO week8_demo.{TARGET_TABLE} VALUES ({','.join(['%s']*len(row))});"
        cursor.execute(insert, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Loaded {len(df)} rows into database")
    return len(df)


# 7. Analyze with PySpark
@task(dag=dag)
def analyze_with_pyspark():
    """Use PySpark to analyze employee data and create visualization"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, avg, round as spark_round
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    
    print("Starting PySpark for analysis...")
    
    spark = SparkSession.builder \
        .appName("EmployeeAnalysis") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        print("PySpark session created!")
        
        # Read from database
        hook = PostgresHook(postgres_conn_id="Postgres")
        query = f"SELECT * FROM week8_demo.{TARGET_TABLE};"
        df_pandas = hook.get_pandas_df(query)
        
        # Convert to Spark DataFrame
        df_spark = spark.createDataFrame(df_pandas)
        
        print(f"Loaded {df_spark.count()} rows into Spark DataFrame")
        
        # PySpark aggregation: Count by salary bracket
        salary_analysis = df_spark.groupBy("salary_bracket") \
            .agg(count("*").alias("employee_count")) \
            .orderBy("salary_bracket")
        
        # PySpark aggregation: Count by company size
        company_analysis = df_spark.groupBy("company_size") \
            .agg(count("*").alias("employee_count")) \
            .orderBy("company_size")
        
        print("PySpark aggregations complete!")
        
        # Convert to pandas for plotting
        salary_data = salary_analysis.toPandas()
        company_data = company_analysis.toPandas()
        
        spark.stop()
        print("PySpark session stopped")
        
        # Create visualization
        fig, axes = plt.subplots(2, 2, figsize=(12, 9))
        fig.suptitle('Employee Data Analysis (PySpark)', fontsize=14, fontweight='bold')
        
        # Plot 1: Salary bracket
        axes[0, 0].bar(salary_data['salary_bracket'], salary_data['employee_count'], color='skyblue')
        axes[0, 0].set_title('Employees by Salary Bracket')
        axes[0, 0].set_ylabel('Count')
        axes[0, 0].tick_params(axis='x', rotation=0)
        
        # Plot 2: Company size
        axes[0, 1].bar(company_data['company_size'], company_data['employee_count'], color='coral')
        axes[0, 1].set_title('Employees by Company Size')
        axes[0, 1].set_ylabel('Count')
        axes[0, 1].tick_params(axis='x', rotation=0)
        
        # Plot 3: Age distribution
        axes[1, 0].hist(df_pandas['age'], bins=15, color='lightgreen', edgecolor='black')
        axes[1, 0].set_title('Age Distribution')
        axes[1, 0].set_xlabel('Age')
        axes[1, 0].set_ylabel('Count')
        
        # Plot 4: Salary distribution
        axes[1, 1].hist(df_pandas['salary'], bins=15, color='mediumpurple', edgecolor='black')
        axes[1, 1].set_title('Salary Distribution')
        axes[1, 1].set_xlabel('Salary ($)')
        axes[1, 1].set_ylabel('Count')
        
        plt.tight_layout()
        
        output = os.path.join(OUTPUT_DIR, 'analysis_chart.png')
        plt.savefig(output, dpi=150, bbox_inches='tight')
        plt.close()
        
        print(f"Chart created using PySpark analysis and saved to {output}")
        return output
        
    except Exception as e:
        spark.stop()
        raise e


# 8. Cleanup intermediate files
@task(dag=dag)
def cleanup():
    """Delete temporary CSV files"""
    files = [
        "persons.csv",
        "companies.csv",
        "persons_transformed.csv",
        "companies_transformed.csv",
        "merged_data.csv"
    ]
    
    count = 0
    for file in files:
        path = os.path.join(OUTPUT_DIR, file)
        if os.path.exists(path):
            os.remove(path)
            count += 1
    
    print(f"Cleaned up {count} files")


persons_data = fetch_persons()
companies_data = fetch_companies()

persons_transformed = transform_persons_pyspark(persons_data)
companies_transformed = transform_companies_pyspark(companies_data)

merged = merge_csvs(persons_transformed, companies_transformed)

loaded = load_to_database(merged)

chart = analyze_with_pyspark()

clean = cleanup()

loaded >> chart >> clean