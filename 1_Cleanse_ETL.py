# Configuration and Setup
import os
os.environ['JAVA_HOME'] = r'C:\Users\anton\.conda\envs\conda-env\Library\lib\jvm'
os.environ['HADOOP_HOME'] = r'C:\Hadoop\hadoop-3.3.6'
os.environ['PATH'] += r';C:\Hadoop\hadoop-3.3.6\bin'
os.environ['SPARK_HOME'] = r'C:\Users\anton\.conda\envs\conda-env\Lib\site-packages\pyspark'

from pyspark.sql.functions import (
    when, col, row_number, current_timestamp, date_format,
    from_utc_timestamp, lit, regexp_replace,
    ) #max as spark_max)
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DateType,
    DecimalType, TimestampType
    )
from pathlib import Path
import pyodbc

sql_server = "PC-W11"
database_name = "ETL_Assignment2_Warehouse" # Do not change this
user = "admin"
user = "admin"
password = "sql"
jdbc_jar_path = r"C:\Spark\sqljdbc_12.10\enu\jars\mssql-jdbc-12.10.0.jre11.jar"

# Initialize Spark session
spark = SparkSession.builder.appName(database_name) \
    .config("spark.jars", jdbc_jar_path) \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.debug.maxToStringFields", "1000") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Configuration
CONFIG = {
    "db_url": f"jdbc:sqlserver://{sql_server}:1433;"
              f"databaseName={database_name};"
              f"user={user};"
              f"password={password};"
              "encrypt=false;"
              "trustServerCertificate=true",

    "pyodbc_conn": f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                   f"SERVER={sql_server};"
                   f"DATABASE={database_name};"
                   f"UID={user};"
                   f"PWD={password};",
    "files": {
        "account": {
            "csv": "data/Raw_Loan_Account.csv",
            "parquet": "data/Loan_Account_cleansed.parquet",
            "table": "Loan_Account_cleansed"
            },
        "balance": {
            "csv": "data/Raw_Loan_Balance.csv",
            "parquet": "data/Loan_Balance_cleansed.parquet",
            "table": "Loan_Balance_cleansed"
            },
        "transactions": {
            "csv": "data/Raw_Loan_Transaction.csv",
            "parquet": "data/Loan_Transaction_cleansed.parquet",
            "table": "Loan_Transaction_cleansed"
            }
        }
}

# Database setup
try:
    conn = pyodbc.connect(CONFIG["pyodbc_conn"], autocommit=True)
    cursor = conn.cursor()

    create_db_query = """
    IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = 'ETL_Assignment2_Warehouse')
    BEGIN
        CREATE DATABASE ETL_Assignment2_Warehouse
        PRINT 'Database ETL_Assignment2_Warehouse created successfully.'
    END
    """
    cursor.execute(create_db_query)
    conn.close()
    print("\nDatabase setup completed.")
except Exception as e:
    print(f"\nDatabase setup error: {e}")

# Schema definitions
account_schema = StructType([
    StructField("LoanAccountId", IntegerType(), False),
    StructField("SourceId", IntegerType(), False),
    StructField("AccountNumber", StringType(), False),
    StructField("IBAN", StringType(), True),
    StructField("BBAN", StringType(), True),
    StructField("AccountCurrencyId", IntegerType(), False),
    StructField("AccountCurrency", StringType(), False),
    StructField("OrganizationId", IntegerType(), False),
    StructField("OrganizationName", StringType(), False),
    StructField("ChannelID", IntegerType(), True),
    StructField("BrokerId", IntegerType(), False),
    StructField("OpenDateId", IntegerType(), False),
    StructField("OpenDate", DateType(), False),
    StructField("CancelledDateId", IntegerType(), True),
    StructField("CancelledDate", DateType(), True),
    StructField("ValueDate", DateType(), False),
    StructField("MaturityDate", DateType(), True),
    StructField("ProductId", IntegerType(), False),
    StructField("Product", StringType(), False),
    StructField("InvoiceDay", IntegerType(), False),
    StructField("CurrentInstallmentAmount", StringType(), False),
    StructField("CurrentInvoiceFee", StringType(), False),
    StructField("RepaymentRate", StringType(), True),
    StructField("NextInvoiceDate", DateType(), False),
    StructField("CalculatedMaturityDate", DateType(), True)
])

balance_schema = StructType([
    StructField("LoanAccountBalanceId", IntegerType(), False),
    StructField("SourceId", IntegerType(), False),
    StructField("BalanceDateId", IntegerType(), False),
    StructField("LoanAccountId", IntegerType(), False),
    StructField("ProductId", IntegerType(), False),
    StructField("AccountCurrencyId", IntegerType(), False),
    StructField("AccountStatusId", IntegerType(), False),
    StructField("NumOfTransactions", IntegerType(), False),
    StructField("NetTransactionAmount", StringType(), False),
    StructField("NetTransactionAmountSek", StringType(), False),
    StructField("AccruedInterest", StringType(), False),
    StructField("AccruedInterestSEK", StringType(), False),
    StructField("Balance", StringType(), False),
    StructField("BalanceSek", StringType(), False),
    StructField("LTV", IntegerType(), False),
    StructField("PrecedingId", IntegerType(), True)
])

transaction_schema = StructType([
    StructField("LoanAccountTransactionId", IntegerType(), False),
    StructField("SourceId", IntegerType(), False),
    StructField("TransactionDateId", IntegerType(), False),
    StructField("ValueDateId", IntegerType(), False),
    StructField("EntryDateID", IntegerType(), False),
    StructField("LoanAccountId", IntegerType(), False),
    StructField("TransactionTypeId", IntegerType(), False),
    StructField("TransactionStatus", StringType(), True),
    StructField("RectifyStatus", StringType(), True),
    StructField("TransactionCurrencyId", IntegerType(), False),
    StructField("TransactionAmount", StringType(), False),
    StructField("TransactionAmountSEK", StringType(), False),
    StructField("CounterpartClearingNumber", StringType(), True),
    StructField("CounterPartBic", StringType(), True),
    StructField("CounterPartIban", StringType(), True),
    StructField("TransactionReference", StringType(), True),
    StructField("ExchangeRateId", IntegerType(), False),
    StructField("TransactionText", StringType(), True),
    StructField("AccountServicerReference", StringType(), True),
    StructField("CounterPartId", IntegerType(), True),
    StructField("CounterPartAccountNumber", StringType(), True),
    StructField("CounterPartBankName", StringType(), True),
    StructField("TransactionDateTime", TimestampType(), True),
    StructField("IsDirectDebit", IntegerType(), True),
    StructField("GLAccount", StringType(), True),
    StructField("EventName", StringType(), True),
    StructField("InvoiceId", IntegerType(), True)
])

print("Configuration and schemas loaded successfully.")

# Utility Functions
def convert_european_decimals(df, decimal_columns):
    """Convert European decimal format to US format and cast to decimal"""
    converted_df = df

    for col_name in decimal_columns:
        if col_name in df.columns:
            converted_df = converted_df.withColumn(
                col_name,
                when(
                    (col(col_name).isNull()) |
                    (col(col_name) == "") |
                    (col(col_name) == "NULL") |
                    (col(col_name) == "null"),
                    lit(0.0)
                ).otherwise(
                    regexp_replace(
                        regexp_replace(col(col_name), r"^\s+|\s+$", ""),
                        ",", "."
                    ).cast(DecimalType(12, 5))
                )
            )
    return converted_df

def check_table_exists(table_name):
    """Check if table exists in database"""
    try:
        conn = pyodbc.connect(CONFIG["pyodbc_conn"])
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = '{table_name}'
        """)
        exists = cursor.fetchone()[0] > 0
        cursor.close()
        conn.close()
        return exists
    except Exception as e:
        print(f"        \nError checking table existence: {e}")
        return False

def check_parquet_exists(parquet_path):
    """Check if parquet file exists and has data"""
    try:
        path = Path(parquet_path)
        if path.exists():
            # Check if directory has parquet files
            parquet_files = list(path.glob("*.parquet"))
            if parquet_files:
                return True
            # Check if it's a directory with parquet files
            if path.is_dir():
                for item in path.iterdir():
                    if item.name.endswith('.parquet') or item.name.startswith('part-'):
                        return True
        return False
    except Exception as e:
        print(f"        \nError checking parquet existence: {e}")
        return False

def load_csv_data(csv_path, schema):
    """Load CSV data"""
    try:
        if not Path(csv_path).exists():
            print(f"CSV file not found: {csv_path}")
            return None

        df = spark.read.option("delimiter", ";") \
            .csv(csv_path, header=True, schema=schema)
        count = df.count()
        print(f"        Loaded {count} records from {csv_path}")
        return df if count > 0 else None
    except Exception as e:
        print(f"        \nError loading CSV {csv_path}: {e}")
        return None

def load_parquet_data(parquet_path):
    """Load existing parquet data"""
    try:
        if not check_parquet_exists(parquet_path):
            print(f"Parquet file not found: {parquet_path}")
            return None

        df = spark.read.parquet(parquet_path)
        count = df.count()
        print(f"        Loaded {count} records from parquet {parquet_path}")
        return df if count > 0 else None
    except Exception as e:
        print(f"        \nError loading parquet {parquet_path}: {e}")
        return None

def load_database_data(table_name):
    """Load data from database table"""
    load_database_none = None
    try:
        df = spark.read.format("jdbc") \
            .option("url", CONFIG["db_url"]) \
            .option("dbtable", table_name) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .load()
        count = df.count()
        print(f"        Loaded {count} records from database table {table_name}")
        return df if count > 0 else load_database_none
    except Exception as e:
        print(f"        \nCould not load from database table {table_name}: {e}")
        return load_database_none

# Table Creation Functions
def create_account_table(table_name):
    """Create loan account table"""
    try:
        conn = pyodbc.connect(CONFIG["pyodbc_conn"])
        cursor = conn.cursor()

        # Drop table if exists
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        create_sql = f"""
        CREATE TABLE {table_name} (
            LoanAccountIdentityId INT IDENTITY(1,1) PRIMARY KEY,
            LoanAccountId INT NOT NULL UNIQUE,
            SourceId INT NOT NULL DEFAULT 8,
            AccountNumber NVARCHAR(50) NOT NULL,
            IBAN NVARCHAR(34),
            BBAN NVARCHAR(34),
            AccountCurrencyId INT NOT NULL,
            AccountCurrency NVARCHAR(3) NOT NULL,
            OrganizationId INT NOT NULL,
            OrganizationName NVARCHAR(50) NOT NULL,
            ChannelID INT,
            BrokerId INT NOT NULL,
            OpenDateId INT NOT NULL,
            OpenDate DATE NOT NULL,
            CancelledDateId INT,
            CancelledDate DATE,
            ValueDate DATE NOT NULL,
            MaturityDate DATE,
            ProductId INT NOT NULL,
            Product NVARCHAR(30) NOT NULL,
            InvoiceDay TINYINT NOT NULL DEFAULT 14,
            CurrentInstallmentAmount DECIMAL(15,5) NOT NULL,
            CurrentInvoiceFee DECIMAL(15,5) NOT NULL,
            RepaymentRate NVARCHAR(50),
            NextInvoiceDate DATE NOT NULL,
            CalculatedMaturityDate DATE,
            IsActive AS (CASE WHEN CancelledDate IS NULL THEN 1 ELSE 0 END) PERSISTED,
            CreatedDate DATETIME DEFAULT GETDATE(),
            UpdatedDate DATE DEFAULT CAST(GETDATE() AS DATE),
            UpdatedTime TIME DEFAULT CAST(GETDATE() AS TIME)
        );
        """
        cursor.execute(create_sql)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"        Table {table_name} created successfully")
        return True
    except Exception as e:
        print(f"        \nError creating table {table_name}: {e}")
        return False

def create_balance_table(table_name):
    """Create loan balance table"""
    try:
        conn = pyodbc.connect(CONFIG["pyodbc_conn"])
        cursor = conn.cursor()

        # Drop table if exists
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        create_sql = f"""
        CREATE TABLE {table_name} (
            LoanAccountBalanceIdentityId INT IDENTITY(1,1) PRIMARY KEY,
            LoanAccountBalanceId INT NOT NULL UNIQUE,
            SourceId INT NOT NULL,
            BalanceDateId INT NOT NULL,
            BalanceDate AS CONVERT(DATE, CAST(BalanceDateId AS CHAR(8)), 112) PERSISTED,
            LoanAccountId INT NOT NULL,
            ProductId INT NOT NULL,
            AccountCurrencyId INT NOT NULL,
            AccountStatusId INT NOT NULL,
            NumOfTransactions INT NOT NULL DEFAULT 0,
            NetTransactionAmount DECIMAL(15,5) DEFAULT 0,
            NetTransactionAmountSek DECIMAL(15,5) DEFAULT 0,
            AccruedInterest DECIMAL(15,5) DEFAULT 0,
            AccruedInterestSEK DECIMAL(15,5) DEFAULT 0,
            Balance DECIMAL(15,5) DEFAULT 0,
            BalanceSek DECIMAL(15,5) DEFAULT 0,
            LTV INT NOT NULL DEFAULT 0,
            PrecedingId INT,
            CreatedDate DATETIME DEFAULT GETDATE(),
            UpdatedDate DATE DEFAULT CAST(GETDATE() AS DATE),
            UpdatedTime TIME DEFAULT CAST(GETDATE() AS TIME)
        );
        """
        cursor.execute(create_sql)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"        Table {table_name} created successfully")
        return True
    except Exception as e:
        print(f"        \nError creating table {table_name}: {e}")
        return False

def create_transaction_table(table_name):
    """Create loan transaction table"""
    try:
        conn = pyodbc.connect(CONFIG["pyodbc_conn"])
        cursor = conn.cursor()

        # Drop table if exists
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        create_sql = f"""
        CREATE TABLE {table_name} (
            LoanAccountTransactionIdentityId INT IDENTITY(1,1) PRIMARY KEY,
            LoanAccountTransactionId INT NOT NULL UNIQUE,
            SourceId INT NOT NULL DEFAULT 8,
            TransactionDateId INT NOT NULL,
            TransactionDate AS CONVERT(DATE, CAST(TransactionDateId AS CHAR(8)), 112) PERSISTED,
            ValueDateId INT NOT NULL,
            ValueDate AS CONVERT(DATE, CAST(ValueDateId AS CHAR(8)), 112) PERSISTED,
            EntryDateID INT NOT NULL,
            EntryDate AS CONVERT(DATE, CAST(EntryDateID AS CHAR(8)), 112) PERSISTED,
            LoanAccountId INT NOT NULL,
            TransactionTypeId INT NOT NULL,
            TransactionStatus NVARCHAR(50),
            RectifyStatus NVARCHAR(50),
            TransactionCurrencyId INT NOT NULL,
            TransactionAmount DECIMAL(15,5) NOT NULL,
            TransactionAmountSEK DECIMAL(15,5) DEFAULT 0,
            CounterpartClearingNumber NVARCHAR(50),
            CounterPartBic NVARCHAR(50),
            CounterPartIban NVARCHAR(50),
            TransactionReference NVARCHAR(100),
            ExchangeRateId INT NOT NULL,
            TransactionText NVARCHAR(255),
            AccountServicerReference NVARCHAR(100),
            CounterPartId INT,
            CounterPartAccountNumber NVARCHAR(50),
            CounterPartBankName NVARCHAR(100),
            TransactionDateTime DATETIME2,
            IsDirectDebit INT,
            GLAccount NVARCHAR(50),
            EventName NVARCHAR(100),
            InvoiceId INT,
            CreatedDate DATETIME DEFAULT GETDATE(),
            UpdatedDate DATE DEFAULT CAST(GETDATE() AS DATE),
            UpdatedTime TIME DEFAULT CAST(GETDATE() AS TIME)
        );
        """
        cursor.execute(create_sql)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"        Table {table_name} created successfully")
        return True
    except Exception as e:
        print(f"        \nError creating table {table_name}: {e}")
        return False

# Data Processing Functions
def clean_account_data(df):
    """Clean and transform account data"""
    decimal_columns = ["CurrentInstallmentAmount", "CurrentInvoiceFee"]
    converted_df = convert_european_decimals(df, decimal_columns)

    cleaned_df = converted_df.withColumn("CancelledDateId",
                                       when(col("CancelledDateId") == -1, None)
                                       .otherwise(col("CancelledDateId")))

    # Deduplication
    window_spec = Window.partitionBy("LoanAccountId").orderBy(col("OpenDateId").desc())
    deduped_df = cleaned_df.withColumn("row_num", row_number().over(window_spec)) \
                          .filter("row_num = 1").drop("row_num")

    print(f"        Cleaned account data: {deduped_df.count()} records after deduplication")
    return deduped_df

def clean_balance_data(df):
    """Clean and transform balance data"""
    decimal_columns = [
        "NetTransactionAmount", "NetTransactionAmountSek",
        "AccruedInterest", "AccruedInterestSEK", "Balance", "BalanceSek"
    ]
    converted_df = convert_european_decimals(df, decimal_columns)

    cleaned_df = converted_df.withColumn("PrecedingId",
                                       when(col("PrecedingId") == -1, None)
                                       .otherwise(col("PrecedingId")))

    # Deduplication
    window_spec = Window.partitionBy("LoanAccountBalanceId").orderBy(col("BalanceDateId").desc())
    deduped_df = cleaned_df.withColumn("row_num", row_number().over(window_spec)) \
                          .filter("row_num = 1").drop("row_num")

    print(f"        Cleaned balance data: {deduped_df.count()} records after deduplication")
    return deduped_df

def clean_transaction_data(df):
    """Clean and transform transaction data"""
    decimal_columns = ["TransactionAmount", "TransactionAmountSEK"]
    converted_df = convert_european_decimals(df, decimal_columns)

    cleaned_df = converted_df \
        .withColumn("CounterPartId", when(col("CounterPartId") == -1, None)
                    .otherwise(col("CounterPartId"))) \
        .withColumn("InvoiceId", when(col("InvoiceId") == -1, None)
                    .otherwise(col("InvoiceId")))

    # Deduplication
    window_spec = Window.partitionBy("LoanAccountTransactionId") \
        .orderBy(col("TransactionDateId").desc())
    deduped_df = cleaned_df.withColumn("row_num", row_number().over(window_spec)) \
                          .filter("row_num = 1").drop("row_num")

    print(f"        Cleaned transaction data: {deduped_df.count()} records after deduplication")
    return deduped_df

def add_metadata_columns(df):
    """Add metadata columns"""
    ts = from_utc_timestamp(current_timestamp(), "Europe/Stockholm")
    return df \
        .withColumn("CreatedDate", ts) \
        .withColumn("UpdatedDate", date_format(ts, "yyyy-MM-dd").cast(DateType())) \
        .withColumn("UpdatedTime", date_format(ts, "HH:mm:ss"))

def detect_changes(new_df, existing_df, compare_cols):
    """Detect new/changed records"""
    try:
        if existing_df is None:
            return new_df, new_df.count()

        # Get counts first for debugging
        new_count = new_df.count()
        existing_count = existing_df.count()
        print(f"        New CSV records: {new_count} \
              \n        Existing records: {existing_count}")

        # If counts are different, we definitely have changes
        if new_count != existing_count:
            print(f"        Count difference detected: {new_count} vs {existing_count}")

            # Find new records by comparing primary keys
            new_keys = new_df.select(compare_cols[0]).distinct()
            existing_keys = existing_df.select(compare_cols[0]).distinct()

            # Get new keys that don't exist in existing data
            new_only_keys = new_keys.exceptAll(existing_keys)

            if new_only_keys.count() > 0:
                # Get full records for new keys
                changed_df = new_df.join(new_only_keys, on=compare_cols[0], how="inner")
                return changed_df, changed_df.count()

        # Select only comparison columns (excluding metadata)
        business_cols = [col for col in compare_cols if col not in [
            "%IdentityId", "CreatedDate", "UpdatedDate", "UpdatedTime"]]

        new_business = new_df.select(business_cols)
        old_business = existing_df.select(business_cols)

        # Find differences
        diff_df = new_business.exceptAll(old_business)
        diff_count = diff_df.count()
        print(f"        Business data differences found: {diff_count}")

        if diff_count == 0:
            return None, 0

        # Get full records for changed data
        changed_df = new_df.join(diff_df, on=business_cols, how="inner")
        return changed_df, changed_df.count()

    except Exception as e:
        print(f"        \nError detecting changes: {e}")
        # If error, process all new data to be safe
        return new_df, new_df.count()

def save_to_database(df, table_name):
    """Save dataframe to database"""
    try:
        # Remove identity columns
        identity_cols = [c for c in df.columns if "IdentityId" in c]
        db_df = df
        for col_name in identity_cols:
            if col_name in db_df.columns:
                db_df = db_df.drop(col_name)

        db_df.write \
            .format("jdbc") \
            .option("url", CONFIG["db_url"]) \
            .option("dbtable", table_name) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .mode("append") \
            .save()
        return True
    except Exception as e:
        print(f"        \nError saving to database table {table_name}: {e}")
        return False

def save_to_parquet(df, parquet_path, mode="overwrite"):
    """Save dataframe to parquet"""
    try:
        # Ensure directory exists
        Path(parquet_path).parent.mkdir(parents=True, exist_ok=True)

        df.write.mode(mode).parquet(parquet_path)
        print(f"        Successfully saved {df.count()} records to parquet {parquet_path}")
        return True
    except Exception as e:
        print(f"        Error saving to parquet {parquet_path}: {e}")
        return False

# Main ETL Functions
def run_table_etl(table_config, schema, clean_func, compare_cols, identity_col, partition_col ,sort_col):
    """Generic ETL function for any table"""
    print(f"\n{'='*60}")
    print(f"STARTING ETL FOR {table_config['table'].upper()}")
    print(f"{'='*60}")

    csv_path = table_config["csv"]
    parquet_path = table_config["parquet"]
    table_name = table_config["table"]

    # 1. Load CSV data
    print("\nStep 1: Loading CSV data...")
    raw_df = load_csv_data(csv_path, schema)
    if raw_df is None:
        print(f"        Failed to load CSV data from {csv_path}")
        return False

    # 2. Clean data
    print("\nStep 2: Cleaning data...")
    cleaned_df = clean_func(raw_df)
    cleaned_df = add_metadata_columns(cleaned_df)

    # 3. Check if table exists
    print("\nStep 3: Checking table existence...")
    table_exists = check_table_exists(table_name)

    # 4. Check if parquet exists
    print("\nStep 4: Checking parquet existence...")
    parquet_exists = check_parquet_exists(parquet_path)

    # 5. Load existing data
    print("\nStep 5: Loading existing data...")
    existing_df = None
    if table_exists:
        existing_df = load_database_data(table_name)
    elif parquet_exists:
        existing_df = load_parquet_data(parquet_path)

    # DEBUG: Show sample of existing data if available
    if existing_df:
        # print("Sample of existing data:")
        # existing_df.select(compare_cols[0], "CreatedDate").show(5)

        # Compare primary key ranges
        new_min_max = cleaned_df.agg(
            {compare_cols[0]: "min", compare_cols[0]: "max"}
            ).collect()[0]
        existing_min_max = existing_df.agg(
            {compare_cols[0]: "min", compare_cols[0]: "max"}
            ).collect()[0]
        print(f"        New data key range: {new_min_max}")
        print(f"        Existing data key range: {existing_min_max}")

    # 6. Detect changes or prepare for initial load
    print("\nStep 6: Detecting changes...")
    if not table_exists:
        print("        Database table missing - processing all data for initial load")
        change_count = cleaned_df.count()

        # Add identity column for initial load
        window_spec = Window.partitionBy(partition_col).orderBy(sort_col)
        final_df = cleaned_df.withColumn(identity_col, row_number().over(window_spec))

        # Reorder columns to put identity first
        columns = [identity_col] + [c for c in final_df.columns if c != identity_col]
        final_df = final_df.select(columns)

    elif existing_df is not None:
        changed_df, change_count = detect_changes(cleaned_df, existing_df, compare_cols)

        if change_count == 0:
            print("        No changes detected. ETL completed.")
            return True

        print(f"        Found {change_count} new/changed records")

        # Get max identity for incremental load
        max_id = existing_df.agg({identity_col: "max"}).collect()[0][0] or 0
        window_spec = Window.partitionBy(partition_col).orderBy(sort_col)
        final_df = changed_df.withColumn(identity_col, row_number().over(window_spec) + max_id)

    else:
        print("        No existing data found. Performing initial load...")
        change_count = cleaned_df.count()

        # Add identity column for initial load
        window_spec = Window.partitionBy(partition_col).orderBy(sort_col)
        final_df = cleaned_df.withColumn(identity_col, row_number().over(window_spec))

        # Reorder columns to put identity first
        columns = [identity_col] + [c for c in final_df.columns if c != identity_col]
        final_df = final_df.select(columns)

    # 7. Create table if needed 
    if not table_exists:
        print("\nStep 7: Creating database table...")
        if table_name == "Loan_Account_cleansed":
            create_success = create_account_table(table_name)
        elif table_name == "Loan_Balance_cleansed":
            create_success = create_balance_table(table_name)
        elif table_name == "Loan_Transaction_cleansed":
            create_success = create_transaction_table(table_name)
        else:
            create_success = False

        if not create_success:
            print("        \nFailed to create database table")
            return False
    else:
        print("\nStep 7: Database table already exists, skipping creation...")

    # 8. Save to database
    print("\nStep 8: Saving to database...")
    if change_count > 0:
        if not save_to_database(final_df, table_name):
            print("        Failed to save to database")
            return False
        print(f"        Successfully saved {change_count} records to database")
    else:
        print("        No data to save to database")

    # 9. Update parquet (full dataset)
    print("\nStep 9: Updating parquet backup...")
    if change_count > 0:
        if not table_exists and existing_df is not None:
            # Table didn't exist but parquet did - use the processed data as complete dataset
            complete_df = final_df
        elif existing_df is not None:
            # Normal incremental update
            new_keys = final_df.select(partition_col).distinct()
            updated_existing = existing_df.join(new_keys, on=compare_cols[0], how="left_anti")

            business_cols = [col for col in compare_cols if col not in [
                "LoanAccountIdentityId", "LoanAccountBalanceIdentityId"
                "LoanAccountTransactionIdentityId", "CreatedDate",
                "UpdatedDate", "UpdatedTime"]
                ]

            if updated_existing.count() > 0:
                updated_existing = updated_existing.select(
                    business_cols + ["CreatedDate", "UpdatedDate", "UpdatedTime"]
                    )
            final_business = final_df.select(
                business_cols + ["CreatedDate", "UpdatedDate", "UpdatedTime"]
                )

            # Combine updated existing + new records
            if updated_existing.count() > 0:
                complete_df = updated_existing.union(final_business)
            else:
                complete_df = final_business

            complete_df = complete_df.withColumn(
                identity_col, row_number().over(
                    Window.partitionBy(partition_col).orderBy(sort_col)
                    )
                )

            columns = (
                [identity_col] +[c for c in complete_df.columns if c != identity_col]
                       )
            complete_df = complete_df.select(columns)
        else:
            # No existing data
            complete_df = final_df

        if not save_to_parquet(complete_df, parquet_path, "overwrite"):
            print("        Warning: Failed to update parquet backup")
    else:
        print("        No changes to save to parquet")

    print(f"        ETL completed successfully for {table_name}")
    print(f"        Processed {change_count} records.")
    return True

# Comparison columns definitions
account_compare_cols = [
    "LoanAccountId", "SourceId", "AccountNumber", "IBAN", "BBAN",
    "AccountCurrencyId", "AccountCurrency", "OrganizationId", "OrganizationName",
    "ChannelID", "BrokerId", "OpenDateId", "OpenDate", "CancelledDateId",
    "CancelledDate", "ValueDate", "MaturityDate", "ProductId", "Product",
    "InvoiceDay", "CurrentInstallmentAmount", "CurrentInvoiceFee",
    "RepaymentRate", "NextInvoiceDate", "CalculatedMaturityDate"
]

balance_compare_cols = [
    "LoanAccountBalanceId", "SourceId", "BalanceDateId", "LoanAccountId",
    "ProductId", "AccountCurrencyId", "AccountStatusId", "NumOfTransactions",
    "NetTransactionAmount", "NetTransactionAmountSek", "AccruedInterest",
    "AccruedInterestSEK", "Balance", "BalanceSek", "LTV", "PrecedingId"
]

transaction_compare_cols = [
    "LoanAccountTransactionId", "SourceId", "TransactionDateId", "ValueDateId",
    "EntryDateID", "LoanAccountId", "TransactionTypeId", "TransactionStatus",
    "RectifyStatus", "TransactionCurrencyId", "TransactionAmount",
    "TransactionAmountSEK", "CounterpartClearingNumber", "CounterPartBic",
    "CounterPartIban", "TransactionReference", "ExchangeRateId", "TransactionText",
    "AccountServicerReference", "CounterPartId", "CounterPartAccountNumber",
    "CounterPartBankName", "TransactionDateTime", "IsDirectDebit", "GLAccount",
    "EventName", "InvoiceId"
]

def run_full_etl_process():
    """Run ETL for all three tables"""
    print("\n" + "="*60)
    print("STARTING COMPREHENSIVE ETL PROCESS")
    print("="*60)

    success_count = 0

    # Run Account ETL
    try:
        if run_table_etl(
            CONFIG["files"]["account"],
            account_schema,
            clean_account_data,
            account_compare_cols,
            "LoanAccountIdentityId",
            "LoanAccountId",
            "OpenDate"
        ):
            success_count += 1
    except Exception as e:
        print(f"        \nAccount ETL failed: {e}")

    # Run Balance ETL
    try:
        if run_table_etl(
            CONFIG["files"]["balance"],
            balance_schema,
            clean_balance_data,
            balance_compare_cols,
            "LoanAccountBalanceIdentityId",
            "LoanAccountBalanceId",
            "BalanceDateId"
        ):
            success_count += 1
    except Exception as e:
        print(f"        \nBalance ETL failed: {e}")

    # Run Transaction ETL
    try:
        if run_table_etl(
            CONFIG["files"]["transactions"],
            transaction_schema,
            clean_transaction_data,
            transaction_compare_cols,
            "LoanAccountTransactionIdentityId",
            "LoanAccountTransactionId",
            "TransactionDateId"
        ):
            success_count += 1
    except Exception as e:
        print(f"        \nTransaction ETL failed: {e}")

    print("\n" + "="*60)
    print(f"Tables Processed: {success_count}/3")
    print("="*60)

    return success_count == 3

# Execute ETL Process
if __name__ == "__main__":
    try:
        success = run_full_etl_process()
        print(f"\nETL PROCESS {'COMPLETED SUCCESSFULLY' \
                            if success else 'COMPLETED WITH ERRORS'}\n")
    except Exception as e:
        print(f"        \nCritical error in ETL process: {e}\n")
    # finally:
    #     # Clean up
    #     spark.stop()