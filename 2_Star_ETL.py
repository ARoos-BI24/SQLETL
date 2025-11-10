
import os
os.environ['JAVA_HOME'] = r'C:\Users\anton\.conda\envs\conda-env\Library\lib\jvm'
os.environ['HADOOP_HOME'] = r'C:\Hadoop\hadoop-3.3.6'
os.environ['PATH'] += r';C:\Hadoop\hadoop-3.3.6\bin'
os.environ['SPARK_HOME'] = r'C:\Users\anton\.conda\envs\conda-env\Lib\site-packages\pyspark'

# Star Schema Design for Loan Data Warehouse
from pyspark.sql import SparkSession
import pyodbc

sql_server = "PC-W11"
database_name = "ETL_Assignment2_Star" # Do not change this
user = "admin"
password = "sql"
jdbc_jar_path = r"C:\Spark\sqljdbc_12.10\enu\jars\mssql-jdbc-12.10.0.jre11.jar"

# Initialize Spark session
spark = SparkSession.builder.appName(database_name) \
    .config("spark.jars", jdbc_jar_path) \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.debug.maxToStringFields", "1000") \
    .getOrCreate()

# Configuration for Star Schema
STAR_CONFIG = {
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
                   f"PWD={password};"
}

# Create the star schema database
try:
    conn = pyodbc.connect(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server};UID={user};PWD={password}", autocommit=True)
    cursor = conn.cursor()

    create_db_query = """
    IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = 'ETL_Assignment2_Star')
    BEGIN
        CREATE DATABASE ETL_Assignment2_Star;
        PRINT 'Database ETL_Assignment2_Star created successfully.'
    END
    ELSE
    BEGIN
        PRINT 'Database ETL_Assignment2_Star already exists.'
    END
    """
    cursor.execute(create_db_query)
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error creating database: {e}")

print("\n" + "=" * 60)
print("ETL ASSIGNMENT 2: STAR SCHEMA DESIGN")
print("=" * 60)
print("\nSTEP 1: Star Schema configuration loaded successfully.")

# Error Handling Function 
def execute_sql_query(sql_query, table_name=""):
    """Execute a SQL query and handle errors with proper connection management"""
    try:
        with pyodbc.connect(STAR_CONFIG["pyodbc_conn"]) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                conn.commit()
        print(f"        {table_name} table created successfully.")
        return True
    except pyodbc.Error as e:
        sql_state = e.args[0]
        if sql_state == "42S01":
            print(f"        {table_name} table already exists, skipping creation.")
            return True
        elif sql_state == "23000":
            print("        Cannot insert duplicate key.")
            return False
        else:
            print(f"        Error creating {table_name}: {e}")
            return False
    except Exception as e:
        print(f"        Unexpected error creating {table_name}: {e}")
        return False

# Star Schema Table Creation Functions
def create_dim_date():
    """Create Date Dimension Table"""
    sql_query = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Dim_Date')
    BEGIN
        CREATE TABLE Dim_Date (
            date_key INT PRIMARY KEY,
            full_date DATE NOT NULL,
            year INT NOT NULL,
            quarter INT NOT NULL,
            month INT NOT NULL,
            month_name NVARCHAR(20) NOT NULL,
            day INT NOT NULL,
            day_of_week INT NOT NULL,
            day_name NVARCHAR(20) NOT NULL,
            week_of_year INT NOT NULL,
            is_weekend BIT NOT NULL,
            is_holiday BIT DEFAULT 0,
            created_date DATETIME DEFAULT GETDATE(),
            updated_date DATE DEFAULT GETDATE(),
            updated_time TIME DEFAULT GETDATE()
        );
    END
    """
    return execute_sql_query(sql_query, "Dim_Date")

def create_dim_account():
    """Create Account Dimension Table"""
    create_sql = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Dim_Account')
    BEGIN
        CREATE TABLE Dim_Account (
            account_key INT IDENTITY(1,1) PRIMARY KEY,
            account_id INT NOT NULL UNIQUE,
            account_number NVARCHAR(50) NOT NULL,
            currency_code NVARCHAR(3) NOT NULL,
            organization_id INT NOT NULL,
            channel_id INT,
            broker_id INT NOT NULL,
            product_name NVARCHAR(30) NOT NULL,
            open_date DATE NOT NULL,
            cancelled_date DATE,
            value_date DATE NOT NULL,
            invoice_day TINYINT NOT NULL,
            current_installment_amount DECIMAL(12,2) NOT NULL,
            current_invoice_fee DECIMAL(12,2) NOT NULL,
            next_invoice_date DATE NOT NULL,
            calculated_maturity_date DATE,
            is_active BIT DEFAULT 0,
            created_date DATETIME NOT NULL,
            updated_date DATE NOT NULL,
            updated_time TIME NOT NULL
        );
    END
    """
    return execute_sql_query(create_sql, "Dim_Account")

def create_dim_organization():
    """Create Organization Dimension Table"""
    create_sql = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Dim_Organization')
    BEGIN
        CREATE TABLE Dim_Organization (
            organization_key INT IDENTITY(1,1) PRIMARY KEY,
            organization_id INT NOT NULL,
            organization_name NVARCHAR(50) NOT NULL,
            source_id INT NOT NULL,
            created_date DATETIME NOT NULL,
            updated_date DATE NOT NULL,
            updated_time TIME NOT NULL
        );
    END
    """
    return execute_sql_query(create_sql, "Dim_Organization")

def create_dim_product():
    """Create Product Dimension Table"""
    create_sql = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Dim_Product')
    BEGIN
        CREATE TABLE Dim_Product (
            product_key INT IDENTITY(1,1) PRIMARY KEY,
            product_id INT NOT NULL,
            product_name NVARCHAR(30) NOT NULL,
            created_date DATETIME NOT NULL,
            updated_date DATE NOT NULL,
            updated_time TIME NOT NULL
        );
    END
    """
    return execute_sql_query(create_sql, "Dim_Product")

def create_dim_currency():
    """Create Currency Dimension Table"""
    create_sql = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Dim_Currency')
    BEGIN
        CREATE TABLE Dim_Currency (
            currency_key INT IDENTITY(1,1) PRIMARY KEY,
            currency_id INT NOT NULL,
            currency_code NVARCHAR(3) NOT NULL,
            created_date DATETIME NOT NULL,
            updated_date DATE NOT NULL,
            updated_time TIME NOT NULL
        );
    END
    """
    return execute_sql_query(create_sql, "Dim_Currency")

def create_dim_transaction_type():
    """Create Transaction Type Dimension Table"""
    create_sql = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Dim_Transaction_Type')
    BEGIN
        CREATE TABLE Dim_Transaction_Type (
            transaction_type_key INT IDENTITY(1,1) PRIMARY KEY,
            transaction_type_id INT NOT NULL UNIQUE,
            created_date DATETIME NOT NULL,
            updated_date DATE NOT NULL,
            updated_time TIME NOT NULL
        );
    END
    """
    return execute_sql_query(create_sql, "Dim_Transaction_Type")

print("        Dimension table creation functions loaded.")

# FIXED Fact Table Creation Functions
def create_fact_loan_balance():
    """Create Loan Balance Fact Table"""
    create_sql = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Fact_Balance')
    BEGIN
        CREATE TABLE Fact_Balance (
            balance_key INT IDENTITY(1,1) PRIMARY KEY,
            balance_id INT NOT NULL,
            preceding_id INT,
            account_id INT NOT NULL,
            account_status_id INT NOT NULL,
            product_id INT NOT NULL,
            currency_id INT NOT NULL,
            accrued_interest_eur DECIMAL(15,5) DEFAULT 0,
            accrued_interest_sek DECIMAL(15,5) DEFAULT 0,
            balance_eur DECIMAL(15,5) DEFAULT 0,
            balance_sek DECIMAL(15,5) DEFAULT 0,
            balance_date DATE NOT NULL,
            created_date DATETIME NOT NULL,
            updated_date DATE NOT NULL,
            updated_time TIME NOT NULL,
            FOREIGN KEY (account_id) REFERENCES Dim_Account(account_id),
            --FOREIGN KEY (product_id) REFERENCES Dim_Product(product_id),
            --FOREIGN KEY (currency_id) REFERENCES Dim_Currency(currency_id)
        );
    END
    """
    return execute_sql_query(create_sql, "Fact_Balance")

def create_fact_loan_transaction():
    """Create Loan Transaction Fact Table"""
    create_sql = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Fact_Transaction')
    BEGIN
        CREATE TABLE Fact_Transaction (
            transaction_key INT IDENTITY(1,1) PRIMARY KEY,
            transaction_id INT NOT NULL,
            transaction_date DATE NOT NULL,
            value_date DATE NOT NULL,
            entry_date DATE NOT NULL,
            account_id INT NOT NULL,
            transaction_type_id INT NOT NULL,
            transaction_amount DECIMAL(12,5) NOT NULL,
            transaction_amount_sek DECIMAL(12,5) NOT NULL,
            transaction_reference NVARCHAR(50),
            currency_id INT NOT NULL,
            exchange_rate_id INT,
            created_date DATETIME NOT NULL,
            updated_date DATE NOT NULL,
            updated_time TIME NOT NULL,
            FOREIGN KEY (account_id) REFERENCES Dim_Account(account_id),
            FOREIGN KEY (transaction_type_id) REFERENCES Dim_Transaction_Type(transaction_type_id)
            --,FOREIGN KEY (currency_id) REFERENCES Dim_Currency(currency_id)
        );
    END
    """
    return execute_sql_query(create_sql, "Fact_Transaction")

print("        Fact table creation functions loaded.")

# Create All Star Schema Tables
def create_star_schema():
    """Create complete star schema"""
    print("\n" + "=" * 60)
    print("CREATING STAR SCHEMA TABLES")
    print("=" * 60)
    
    success_count = 0
    total_tables = 8
    
    # Create Dimension Tables
    print("\nSTEP 2: Creating Dimension Tables...")
    if create_dim_date():
        success_count += 1
    if create_dim_account():
        success_count += 1
    if create_dim_organization():
        success_count += 1
    if create_dim_product():
        success_count += 1
    if create_dim_currency():
        success_count += 1
    if create_dim_transaction_type():
        success_count += 1

    # Create Fact Tables
    print("\nSTEP 3: Creating Fact Tables...")
    if create_fact_loan_balance():
        success_count += 1
    if create_fact_loan_transaction():
        success_count += 1
    
    print(f"\n        Star Schema Tables COMPLETED: "
          f"{success_count}/{total_tables} tables created successfully\n")
    
    return success_count == total_tables

# Function to populate star schema tables from source tables
def populate_star_tables(populate_sql, table_to, table_from):
    """Populate a star schema table from a source table"""
    try:
        with pyodbc.connect(STAR_CONFIG["pyodbc_conn"]) as conn:
            with conn.cursor() as cursor:
                cursor.execute(populate_sql)
                conn.commit()
        print(f"        Table {table_to} populated successfully.")
        return True
    except Exception as e:
        print(f"\n        Error populating table {table_to} from {table_from}:")
        print(f"        Error: {e}")
        return False

# Populate Date Dimension Tables
def populate_dim_date():
    """Populate date dimension with date range"""
    try:
        with pyodbc.connect(STAR_CONFIG["pyodbc_conn"]) as conn:
            with conn.cursor() as cursor:
                populate_sql = """
                WITH DateRange AS (
                    SELECT CAST('2017-01-01' AS DATE) AS DateValue
                    UNION ALL
                    SELECT DATEADD(day, 1, DateValue)
                    FROM DateRange
                    WHERE DateValue < '2025-12-31'
                )
                INSERT INTO Dim_Date (
                    date_key, full_date, year, quarter, month, month_name,
                    day, day_of_week, day_name, week_of_year, is_weekend
                )
                SELECT
                    YEAR(DateValue) * 10000 + MONTH(DateValue) * 100 + DAY(DateValue) AS date_key,
                    DateValue AS full_date,
                    YEAR(DateValue) AS year,
                    DATEPART(quarter, DateValue) AS quarter,
                    MONTH(DateValue) AS month,
                    DATENAME(month, DateValue) AS month_name,
                    DAY(DateValue) AS day,
                    DATEPART(weekday, DateValue) AS day_of_week,
                    DATENAME(weekday, DateValue) AS day_name,
                    DATEPART(week, DateValue) AS week_of_year,
                    CASE WHEN DATEPART(weekday, DateValue) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend
                FROM DateRange
                OPTION (MAXRECURSION 0);
                """
                cursor.execute(populate_sql)
                conn.commit()
        print("        Table [Dim_Date] populated successfully.")
        return True
    except Exception as e:
        print(f"\n        Error populating Dim_Date: {e}")
        return False

# Populate Dim_Account from Loan_Account_cleansed
table_to_acc = "[Dim_Account]"
table_from_acc = "[Loan_Account_cleansed]"
populate_sql_acc = f"""
INSERT INTO [ETL_Assignment2_Star].[dbo].{table_to_acc}(
    account_id,
    account_number,
    currency_code,
    organization_id,
    channel_id,
    broker_id,
    product_name,
    open_date,
    cancelled_date,
    value_date,
    invoice_day,
    current_installment_amount,
    current_invoice_fee,
    next_invoice_date,
    calculated_maturity_date,
    is_active,
    created_date,
    updated_date,
    updated_time
)
SELECT
    LoanAccountId,
    AccountNumber,
    AccountCurrency,
    OrganizationId,
    ChannelID,
    BrokerId,
    Product,
    OpenDate,
    CancelledDate,
    ValueDate,
    InvoiceDay,
    CurrentInstallmentAmount,
    CurrentInvoiceFee,
    NextInvoiceDate,
    CalculatedMaturityDate,
    IsActive,
    CreatedDate,
    UpdatedDate,
    UpdatedTime
FROM [ETL_Assignment2_Warehouse].[dbo].{table_from_acc};
"""

# Populate Dim_Organization from Loan_Account_cleansed
table_to_org = "[Dim_Organization]"
table_from_org = "[Loan_Account_cleansed]"
populate_sql_org = f"""
INSERT INTO [ETL_Assignment2_Star].[dbo].{table_to_org}(
    organization_id,
    organization_name,
    source_id,
    created_date,
    updated_date,
    updated_time
)
SELECT DISTINCT
    OrganizationId,
    OrganizationName,
    SourceId,
    CreatedDate,
    UpdatedDate,
    UpdatedTime
FROM [ETL_Assignment2_Warehouse].[dbo].{table_from_org};
"""

# Populate Dim_Product from Loan_Account_cleansed
table_to_prod = "[Dim_Product]"
table_from_prod = "[Loan_Account_cleansed]"
populate_sql_prod = f"""
INSERT INTO [ETL_Assignment2_Star].[dbo].{table_to_prod}(
    product_id,
    product_name,
    created_date,
    updated_date,
    updated_time
)
SELECT DISTINCT
    ProductId,
    Product,
    CreatedDate,
    UpdatedDate,
    UpdatedTime
FROM [ETL_Assignment2_Warehouse].[dbo].{table_from_prod};
"""

# Populate Dim_Currency from Loan_Account_cleansed
table_to_currency = "[Dim_Currency]"
table_from_currency = "[Loan_Account_cleansed]"
populate_sql_currency = f"""
INSERT INTO [ETL_Assignment2_Star].[dbo].{table_to_currency}(
    currency_id,
    currency_code,
    created_date,
    updated_date,
    updated_time
)
SELECT DISTINCT
    AccountCurrencyId,
    AccountCurrency,
    CreatedDate,
    UpdatedDate,
    UpdatedTime
FROM [ETL_Assignment2_Warehouse].[dbo].{table_from_currency};
"""

# Populate Dim_Transaction_Type from Loan_Transaction_cleansed
table_to_transaction_type = "[Dim_Transaction_Type]"
table_from_transaction_type = "[Loan_Transaction_cleansed]"
populate_sql_transaction_type = f"""
INSERT INTO [ETL_Assignment2_Star].[dbo].{table_to_transaction_type}(
    transaction_type_id,
    created_date,
    updated_date,
    updated_time
)
SELECT DISTINCT
    TransactionTypeId,
    CreatedDate,
    UpdatedDate,
    UpdatedTime
FROM [ETL_Assignment2_Warehouse].[dbo].{table_from_transaction_type};
"""

# Populate Fact_Balance from Loan_Balance_cleansed
table_to_fact_balance = "[Fact_Balance]"
table_from_fact_balance = "[Loan_Balance_cleansed]"
populate_sql_fact_balance = f"""
INSERT INTO [ETL_Assignment2_Star].[dbo].{table_to_fact_balance}(
    balance_id,
    preceding_id,
    account_id,
    account_status_id,
    product_id,
    currency_id,
    accrued_interest_eur,
    accrued_interest_sek,
    balance_eur,
    balance_sek,
    balance_date,
    created_date,
    updated_date,
    updated_time
)
SELECT
    LoanAccountBalanceId,
    PrecedingId,
    LoanAccountId,
    AccountStatusId,
    ProductId,
    AccountCurrencyId,
    AccruedInterest,
    AccruedInterestSEK,
    Balance,
    BalanceSek,
    BalanceDate,
    CreatedDate,
    UpdatedDate,
    UpdatedTime
FROM [ETL_Assignment2_Warehouse].[dbo].{table_from_fact_balance} lbc
"""

# Populate Fact_Transaction from Loan_Transaction_cleansed
table_to_fact_transaction = "[Fact_Transaction]"
table_from_fact_transaction = "[Loan_Transaction_cleansed]"
populate_sql_fact_transaction = f"""
INSERT INTO [ETL_Assignment2_Star].[dbo].{table_to_fact_transaction}(
    transaction_id,
    transaction_date,
    value_date,
    entry_date,
    account_id,
    transaction_type_id,
    transaction_amount,
    transaction_amount_sek,
    transaction_reference,
    currency_id,
    exchange_rate_id,
    created_date,
    updated_date,
    updated_time
)
 SELECT
    LoanAccountTransactionId,
    TransactionDate,
    ValueDate,
    EntryDate,
    LoanAccountId,
    TransactionTypeId,
    TransactionAmount,
    TransactionAmountSEK,
    TransactionReference,
    TransactionCurrencyId,
    ExchangeRateId,
    CreatedDate,
    UpdatedDate,
    UpdatedTime
FROM [ETL_Assignment2_Warehouse].[dbo].{table_from_fact_transaction}
"""

# Create All Star Schema Tables
def populate_star_schema():
    """Load data into star schema"""
    print("=" * 60)
    print("POPULATING STAR SCHEMA TABLES")
    print("=" * 60)

    success_count = 0
    total_tables = 8

    # Populate Dimension Tables
    print("\nSTEP 4: Populating Dimension Tables...")
    if populate_dim_date():
        success_count += 1
    if populate_star_tables(
        populate_sql=populate_sql_acc,
        table_to=table_to_acc,
        table_from=table_from_acc):
        success_count += 1
    if populate_star_tables(
        populate_sql=populate_sql_org,
        table_to=table_to_org,
        table_from=table_from_org):
        success_count += 1
    if populate_star_tables(
        populate_sql=populate_sql_prod,
        table_to=table_to_prod,
        table_from=table_from_prod):
        success_count += 1
    if populate_star_tables(
        populate_sql=populate_sql_currency,
        table_to=table_to_currency,
        table_from=table_from_currency):
        success_count += 1
    if populate_star_tables(
        populate_sql=populate_sql_transaction_type,
        table_to=table_to_transaction_type,
        table_from=table_from_transaction_type):
        success_count += 1

    # Populate Fact Tables
    print("\nSTEP 5: Populating Fact Tables...")
    if populate_star_tables(
        populate_sql=populate_sql_fact_balance,
        table_to=table_to_fact_balance,
        table_from=table_from_fact_balance):
        success_count += 1

    if populate_star_tables(
        populate_sql=populate_sql_fact_transaction,
        table_to=table_to_fact_transaction,
        table_from=table_from_fact_transaction):
        success_count += 1

    print(f"\n        Star Schema Data Load COMPLETED: "
          f"{success_count}/{total_tables} tables populated successfully")
    print("\n" + "=" * 60 + "\n")

    return success_count == total_tables

# Execute star schema creation
print("Executing star schema creation...")
create_star_schema()

# Execute star schema population
print("Executing star schema population...")
populate_star_schema()

print("Star schema ETL completed successfully!")

# Clean up Spark session
spark.stop()